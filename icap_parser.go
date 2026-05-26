// === FILE: icap_parser.go ===
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// ICAPRequest represents a parsed ICAP request
type ICAPRequest struct {
	Method     string
	URI        string
	Version    string
	Headers    map[string]string
	Preview    int64
	Encapsulated map[string]int // section -> offset in raw body
	RawBody    []byte
	HTTPReq    *http.Request // Encapsulated HTTP request (for REQMOD)
	HTTPRes    *http.Response // Encapsulated HTTP response (for RESPMOD)
}

// ICAPResponse represents an ICAP response
type ICAPResponse struct {
	Version    string
	StatusCode int
	StatusText string
	Headers    map[string]string
	Preview    int64
	Encapsulated map[string]int
	Body       []byte
	HTTPReq    *http.Request
	HTTPRes    *http.Response
}

// ParseICAPRequest parses an ICAP request from a reader
func ParseICAPRequest(r *bufio.Reader) (*ICAPRequest, error) {
	req := &ICAPRequest{
		Headers: make(map[string]string),
		Encapsulated: make(map[string]int),
	}

	// Read request line
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read request line: %w", err)
	}
	line = strings.TrimSpace(line)
	parts := strings.Fields(line)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid ICAP request line: %s", line)
	}

	req.Method = parts[0]
	req.URI = parts[1]
	req.Version = parts[2]

	// Read headers
	for {
		headerLine, err := r.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
		headerLine = strings.TrimSpace(headerLine)
		if headerLine == "" {
			break
		}

		idx := strings.Index(headerLine, ":")
		if idx == -1 {
			continue
		}
		key := strings.TrimSpace(headerLine[:idx])
		value := strings.TrimSpace(headerLine[idx+1:])
		req.Headers[key] = value

		// Parse Preview header
		if strings.EqualFold(key, "Preview") {
			req.Preview, _ = strconv.ParseInt(value, 10, 64)
		}

		// Parse Encapsulated header
		if strings.EqualFold(key, "Encapsulated") {
			parseEncapsulated(value, req.Encapsulated)
		}
	}

	// Read body if present
	if len(req.Encapsulated) > 0 {
		body, err := readChunkedBody(r, req.Preview)
		if err != nil {
			return nil, fmt.Errorf("failed to read body: %w", err)
		}
		req.RawBody = body

		// Parse encapsulated HTTP request/response
		if offset, ok := req.Encapsulated["req-hdr"]; ok {
			httpReq, err := parseEncapsulatedHTTPRequest(body[offset:])
			if err == nil {
				req.HTTPReq = httpReq
			}
		}
		if offset, ok := req.Encapsulated["res-hdr"]; ok {
			httpRes, err := parseEncapsulatedHTTPResponse(body[offset:])
			if err == nil {
				req.HTTPRes = httpRes
			}
		}
	}

	return req, nil
}

// parseEncapsulated parses the Encapsulated header value
func parseEncapsulated(value string, result map[string]int) {
	parts := strings.Split(value, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if eqIdx := strings.Index(part, "="); eqIdx != -1 {
			key := strings.TrimSpace(part[:eqIdx])
			valStr := strings.TrimSpace(part[eqIdx+1:])
			if val, err := strconv.Atoi(valStr); err == nil {
				result[key] = val
			}
		}
	}
}

// readChunkedBody reads a chunked ICAP body
func readChunkedBody(r *bufio.Reader, preview int64) ([]byte, error) {
	var body bytes.Buffer
	totalRead := int64(0)
	maxSize := int64(10 * 1024 * 1024) // 10MB limit

	for {
		// Read chunk size line
		sizeLine, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF && body.Len() > 0 {
				break
			}
			return nil, err
		}
		sizeLine = strings.TrimSpace(sizeLine)
		
		// Parse chunk size (may have extensions after ;)
		sizeStr := sizeLine
		if semiIdx := strings.Index(sizeStr, ";"); semiIdx != -1 {
			sizeStr = sizeStr[:semiIdx]
		}
		chunkSize, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid chunk size: %s", sizeLine)
		}

		if chunkSize == 0 {
			// End of chunked data, read trailing CRLF and any trailers
			for {
				trailer, err := r.ReadString('\n')
				if err != nil || strings.TrimSpace(trailer) == "" {
					break
				}
			}
			break
		}

		if totalRead+chunkSize > maxSize {
			return nil, fmt.Errorf("body exceeds maximum size")
		}

		// Read chunk data
		chunk := make([]byte, chunkSize)
		_, err = io.ReadFull(r, chunk)
		if err != nil {
			return nil, err
		}
		body.Write(chunk)
		totalRead += chunkSize

		// Read CRLF
		crlf := make([]byte, 2)
		_, err = io.ReadFull(r, crlf)
		if err != nil {
			return nil, err
		}
	}

	return body.Bytes(), nil
}

// parseEncapsulatedHTTPRequest parses an encapsulated HTTP request
func parseEncapsulatedHTTPRequest(data []byte) (*http.Request, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	req, err := http.ReadRequest(reader)
	if err != nil {
		return nil, err
	}
	
	// Read body if Content-Length or Transfer-Encoding is present
	if req.ContentLength > 0 {
		body := make([]byte, req.ContentLength)
		_, err := io.ReadFull(reader, body)
		if err == nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
	}
	
	return req, nil
}

// parseEncapsulatedHTTPResponse parses an encapsulated HTTP response
func parseEncapsulatedHTTPResponse(data []byte) (*http.Response, error) {
	reader := bufio.NewReader(bytes.NewReader(data))
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		return nil, err
	}
	
	// Read body if Content-Length or Transfer-Encoding is present
	if resp.ContentLength > 0 {
		body := make([]byte, resp.ContentLength)
		_, err := io.ReadFull(reader, body)
		if err == nil {
			resp.Body = io.NopCloser(bytes.NewReader(body))
		}
	}
	
	return resp, nil
}

// BuildICAPResponse builds an ICAP response string
func BuildICAPResponse(resp *ICAPResponse, w io.Writer) error {
	// Write status line
	statusLine := fmt.Sprintf("%s %d %s\r\n", resp.Version, resp.StatusCode, resp.StatusText)
	if _, err := w.Write([]byte(statusLine)); err != nil {
		return err
	}

	// Write headers
	for key, value := range resp.Headers {
		headerLine := fmt.Sprintf("%s: %s\r\n", key, value)
		if _, err := w.Write([]byte(headerLine)); err != nil {
			return err
		}
	}

	// End headers
	if _, err := w.Write([]byte("\r\n")); err != nil {
		return err
	}

	// Write body if present
	if len(resp.Body) > 0 {
		if err := writeChunkedBody(w, resp.Body); err != nil {
			return err
		}
	} else {
		// Write zero-length chunk to end
		if _, err := w.Write([]byte("0\r\n\r\n")); err != nil {
			return err
		}
	}

	return nil
}

// writeChunkedBody writes data in chunked encoding
func writeChunkedBody(w io.Writer, data []byte) error {
	if len(data) == 0 {
		_, err := w.Write([]byte("0\r\n\r\n"))
		return err
	}

	// Write chunk with size
	sizeLine := fmt.Sprintf("%x\r\n", len(data))
	if _, err := w.Write([]byte(sizeLine)); err != nil {
		return err
	}

	// Write chunk data
	if _, err := w.Write(data); err != nil {
		return err
	}

	// Write final CRLF for chunk
	if _, err := w.Write([]byte("\r\n")); err != nil {
		return err
	}

	// Write terminating chunk
	if _, err := w.Write([]byte("0\r\n\r\n")); err != nil {
		return err
	}

	return nil
}

// BuildICAPRequest builds an ICAP request string to send to backend
func BuildICAPRequest(req *ICAPRequest, rawBody []byte) []byte {
	var buf bytes.Buffer

	// Request line
	buf.WriteString(fmt.Sprintf("%s %s %s\r\n", req.Method, req.URI, req.Version))

	// Headers
	for key, value := range req.Headers {
		buf.WriteString(fmt.Sprintf("%s: %s\r\n", key, value))
	}

	// End headers
	buf.WriteString("\r\n")

	// Body if present
	if len(rawBody) > 0 {
		writeChunkedBody(&buf, rawBody)
	} else {
		buf.WriteString("0\r\n\r\n")
	}

	return buf.Bytes()
}

// url.URL helper for parsing ICAP service URL
func parseICAPURL(rawURL string) (*url.URL, error) {
	// ICAP URLs are typically like: icap://server:port/service
	if !strings.HasPrefix(rawURL, "icap://") && !strings.HasPrefix(rawURL, "http://") {
		rawURL = "icap://" + rawURL
	}
	return url.Parse(rawURL)
}
