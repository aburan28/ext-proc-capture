package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type CapturedExchange struct {
	ID        string           `json:"id"`
	Timestamp string           `json:"timestamp"`
	Request   CapturedRequest  `json:"request"`
	Response  CapturedResponse `json:"response"`
}

type CapturedRequest struct {
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
}

type CapturedResponse struct {
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
}

type captureServer struct {
	extprocv3.UnimplementedExternalProcessorServer
	outputDir  string
	sampleRate float64
}

func newCaptureServer(outputDir string, sampleRate float64) *captureServer {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("Failed to create output directory %s: %v", outputDir, err)
	}
	return &captureServer{
		outputDir:  outputDir,
		sampleRate: sampleRate,
	}
}

// Process handles the bidirectional ext_proc stream. It is designed to work
// with both normal and observability_mode. In observability_mode Envoy ignores
// responses, but we still send them for protocol correctness. The sampling
// decision is made once per stream so partial exchanges are never written.
func (s *captureServer) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	sampled := s.sampleRate >= 1.0 || rand.Float64() < s.sampleRate

	var exchange *CapturedExchange
	if sampled {
		exchange = &CapturedExchange{
			ID:        uuid.New().String(),
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.Canceled {
				break
			}
			// In observability_mode Envoy may close the stream at any time.
			break
		}

		if sampled {
			s.recordPhase(exchange, req)
		}

		resp := buildResponse(req)
		if err := stream.Send(resp); err != nil {
			// Send failures are expected when the proxy closes the stream
			// (observability_mode + deferred_close_timeout).
			break
		}
	}

	if sampled && exchange != nil && (len(exchange.Request.Headers) > 0 || len(exchange.Response.Headers) > 0) {
		go s.writeCapture(exchange)
	}
	return nil
}

func (s *captureServer) recordPhase(ex *CapturedExchange, req *extprocv3.ProcessingRequest) {
	switch v := req.Request.(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		ex.Request.Headers = headerMapToMap(v.RequestHeaders.GetHeaders())
	case *extprocv3.ProcessingRequest_RequestBody:
		ex.Request.Body = append(ex.Request.Body, v.RequestBody.GetBody()...)
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		ex.Response.Headers = headerMapToMap(v.ResponseHeaders.GetHeaders())
	case *extprocv3.ProcessingRequest_ResponseBody:
		ex.Response.Body = append(ex.Response.Body, v.ResponseBody.GetBody()...)
	}
}

func buildResponse(req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	switch req.Request.(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return &extprocv3.ProcessingResponse{
			Response: &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{},
			},
		}
	case *extprocv3.ProcessingRequest_RequestBody:
		return &extprocv3.ProcessingResponse{
			Response: &extprocv3.ProcessingResponse_RequestBody{
				RequestBody: &extprocv3.BodyResponse{},
			},
		}
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return &extprocv3.ProcessingResponse{
			Response: &extprocv3.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &extprocv3.HeadersResponse{},
			},
		}
	case *extprocv3.ProcessingRequest_ResponseBody:
		return &extprocv3.ProcessingResponse{
			Response: &extprocv3.ProcessingResponse_ResponseBody{
				ResponseBody: &extprocv3.BodyResponse{},
			},
		}
	default:
		return &extprocv3.ProcessingResponse{}
	}
}

func headerMapToMap(hm *corev3.HeaderMap) map[string]string {
	if hm == nil {
		return nil
	}
	out := make(map[string]string, len(hm.GetHeaders()))
	for _, h := range hm.GetHeaders() {
		val := h.GetValue()
		if val == "" && len(h.GetRawValue()) > 0 {
			val = string(h.GetRawValue())
		}
		out[h.GetKey()] = val
	}
	return out
}

func (s *captureServer) writeCapture(exchange *CapturedExchange) {
	data, err := json.MarshalIndent(exchange, "", "  ")
	if err != nil {
		log.Printf("ERROR: failed to marshal capture %s: %v", exchange.ID, err)
		return
	}

	filename := fmt.Sprintf("%s_%s.json",
		time.Now().UTC().Format("20060102T150405Z"),
		exchange.ID[:8],
	)
	path := filepath.Join(s.outputDir, filename)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("ERROR: failed to write capture to %s: %v", path, err)
		return
	}
	log.Printf("Captured exchange %s -> %s", exchange.ID, path)
}

func main() {
	port := getEnv("EXT_PROC_PORT", "9002")
	outputDir := getEnv("CAPTURE_DIR", "/captures")
	sampleRate := parseSampleRate(getEnv("SAMPLE_RATE", "1.0"))

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	srv := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(srv, newCaptureServer(outputDir, sampleRate))

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	log.Printf("ext-proc-capture listening on :%s, writing to %s (sample rate: %.1f%%)",
		port, outputDir, sampleRate*100)

	if err := srv.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}

func parseSampleRate(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		log.Printf("WARN: invalid SAMPLE_RATE %q, defaulting to 1.0", s)
		return 1.0
	}
	if v > 1.0 {
		return 1.0
	}
	return v
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
