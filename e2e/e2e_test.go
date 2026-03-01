package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	clusterName = "ext-proc-e2e"
	egVersion   = "v1.4.0"
	gatewayHost = "e2e.test.local"
)

type capturedExchange struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Request   struct {
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	} `json:"request"`
	Response struct {
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	} `json:"response"`
}

func TestE2E(t *testing.T) {
	if os.Getenv("E2E") != "true" {
		t.Skip("Skipping e2e tests. Set E2E=true to run.")
	}

	rootDir := mustGetRootDir(t)
	ctx := context.Background()

	t.Log("=== Phase 1: Create kind cluster ===")
	createKindCluster(ctx, t)
	t.Cleanup(func() {
		if os.Getenv("E2E_KEEP_CLUSTER") != "true" {
			t.Log("Deleting kind cluster...")
			run(t, "kind", "delete", "cluster", "--name", clusterName)
		}
	})

	t.Log("=== Phase 2: Install Envoy Gateway ===")
	installEnvoyGateway(ctx, t)

	t.Log("=== Phase 3: Build and load ext-proc-capture image ===")
	buildAndLoadImage(ctx, t, rootDir)

	t.Log("=== Phase 4: Deploy test manifests ===")
	deployManifests(ctx, t, rootDir)

	t.Log("=== Phase 5: Wait for all pods ready ===")
	waitForPods(ctx, t)

	t.Log("=== Phase 6: Port-forward and send traffic ===")
	localPort := portForwardGateway(ctx, t)

	t.Run("GET request is captured", func(t *testing.T) {
		clearCaptures(t)

		sendHTTPRequest(t, localPort, "GET", "/test-get", "")

		captures := waitForCaptures(t, 1, 30*time.Second)
		c := captures[0]

		if c.Request.Headers[":method"] != "GET" {
			t.Errorf("expected :method=GET, got %s", c.Request.Headers[":method"])
		}
		if c.Request.Headers[":path"] != "/test-get" {
			t.Errorf("expected :path=/test-get, got %s", c.Request.Headers[":path"])
		}
		if c.Response.Headers[":status"] != "200" {
			t.Errorf("expected :status=200, got %s", c.Response.Headers[":status"])
		}
		if c.ID == "" {
			t.Error("expected non-empty capture ID")
		}
		if c.Timestamp == "" {
			t.Error("expected non-empty timestamp")
		}
	})

	t.Run("POST request with body is captured", func(t *testing.T) {
		clearCaptures(t)

		body := `{"message":"hello from e2e"}`
		sendHTTPRequest(t, localPort, "POST", "/test-post", body)

		captures := waitForCaptures(t, 1, 30*time.Second)
		c := captures[0]

		if c.Request.Headers[":method"] != "POST" {
			t.Errorf("expected :method=POST, got %s", c.Request.Headers[":method"])
		}
		if len(c.Request.Body) == 0 {
			t.Error("expected non-empty request body in capture")
		}
	})

	t.Run("Multiple requests produce multiple captures", func(t *testing.T) {
		clearCaptures(t)

		for i := range 5 {
			sendHTTPRequest(t, localPort, "GET", fmt.Sprintf("/multi-%d", i), "")
			time.Sleep(200 * time.Millisecond)
		}

		captures := waitForCaptures(t, 5, 60*time.Second)
		if len(captures) < 5 {
			t.Errorf("expected at least 5 captures, got %d", len(captures))
		}
	})

	t.Run("Traffic still flows when ext-proc is unavailable (failOpen)", func(t *testing.T) {
		run(t, "kubectl", "scale", "deploy/ext-proc-capture",
			"-n", "ext-proc-capture", "--replicas=0")
		waitForScale(t, "ext-proc-capture", "ext-proc-capture", 0, 60*time.Second)

		statusCode := sendHTTPRequest(t, localPort, "GET", "/failopen-test", "")
		if statusCode != 200 {
			t.Errorf("expected 200 with ext-proc down (failOpen), got %d", statusCode)
		}

		run(t, "kubectl", "scale", "deploy/ext-proc-capture",
			"-n", "ext-proc-capture", "--replicas=1")
		waitForScale(t, "ext-proc-capture", "ext-proc-capture", 1, 120*time.Second)
		waitForPodsInNamespace(t, "ext-proc-capture", 120*time.Second)
	})
}

// --- Helpers ---

func mustGetRootDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join(".."))
	if err != nil {
		t.Fatalf("failed to get root dir: %v", err)
	}
	return dir
}

func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	t.Logf("  $ %s %s", name, strings.Join(args, " "))
	if err := cmd.Run(); err != nil {
		t.Fatalf("command failed: %s %s\nstdout: %s\nstderr: %s\nerr: %v",
			name, strings.Join(args, " "), stdout.String(), stderr.String(), err)
	}
	return stdout.String()
}

func runNoFail(t *testing.T, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String() + stderr.String(), err
	}
	return stdout.String(), nil
}

func createKindCluster(ctx context.Context, t *testing.T) {
	t.Helper()

	out, _ := runNoFail(t, "kind", "get", "clusters")
	if strings.Contains(out, clusterName) {
		t.Logf("Kind cluster %q already exists, reusing", clusterName)
		run(t, "kubectl", "cluster-info", "--context", "kind-"+clusterName)
		return
	}

	run(t, "kind", "create", "cluster", "--name", clusterName, "--wait", "120s")
	run(t, "kubectl", "cluster-info", "--context", "kind-"+clusterName)
}

func installEnvoyGateway(ctx context.Context, t *testing.T) {
	t.Helper()

	run(t, "helm", "upgrade", "--install", "eg",
		"oci://docker.io/envoyproxy/gateway-helm",
		"--version", egVersion,
		"-n", "envoy-gateway-system", "--create-namespace",
		"--wait", "--timeout", "5m",
	)

	waitForPodsInNamespace(t, "envoy-gateway-system", 180*time.Second)

	// Apply the quickstart Gateway + GatewayClass (always apply -- idempotent)
	run(t, "kubectl", "apply", "-f",
		fmt.Sprintf("https://github.com/envoyproxy/gateway/releases/download/%s/quickstart.yaml", egVersion),
		"-n", "default",
	)

	// Wait for the Gateway to be accepted (in kind, Programmed stays False
	// because there is no LoadBalancer to assign addresses).
	retryUntil(t, 180*time.Second, 5*time.Second, func() error {
		out, err := runNoFail(t, "kubectl", "get", "gateway", "eg", "-n", "default",
			"-o", "jsonpath={.status.conditions[?(@.type=='Accepted')].status}")
		if err != nil {
			return fmt.Errorf("gateway not found: %v", err)
		}
		if !strings.Contains(out, "True") {
			return fmt.Errorf("gateway not accepted yet: %s", out)
		}
		return nil
	})

	// Wait for the Envoy proxy pod to be running
	waitForPodsInNamespace(t, "envoy-gateway-system", 180*time.Second)
}

func buildAndLoadImage(ctx context.Context, t *testing.T, rootDir string) {
	t.Helper()
	run(t, "docker", "build", "-t", "ext-proc-capture:e2e",
		"-f", filepath.Join(rootDir, "e2e", "Dockerfile"), rootDir)
	run(t, "kind", "load", "docker-image", "ext-proc-capture:e2e", "--name", clusterName)
}

func deployManifests(ctx context.Context, t *testing.T, rootDir string) {
	t.Helper()
	manifestPath := filepath.Join(rootDir, "e2e", "manifests.yaml")
	run(t, "kubectl", "apply", "-f", manifestPath)
}

func waitForPods(ctx context.Context, t *testing.T) {
	t.Helper()
	waitForPodsInNamespace(t, "default", 180*time.Second)
	waitForPodsInNamespace(t, "ext-proc-capture", 180*time.Second)
	waitForPodsInNamespace(t, "envoy-gateway-system", 180*time.Second)
}

func waitForPodsInNamespace(t *testing.T, ns string, timeout time.Duration) {
	t.Helper()
	retryUntil(t, timeout, 5*time.Second, func() error {
		out, err := runNoFail(t, "kubectl", "get", "pods", "-n", ns,
			"-o", "jsonpath={.items[*].status.conditions[?(@.type=='Ready')].status}")
		if err != nil {
			return fmt.Errorf("kubectl get pods failed: %v", err)
		}
		if out == "" {
			return fmt.Errorf("no pods found in namespace %s", ns)
		}
		for _, s := range strings.Fields(out) {
			if s != "True" {
				return fmt.Errorf("not all pods ready in %s: %s", ns, out)
			}
		}
		return nil
	})
}

func waitForScale(t *testing.T, deploy, ns string, target int, timeout time.Duration) {
	t.Helper()
	retryUntil(t, timeout, 3*time.Second, func() error {
		out, err := runNoFail(t, "kubectl", "get", "deploy", deploy, "-n", ns,
			"-o", "jsonpath={.status.readyReplicas}")
		if err != nil {
			return err
		}
		out = strings.TrimSpace(out)
		if target == 0 && (out == "" || out == "0" || out == "<nil>") {
			return nil
		}
		if out == fmt.Sprintf("%d", target) {
			return nil
		}
		return fmt.Errorf("deploy %s/%s readyReplicas=%s, want %d", ns, deploy, out, target)
	})
}

func portForwardGateway(ctx context.Context, t *testing.T) int {
	t.Helper()

	envoyService := strings.TrimSpace(run(t, "kubectl", "get", "svc", "-n", "envoy-gateway-system",
		"--selector=gateway.envoyproxy.io/owning-gateway-namespace=default,gateway.envoyproxy.io/owning-gateway-name=eg",
		"-o", "jsonpath={.items[0].metadata.name}",
	))
	if envoyService == "" {
		t.Fatal("could not find envoy gateway service")
	}

	localPort := 18080
	cmd := exec.Command("kubectl", "-n", "envoy-gateway-system",
		"port-forward", fmt.Sprintf("svc/%s", envoyService),
		fmt.Sprintf("%d:80", localPort))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("port-forward failed to start: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
	})

	// Wait for port-forward to be ready (any response, even 404, means it's up)
	retryUntil(t, 30*time.Second, 1*time.Second, func() error {
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/healthz", localPort))
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	})

	return localPort
}

func sendHTTPRequest(t *testing.T, port int, method, path, body string) int {
	t.Helper()

	var reqBody *bytes.Reader
	if body != "" {
		reqBody = bytes.NewReader([]byte(body))
	} else {
		reqBody = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, fmt.Sprintf("http://localhost:%d%s", port, path), reqBody)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Host = gatewayHost
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Logf("WARN: HTTP %d for %s %s", resp.StatusCode, method, path)
	}
	return resp.StatusCode
}

func clearCaptures(t *testing.T) {
	t.Helper()
	pod := getExtProcPod(t)
	if pod == "" {
		return
	}
	runNoFail(t, "kubectl", "exec", "-n", "ext-proc-capture", pod,
		"--", "sh", "-c", "rm -f /captures/*.json")
}

func getExtProcPod(t *testing.T) string {
	t.Helper()
	out, err := runNoFail(t, "kubectl", "get", "pods", "-n", "ext-proc-capture",
		"-l", "app=ext-proc-capture",
		"-o", "jsonpath={.items[0].metadata.name}",
		"--field-selector=status.phase=Running")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func waitForCaptures(t *testing.T, minCount int, timeout time.Duration) []capturedExchange {
	t.Helper()
	var captures []capturedExchange

	retryUntil(t, timeout, 2*time.Second, func() error {
		pod := getExtProcPod(t)
		if pod == "" {
			return fmt.Errorf("ext-proc pod not running")
		}
		out, err := runNoFail(t, "kubectl", "exec", "-n", "ext-proc-capture", pod,
			"--", "sh", "-c", "ls /captures/*.json 2>/dev/null | wc -l")
		if err != nil {
			return fmt.Errorf("ls captures failed: %v", err)
		}
		count := strings.TrimSpace(out)
		if count == "0" || count == "" {
			return fmt.Errorf("no captures yet")
		}

		// Read all capture files
		filesOut, err := runNoFail(t, "kubectl", "exec", "-n", "ext-proc-capture", pod,
			"--", "sh", "-c", "cat /captures/*.json")
		if err != nil {
			return fmt.Errorf("cat captures failed: %v", err)
		}

		captures = parseCaptures(filesOut)
		if len(captures) < minCount {
			return fmt.Errorf("got %d captures, want at least %d", len(captures), minCount)
		}
		return nil
	})

	return captures
}

func parseCaptures(raw string) []capturedExchange {
	var captures []capturedExchange
	decoder := json.NewDecoder(strings.NewReader(raw))
	for decoder.More() {
		var c capturedExchange
		if err := decoder.Decode(&c); err != nil {
			continue
		}
		captures = append(captures, c)
	}
	return captures
}

func retryUntil(t *testing.T, timeout, interval time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = fn()
		if lastErr == nil {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("timed out after %v: %v", timeout, lastErr)
}
