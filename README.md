# ext-proc-capture

An Envoy external processing (ext_proc) gRPC service that captures HTTP/gRPC request/response traffic and writes each exchange to a JSON file. Designed to stay **off the critical path** and integrate with [Envoy Gateway](https://gateway.envoyproxy.io/).

## How It Works

The service implements the Envoy `ExternalProcessor` bidirectional gRPC stream. For each HTTP exchange flowing through the Envoy proxy, it:

1. Makes a per-stream sampling decision based on the configured `SAMPLE_RATE`
2. Receives request headers, body, response headers, and body
3. Writes a JSON file containing the captured exchange to disk (asynchronously)
4. Passes all traffic through **unmodified**

### Off the Critical Path

The Envoy Gateway `EnvoyExtensionPolicy` is configured with:

- **`failOpen: true`** -- if the capture service is down or slow, traffic flows through unaffected
- **`Streamed` body mode** -- bodies stream to the processor without Envoy buffering the full payload
- **`messageTimeout: 1s`** -- Envoy abandons the ext_proc call quickly if the service is slow

The server itself writes captures asynchronously in a goroutine so file I/O never blocks the gRPC stream.

### Percentage-Based Sampling

Set `SAMPLE_RATE` (0.0 to 1.0) to control what fraction of traffic gets captured. The decision is made once at the start of each stream so you never get partial exchanges.

## Output Format

Each capture file is named `<timestamp>_<short-id>.json`:

```json
{
  "id": "full-uuid",
  "timestamp": "2026-02-28T12:00:00.000Z",
  "request": {
    "headers": { ":method": "GET", ":path": "/foo" },
    "body": "<raw bytes if present>"
  },
  "response": {
    "headers": { ":status": "200", "content-type": "application/json" },
    "body": "<raw bytes if present>"
  }
}
```

## Configuration

| Environment Variable | Default     | Description                                      |
|---------------------|-------------|--------------------------------------------------|
| `EXT_PROC_PORT`     | `9002`      | gRPC listen port                                 |
| `CAPTURE_DIR`       | `/captures` | Directory to write capture JSON files             |
| `SAMPLE_RATE`       | `1.0`       | Fraction of traffic to capture (0.0 = none, 1.0 = all) |

## Build

```bash
go build -o ext-proc-capture .
```

## Docker

```bash
docker build -t ext-proc-capture:latest .
```

## Deploy with Envoy Gateway

### Prerequisites

- A Kubernetes cluster with [Envoy Gateway](https://gateway.envoyproxy.io/) installed
- One or more `HTTPRoute` or `GRPCRoute` resources you want to capture traffic from

### Steps

1. **Build and push the image** to a registry accessible by your cluster:

```bash
docker build -t <your-registry>/ext-proc-capture:latest .
docker push <your-registry>/ext-proc-capture:latest
```

2. **Update the image** in `deploy/ext-proc-capture.yaml` to reference your registry. Adjust `SAMPLE_RATE` as needed (default in the manifest is `0.1` = 10%).

3. **Deploy the capture service:**

```bash
kubectl apply -f deploy/ext-proc-capture.yaml
```

4. **Attach the extension policy** to your routes:

```bash
kubectl apply -f deploy/extension-policy.yaml
```

### Per-Route Targeting

`deploy/extension-policy.yaml` contains two example `EnvoyExtensionPolicy` resources -- one targeting an `HTTPRoute` and one targeting a `GRPCRoute`. Edit the `targetRefs` to match your route names:

```yaml
# For an HTTPRoute
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: my-http-route

# For a GRPCRoute
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: GRPCRoute
      name: my-grpc-route
```

Create one `EnvoyExtensionPolicy` per route you want to instrument. The `ReferenceGrant` in the manifest allows the cross-namespace reference from the policy's namespace to the `ext-proc-capture` service namespace.

If the policy and the ext-proc-capture Service are in the same namespace, the `ReferenceGrant` is not needed.

### Reading Captures

Exec into the pod and view the capture files:

```bash
kubectl exec -n ext-proc-capture deploy/ext-proc-capture -- ls /captures
kubectl exec -n ext-proc-capture deploy/ext-proc-capture -- cat /captures/<filename>.json
```

Or copy them locally:

```bash
kubectl cp ext-proc-capture/$(kubectl get pod -n ext-proc-capture -l app=ext-proc-capture -o jsonpath='{.items[0].metadata.name}'):/captures ./captures
```

## Local Development

Run the service locally:

```bash
CAPTURE_DIR=./captures SAMPLE_RATE=1.0 go run .
```

Then configure Envoy (or an Envoy Gateway dev setup) to point its ext_proc filter at `localhost:9002`.
