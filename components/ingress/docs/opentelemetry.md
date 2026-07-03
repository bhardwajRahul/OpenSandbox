# OpenTelemetry Metrics — Ingress

Meter: `opensandbox/ingress` · Service name: `opensandbox-ingress-<version>`

## Metrics

| Metric | Type | Unit | Attributes | Description |
|---|---|---|---|---|
| `ingress.http.request.count` | Counter | — | `http.method`, `http.status_code`, `proxy_type` | Request count (QPS derivable) |
| `ingress.http.request.duration` | Histogram | `ms` | `http.method`, `http.status_code`, `proxy_type` | Full request duration |
| `ingress.routing.resolutions.count` | Counter | — | `routing.result` | Routing resolution count |
| `ingress.routing.resolution.duration` | Histogram | `ms` | `routing.result` | Routing resolution latency |
| `ingress.proxy.http.requests_total` | Counter | — | — | HTTP proxy requests |
| `ingress.proxy.websocket.connections_total` | Counter | — | — | WebSocket connections |
| `ingress.connections.active` | Observable Gauge | — | — | TCP ESTABLISHED connections (from `/proc/net/tcp`) |
| `ingress.system.cpu.usage` | Observable Gauge | `1` | — | CPU busy ratio `[0,1]` (Linux only) |
| `ingress.system.memory.usage_bytes` | Observable Gauge | `By` | — | Memory used bytes (Linux only) |

**Attribute values:** `proxy_type`: `http` | `websocket`. `routing.result`: `success` | `not_found` | `not_ready` | `error`.

## Configuration

```bash
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://otel-collector:4318"
```

Fallback: `OTEL_EXPORTER_OTLP_ENDPOINT`. Both unset = no export.
