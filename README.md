# kafka-to-smtp

A tiny Go worker that consumes email **envelopes** from a Kafka topic and submits
them to an SMTP server (e.g. [Maddy](https://maddy.email)) for delivery.

It exists to give apps a **fire-and-forget, scale-to-zero** way to send mail:

```
your apps ──produce──▶ Kafka topic `email-outbound`
                              │
                  KEDA Kafka scaler (scales on consumer-group lag, 0→N→0)
                              │
                       kafka-to-smtp  ◀── this worker; idle ⇒ zero replicas
                              │ SMTP submission
                              ▼
                          Maddy MTA (DKIM-sign, MX lookup, queue, retry)
                              │
                              ▼
                          the internet
```

## Why a queue?

- **Apps never block** on SMTP — they publish JSON and move on.
- **Durable + lossless.** Offsets are committed only after the SMTP server
  accepts the message. If the MTA is down, the worker keeps retrying and never
  commits, so mail waits safely in Kafka (and KEDA keeps a replica alive until
  the backlog drains) instead of being dropped.
- **Scale to zero** falls out naturally from KEDA's Kafka scaler watching the
  consumer group's lag.

## Message format

Publish UTF-8 JSON to the topic. `to` accepts a string or an array of strings.

```json
{
  "from": "noreply@sindrema.com",
  "to": ["alice@example.com"],
  "cc": [],
  "bcc": [],
  "replyTo": "support@sindrema.com",
  "subject": "Hello",
  "text": "plain text body",
  "html": "<p>html body</p>",
  "headers": { "X-Campaign": "welcome" }
}
```

- `from` is optional if `DEFAULT_FROM` is configured.
- At least one recipient (`to`/`cc`/`bcc`) and at least one body (`text`/`html`) are required.
- If both `text` and `html` are present the message is sent as `multipart/alternative`.
- `bcc` recipients receive the mail but are not written into the headers.
- Reserved structural headers (From, To, Subject, Content-Type, …) cannot be overridden via `headers`.

See [example-message.json](example-message.json).

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `kafka-kafka-bootstrap.events:9092` | Comma-separated bootstrap brokers |
| `KAFKA_TOPIC` | `email-outbound` | Topic to consume |
| `KAFKA_GROUP_ID` | `email-worker` | Consumer group (KEDA watches its lag) |
| `SMTP_HOST` | `maddy.communication.svc.cluster.local` | SMTP server host |
| `SMTP_PORT` | `25` | SMTP server port |
| `SMTP_TLS` | `none` | `none`, `starttls`, or `tls` |
| `SMTP_TLS_INSECURE` | `false` | Skip TLS cert verification (self-signed internal MTAs) |
| `SMTP_USERNAME` | _(empty)_ | Optional SMTP AUTH user (PLAIN; requires TLS) |
| `SMTP_PASSWORD` | _(empty)_ | Optional SMTP AUTH password |
| `DEFAULT_FROM` | _(empty)_ | Fallback `From` when a message omits it |
| `SMTP_HELO` | _(hostname)_ | HELO/EHLO name |
| `SMTP_DIAL_TIMEOUT` | `10s` | TCP dial timeout |
| `SMTP_RETRY_BASE` | `2s` | Base backoff between send retries |
| `SMTP_RETRY_MAX` | `60s` | Max backoff between send retries |
| `SMTP_MAX_ATTEMPTS` | `0` | Max send attempts per message; `0` = retry forever (lossless) |
| `HEALTH_PORT` | `8080` | Port for `/healthz` and `/readyz` (empty disables) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

**Retry semantics.** Unparseable/invalid messages and 5xx SMTP rejections are
treated as *poison* and skipped (logged at `error`). Transient failures
(connection refused, timeouts, 4xx) are retried with capped exponential backoff.

## Build & deploy

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) builds a minimal
distroless image, pushes it to Harbor as
`registry.k8s.sindrema.com/images/kafka-to-smtp:<timestamp>`, then bumps the
`image:` line in `SindreMA/sindre-k8s` at
`manifests/communication/email-worker-deployment.yaml`.

Required repository secrets: `HARBOR_USERNAME`, `HARBOR_PASSWORD`, `GH_PAT`.

> The `deploy` job only succeeds once `email-worker-deployment.yaml` exists in
> the manifests repo. Pair this with the Maddy MTA, the `email-outbound`
> KafkaTopic, and a KEDA `ScaledObject` (trigger `type: kafka`).

### The image

- Single pure-Go dependency (`segmentio/kafka-go`); SMTP/MIME built on the
  standard library.
- Static binary (`CGO_ENABLED=0`, stripped) on `gcr.io/distroless/static`,
  running as nonroot. No shell, no package manager.

Local build (requires Go 1.23+; run `go mod tidy` first to pin `go.sum`):

```sh
go mod tidy
go build -o kafka-to-smtp .
```

## ⚠️ Deliverability note

This worker only hands mail to your MTA — it does not solve deliverability.
Sending directly from a self-hosted/residential IP, big providers (Gmail and
especially Outlook/Microsoft) will distrust or reject mail unless the sending
IP has a matching PTR record and aligned SPF/DKIM/DMARC. Configure those on the
MTA + DNS side.
