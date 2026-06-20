# kafka-to-smtp

A tiny Go worker that consumes email **envelopes** from a Kafka topic and sends
them through one of several **SMTP providers**, rotating between them to stay
under each provider's free-tier daily limit.

```
your apps ──produce──▶ Kafka topic `email-outbound`
                              │
                  KEDA Kafka scaler (scales on lag, 0↔1)
                              │
                       kafka-to-smtp  ──┬─▶ SendGrid   (≤ N/day)
                              │          ├─▶ Brevo      (≤ N/day)
                              │          └─▶ Mailjet …  (≤ N/day)
                              │
                       appends one event per send ──▶ Kafka topic `email-sent`
                                                          (the daily-count ledger)
```

## How provider rotation works

- Providers are tried in **priority order** (lowest first).
- A provider is skipped once its **daily count** would exceed its `dailyLimit`
  (set each limit a bit below the real free cap as a buffer; `0` = unlimited).
- On send failure the worker **fails over** to the next provider.
- If every provider is **capped**, the message waits in Kafka until the counts
  reset at **UTC midnight**. If every provider is **erroring**, it backs off and
  retries. Either way the offset isn't committed, so nothing is lost.

### Daily counts come from Kafka, not a database

After each successful send the worker appends a `{provider, count, day}` event to
the **`email-sent`** ledger topic. On startup (every scale-from-zero) it
**replays the current UTC day** from that topic to rebuild per-provider counts in
memory. This is a **single-replica** design (`maxReplicas: 1`) — the worker is the
only writer, so counts stay accurate and survive restarts with no DB. It still
scales to **zero** when idle.

## Message format

Publish UTF-8 JSON to the configured topic. `to` accepts a string or array.

```json
{
  "from": "noreply@sindrema.com",
  "to": ["alice@example.com"],
  "cc": [], "bcc": [],
  "replyTo": "support@sindrema.com",
  "subject": "Hello",
  "text": "plain text body",
  "html": "<p>html body</p>",
  "headers": { "X-Campaign": "welcome" }
}
```

- `from` is optional if `DEFAULT_FROM` (or a provider's `from`) is set.
- Needs ≥1 recipient and ≥1 body (`text`/`html`); both bodies → `multipart/alternative`.
- `bcc` recipients get the mail but aren't written into headers.

See [example-message.json](example-message.json).

## Provider configuration

A JSON array, supplied via `PROVIDERS_FILE` (a mounted Secret, preferred) or the
`PROVIDERS` env var. See [providers.example.json](providers.example.json):

| field | meaning |
|---|---|
| `name` | label used in logs + the ledger |
| `host` / `port` | SMTP relay (`port` default 587) |
| `username` / `password` | SMTP credentials / API key |
| `tls` | `starttls` (default), `tls`, or `none` |
| `tlsInsecure` | skip cert verification (self-signed relays) |
| `dailyLimit` | daily cap; `0` = unlimited |
| `priority` | lower = preferred |
| `from` | optional per-provider From override |

> Each provider needs its own **domain authentication** (the DKIM CNAME/TXT
> records it gives you, added to your DNS) and the From address verified in that
> provider — otherwise mail is rejected or unsigned.

## Configuration (env)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `kafka-kafka-bootstrap.events:9092` | Bootstrap brokers (CSV) |
| `KAFKA_TOPIC` | `email-outbound` | Input topic |
| `KAFKA_GROUP_ID` | `email-worker` | Consumer group (KEDA watches its lag) |
| `KAFKA_SENT_TOPIC` | `email-sent` | Ledger topic (single partition) |
| `PROVIDERS_FILE` | _(unset)_ | Path to providers JSON (e.g. mounted Secret) |
| `PROVIDERS` | _(unset)_ | Providers JSON inline (alternative to file) |
| `COUNT_BY` | `recipient` | Count quota per `recipient` or per `message` |
| `DEFAULT_FROM` | _(empty)_ | Fallback From when a message omits it |
| `SMTP_HELO` | _(hostname)_ | HELO/EHLO name |
| `SMTP_DIAL_TIMEOUT` | `10s` | TCP dial timeout |
| `SMTP_RETRY_BASE` / `SMTP_RETRY_MAX` | `2s` / `60s` | Backoff when all providers error |
| `ALL_CAPPED_WAIT` | `5m` | Wait between checks when all providers are capped |
| `SMTP_MAX_ATTEMPTS` | `0` | Max retry rounds when all error; `0` = unlimited |
| `HEALTH_PORT` | `8080` | `/healthz` + `/readyz` (empty disables) |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

## Build & deploy

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) builds a minimal
distroless image, pushes it to Harbor as
`registry.k8s.sindrema.com/images/kafka-to-smtp:<timestamp>`, then bumps the
image in `SindreMA/sindre-k8s` at
`manifests/communication/email-worker-deployment.yaml`.

Image: static `CGO_ENABLED=0` binary on `gcr.io/distroless/static:nonroot`, one
pure-Go dependency (`segmentio/kafka-go`); SMTP/MIME on the standard library.

Local build (Go 1.23+):

```sh
go mod tidy
go build -o kafka-to-smtp .
```

## Delivery / deliverability notes

- **At-least-once:** the input offset is committed only after a provider accepts
  the message (the ledger event is written first).
- This worker doesn't fix deliverability — that's the provider's job. Set up each
  provider's domain authentication and keep `From` on an authenticated domain.
