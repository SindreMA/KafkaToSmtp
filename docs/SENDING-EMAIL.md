# Sending email — app integration guide

To send an email, your app **publishes a JSON message to the Kafka topic `email-outbound`**. That's the whole contract. You do *not* talk SMTP, hold connections, or wait for delivery — you produce one message and move on. The `kafka-to-smtp` worker consumes it, picks a provider that's under its daily free-tier cap, sends it, and tracks usage.

```
your app ──produce JSON──▶ Kafka topic "email-outbound" ──▶ kafka-to-smtp ──▶ provider (Brevo, …) ──▶ recipient
```

## Connection

| | |
|---|---|
| **Brokers** | `kafka-kafka-bootstrap.events:9092` |
| **Topic** | `email-outbound` |
| **Security** | none — plaintext, no auth (in-cluster only; not exposed outside) |
| **Message key** | optional (ignored; use `null` or anything) |
| **Message value** | a single UTF-8 JSON object (the "envelope", below) |

The brokers are only reachable from inside the cluster, so this works from any pod. No credentials needed.

## Message format (the envelope)

```jsonc
{
  "from": "noreply@sindrema.com",   // optional — defaults to noreply@sindrema.com
  "to": ["alice@example.com"],        // required: string OR array of strings
  "cc": [],                           // optional: string or array
  "bcc": [],                          // optional: string or array (not shown in headers)
  "replyTo": "support@sindrema.com",  // optional
  "subject": "Welcome",
  "text": "Plain-text body",          // text and/or html — at least one required
  "html": "<p>HTML body</p>",
  "headers": { "X-Campaign": "welcome" } // optional extra headers
}
```

Rules (a message that breaks these is **dropped and logged**, not retried):
- At least one recipient across `to` / `cc` / `bcc`.
- At least one body: `text`, `html`, or both (both → `multipart/alternative`).
- `from` is optional; if omitted it uses `noreply@sindrema.com`. **Any `from` must be on an authenticated domain (`sindrema.com`)** or it won't deliver.
- `to`/`cc`/`bcc` accept either a single string (`"a@b.com"`) or an array (`["a@b.com","c@d.com"]`).

Minimal valid message:
```json
{ "to": "alice@example.com", "subject": "Hi", "text": "Hello!" }
```

## Delivery semantics — important for a correct rewrite

- **Fire-and-forget.** A successful `produce` means *"queued in Kafka"*, **not** *"email delivered."* There is no synchronous send result. If you need delivery status, watch provider dashboards / DMARC reports, not the produce call.
- **At-least-once.** In rare worker-crash scenarios a message may be sent more than once. Don't rely on exactly-once; make emails tolerant of a rare duplicate.
- **Resilient & lossless.** If a provider errors, the worker fails over to the next; if every provider is at its daily cap, the message waits in Kafka until caps reset (UTC midnight). Valid mail is not dropped.
- **Ordering** is per-partition and not guaranteed globally — irrelevant for email, just don't assume order.
- **Counts toward quota by recipient** by default (a 3-recipient email = 3 against the provider's daily cap), matching how providers meter.

## Examples

> In every real Kafka client the message value is raw UTF-8 bytes — just serialize the envelope to JSON and send it. (The "UTF-8 BOM" caveat you may see elsewhere only applies to shell/CLI producers, not these libraries.) Connect the producer **once at startup**, not per email.

### TypeScript / Node (kafkajs)
```ts
import { Kafka } from "kafkajs";

const kafka = new Kafka({ clientId: "my-app", brokers: ["kafka-kafka-bootstrap.events:9092"] });
const producer = kafka.producer();
await producer.connect(); // once, at startup

export async function sendEmail(envelope: Record<string, unknown>) {
  await producer.send({
    topic: "email-outbound",
    messages: [{ value: JSON.stringify(envelope) }], // UTF-8, no BOM
  });
}

// usage
await sendEmail({ to: "alice@example.com", subject: "Welcome", html: "<p>Hi!</p>" });
```

### C# / .NET (Confluent.Kafka)
```csharp
using Confluent.Kafka;
using System.Text.Json;

var config = new ProducerConfig { BootstrapServers = "kafka-kafka-bootstrap.events:9092" };
using var producer = new ProducerBuilder<Null, string>(config).Build(); // once, at startup

var envelope = new {
    to = new[] { "alice@example.com" },
    subject = "Welcome",
    html = "<p>Hi!</p>",
};

await producer.ProduceAsync(
    "email-outbound",
    new Message<Null, string> { Value = JsonSerializer.Serialize(envelope) });
```

### Python (confluent-kafka)
```python
import json
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "kafka-kafka-bootstrap.events:9092"})  # once

def send_email(envelope: dict) -> None:
    producer.produce("email-outbound", json.dumps(envelope).encode("utf-8"))
    producer.poll(0)

send_email({"to": "alice@example.com", "subject": "Welcome", "html": "<p>Hi!</p>"})
producer.flush()  # before shutdown
```

### Go (segmentio/kafka-go)
```go
w := &kafka.Writer{
    Addr:     kafka.TCP("kafka-kafka-bootstrap.events:9092"),
    Topic:    "email-outbound",
    Balancer: &kafka.LeastBytes{},
}
defer w.Close()

b, _ := json.Marshal(envelope)
_ = w.WriteMessages(ctx, kafka.Message{Value: b})
```

## Migrating an app

Replace direct SMTP / mailpit config with a Kafka producer:

- **Before:** app holds SMTP host/port/credentials and calls an SMTP/`nodemailer`/`SmtpClient` send.
- **After:** app holds one env var `KAFKA_BROKERS=kafka-kafka-bootstrap.events:9092` and produces an envelope to `email-outbound`. No SMTP credentials in the app at all.

## Verifying

- Publish a test and watch the worker: `kubectl logs -n communication -l app=email-worker` → look for `email sent provider=…`.
- Browse the topic in **kafka-ui** (in the `events` namespace).
