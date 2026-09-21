# Message Relay

A small Java 21 client/server message relay implemented with gRPC. Clients open one bidirectional `Connect` stream, register a client ID, send uniquely identified messages to another client, receive deliveries, and explicitly acknowledge them.

The relay owns registration, mailbox persistence, delivery, retry and acknowledgement logic directly; no broker or queue product is used.

## Requirements

- JDK 21
- The included Gradle wrapper (`./gradlew`)

## Build and test

```bash
./gradlew clean test
./gradlew build
```

The runnable fat JAR is:

```text
build/libs/message-relay-1.0.0.jar
```

Run the server with:

```bash
java -jar build/libs/message-relay-1.0.0.jar
```

The default gRPC port is `50051`. The default H2 database is file-backed at `./data/relay.mv.db`, so queued and unacknowledged messages survive a normal server restart.

## Configuration

The following values can be overridden with JVM system properties:

| Property | Default | Purpose |
| --- | ---: | --- |
| `relay.port` | `50051` | gRPC listen port |
| `relay.dataDir` | `./data` | directory used for the H2 file database |
| `relay.jdbcUrl` | derived from `dataDir` | complete JDBC URL; overrides `dataDir` |
| `relay.maxActiveSessions` | `1000` | maximum registered connections |
| `relay.maxMessagesPerMailbox` | `1000` | maximum unexpired messages for one recipient |
| `relay.maxPendingMessages` | `100000` | server-wide unexpired message limit |
| `relay.executorThreads` | `2 x CPU`, clamped to `4..16` | delivery worker count |

Example:

```bash
java -Drelay.port=6000 -Drelay.dataDir=/tmp/message-relay \
  -jar build/libs/message-relay-1.0.0.jar
```

Other limits are defined centrally in `RelayConfig`, including the 64 KiB message-content limit, 128-byte client-ID limit, 32-event slow-consumer allowance, 30-second ACK timeout, 5-second retry scan and 24-hour message TTL.

## Protocol

The protocol is defined in `src/main/proto/relay.proto`.

```text
MessageRelay.Connect(stream ClientEvent) returns (stream ServerEvent)
```

Client events:

- `Register(client_id)`
- `Send(message_id, recipient_id, content)`
- `Ack(message_id)`

Server events:

- `Registered`
- `SendAccepted`
- `SendRejected`
- `Delivery`
- `ProtocolError`

`message_id` is a client-generated UUID. A client should register before sending or acknowledging. Application-level validation errors are returned on the stream and are non-fatal; transport-level failures may terminate the stream.

## Delivery semantics

- A send is accepted only after the message is persisted.
- Messages for an offline recipient remain in its mailbox.
- Only one message per recipient is considered in-flight at a time.
- The next FIFO message is delivered after the current message is acknowledged, expires, or is requeued for retry.
- An unacknowledged delivery may be delivered again, so delivery is **at least once**.
- A successful ACK deletes the stored row.
- Re-registering the same client ID after disconnect reuses the same logical mailbox because mailbox state is keyed by recipient ID in H2, not by the connection object.

## Persistence and schema

`MailboxStore` uses one H2 `MAILBOX` table. `SEQ_NO` is an H2 auto-increment identity and is persisted with the database, so sequence allocation continues after a server restart. FIFO for a recipient is obtained with `ORDER BY SEQ_NO`.

Schema initialization runs at startup using `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`; it does not clear existing rows.

## Project structure

```text
src/main/java/com/example/relay/
  ClientConnection.java       per-client stream state and serialized writes
  ConnectionRegistry.java     active client-ID to connection mapping
  DeliveryService.java        send, delivery, ACK, retry and recovery orchestration
  RelayConfig.java            limits, timers and runtime configuration
  RelayServer.java            process entry point and lifecycle
  RelayServiceImpl.java       gRPC protocol handling

src/main/java/com/example/relay/store/
  MailboxStore.java           H2 mailbox persistence and state transitions
  StoredMessage.java          stored-message record
  MessageStatus.java          QUEUED / IN_FLIGHT state
  DuplicateIdException.java   duplicate UUID handling
  MailboxFullException.java   capacity rejection
  StorageException.java       JDBC error abstraction

src/main/proto/relay.proto     wire protocol
src/test/java/...             unit and integration tests
APPROACH.md                   design decisions, concurrency, trade-offs and test boundaries
```

## Testing

The suite combines focused unit tests with in-process gRPC integration tests backed by isolated in-memory H2 databases. It covers registration, validation, offline retention, reconnect replay, FIFO delivery, ACK deletion, retry after missing ACKs, persistence behavior, resource limits, failure paths and selected concurrency races.

All waits in the integration harness are bounded. The client-side implementation itself is intentionally outside this repository; in particular, client retry/backoff policy for responses marked `retryable=true` is outside the test boundary.

JaCoCo reports are generated by the Gradle build under `build/reports/jacoco/test/`.

## Connect helper script

Make the script executable once:

```bash
chmod +x scripts/connect.sh
```

### Terminal 1 - Start the server

```bash
./gradlew build
java -jar build/libs/message-relay-1.0.0.jar
```

The server listens on `localhost:50051` by default.

### Terminal 2 - Connect and register Alice

```bash
scripts/connect.sh
```

Then enter:

```json
{"register":{"clientId":"alice"}}
```

Keep this terminal open.

### Terminal 3 - Connect and register Bob

```bash
scripts/connect.sh
```

Then enter:

```json
{"register":{"clientId":"bob"}}
```

Keep this terminal open.

### Send a message from Alice to Bob

Generate a UUID if needed:

```bash
uuidgen
```

Then, in Alice's terminal, send:

```json
{"send":{"messageId":"550e8400-e29b-41d4-a716-446655440000","recipientId":"bob","content":"hello bob"}}
```

Alice should receive a `sendAccepted` response, and Bob should receive a `delivery` event for the same `messageId`.

### Acknowledge the message from Bob

In Bob's terminal, send:

```json
{"ack":{"messageId":"550e8400-e29b-41d4-a716-446655440000"}}
```

After the acknowledgement succeeds, the message is removed from Bob's unacknowledged mailbox.

### Optional - Verify offline delivery

1. Stop Bob's `grpcurl` session.
2. Send another message from Alice to `bob` using a new UUID.
3. Start a new `grpcurl` session for Bob using the same command as above.
4. Register again with the same client ID:

```json
{"register":{"clientId":"bob"}}
```

The queued message should be delivered after registration.
