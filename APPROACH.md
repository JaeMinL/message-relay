# Approach

## Acceptance criteria

The implementation targets the following behavior:

1. Multiple clients can connect and register unique IDs.
2. A registered sender can submit a UUID-identified message for another client.
3. Every send is explicitly accepted or rejected.
4. A connected recipient receives the message and explicitly acknowledges it.
5. Messages for offline recipients are retained within configured limits.
6. Reconnecting with the same ID reattaches to the same logical mailbox.
7. A delivered but unacknowledged message remains eligible for redelivery.
8. Invalid input and resource-limit failures are reported without crashing unrelated clients.
9. Slow, malformed or disconnected clients do not block unrelated clients.
10. Delivery is FIFO per recipient and at-least-once.
11. Queued and unacknowledged messages survive a normal process restart through the H2 file store.
12. Server shutdown stops transport work before delivery executors and storage are released.

## Architecture

The implementation uses Java 21, plain gRPC and H2 without an application framework or message broker.

```text
Client stream
    |
    v
RelayServiceImpl        protocol parsing and responses
    |
    +------> ConnectionRegistry ----> ClientConnection
    |               active ID          stream + write state
    |
    v
DeliveryService        delivery, ACK, retry, recovery
    |
    v
MailboxStore           durable mailbox and state transitions
    |
    v
H2 MAILBOX table
```

Responsibilities are intentionally separated so the protocol layer does not own persistence or delivery state, and the storage layer does not know about gRPC.

## Protocol and connection lifecycle

`relay.proto` exposes one bidirectional streaming RPC, `MessageRelay.Connect`.

A stream initially has no identity. The client sends `Register(client_id)`. A successful registration reserves that ID in `ConnectionRegistry`, restores any previously in-flight mailbox rows to `QUEUED`, reports the current unacknowledged count, and starts delivery. A disconnected stream is removed from the active registry but its mailbox remains in H2.

Requests before registration receive `REGISTER_REQUIRED`. Validation and application-level rejection responses are non-fatal, so the client may correct the request and continue on the same stream. Transport failures and slow-consumer overflow may terminate the stream.

## Message state and delivery semantics

The H2 `MAILBOX` table is the source of truth for all unacknowledged messages.

- `QUEUED`: persisted and available for delivery.
- `IN_FLIGHT`: delivered and waiting for an ACK.
- ACKed messages are deleted rather than moved to an `ACKED` state.

`SEQ_NO` is a database-generated identity. It is global rather than per recipient, but filtering by `RECIPIENT_ID` and ordering by `SEQ_NO` gives an unambiguous FIFO order for each recipient. The sequence is persisted by H2 and therefore continues across restarts.

Only one message is kept in-flight for a recipient connection. After an ACK, the in-memory in-flight slot is cleared and the next mailbox message is scheduled. If the connection drops, its in-flight database rows are requeued. If an ACK is not received before the timeout, the maintenance scan requeues the row and schedules another delivery. This produces at-least-once semantics: duplicate delivery is possible, but an unacknowledged message is not silently lost.

## Concurrency model and locking

Concurrency protection is deliberately placed only where a multi-step invariant or non-thread-safe stream operation requires it.

### Connection registry lock

`ConnectionRegistry.register()` and `unregister()` are `synchronized`. Registration performs a sequence of operations that must behave atomically with respect to other registration/removal operations: check the current owner, check the active-session limit, then insert or replace a closed connection. Protecting the whole operation prevents two concurrent registrations from both passing those checks.

The backing map is a `ConcurrentHashMap`, so read-only lookup through `findActiveConnection()` does not need the registry monitor. Reads only need a safe current view; they are not enforcing a multi-step capacity invariant.

### Per-connection write lock

Each `ClientConnection` has a `ReentrantLock` used for two related responsibilities:

1. serialize calls to the server-side `StreamObserver`, because concurrent writes to the same gRPC stream are not allowed;
2. protect `inFlightSeqNo`, the connection-local record of the message currently awaiting an ACK.

`DeliveryService.deliverNextMessageIfReady()` uses `tryLock()` rather than blocking a delivery worker behind another operation on the same client. If that connection is busy, the attempt returns and periodic recovery or the next ACK/on-ready callback will try again. This keeps a slow or busy recipient from occupying a worker while unrelated clients continue.

ACK handling and timeout recovery take the same connection lock before clearing the in-flight slot, so delivery, ACK and retry do not independently mutate that state.

### Mailbox admission lock

`MailboxStore.enqueue()` uses one `admissionLock`. Enqueue is not a single SQL statement: it checks duplicate UUID state, checks the per-recipient count, checks the server-wide count, and then inserts. Those count-and-insert checks form one admission invariant. Without serialization, two concurrent transactions could both observe remaining capacity and both insert, exceeding a configured limit.

The entire sequence runs inside one database transaction and rolls back on failure.

### Why most database operations do not take the Java admission lock

Most other mailbox operations are implemented as atomic SQL predicates and therefore do not need the application-level admission lock.

For example, claiming a message uses the equivalent of:

```sql
UPDATE MAILBOX
SET STATUS = 'IN_FLIGHT', ...
WHERE SEQ_NO = ? AND STATUS = 'QUEUED'
```

If two workers race to claim the same row, the database serializes the row update and only one statement can update one row; the other receives an update count of zero. The same pattern is used when requeueing an `IN_FLIGHT` row. ACK deletion also has the recipient in its SQL predicate, so another client's ACK cannot delete the row.

Simple reads such as `findByMessageId`, `loadNext`, timeout scans and count diagnostics also do not need the Java lock. A read can become stale immediately under concurrency, so correctness does not depend on the read alone: the following conditional `UPDATE` or `DELETE` is the arbitration point. For example, two workers may both read the same oldest queued message, but only one can change `QUEUED -> IN_FLIGHT`.

H2 also owns `SEQ_NO` generation through its auto-increment identity column, so Java does not keep or lock a sequence counter. The file database persists both rows and identity state across restart.

## Resource bounds and isolation

The implementation places explicit bounds on active sessions, message size, client-ID size, mailbox depth, total pending messages, the delivery executor queue and slow-client outbound attempts. Delivery is submitted to a worker pool rather than performed on the sender's request thread, so an online recipient that is slow to read does not directly block the sender or unrelated recipients.

A slow stream eventually exceeds its outbound allowance and is closed rather than allowing an unbounded transport backlog.

## Retry behavior

Retries exist on the server only for delivery that has already been accepted and persisted.

- A message not ACKed within the configured timeout is requeued.
- The retry scan finds timed-out `IN_FLIGHT` rows in bounded batches.
- Failed or rejected delivery-executor submissions do not lose the durable row; periodic recovery schedules pending work again.
- Failed writes requeue the row immediately when possible.

The server does not implement a client request retry loop. Protocol responses include a `retryable` flag where appropriate, but deciding whether and when the calling client resubmits a rejected `Send` or `Register` request is a **client-side concern and outside this exercise's test boundary**.

## Persistence and startup

The production JDBC URL uses H2 file mode. `MailboxStore` runs idempotent schema statements during construction:

- `CREATE TABLE IF NOT EXISTS`
- `CREATE INDEX IF NOT EXISTS`

Running those statements on every startup is intentional: they ensure a new database is initialized while leaving existing rows intact. There is no startup `DROP`, truncate or delete step. The database file is therefore reusable after a normal server restart.

## Error handling

Protocol validation is kept separate from transport termination. Invalid registration, malformed sends, UUID errors, duplicate IDs, mailbox limits and ACK errors are represented as protocol responses where recovery on the existing stream is possible. Storage failures are surfaced as retryable application errors where the operation can safely be attempted later.

A server-side outbound overflow or unrecoverable internal/transport failure closes the gRPC stream instead of attempting to enqueue more responses to a client that is already unable to consume them.

## Testing strategy and boundaries

Tests are intentionally focused on the documented relay behavior rather than exhaustive interleaving coverage.

Covered areas include:

- registration and duplicate/limit behavior;
- send validation and rejection mapping;
- direct H2 mailbox state transitions and capacity handling;
- concurrent claim of one queued row;
- concurrent duplicate UUID insertion;
- online delivery and ACK deletion;
- offline retention and replay after reconnect;
- redelivery after disconnect and ACK timeout;
- recipient FIFO behavior, including concurrent senders;
- TTL expiry and recovery after an expired in-flight row;
- storage/executor failure handling and shutdown paths;
- persistence behavior using file-backed storage where relevant.

The integration harness uses an in-process gRPC transport with an executor and isolated in-memory H2 databases. The executor is intentional: `directExecutor()` can create re-entrant call ordering that is not representative of the production Netty transport and can manufacture lock behavior that would not occur in the deployed server.

Not covered:

- client-side retry/backoff behavior after a `retryable=true` response; the client implementation is outside the repository;
- throughput/latency benchmarking, because no performance target is claimed;
- multi-node coordination, because the implementation is explicitly single-node;
- TLS/authentication, which are excluded by the exercise;
- every theoretically possible thread interleaving; targeted race tests instead verify the important database and delivery arbitration points.

All waits in integration tests are bounded so failures terminate deterministically rather than hanging the test suite.

## Trade-offs and limitations

**One in-flight message per recipient.** This simplifies FIFO and ACK state but limits per-recipient throughput to roughly one delivery/ACK round trip at a time.

**Global sequence rather than a per-recipient sequence table.** H2 identity generation provides safe monotonic allocation without a separate counter row or another lock. Recipient FIFO is derived by filtering and ordering that global sequence.

**Serialized admission.** `admissionLock` makes count-based resource limits straightforward and exact within one process, at the cost of serializing message admission. For the exercise scale this favors correctness and explainability over maximum ingest throughput.

**Single node.** Connection ownership is in memory. Multiple relay instances sharing the same database would require distributed ownership/leases before they could safely deliver the same mailbox.

**H2 file persistence.** This provides restart recovery without introducing an external database or broker, but it is still local single-node storage rather than replicated durable infrastructure.

**Fixed retry timing.** A fixed ACK timeout and periodic scan are simpler than implementing delivery backoff. A production system with much larger scale could use more adaptive scheduling.

## AI-assisted development

I used AI-assisted development tools primarily for design and review support.

AI was used to help:
- interpret and break down the exercise requirements;
- identify missing or ambiguous behaviours, including retry, acknowledgement, reconnect, duplicate-message and resource-limit semantics;
- propose the overall project structure, class responsibilities, method boundaries, dependencies, and directory layout;
- review concurrency concerns and suggest where application-level locking or database-level atomic operations were appropriate;
- suggest test cases and identify edge cases or failure scenarios that should be covered;
- review documentation for consistency with the implemented behaviour and known limitations.

I reviewed the suggestions against the exercise requirements and the actual implementation, and adjusted or rejected suggestions where they added unnecessary complexity or did not match the intended behaviour.

## Next steps

If this were extended beyond the exercise, the first improvements would be multi-node connection ownership, production metrics/health checks, an explicit schema migration mechanism, and load testing of the admission and delivery paths. A client library could also implement a documented retry/backoff policy based on the protocol's `retryable` field.
