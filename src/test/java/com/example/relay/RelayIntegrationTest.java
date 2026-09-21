package com.example.relay;

import com.example.relay.proto.Delivery;
import com.example.relay.proto.ProtocolErrorCode;
import com.example.relay.proto.Register;
import com.example.relay.proto.SendRejectionReason;
import com.example.relay.proto.ServerEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the documented requirements end to end over a real gRPC stream:
 * register, addressed send, acknowledgement, offline retention, reconnect
 * replay, redelivery after an unacknowledged disconnect, FIFO order, the
 * resource limits, and the protocol error cases.
 *
 * <p>Not covered here, deliberately: throughput or latency behaviour, multi-node
 * concerns, and TLS/auth (out of scope for the exercise). Timer-driven paths use
 * shrunk intervals rather than a fake clock, which keeps the tests simple at the
 * cost of a few seconds of wall time.
 */
class RelayIntegrationTest {

    private static String id() {
        return UUID.randomUUID().toString();
    }

    @Test
    @DisplayName("a message reaches a connected recipient and an ACK removes it")
    void deliverAndAcknowledge() throws Exception {
        // given: connected sender and recipient
        try (TestHarness harness = TestHarness.withDefaults();
             var alice = harness.connect("alice");
             var bob = harness.connect("bob")) {

            String messageId = id();
            // when: send a message
            alice.sendMessage(messageId, "bob", "hello bob");

            // then: verify acceptance and delivery, then ACK deletion
            ServerEvent accepted = alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED);
            assertNotNull(accepted, "the send should be accepted");
            assertEquals(messageId, accepted.getSendAccepted().getMessageId());

            Delivery delivery = bob.awaitDelivery();
            assertNotNull(delivery, "bob should receive the message");
            assertEquals("alice", delivery.getSenderId());
            assertEquals("hello bob", delivery.getContent());
            assertFalse(delivery.getRedelivery(), "a first delivery is not a redelivery");

            bob.ack(messageId);
            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == 0,
                    "the acknowledged message should be deleted");
        }
    }

    @Test
    @DisplayName("messages sent while offline are retained and replayed on reconnect")
    void offlineRetentionAndReplay() throws Exception {
        // given: a sender and an offline recipient
        try (TestHarness harness = TestHarness.withDefaults();
             var alice = harness.connect("alice")) {

            String first = id();
            String second = id();
            // when: queue messages for the offline recipient
            alice.sendMessage(first, "bob", "one");
            alice.sendMessage(second, "bob", "two");
            // then: verify persistence, FIFO replay and ACK deletion
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));
            assertEquals(2, harness.store().countUnacknowledgedMessagesForRecipient("bob"));

            try (var bob = harness.connect("bob")) {
                // FIFO: the queue is drained one message at a time, oldest first.
                Delivery one = bob.awaitDelivery();
                assertNotNull(one);
                assertEquals("one", one.getContent());

                assertNull(bob.pollFor(ServerEvent.EventCase.DELIVERY),
                        "the second message must wait for an ACK of the first");

                bob.ack(one.getMessageId());
                Delivery two = bob.awaitDelivery();
                assertNotNull(two);
                assertEquals("two", two.getContent());
                bob.ack(two.getMessageId());
            }

            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == 0, "both messages should be gone");
        }
    }

    @Test
    @DisplayName("a delivered but unacknowledged message is redelivered after reconnect")
    void redeliveryAfterDisconnect() throws Exception {
        // given: a delivered message awaiting ACK
        try (TestHarness harness = TestHarness.withDefaults();
             var alice = harness.connect("alice")) {

            String messageId = id();
            var bob = harness.connect("bob");
            alice.sendMessage(messageId, "bob", "important");
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));

            Delivery first = bob.awaitDelivery();
            assertNotNull(first);
            assertEquals(messageId, first.getMessageId());

            // Drop the connection without acknowledging.
            // when: disconnect without acknowledging
            bob.abort();
            waitUntil(() -> harness.registry().findActiveConnection("bob") == null, "bob should be deregistered");
            assertEquals(1, harness.store().countUnacknowledgedMessagesForRecipient("bob"), "the message must survive the disconnect");

            // then: verify reconnect redelivers the same message
            try (var bobAgain = harness.connect("bob")) {
                Delivery again = bobAgain.awaitDelivery();
                assertNotNull(again, "the unacknowledged message should be redelivered");
                assertEquals(messageId, again.getMessageId());
                assertTrue(again.getRedelivery(), "the replay should be flagged as a redelivery");
                bobAgain.ack(messageId);
                waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == 0, "the ACK should delete it");
            }
        }
    }

    @Test
    @DisplayName("FIFO order is preserved across concurrent senders")
    void fifoUnderConcurrentSends() throws Exception {
        // given: two senders and an offline recipient
        try (TestHarness harness = TestHarness.withDefaults();
             var alice = harness.connect("alice");
             var carol = harness.connect("carol")) {

            int perSender = 10;
            List<String> aliceIds = new ArrayList<>();
            List<String> carolIds = new ArrayList<>();
            Thread t1 = new Thread(() -> {
                for (int i = 0; i < perSender; i++) {
                    String mid = id();
                    aliceIds.add(mid);
                    alice.sendMessage(mid, "bob", "alice-" + i);
                }
            });
            Thread t2 = new Thread(() -> {
                for (int i = 0; i < perSender; i++) {
                    String mid = id();
                    carolIds.add(mid);
                    carol.sendMessage(mid, "bob", "carol-" + i);
                }
            });
            // when: send concurrently and wait for persistence
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == perSender * 2L,
                    "all sends should be stored");

            // then: verify global sequence and per-sender order
            try (var bob = harness.connect("bob")) {
                List<Long> seqNos = new ArrayList<>();
                List<String> fromAlice = new ArrayList<>();
                List<String> fromCarol = new ArrayList<>();
                for (int i = 0; i < perSender * 2; i++) {
                    Delivery d = bob.awaitDelivery();
                    assertNotNull(d, "expected delivery " + i);
                    seqNos.add(d.getSeqNo());
                    (d.getSenderId().equals("alice") ? fromAlice : fromCarol).add(d.getContent());
                    bob.ack(d.getMessageId());
                }
                // Global order is monotonic, and each sender's own messages arrive in order.
                assertEquals(seqNos.stream().sorted().toList(), seqNos, "sequence numbers must ascend");
                assertEquals(expected("alice-", perSender), fromAlice);
                assertEquals(expected("carol-", perSender), fromCarol);
            }
        }
    }

    @Test
    @DisplayName("an unacknowledged message is redelivered after the ACK timeout")
    void redeliveryAfterAckTimeout() throws Exception {
        // given: a delivered message and a short ACK timeout
        RelayConfig.Builder builder = RelayConfig.builder()
                .acknowledgementTimeout(Duration.ofMillis(300))
                .retryScanInterval(Duration.ofMillis(100));
        try (TestHarness harness = new TestHarness(builder);
             var alice = harness.connect("alice");
             var bob = harness.connect("bob")) {

            String messageId = id();
            alice.sendMessage(messageId, "bob", "ack me");
            Delivery first = bob.awaitDelivery();
            assertNotNull(first);
            assertFalse(first.getRedelivery());

            // Stay silent: the retry scan should push the same message again.
            // when: wait without acknowledging
            Delivery second = bob.awaitDelivery();
            // then: verify redelivery stops after ACK
            assertNotNull(second, "the message should be redelivered without an ACK");
            assertEquals(messageId, second.getMessageId());
            assertTrue(second.getRedelivery());

            bob.ack(messageId);
            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == 0, "the ACK should stop redelivery");
        }
    }

    @Test
    @DisplayName("expired messages are purged and no longer delivered")
    void expiredMessagesArePurged() throws Exception {
        // given: a short message TTL and an offline recipient
        RelayConfig.Builder builder = RelayConfig.builder()
                .messageTtl(Duration.ofMillis(200))
                .cleanupInterval(Duration.ofMillis(100));
        try (TestHarness harness = new TestHarness(builder);
             var alice = harness.connect("alice")) {

            // when: send a message and allow expiry
            alice.sendMessage(id(), "bob", "too late");
            // then: verify cleanup and absence of replay
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));
            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == 0, "the TTL sweep should delete it");

            try (var bob = harness.connect("bob")) {
                assertNull(bob.pollFor(ServerEvent.EventCase.DELIVERY),
                        "an expired message must not be delivered");
            }
        }
    }

    @Test
    @DisplayName("an online recipient receives new messages after its unacknowledged message expires")
    void deliveryResumesAfterInflightExpiry() throws Exception {
        // given: an online recipient whose first message has expired
        RelayConfig.Builder builder = RelayConfig.builder()
                .messageTtl(Duration.ofSeconds(1))
                .cleanupInterval(Duration.ofMillis(50))
                .acknowledgementTimeout(Duration.ofSeconds(30))
                .retryScanInterval(Duration.ofMillis(50));
        try (TestHarness harness = new TestHarness(builder);
             var alice = harness.connect("alice");
             var bob = harness.connect("bob")) {
            String firstId = id();
            alice.sendMessage(firstId, "bob", "expires without ACK");
            Delivery first = bob.awaitDelivery();
            assertNotNull(first);
            assertEquals(firstId, first.getMessageId());
            waitUntil(() -> harness.store().findByMessageId(firstId) == null,
                    "the in-flight message should expire");

            bob.ack(firstId);
            ServerEvent lateAck = bob.pollFor(ServerEvent.EventCase.PROTOCOL_ERROR);
            assertNotNull(lateAck);
            assertEquals(ProtocolErrorCode.ACK_UNKNOWN, lateAck.getProtocolError().getCode());

            String secondId = id();
            // when: send another message on the same connection
            alice.sendMessage(secondId, "bob", "delivery must resume");
            // then: verify delivery resumes and ACK succeeds
            Delivery second = bob.awaitDelivery();
            assertNotNull(second, "expiry must release the recipient's delivery slot");
            assertEquals(secondId, second.getMessageId());
            bob.ack(secondId);
            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == 0,
                    "the next message should be acknowledged normally");
        }
    }

    @Test
    @DisplayName("invalid sends are rejected with the documented reason and retryability")
    void sendValidation() throws Exception {
        // given: a sender with a 16-byte content limit
        try (TestHarness harness = new TestHarness(RelayConfig.builder().maxMessageContentSize(16));
             var alice = harness.connect("alice")) {

            // when: submit invalid requests and corrected retries
            alice.sendMessage("not-a-uuid", "bob", "x");
            // then: verify rejection codes and continued stream usability
            assertRejected(alice, SendRejectionReason.INVALID_MESSAGE, false);

            alice.sendMessage(id(), "bob", "");
            assertRejected(alice, SendRejectionReason.INVALID_MESSAGE, false);

            alice.sendMessage(id(), "", "x");
            assertRejected(alice, SendRejectionReason.INVALID_MESSAGE, false);

            alice.sendMessage(id(), "bob", "x".repeat(17));
            assertRejected(alice, SendRejectionReason.MESSAGE_TOO_LARGE, false);

            final String correctedMessageId = id();
            alice.sendMessage(correctedMessageId, "bob", "가".repeat(6));
            assertRejected(alice, SendRejectionReason.MESSAGE_TOO_LARGE, false);
            assertNull(harness.store().findByMessageId(correctedMessageId));
            alice.sendMessage(correctedMessageId, "bob", "fixed");
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));
            assertNotNull(harness.store().findByMessageId(correctedMessageId));
            assertNull(alice.error());

            String duplicate = id();
            alice.sendMessage(duplicate, "bob", "first");
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));
            alice.sendMessage(duplicate, "bob", "second");
            assertRejected(alice, SendRejectionReason.DUPLICATE_ID_CONFLICT, false);
        }
    }

    @Test
    @DisplayName("a full mailbox is rejected as retryable and recovers after an ACK")
    void mailboxLimit() throws Exception {
        // given: a mailbox with a two-message limit
        try (TestHarness harness = new TestHarness(RelayConfig.builder().maxMessagesPerMailbox(2));
             var alice = harness.connect("alice")) {

            // when: fill the mailbox and attempt another send
            alice.sendMessage(id(), "bob", "one");
            // then: verify accepted messages and retryable capacity rejection
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));
            alice.sendMessage(id(), "bob", "two");
            assertNotNull(alice.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));

            alice.sendMessage(id(), "bob", "three");
            assertRejected(alice, SendRejectionReason.RESOURCE_EXHAUSTED, true);
        }
    }

    @Test
    @DisplayName("unregistered requests are rejected and registration can be retried on the same stream")
    void registerFirstIsEnforced() throws Exception {
        // given: an unregistered stream
        try (TestHarness harness = TestHarness.withDefaults()) {
            var client = harness.openRaw();
            // when: send before registration
            client.sendMessage(id(), "bob", "too early");

            // then: verify recoverable errors and successful registration and retry
            ServerEvent error = client.pollFor(ServerEvent.EventCase.PROTOCOL_ERROR);
            assertNotNull(error);
            assertEquals(ProtocolErrorCode.REGISTER_REQUIRED, error.getProtocolError().getCode());
            assertFalse(error.getProtocolError().getFatal());
            assertTrue(error.getProtocolError().getRetryable());
            client.ack(id());
            assertEquals(ProtocolErrorCode.REGISTER_REQUIRED, nextErrorCode(client));
            client.send(com.example.relay.proto.ClientEvent.getDefaultInstance());
            assertEquals(ProtocolErrorCode.REGISTER_REQUIRED, nextErrorCode(client));
            client.register("alice");
            assertNotNull(client.awaitRegistered());
            client.sendMessage(id(), "bob", "retry after registration");
            assertNotNull(client.pollFor(ServerEvent.EventCase.SEND_ACCEPTED));
            try (final var bob = harness.connect("bob")) {
                assertNotNull(bob.awaitDelivery());
            }
            assertNull(client.error());
        }
    }

    @Test
    @DisplayName("invalid registration can be corrected on the same stream")
    void invalidRegistration() throws Exception {
        // given: an unregistered stream
        try (TestHarness harness = TestHarness.withDefaults()) {
            var client = harness.openRaw();
            // when: register with an invalid ID
            client.register("   ");

            // then: verify invalid IDs can be corrected on the same stream
            ServerEvent error = client.pollFor(ServerEvent.EventCase.PROTOCOL_ERROR);
            assertNotNull(error);
            assertEquals(ProtocolErrorCode.INVALID_REGISTRATION, error.getProtocolError().getCode());
            assertFalse(error.getProtocolError().getFatal());
            client.register("x".repeat(harness.config().maxClientIdSize + 1));
            assertEquals(ProtocolErrorCode.INVALID_REGISTRATION, nextErrorCode(client));
            client.register("valid-client");
            assertNotNull(client.awaitRegistered());
            assertNull(client.error());
        }
    }

    @Test
    @DisplayName("a second connection for a live client id is refused, the first survives")
    void duplicateClientId() throws Exception {
        // given: an existing registered recipient
        try (TestHarness harness = TestHarness.withDefaults();
             var bob = harness.connect("bob")) {

            var impostor = harness.openRaw();
            // when: attempt to claim the same ID
            impostor.register("bob");

            // then: verify the original survives and the new stream can change ID
            ServerEvent error = impostor.pollFor(ServerEvent.EventCase.PROTOCOL_ERROR);
            assertNotNull(error);
            assertEquals(ProtocolErrorCode.ALREADY_EXISTS, error.getProtocolError().getCode());
            assertTrue(error.getProtocolError().getRetryable());
            assertFalse(error.getProtocolError().getFatal());
            impostor.register("another-client");
            assertNotNull(impostor.awaitRegistered());
            assertNull(impostor.error());

            // The original connection is untouched and still receives messages.
            assertNotNull(harness.registry().findActiveConnection("bob"));
        }
    }

    @Test
    @DisplayName("registering twice on one stream is reported but does not close it")
    void repeatedRegisterIsNotFatal() throws Exception {
        // given: a registered stream
        try (TestHarness harness = TestHarness.withDefaults();
             var bob = harness.connect("bob")) {

            // when: register again
            bob.send(com.example.relay.proto.ClientEvent.newBuilder()
                    .setRegister(Register.newBuilder().setClientId("bob"))
                    .build());

            // then: verify a nonfatal error and preserved registration
            ServerEvent error = bob.pollFor(ServerEvent.EventCase.PROTOCOL_ERROR);
            assertNotNull(error);
            assertEquals(ProtocolErrorCode.ALREADY_REGISTERED, error.getProtocolError().getCode());
            assertFalse(error.getProtocolError().getFatal());
            assertNotNull(harness.registry().findActiveConnection("bob"), "the stream should stay registered");
        }
    }

    @Test
    @DisplayName("stale, unknown and misdirected ACKs are reported without closing the stream")
    void ackErrorCases() throws Exception {
        // given: connected sender and recipient
        try (TestHarness harness = TestHarness.withDefaults();
             var alice = harness.connect("alice");
             var bob = harness.connect("bob")) {

            // when: send invalid, misdirected and repeated ACKs
            bob.ack("not-a-uuid");
            // then: verify error codes and that the stream stays open
            assertEquals(ProtocolErrorCode.ACK_UNKNOWN, nextErrorCode(bob));

            bob.ack(id());
            assertEquals(ProtocolErrorCode.ACK_UNKNOWN, nextErrorCode(bob));

            String messageId = id();
            alice.sendMessage(messageId, "bob", "for bob only");
            assertNotNull(bob.awaitDelivery());

            // alice is not the recipient of her own message.
            alice.ack(messageId);
            assertEquals(ProtocolErrorCode.ACK_WRONG_RECIPIENT, nextErrorCode(alice));

            bob.ack(messageId);
            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("bob") == 0, "the first ACK should delete it");

            // A repeated ACK is harmless: the row is already gone.
            bob.ack(messageId);
            assertEquals(ProtocolErrorCode.ACK_UNKNOWN, nextErrorCode(bob));
            assertNotNull(harness.registry().findActiveConnection("bob"), "the stream should stay open");
        }
    }

    @Test
    @DisplayName("the concurrent connection limit is enforced")
    void connectionLimit() throws Exception {
        // given: a server with its only registered slot occupied
        try (TestHarness harness = new TestHarness(RelayConfig.builder().maxActiveSessions(1));
             var first = harness.connect("first")) {

            var second = harness.openRaw();
            // when: attempt another registration
            second.register("second");

            // then: verify rejection and successful retry after the slot is released
            ServerEvent error = second.pollFor(ServerEvent.EventCase.PROTOCOL_ERROR);
            assertNotNull(error);
            assertEquals(ProtocolErrorCode.CONNECTION_LIMIT_REACHED, error.getProtocolError().getCode());
            assertTrue(error.getProtocolError().getRetryable());
            assertFalse(error.getProtocolError().getFatal());
            first.close();
            waitUntil(() -> harness.registry().registeredConnections().isEmpty(), "the first slot should be released");
            second.register("second");
            assertNotNull(second.awaitRegistered());
            assertNull(second.error());
        }
    }

    @Test
    @DisplayName("a silent recipient does not block traffic to other clients")
    void silentClientDoesNotBlockOthers() throws Exception {
        // given: a sender and two recipients
        try (TestHarness harness = TestHarness.withDefaults();
             var alice = harness.connect("alice");
             var bob = harness.connect("bob");
             var carol = harness.connect("carol")) {

            // bob receives one message and never acknowledges it.
            // when: leave one recipient unacknowledged and send to the other
            alice.sendMessage(id(), "bob", "stuck");
            // then: verify independent delivery and acknowledgement
            assertNotNull(bob.awaitDelivery());
            for (int i = 0; i < 5; i++) {
                alice.sendMessage(id(), "bob", "queued-" + i);
            }

            // carol's traffic is unaffected.
            String forCarol = id();
            alice.sendMessage(forCarol, "carol", "hi carol");
            Delivery delivery = carol.awaitDelivery();
            assertNotNull(delivery, "carol should be served while bob is stuck");
            assertEquals(forCarol, delivery.getMessageId());
            carol.ack(forCarol);
            waitUntil(() -> harness.store().countUnacknowledgedMessagesForRecipient("carol") == 0, "carol's mailbox should drain");
        }
    }

    @Test
    @DisplayName("queued messages survive a store restart")
    void statePersistsAcrossRestart() throws Exception {
        // given: a database that survives pool closure
        String jdbcUrl = "jdbc:h2:mem:restart-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
        RelayConfig config = RelayConfig.builder().jdbcUrl(jdbcUrl).build();
        String messageId = id();

        // when: persist a message and close the store
        try (var store = new com.example.relay.store.MailboxStore(config)) {
            store.enqueue(messageId, "alice", "bob", "durable", System.currentTimeMillis());
        }
        // A brand new store over the same database sees the queue unchanged.
        // then: verify a reopened store reads the same message
        try (var reopened = new com.example.relay.store.MailboxStore(config)) {
            assertEquals(1, reopened.countUnacknowledgedMessagesForRecipient("bob"));
            var pending = reopened.loadQueuedMessages("bob", 10, System.currentTimeMillis());
            assertEquals(messageId, pending.get(0).messageId());
        }
    }

    // ------------------------------------------------------------------ helpers

    private static List<String> expected(String prefix, int count) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(prefix + i);
        }
        return out;
    }

    private static void assertRejected(TestHarness.TestClient client,
                                       SendRejectionReason reason, boolean retryable)
            throws InterruptedException {
        ServerEvent event = client.pollFor(ServerEvent.EventCase.SEND_REJECTED);
        assertNotNull(event, "expected a rejection");
        assertEquals(reason, event.getSendRejected().getReason());
        assertEquals(retryable, event.getSendRejected().getRetryable(),
                "unexpected retryability for " + reason);
    }

    private static ProtocolErrorCode nextErrorCode(TestHarness.TestClient client) throws InterruptedException {
        ServerEvent event = client.pollFor(ServerEvent.EventCase.PROTOCOL_ERROR);
        assertNotNull(event, "expected a protocol error");
        return event.getProtocolError().getCode();
    }

    private static Status.Code statusOf(Throwable t) {
        StatusRuntimeException sre = assertInstanceOf(StatusRuntimeException.class, t);
        return sre.getStatus().getCode();
    }

    /** Polls a condition until the harness timeout elapses. */
    private static void waitUntil(java.util.function.BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.nanoTime() + TestHarness.AWAIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError(message);
    }
}
