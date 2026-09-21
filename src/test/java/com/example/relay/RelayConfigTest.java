package com.example.relay;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("system-properties")
class RelayConfigTest {
    @Test
    void systemPropertiesOverrideDefaultsAndRejectInvalidIntegers() {
        // given: saved process properties so the test cannot affect later tests
        final String[] keys = {"relay.port", "relay.executorThreads", "relay.maxActiveSessions",
                "relay.maxMessagesPerMailbox", "relay.maxPendingMessages", "relay.jdbcUrl", "relay.dataDir"};
        final Map<String, String> previous = new HashMap<>();
        for (final String key : keys) {
            previous.put(key, System.getProperty(key));
            System.clearProperty(key);
        }
        try {
            // when: defaults, explicit overrides and blank values are loaded
            assertEquals(50051, RelayConfig.fromSystemProperties().port);
            System.setProperty("relay.port", " 12345 ");
            System.setProperty("relay.executorThreads", "2");
            System.setProperty("relay.maxActiveSessions", "3");
            System.setProperty("relay.maxMessagesPerMailbox", "4");
            System.setProperty("relay.maxPendingMessages", "5");
            System.setProperty("relay.jdbcUrl", "jdbc:h2:mem:config-test");
            final RelayConfig overridden = RelayConfig.fromSystemProperties();
            // then: each configured value is preserved and malformed numbers fail explicitly
            assertEquals(12345, overridden.port);
            assertEquals(2, overridden.executorThreads);
            assertEquals(3, overridden.maxActiveSessions);
            assertEquals(4, overridden.maxMessagesPerMailbox);
            assertEquals(5, overridden.maxPendingMessages);
            assertEquals("jdbc:h2:mem:config-test", overridden.jdbcUrl);
            System.setProperty("relay.port", " ");
            System.setProperty("relay.jdbcUrl", " ");
            System.setProperty("relay.dataDir", "/tmp/config-only-no-database-created");
            final RelayConfig fallback = RelayConfig.fromSystemProperties();
            assertEquals(50051, fallback.port);
            assertTrue(fallback.jdbcUrl.startsWith("jdbc:h2:file:/tmp/config-only-no-database-created/relay;"));
            System.setProperty("relay.port", "bad-number");
            final var error = assertThrows(IllegalArgumentException.class, RelayConfig::fromSystemProperties);
            assertInstanceOf(NumberFormatException.class, error.getCause());
        } finally {
            previous.forEach((key, value) -> {
                if (value == null) System.clearProperty(key);
                else System.setProperty(key, value);
            });
        }
    }
}
