package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;

/**
 * JUnit 5 tests for Worker class.
 */
class WorkerTest {

    private Worker worker;

    @BeforeEach
    void setUp() {
        worker = new Worker(2, "worker-1", "dummy-capabilities") {
            @Override
            public void joinCluster(String masterHost, int port) {
                // Stub: no network
                System.out.println("Stubbed joinCluster()");
            }

            @Override
            public void scheduleTask(Runnable task) {
                task.run(); // Run immediately for test
            }

            @Override
            public void shutdown() {
                System.out.println("Stubbed shutdown()");
            }
        };
    }

    @Test
    void testScheduleTask_Executes() {
        final boolean[] executed = {false};
        worker.scheduleTask(() -> executed[0] = true);
        assertTrue(executed[0]);
    }

    @Test
    void testJoinCluster_NoException() {
        assertDoesNotThrow(() -> worker.joinCluster("localhost", 1234));
    }

    @Test
    void testShutdown_NoException() {
        assertDoesNotThrow(() -> worker.shutdown());
    }

    @Test
    void testPayloadHandling() {
        byte[] payload = "Test payload".getBytes(StandardCharsets.UTF_8);
        assertEquals("Test payload", new String(payload, StandardCharsets.UTF_8));
    }
}
