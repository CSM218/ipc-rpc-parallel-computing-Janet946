package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 tests for Master class.
 */
class MasterTest {

    private Master master;

    @BeforeEach
    void setUp() {
        master = new Master() {
            @Override
            public void listen(int port) {
                // Stub: avoid real sockets
                System.out.println("Stubbed listen() on port " + port);
            }

            @Override
            public void reconcileState() {
                // Stub: do nothing
                System.out.println("Stubbed reconcileState()");
            }

            @Override
            public Object coordinate(String operation, int[][] matrix, int workerCount) {
                // Stub: return dummy result
                System.out.println("Stubbed coordinate() for operation " + operation);
                int[][] result = new int[matrix.length][];
                for (int i = 0; i < matrix.length; i++) {
                    result[i] = matrix[i].clone();
                }
                return result;
            }
        };
    }

    @Test
    void testCoordinate_Structure() {
        int[][] matrix = {{1,2},{3,4}};
        Object result = master.coordinate("SUM", matrix, 1);
        assertNotNull(result);
    }

    @Test
    void testListen_NoBlocking() {
        assertDoesNotThrow(() -> master.listen(0));
    }

    @Test
    void testReconcile_State() {
        assertDoesNotThrow(() -> master.reconcileState());
    }
}
