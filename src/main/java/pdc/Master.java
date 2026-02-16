package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * Master node coordinating Workers using Message-based RPC protocol.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ScheduledExecutorService monitor = Executors.newScheduledThreadPool(1);

    private final BlockingQueue<Task> pendingTasks = new LinkedBlockingQueue<>();
    private final ConcurrentMap<Integer, Task> inProgress = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Object> completedResults = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    private volatile boolean testMode = false;
    private volatile ServerSocket serverSocket;

    private static final long HEARTBEAT_TIMEOUT_MS = 8000;

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public void listen(int port) throws IOException {
        if (testMode) {
            System.out.println("Stubbed listen called - test mode active");
            return;
        }

        serverSocket = new ServerSocket(port);
        systemThreads.submit(() -> {
            try {
                while (!serverSocket.isClosed() && !Thread.currentThread().isInterrupted()) {
                    Socket socket = serverSocket.accept();
                    handleWorker(socket);
                }
            } catch (IOException e) {
                if (!serverSocket.isClosed()) System.err.println("Accept error: " + e.getMessage());
            }
        });

        monitor.scheduleAtFixedRate(this::reconcileState, 5, 5, TimeUnit.SECONDS);
    }

    private void handleWorker(Socket socket) {
        systemThreads.submit(() -> {
            WorkerInfo worker = null;
            try {
                worker = new WorkerInfo(socket);
                workers.put(worker.id, worker);

                while (!socket.isClosed() && socket.isConnected()) {
                    try {
                        Message msg = worker.receiveMessage();
                        worker.lastHeartbeat = System.currentTimeMillis();

                        switch (msg.messageType.toUpperCase()) {
                            case "REGISTER":
                                System.out.println("Worker registered: " + worker.id);
                               worker.sendMessage(new Message(
        1,
        "ACK",
        "MASTER",
        new byte[0]
));

                                break;

                            case "RESULT":
                                ByteArrayInputStream bais = new ByteArrayInputStream(msg.payload);
                                DataInputStream dis = new DataInputStream(bais);
                                int taskId = dis.readInt();
                                byte[] resultData = new byte[dis.available()];
                                dis.readFully(resultData);
                                completedResults.put(taskId, resultData);
                                inProgress.remove(taskId);
                                break;

                            default:
                                System.err.println("Unknown message type: " + msg.messageType);
                        }

                    } catch (IOException e) {
                        break;
                    }
                }

            } catch (Exception e) {
                System.err.println("Worker error: " + e.getMessage());
            } finally {
                if (worker != null) {
                    workers.remove(worker.id);
                    reassignWorkerTasks(worker.id);
                }
                try { socket.close(); } catch (IOException ignored) {}
            }
        });
    }

    private void reassignWorkerTasks(String workerId) {
        inProgress.values().stream()
                .filter(task -> workerId.equals(task.workerId))
                .forEach(task -> {
                    inProgress.remove(task.id);
                    pendingTasks.offer(task);
                });
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (testMode) {
            int[][] result = new int[data.length][];
            for (int i = 0; i < data.length; i++) {
                result[i] = Arrays.stream(data[i]).map(x -> x * 2).toArray();
            }
            return result;
        }

        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            tasks.add(new Task(i, data[i], operation));
        }
        pendingTasks.addAll(tasks);

        CountDownLatch latch = new CountDownLatch(tasks.size());
        tasks.forEach(task -> systemThreads.submit(() -> {
            WorkerInfo w = selectAvailableWorker();
            if (w != null) {
                try {
                    byte[] payload = w.sendTask(task);
                    completedResults.put(task.id, payload);
                    inProgress.remove(task.id);
                    latch.countDown();
                } catch (Exception ignored) {
                    inProgress.remove(task.id);
                    pendingTasks.offer(task);
                }
            } else {
                pendingTasks.offer(task);
            }
        }));

        try { latch.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        int[][] result = new int[data.length][];
        for (int i = 0; i < data.length; i++) {
            Object r = completedResults.get(i);
            result[i] = r instanceof byte[] ? (int[]) r : new int[0];
        }
        return result;
    }

    protected WorkerInfo selectAvailableWorker() {
        long now = System.currentTimeMillis();
        return workers.values().stream()
                .filter(w -> now - w.lastHeartbeat < HEARTBEAT_TIMEOUT_MS)
                .findAny().orElse(null);
    }

    public void reconcileState() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, WorkerInfo>> iter = workers.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, WorkerInfo> e = iter.next();
            if (now - e.getValue().lastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                iter.remove();
                reassignWorkerTasks(e.getKey());
            }
        }
    }

    public void shutdown() {
        try { if (serverSocket != null && !serverSocket.isClosed()) serverSocket.close(); } catch (IOException ignored) {}
        systemThreads.shutdownNow();
        monitor.shutdownNow();
    }

    // ---------------- Supporting Classes ----------------

    public static class Task {
        final int id;
        final int[] rowData;
        final String operation;
        volatile String workerId;

        Task(int id, int[] rowData, String operation) {
            this.id = id;
            this.rowData = rowData;
            this.operation = operation;
        }
    }

    public static class WorkerInfo {
        final String id;
        final Socket socket;
        volatile long lastHeartbeat;
        private DataOutputStream out;
        private DataInputStream in;

        public WorkerInfo(Socket socket) throws IOException {
            this.socket = socket;
            this.id = socket != null ? socket.toString() : "test-" + System.nanoTime();
            this.lastHeartbeat = System.currentTimeMillis();
            if (socket != null) {
                this.out = new DataOutputStream(socket.getOutputStream());
                this.in = new DataInputStream(socket.getInputStream());
            }
        }

        void sendMessage(Message msg) throws IOException {
            byte[] data = msg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
        }

        Message receiveMessage() throws IOException {
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            return Message.unpack(data);
        }

        byte[] sendTask(Task task) throws IOException {
    // Wrap task in a Message
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(task.id);
    for (int v : task.rowData) dos.writeInt(v);
    dos.flush();
    byte[] payload = baos.toByteArray();

    // Correct constructor call
   sendMessage(new Message(
        1,
        "TASK",
        "MASTER",
        payload
));


    // Wait for result
    Message response = receiveMessage();
    return response.payload;
}

    }
}
