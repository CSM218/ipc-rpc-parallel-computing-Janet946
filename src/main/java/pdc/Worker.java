package pdc;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

/**
 * Worker node capable of high-concurrency computation in a cluster.
 * Communicates with Master using Message protocol.
 */
public class Worker {

    private final ExecutorService executor;
    private Socket masterSocket;
    private DataOutputStream out;
    private DataInputStream in;
    private final String identity;
    private final String capabilities;

    public Worker(int threads, String identity, String capabilities) {
        this.executor = Executors.newFixedThreadPool(threads);
        this.identity = identity;
        this.capabilities = capabilities;
    }

   public void joinCluster(String masterHost, int port) throws IOException {
    masterSocket = new Socket(masterHost, port);
    out = new DataOutputStream(masterSocket.getOutputStream());
    in = new DataInputStream(masterSocket.getInputStream());

    // Send registration message
    String studentId = identity; // or fetch from environment
Message registration = new Message(
        1,
        "REGISTER",
        identity,
        new byte[0]
);


    sendMessage(registration);

    // Wait for acknowledgment
    Message ack = receiveMessage();
    System.out.println("Master response: " + ack.messageType);
}


    public void scheduleTask(Runnable task) {
        executor.submit(() -> {
            long start = System.currentTimeMillis();
            try {
                task.run();
            } finally {
                long end = System.currentTimeMillis();
                System.out.println("Task completed in " + (end - start) + "ms by " + identity);
            }
        });
    }

    public void shutdown() {
    executor.shutdownNow();

    try {
        if (masterSocket != null && !masterSocket.isClosed()) {
            masterSocket.close();
        }
    } catch (IOException ignored) {}
}


    // ------------------- MESSAGE HELPERS -------------------

    private void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    private Message receiveMessage() throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return Message.unpack(data);
    }
}
