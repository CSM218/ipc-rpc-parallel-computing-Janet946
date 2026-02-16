package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Message represents a network communication unit with a custom wire format.
 * Includes messageType, studentId, and sender fields for autograder compliance.
 */
public class Message {

    // ======== REQUIRED CONSTANTS ========
    public static final String PROTOCOL_MAGIC = "CSM218";

    public static final String STUDENT_ID =
            System.getenv("STUDENT_ID") != null
                    ? System.getenv("STUDENT_ID")
                    : "UNKNOWN";

    // ======== FIELDS ========
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public String sender;
    public long timestamp;
    public byte[] payload;

    // Default constructor
    public Message() {
        this.magic = PROTOCOL_MAGIC;
        this.studentId = STUDENT_ID;
        this.timestamp = System.currentTimeMillis();
    }

    // Main constructor (magic enforced internally)
    public Message(int version, String messageType, String sender, byte[] payload) {
        this.magic = PROTOCOL_MAGIC;
        this.version = version;
        this.messageType = messageType;
        this.studentId = STUDENT_ID;
        this.sender = sender;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    // ================== Serialization ==================

    /**
     * Packs the Message into a byte array using a length-prefixed wire format:
     * [magicLength][magicBytes][version]
     * [messageTypeLength][messageTypeBytes]
     * [studentIdLength][studentIdBytes]
     * [senderLength][senderBytes]
     * [timestamp]
     * [payloadLength][payloadBytes]
     */
    public byte[] pack() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        writeString(dos, magic);
        dos.writeInt(version);
        writeString(dos, messageType);
        writeString(dos, studentId);
        writeString(dos, sender);
        dos.writeLong(timestamp);

        if (payload != null) {
            dos.writeInt(payload.length);
            dos.write(payload);
        } else {
            dos.writeInt(0);
        }

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Unpacks a Message from a byte array using the same format.
     * Also validates protocol magic.
     */
    public static Message unpack(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        Message msg = new Message();

        msg.magic = readString(dis);

        // ===== PROTOCOL VALIDATION =====
        if (!PROTOCOL_MAGIC.equals(msg.magic)) {
            throw new IOException("Invalid protocol magic. Expected CSM218.");
        }

        msg.version = dis.readInt();
        msg.messageType = readString(dis);
        msg.studentId = readString(dis);
        msg.sender = readString(dis);
        msg.timestamp = dis.readLong();

        int payloadLength = dis.readInt();
        if (payloadLength > 0) {
            msg.payload = new byte[payloadLength];
            dis.readFully(msg.payload);
        } else {
            msg.payload = new byte[0];
        }

        return msg;
    }

    // ================== Helper Methods ==================

    private static void writeString(DataOutputStream dos, String str) throws IOException {
        if (str != null) {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else {
            dos.writeInt(0);
        }
    }

    private static String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if (length > 0) {
            byte[] bytes = new byte[length];
            dis.readFully(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } else {
            return "";
        }
    }

    // ================== Example Usage ==================
    public static void main(String[] args) throws IOException {

        Message original = new Message(
                1,
                "TASK",
                "Worker1",
                "Hello Payload".getBytes(StandardCharsets.UTF_8)
        );

        byte[] bytes = original.pack();
        System.out.println("Packed bytes length: " + bytes.length);

        Message received = Message.unpack(bytes);

        System.out.println("Unpacked Message:");
        System.out.println("Magic: " + received.magic);
        System.out.println("Version: " + received.version);
        System.out.println("MessageType: " + received.messageType);
        System.out.println("StudentId: " + received.studentId);
        System.out.println("Sender: " + received.sender);
        System.out.println("Timestamp: " + received.timestamp);
        System.out.println("Payload: " + new String(received.payload, StandardCharsets.UTF_8));
    }
}
