package com.distqueue.core;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private final UUID messageId;
    private final byte[] payload;
    private final Instant timestamp;
    private final String topic;
    private final int partition;

    public Message(String topic, int partition, byte[] payload) {
        this.messageId = UUID.randomUUID();
        this.payload = payload;
        this.timestamp = Instant.now();
        this.topic = topic;
        this.partition = partition;
        System.out.println("Generated Message ID: " + this.messageId); // Debug 
    }

    // Getters and basic methods
    public UUID getMessageId() {
        return messageId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId=" + messageId +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", timestamp=" + timestamp +
                '}';
    }
}
