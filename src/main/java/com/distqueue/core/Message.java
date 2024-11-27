package com.distqueue.core;

import com.google.gson.Gson;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    // Regular message fields
    private final UUID messageId;
    private final byte[] payload;
    private final Instant timestamp;
    private final String topic;
    private final int partition;

    // Gossip-related fields
    private String senderId;      // ID of the node that sent this gossip
    private Instant gossipTimestamp; // Timestamp of the gossip message
    private String gossipMetadata;  // Metadata about the gossip (partition/topic state, etc.)

    // Regular message constructor
    public Message(String topic, int partition, byte[] payload) {
        this.messageId = UUID.randomUUID();
        this.payload = payload;
        this.timestamp = Instant.now();
        this.topic = topic;
        this.partition = partition;
    }

    // Gossip message constructor
    public Message(String senderId, String gossipMetadata, Instant gossipTimestamp) {
        this.messageId = UUID.randomUUID();  // For gossip, this is just an identifier for the gossip message
        this.payload = null; // No payload for gossip messages
        this.timestamp = Instant.now(); // The time this gossip message was created
        this.topic = null; // Not used for gossip
        this.partition = -1; // Not used for gossip
        this.senderId = senderId;
        this.gossipTimestamp = gossipTimestamp;
        this.gossipMetadata = gossipMetadata;
    }

    public Message(byte[] payload, Instant timestamp) {
        this.messageId = UUID.randomUUID(); // Unique message ID for every message
        this.payload = payload;
        this.timestamp = timestamp != null ? timestamp : Instant.now(); // If no timestamp provided, use the current time
        this.topic = null;  // Set topic to null (can be set later if needed)
        this.partition = -1; // Set partition to -1 (can be set later if needed)
        this.senderId = null;  // For non-gossip messages, senderId is null
        this.gossipTimestamp = null;  // For non-gossip messages, gossipTimestamp is null
        this.gossipMetadata = null;  // For non-gossip messages, gossipMetadata is null
    }

    // Getters for regular message fields
    public UUID getMessageId() {
        return messageId;  // Ensure messageId is a UUID in your Message class
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

    // Getters for gossip-related fields
    public String getSenderId() {
        return senderId;
    }

    public Instant getGossipTimestamp() {
        return gossipTimestamp;
    }

    public String getGossipMetadata() {
        return gossipMetadata;
    }

    // Method to serialize the Message object to JSON using Gson
    public String serializeMessage() {
        Gson gson = new Gson();
        return gson.toJson(this);  // Serialize the current object into JSON format
    }

    // Method to convert the message to a String representation (for printing)
    @Override
    public String toString() {
        if (gossipMetadata != null) {
            return "GossipMessage{" +
                    "senderId='" + senderId + '\'' +
                    ", gossipMetadata='" + gossipMetadata + '\'' +
                    ", gossipTimestamp=" + gossipTimestamp +
                    ", timestamp=" + timestamp + '}';
        } else {
            return "Message{" +
                    "messageId=" + messageId +
                    ", topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", timestamp=" + timestamp + '}';
        }
    }

    // Method to create a Message object from JSON using Gson
    public static Message fromJson(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, Message.class);  // Deserialize the JSON string back to a Message object
    }
}
