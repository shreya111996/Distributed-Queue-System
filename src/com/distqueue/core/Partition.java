package com.distqueue.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Partition {
    private final int partitionId;
    private final int replicationFactor;
    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger offset = new AtomicInteger(0);
    private final List<Partition> replicas = new ArrayList<>(); // Follower replicas

    public Partition(int partitionId, int replicationFactor) {
        this.partitionId = partitionId;
        this.replicationFactor = replicationFactor;
        initializeReplicas();
    }

    private void initializeReplicas() {
        // Initialize follower replicas based on the replication factor
        for (int i = 1; i < replicationFactor; i++) {
            replicas.add(new Partition(partitionId, 1)); // Simplified setup for followers
        }
    }

    public void addMessage(Message message) {
        // Leader writes message to its own queue and increments offset
        messageQueue.add(message);
        offset.incrementAndGet();
        replicateToFollowers(message);
    }

    private void replicateToFollowers(Message message) {
        // Replicate message to each follower replica
        for (Partition replica : replicas) {
            replica.messageQueue.add(message); // In production, this would be asynchronous
        }
    }

    public List<Message> getMessages(int startOffset) {
        // Retrieve messages from the queue starting from a specific offset
        return new ArrayList<>(messageQueue).subList(startOffset, messageQueue.size());
    }

    public int getCurrentOffset() {
        return offset.get();
    }

    public List<Partition> getReplicas() {
        return replicas;
    }
}
