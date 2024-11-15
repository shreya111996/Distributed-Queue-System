package com.distqueue.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Partition implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int partitionId;
    private final int replicationFactor;
    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger offset = new AtomicInteger(0);
    private boolean isLeader;

    public Partition(int partitionId, int replicationFactor) {
        this.partitionId = partitionId;
        this.replicationFactor = replicationFactor;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void addMessage(Message message) {
        messageQueue.add(message);
        offset.incrementAndGet();
        // No replication here; handled externally
    }

    public List<Message> getMessages(int startOffset) {
        List<Message> messages = new ArrayList<>(messageQueue);
        if (startOffset >= messages.size()) {
            return new ArrayList<>();
        }
        return messages.subList(startOffset, messages.size());
    }

    public int getCurrentOffset() {
        return offset.get();
    }
}
