package com.distqueue.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;


public class Consumer {

    private final Broker broker;
    private final Map<String, Map<Integer, Integer>> offsetMap = new ConcurrentHashMap<>();

    public Consumer(Broker broker, String consumerGroup) {
        this.broker = broker;
    }

    public List<Message> consumeMessages(String topic, int partition) {
        int offset = offsetMap.getOrDefault(topic, new HashMap<>()).getOrDefault(partition, 0);
        List<Message> messages = broker.getMessagesForPartition(topic, partition, offset);

        // Update offset for this consumer group
        offsetMap.computeIfAbsent(topic, k -> new ConcurrentHashMap<>()).put(partition, offset + messages.size());
        return messages;
    }
}
