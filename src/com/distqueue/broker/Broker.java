package com.distqueue.broker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.distqueue.controller.Controller;
import com.distqueue.core.Message;
import com.distqueue.core.Partition;
import com.distqueue.core.Topic;


public class Broker {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Controller controller;

    public Broker(Controller controller) {
        this.controller = controller;
    }

    public void createTopic(String topicName, int partitionCount, int replicationFactor) {
        Topic topic = new Topic(topicName, partitionCount, replicationFactor);
        topics.put(topicName, topic);
        controller.addTopicMetadata(topicName, partitionCount, replicationFactor);
    }

    public void publishMessage(Message message) {
        Topic topic = topics.get(message.getTopic());
        Partition partition = topic.getPartition(message.getPartition());
        partition.addMessage(message);

        // Replicate message to followers
        controller.replicateMessageToFollowers(message, this);
    }

    public List<Message> getMessagesForPartition(String topic, int partitionId, int offset) {
        return topics.get(topic).getPartition(partitionId).getMessages(offset);
    }
}
