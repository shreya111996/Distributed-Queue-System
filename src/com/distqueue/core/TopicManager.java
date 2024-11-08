package com.distqueue.core;

import java.util.HashMap;
import java.util.Map;

public class TopicManager {

    private final Map<String, Topic> topics = new HashMap<>();

    // Method to create a new topic with given number of partitions and replicas
    public void createTopic(String topicName, int numPartitions, int replicationFactor) {
        Topic topic = new Topic(topicName, numPartitions, replicationFactor);
        topics.put(topicName, topic);
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }


}
