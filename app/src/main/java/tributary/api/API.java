package tributary.api;

import tributary.core.*;

public class API {

    // Method to create a new Tributary instance
    public static Tributary createTributary() {
        return new Tributary();
    }

    // Method to create a new Topic in a given Tributary
    public static <T> void createTopic(Tributary tributary, String topicId, Class<T> type) {
        tributary.createTopic(topicId, type);
    }

    // Method to create a new Partition in a given Topic
    public static <T> void createPartition(Topic<T> topic, String partitionId) {
        topic.createPartition(partitionId);
    }

    // Method to create a new Producer
    public static <T> Producer<T> createProducer(String producerId, Class<T> type, String allocation, Topic<T> topic) {
        return new Producer<>(producerId, type, allocation, topic);
    }

    // Method to create a new Consumer
    public static Consumer createConsumer(String consumerId, String groupId) {
        return new Consumer(consumerId, groupId);
    }

    // Method to create a new Consumer Group
    public static <T> ConsumerGroup<T> createConsumerGroup(String groupId, Topic<T> topic, String rebalancingStrategy) {
        return new ConsumerGroup<>(groupId, topic, rebalancingStrategy);
    }
}
