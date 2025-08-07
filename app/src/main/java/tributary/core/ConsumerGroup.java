package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup<T> {
    private String id;
    private Topic<T> topic;
    private String rebalancingStrategy;
    private List<Consumer> consumers;

    public ConsumerGroup(String id, Topic<T> topic, String rebalancingStrategy) {
        this.id = id;
        this.topic = topic;
        setRebalancingStrategy(rebalancingStrategy);
        this.consumers = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public String getRebalancingStrategy() {
        return rebalancingStrategy;
    }

    public void setRebalancingStrategy(String rebalancingStrategy) {
        if (!rebalancingStrategy.equals("Range") && !rebalancingStrategy.equals("RoundRobin")) {
            throw new IllegalArgumentException("Invalid rebalancing strategy: " + rebalancingStrategy);
        }
        this.rebalancingStrategy = rebalancingStrategy;
    }

    public void addConsumer(Consumer consumer) {
        consumers.add(consumer);
    }

    public List<Consumer> getConsumers() {
        return consumers;
    }
}
