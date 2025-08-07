package tributary.core;

public class Producer<T> {
    private String id;
    private Class<T> type;
    private String allocationMethod;
    private Topic<T> topic;

    public Producer(String id, Class<T> type, String allocationMethod, Topic<T> topic) {
        this.id = id;
        this.type = type;
        this.allocationMethod = allocationMethod;
        this.topic = topic;
    }

    public String getId() {
        return id;
    }

    public Class<T> getType() {
        return type;
    }

    public String getAllocationMethod() {
        return allocationMethod;
    }

    public void produceEvent(T event, String partitionId) {
        Partition<T> partition = topic.getPartition(partitionId);
        if (partition != null) {
            partition.addMessage(new Message<>(event));
        } else {
            System.out.println("Partition not found: " + partitionId);
        }
    }
}
