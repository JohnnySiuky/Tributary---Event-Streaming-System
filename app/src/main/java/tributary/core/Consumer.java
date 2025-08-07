package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Consumer {
    private String id;
    private String group;
    private List<Partition<?>> partitions;

    public Consumer(String id, String group) {
        this.id = id;
        this.group = group;
        this.partitions = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public String getGroup() {
        return group;
    }

    public void consumeEvent(Partition<?> partition) {
        Message<?> message = partition.getMessage();
        if (message != null) {
            System.out.println("Consumed message: " + message.getPayload());
        }
    }

    public void playback(Partition<?> partition, int offset) {
        List<Message<?>> messages = new ArrayList<>(partition.getMessages());
        for (int i = offset; i < messages.size(); i++) {
            System.out.println("Replayed message: " + messages.get(i).getPayload());
        }
    }

    public List<Partition<?>> getPartitions() {
        return partitions;
    }

    public void addPartition(Partition<?> partition) {
        this.partitions.add(partition);
    }
}
