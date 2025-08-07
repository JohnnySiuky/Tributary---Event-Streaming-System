package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Topic<T> {
    private List<Partition<T>> partitions;
    private Class<T> type;

    public Topic(Class<T> type) {
        this.partitions = new ArrayList<>();
        this.type = type;
    }

    public void createPartition(String id) {
        partitions.add(new Partition<>(id));
    }

    public Partition<T> getPartition(String id) {
        for (Partition<T> partition : partitions) {
            if (partition.getId().equals(id)) {
                return partition;
            }
        }
        return null;
    }

    public List<Partition<T>> getPartitions() {
        return partitions;
    }

    public Class<T> getType() {
        return type;
    }
}
