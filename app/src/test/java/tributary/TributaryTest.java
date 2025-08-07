package tributary;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tributary.core.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TributaryTest {
    private Tributary tributary;
    private Map<String, Consumer> consumers;
    private Map<String, ConsumerGroup<?>> consumerGroups;

    @BeforeEach
    public void setUp() {
        tributary = new Tributary();
        consumers = new HashMap<>();
        consumerGroups = new HashMap<>();
    }

    @Test
    public void testCreateTopic() {
        tributary.createTopic("user_profiles", String.class);
        Topic<?> topic = tributary.getTopic("user_profiles");
        assertNotNull(topic);
        assertEquals(String.class, topic.getType());
    }

    @Test
    public void testCreatePartition() {
        tributary.createTopic("user_profiles", String.class);
        Topic<?> topic = tributary.getTopic("user_profiles");
        topic.createPartition("partition_1");
        Partition<?> partition = topic.getPartition("partition_1");
        assertNotNull(partition);
        assertEquals("partition_1", partition.getId());
    }

    @Test
    public void testCreateConsumerGroup() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        assertNotNull(group);
        assertEquals("group_A", group.getId());
        assertEquals("Range", group.getRebalancingStrategy());
    }

    @Test
    public void testCreateProducer() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        Producer<String> producer = new Producer<>("producer_A", String.class, "Manual", topic);
        assertNotNull(producer);
        assertEquals("producer_A", producer.getId());
        assertEquals(String.class, producer.getType());
        assertEquals("Manual", producer.getAllocationMethod());
    }

    @Test
    public void testProduceEvent() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        topic.createPartition("partition_1");
        Producer<String> producer = new Producer<>("producer_A", String.class, "Manual", topic);
        producer.produceEvent("{\"id\":1,\"name\":\"Alice\"}", "partition_1");
        Partition<String> partition = (Partition<String>) topic.getPartition("partition_1");
        assertEquals(1, partition.getMessages().size());
        assertEquals("{\"id\":1,\"name\":\"Alice\"}", partition.getMessages().get(0).getPayload());
    }

    @Test
    public void testConsumeEvent() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        topic.createPartition("partition_1");
        Producer<String> producer = new Producer<>("producer_A", String.class, "Manual", topic);
        producer.produceEvent("{\"id\":1,\"name\":\"Alice\"}", "partition_1");

        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        Consumer consumer = new Consumer("consumer_1", "group_A");
        group.addConsumer(consumer);
        consumers.put("consumer_1", consumer);

        Partition<String> partition = (Partition<String>) topic.getPartition("partition_1");
        consumer.consumeEvent(partition);
        assertEquals(0, partition.getMessages().size()); // Assumes messages are removed after consumption
    }

    @Test
    public void testShowTopic() {
        tributary.createTopic("user_profiles", String.class);
        Topic<?> topic = tributary.getTopic("user_profiles");
        assertNotNull(topic);
    }

    @Test
    public void testShowConsumerGroup() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        consumerGroups.put("group_A", group);
        ConsumerGroup<?> retrievedGroup = consumerGroups.get("group_A");
        assertNotNull(retrievedGroup);
    }

    @Test
    public void testSetRebalancingStrategy() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        group.setRebalancingStrategy("RoundRobin");
        assertEquals("RoundRobin", group.getRebalancingStrategy());
    }

    @Test
    public void testPlaybackMessages() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        topic.createPartition("partition_1");
        Producer<String> producer = new Producer<>("producer_A", String.class, "Manual", topic);
        producer.produceEvent("{\"id\":1,\"name\":\"Alice\"}", "partition_1");

        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        Consumer consumer = new Consumer("consumer_1", "group_A");
        group.addConsumer(consumer);
        consumers.put("consumer_1", consumer);

        Partition<String> partition = (Partition<String>) topic.getPartition("partition_1");
        consumer.playback(partition, 0);
        assertEquals(1, partition.getMessages().size());
    }

    @Test
    public void testCreateDuplicateTopic() {
        tributary.createTopic("user_profiles", String.class);
        assertThrows(IllegalArgumentException.class, () -> {
            tributary.createTopic("user_profiles", String.class);
        });
    }

    @Test
    public void testCreatePartitionInNonExistentTopic() {
        assertThrows(IllegalArgumentException.class, () -> {
            Topic<?> topic = tributary.getTopic("non_existent_topic");
            topic.createPartition("partition_1");
        });
    }

    @Test
    public void testProduceEventToNonExistentPartition() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        Producer<String> producer = new Producer<>("producer_A", String.class, "Manual", topic);
        producer.produceEvent("{\"id\":1,\"name\":\"Alice\"}", "non_existent_partition");
        Partition<String> partition = (Partition<String>) topic.getPartition("non_existent_partition");
        assertNull(partition); // Non-existent partition should be null
    }

    @Test
    public void testConsumeEventFromEmptyPartition() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        topic.createPartition("partition_1");

        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        Consumer consumer = new Consumer("consumer_1", "group_A");
        group.addConsumer(consumer);
        consumers.put("consumer_1", consumer);

        Partition<String> partition = (Partition<String>) topic.getPartition("partition_1");
        consumer.consumeEvent(partition);
        assertEquals(0, partition.getMessages().size()); // Should still be 0 as no messages were added
    }

    @Test
    public void testShowEmptyTopic() {
        tributary.createTopic("user_profiles", String.class);
        Topic<?> topic = tributary.getTopic("user_profiles");
        assertNotNull(topic);
        assertTrue(topic.getPartitions().isEmpty()); // Topic should have no partitions initially
    }

    @Test
    public void testCreateTopicWithDifferentType() {
        tributary.createTopic("orders", Integer.class);
        Topic<?> topic = tributary.getTopic("orders");
        assertNotNull(topic);
        assertEquals(Integer.class, topic.getType());
    }

    @Test
    public void testPlaybackMessagesWithNonExistentOffset() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        topic.createPartition("partition_1");
        Producer<String> producer = new Producer<>("producer_A", String.class, "Manual", topic);
        producer.produceEvent("{\"id\":1,\"name\":\"Alice\"}", "partition_1");

        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        Consumer consumer = new Consumer("consumer_1", "group_A");
        group.addConsumer(consumer);
        consumers.put("consumer_1", consumer);

        Partition<String> partition = (Partition<String>) topic.getPartition("partition_1");
        consumer.playback(partition, 100); // Non-existent offset
        assertEquals(1, partition.getMessages().size());
    }

    @Test
    public void testSetInvalidRebalancingStrategy() {
        tributary.createTopic("user_profiles", String.class);
        Topic<String> topic = (Topic<String>) tributary.getTopic("user_profiles");
        ConsumerGroup<String> group = new ConsumerGroup<>("group_A", topic, "Range");
        assertThrows(IllegalArgumentException.class, () -> {
            group.setRebalancingStrategy("InvalidStrategy");
        });
    }
}
