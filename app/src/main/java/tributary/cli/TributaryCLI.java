package tributary.cli;

import tributary.core.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class TributaryCLI {
    private Tributary tributary;
    private Map<String, Producer<?>> producers;
    private Map<String, Consumer> consumers;
    private Map<String, ConsumerGroup<?>> consumerGroups;

    public TributaryCLI() {
        this.tributary = new Tributary();
        this.producers = new HashMap<>();
        this.consumers = new HashMap<>();
        this.consumerGroups = new HashMap<>();
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String command = scanner.nextLine();
            String[] parts = command.split(" ");
            try {
                switch (parts[0]) {
                    case "create":
                        handleCreate(parts);
                        break;
                    case "produce":
                        handleProduce(parts);
                        break;
                    case "consume":
                        handleConsume(parts);
                        break;
                    case "show":
                        handleShow(parts);
                        break;
                    case "set":
                        handleSet(parts);
                        break;
                    case "playback":
                        handlePlayback(parts);
                        break;
                    case "exit":
                        return;
                    default:
                        System.out.println("Unknown command");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void handleCreate(String[] parts) {
        switch (parts[1]) {
            case "topic":
                if (parts[3].equals("String")) {
                    tributary.createTopic(parts[2], String.class);
                } else if (parts[3].equals("Integer")) {
                    tributary.createTopic(parts[2], Integer.class);
                } else {
                    System.out.println("Unsupported type");
                }
                System.out.println("Created topic " + parts[2] + " with type " + parts[3]);
                break;
            case "partition":
                Topic<?> topic = tributary.getTopic(parts[2]);
                if (topic != null) {
                    topic.createPartition(parts[3]);
                    System.out.println("Created partition " + parts[3] + " in topic " + parts[2]);
                } else {
                    System.out.println("Topic not found");
                }
                break;
            case "consumer":
                if (parts[2].equals("group")) {
                    Topic<?> topicForGroup = tributary.getTopic(parts[4]);
                    if (topicForGroup != null) {
                        ConsumerGroup<?> group = new ConsumerGroup<>(parts[3], topicForGroup, parts[5]);
                        consumerGroups.put(parts[3], group);
                        System.out.println("Created consumer group " + parts[3] + " for topic " + parts[4]
                                + " with rebalancing strategy " + parts[5]);
                    } else {
                        System.out.println("Topic not found for consumer group");
                    }
                } else {
                    ConsumerGroup<?> group = consumerGroups.get(parts[2]);
                    if (group != null) {
                        Consumer consumer = new Consumer(parts[3], parts[2]);
                        consumers.put(parts[3], consumer);
                        group.addConsumer(consumer);
                        System.out.println("Created consumer " + parts[3] + " in group " + parts[2]);
                    } else {
                        System.out.println("Consumer group not found: " + parts[2]);
                        System.out.println("Available consumer groups: " + consumerGroups.keySet());
                    }
                }
                break;
            case "producer":
                Topic<?> topicForProducer = tributary.getTopic(parts[3]);
                if (topicForProducer != null) {
                    if (parts[4].equals("String")) {
                        Producer<String> producer = new Producer<>(parts[2], String.class, parts[5],
                                (Topic<String>) topicForProducer);
                        producers.put(parts[2], producer);
                    } else if (parts[4].equals("Integer")) {
                        Producer<Integer> producer = new Producer<>(parts[2], Integer.class, parts[5],
                                (Topic<Integer>) topicForProducer);
                        producers.put(parts[2], producer);
                    } else {
                        System.out.println("Unsupported type");
                    }
                    System.out.println("Created producer " + parts[2] + " for topic " + parts[3] + " with type "
                            + parts[4] + " and allocation method " + parts[5]);
                } else {
                    System.out.println("Topic not found for producer");
                }
                break;
            default:
                System.out.println("Unsupported create command");
                break;
        }
    }


    private void handleProduce(String[] parts) {
        Producer<?> producer = producers.get(parts[2]);
        Topic<?> topic = tributary.getTopic(parts[3]);
        if (producer != null && topic != null) {
            String event = parts[4];
            String partitionId = parts.length > 5 ? parts[5] : null;

            if (partitionId == null) {
                System.out.println("Partition ID must be provided.");
                return;
            }

            if (producer.getType() == String.class) {
                ((Producer<String>) producer).produceEvent(event, partitionId);
            } else if (producer.getType() == Integer.class) {
                ((Producer<Integer>) producer).produceEvent(Integer.parseInt(event), partitionId);
            }
            System.out.println("Produced event to topic " + parts[3]);
        } else {
            System.out.println("Producer or topic not found");
        }
    }


    private void handleConsume(String[] parts) {
        Consumer consumer = consumers.get(parts[2]);
        if (consumer == null) {
            System.out.println("Consumer not found");
            return;
        }
        Topic<?> topic = tributary.getTopic(parts[3]);
        if (topic == null) {
            System.out.println("Topic not found: " + parts[3]);
            return;
        }
        Partition<?> partition = topic.getPartition(parts[4]);
        if (partition == null) {
            System.out.println("Partition not found: " + parts[4]);
            return;
        }
        consumer.consumeEvent(partition);
        System.out.println("Consumed event from partition " + parts[4] + " in topic " + parts[3]);
    }


    private void handleShow(String[] parts) {
        switch (parts[1]) {
            case "topic":
                Topic<?> topic = tributary.getTopic(parts[2]);
                if (topic != null) {
                    System.out.println("Topic: " + parts[2]);
                    for (Partition<?> partition : topic.getPartitions()) {
                        System.out.println("  Partition: " + partition.getId());
                        for (Message<?> message : partition.getMessages()) {
                            System.out.println("    Message: " + message.getPayload());
                        }
                    }
                } else {
                    System.out.println("Topic not found");
                }
                break;
            case "consumer":
                if ("group".equals(parts[2])) {
                    ConsumerGroup<?> group = consumerGroups.get(parts[3]);
                    if (group != null) {
                        System.out.println("Consumer Group: " + parts[3]);
                        for (Consumer consumer : group.getConsumers()) {
                            System.out.println("  Consumer: " + consumer.getId());
                            for (Partition<?> partition : consumer.getPartitions()) {
                                System.out.println("    Partition: " + partition.getId());
                            }
                        }
                    } else {
                        System.out.println("Consumer group not found: " + parts[3]);
                        System.out.println("Available consumer groups: " + consumerGroups.keySet());
                    }
                } else {
                    System.out.println("Invalid command for show");
                }
                break;
            default:
                System.out.println("Invalid command for show");
        }
    }


    private void handleSet(String[] parts) {
        if ("consumer".equals(parts[1]) && "group".equals(parts[2]) && "rebalancing".equals(parts[3])) {
            ConsumerGroup<?> group = consumerGroups.get(parts[4]);
            if (group != null) {
                group.setRebalancingStrategy(parts[5]);
                System.out.println("Set rebalancing strategy for group " + parts[4] + " to " + parts[5]);
            } else {
                System.out.println("Consumer group not found: " + parts[4]);
                System.out.println("Available consumer groups: " + consumerGroups.keySet());
            }
        } else {
            System.out.println("Invalid command for set");
        }
    }


    private void handlePlayback(String[] parts) {
        Consumer consumer = consumers.get(parts[1]);
        Topic<?> topic = tributary.getTopic(parts[2]);
        if (consumer != null && topic != null) {
            Partition<?> partition = topic.getPartition(parts[3]);
            if (partition != null) {
                consumer.playback(partition, Integer.parseInt(parts[4]));
                System.out.println("Playback events from partition " + parts[3] + " in topic " + parts[2]);
            } else {
                System.out.println("Partition not found: " + parts[3]);
            }
        } else {
            System.out.println("Consumer or topic not found");
            System.out.println("Available consumers: " + consumers.keySet());
            System.out.println("Available topics: " + tributary.getTopics().keySet());
        }
    }


    public static void main(String[] args) {
        new TributaryCLI().start();
    }
}
