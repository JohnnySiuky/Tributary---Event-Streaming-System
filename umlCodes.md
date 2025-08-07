## This is the code for initial UML diagram:
<blockquote>
@startuml
class Tributary {
- Map<String, Topic<?>> topics
    + <T> createTopic(String id, Class<T> type)
    + Topic<?> getTopic(String id)
}

class Topic<T> {
- List<Partition<T>> partitions
- Class<T> type
+ createPartition(String id)
+ Partition<T> getPartition(String id)
}

class Partition<T> {
- String id
- Queue<Message<T>> messages
+ addMessage(Message<T> message)
+ Message<T> getMessage()
}

class Producer<T> {
- String id
- Class<T> type
- String allocation
- Topic<T> topic
+ produceEvent(T event, String partitionId)
}

class Consumer {
- String id
- String group
+ consumeEvent(Partition<?> partition)
}

class Message<T> {
- T payload
+ T getPayload()
}

Tributary "1" --> "*" Topic
Topic "1" --> "*" Partition
Partition "1" --> "*" Message
Producer --> Topic
Consumer --> Partition
@enduml
</blockquote>

## This is the code for final UML diagram:
<blockquote>
@startuml
class Tributary {
- topics: Map<String, Topic<?>>
    + createTopic(id: String, type: Class<T>): void
    + getTopic(id: String): Topic<?>
+ getTopics(): Map<String, Topic<?>>
}

class Topic<T> {
- partitions: List<Partition<T>>
- type: Class<T>
+ createPartition(id: String): void
+ getPartition(id: String): Partition<T>
+ getPartitions(): List<Partition<T>>
+ getType(): Class<T>
}

class Partition<T> {
- id: String
- messages: Queue<Message<T>>
+ getId(): String
+ addMessage(message: Message<T>): void
+ getMessage(): Message<T>
+ getMessages(): List<Message<T>>
}

class Message<T> {
- payload: T
+ getPayload(): T
}

class Consumer {
- id: String
- group: String
- partitions: List<Partition<?>>
    + getId(): String
    + getGroup(): String
    + consumeEvent(partition: Partition<?>): void
+ playback(partition: Partition<?>, offset: int): void
    + getPartitions(): List<Partition<?>>
+ addPartition(partition: Partition<?>): void
}

class ConsumerGroup<T> {
- id: String
- topic: Topic<T>
- rebalancingStrategy: String
- consumers: List<Consumer>
+ getId(): String
+ getRebalancingStrategy(): String
+ setRebalancingStrategy(rebalancingStrategy: String): void
+ addConsumer(consumer: Consumer): void
+ getConsumers(): List<Consumer>
}

class Producer<T> {
- id: String
- type: Class<T>
- allocationMethod: String
- topic: Topic<T>
+ getId(): String
+ getType(): Class<T>
+ getAllocationMethod(): String
+ produceEvent(event: T, partitionId: String): void
}

class TributaryCLI {
- tributary: Tributary
- producers: Map<String, Producer<?>>
    - consumers: Map<String, Consumer>
    - consumerGroups: Map<String, ConsumerGroup<?>>
+ start(): void
+ handleCreate(parts: String[]): void
+ handleProduce(parts: String[]): void
+ handleConsume(parts: String[]): void
+ handleShow(parts: String[]): void
+ handleSet(parts: String[]): void
+ handlePlayback(parts: String[]): void
+ main(args: String[]): void
}

Tributary "1" *-- "many" Topic : aggregates
Topic "1" *-- "many" Partition : contains
Partition "1" *-- "many" Message : contains
ConsumerGroup "1" *-- "many" Consumer : contains
ConsumerGroup "1" *-- "1" Topic : has
Consumer "1" *-- "many" Partition : has
Producer "1" *-- "1" Topic : associated with
TributaryCLI "1" *-- "1" Tributary : interacts with
TributaryCLI "1" *-- "many" Producer : interacts with
TributaryCLI "1" *-- "many" Consumer : interacts with
TributaryCLI "1" *-- "many" ConsumerGroup : interacts with
@enduml
</blockquote>