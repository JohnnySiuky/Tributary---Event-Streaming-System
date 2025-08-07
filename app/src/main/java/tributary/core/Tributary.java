package tributary.core;

import java.util.HashMap;
import java.util.Map;

public class Tributary {
    private Map<String, Topic<?>> topics;

    public Tributary() {
        topics = new HashMap<>();
    }

    public <T> void createTopic(String id, Class<T> type) {
        if (topics.containsKey(id)) {
            throw new IllegalArgumentException("Topic already exists: " + id);
        }
        topics.put(id, new Topic<>(type));
    }

    public Topic<?> getTopic(String id) {
        Topic<?> topic = topics.get(id);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + id);
        }
        return topic;
    }

    public Map<String, Topic<?>> getTopics() {
        return topics;
    }
}
