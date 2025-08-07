package tributary.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Partition<T> {
    private String id;
    private Queue<Message<T>> messages;

    public Partition(String id) {
        this.id = id;
        this.messages = new LinkedList<>();
    }

    public String getId() {
        return id;
    }

    public void addMessage(Message<T> message) {
        messages.add(message);
    }

    public Message<T> getMessage() {
        return messages.poll();
    }

    public List<Message<T>> getMessages() {
        return new LinkedList<>(messages);
    }
}
