package tributary.core;

public class Message<T> {
    private T payload;

    public Message(T payload) {
        this.payload = payload;
    }

    public T getPayload() {
        return payload;
    }
}
