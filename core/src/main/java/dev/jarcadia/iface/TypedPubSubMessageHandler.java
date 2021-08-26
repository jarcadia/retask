package dev.jarcadia.iface;

@FunctionalInterface
public interface TypedPubSubMessageHandler<T> {
    void handle(T message) throws Exception;
}
