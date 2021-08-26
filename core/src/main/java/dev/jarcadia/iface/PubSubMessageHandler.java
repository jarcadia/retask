package dev.jarcadia.iface;

@FunctionalInterface
public interface PubSubMessageHandler {
    void handle(String message) throws Exception;
}
