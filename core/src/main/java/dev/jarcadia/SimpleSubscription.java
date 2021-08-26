package dev.jarcadia;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.PubSubException;
import dev.jarcadia.iface.PubSubMessageHandler;
import dev.jarcadia.iface.TypedPubSubMessageHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class SimpleSubscription implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(SimpleSubscription.class);

    private final StatefulRedisPubSubConnection<String, String> pubsubConnection;

    protected static SimpleSubscription create(RedisClient client, String channel, PubSubMessageHandler handler) {
        return new SimpleSubscription(client, channel, new PassThroughAdapter(handler));
    }

    protected static <T> SimpleSubscription create(RedisClient client, ObjectMapper objectMapper, String channel,
            Class<T> type, TypedPubSubMessageHandler<T> handler) {
        return new SimpleSubscription(client, channel, new DeserializeToClassAdapter<T>(objectMapper, type, handler));
    }

    protected static <T> SimpleSubscription create(RedisClient client, ObjectMapper objectMapper, String channel,
            TypeReference<T> typeRef, TypedPubSubMessageHandler<T> handler) {
        return new SimpleSubscription(client, channel, new DeserializeToTypeRefAdapter<T>(objectMapper, typeRef, handler));
    }

    private SimpleSubscription(RedisClient client, String channel, RedisPubSubListener listener) {
        this.pubsubConnection = client.connectPubSub();
        RedisPubSubCommands commands = pubsubConnection.sync();
        this.pubsubConnection.addListener(listener);
        commands.subscribe(channel);
    }

    @Override
    public void close() {
        this.pubsubConnection.close();
    }

    private static class PassThroughAdapter extends RedisPubSubAdapter<String, String> {

        private final PubSubMessageHandler handler;

        protected PassThroughAdapter(PubSubMessageHandler handler) {
            this.handler = handler;
        }

        @Override
        public void message(String channel, String message) {
            try {
                handler.handle(message);
            } catch (Exception e) {
                throw new PubSubException(e);
            }
        }
    }

    private static class DeserializeToClassAdapter<T> extends RedisPubSubAdapter<String, String> {

        private final ObjectMapper objectMapper;
        private final Class<T> type;
        private final TypedPubSubMessageHandler<T> handler;

    	protected DeserializeToClassAdapter(ObjectMapper objectMapper, Class<T> type,
                TypedPubSubMessageHandler<T> handler) {
            this.objectMapper = objectMapper;
            this.type = type;
            this.handler = handler;
        }

        @Override
        public void message(String channel, String message) {
            try {
                handler.handle(objectMapper.readValue(message, type));
            } catch (Exception ex) {
                throw new PubSubException(ex);
            }
        }
    }

    private static class DeserializeToTypeRefAdapter<T> extends RedisPubSubAdapter<String, String> {

        private final ObjectMapper objectMapper;
        private final TypeReference<T> type;
        private final TypedPubSubMessageHandler<T> handler;

        protected DeserializeToTypeRefAdapter(ObjectMapper objectMapper, TypeReference<T> type,
                TypedPubSubMessageHandler<T> handler) {
            this.objectMapper = objectMapper;
            this.type = type;
            this.handler = handler;
        }

        @Override
        public void message(String channel, String message) {
            try {
                handler.handle(objectMapper.readValue(message, type));
            } catch (Exception e) {
                throw new PubSubException(e);
            }
        }
    }
}
