package dev.jarcadia;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

class QueuePopperRepository implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(QueuePopperRepository.class);

    private final RedisConnection redisConnection;
    private final Consumer<String> consumer;
    private final TypeReference<Map<String, String>> mapTypeReference;

    protected QueuePopperRepository(RedisClient redisClient, ObjectMapper objectMapper, String consumerName) {
        this.redisConnection = new RedisConnection(redisClient, objectMapper);
        this.consumer = Consumer.from(Keys.CONSUMER_GROUP, consumerName);
        this.mapTypeReference = new TypeReference<>() {};
    }

   protected void initialize() {
        try {
            redisConnection.commands().xgroupCreate(XReadArgs.StreamOffset.latest(Keys.QUEUE), Keys.CONSUMER_GROUP,
                    XGroupCreateArgs.Builder.mkstream());
        } catch (RedisBusyException ex) {
            if (!"BUSYGROUP Consumer Group name already exists".equals(ex.getMessage())) {
                throw ex;
            }
        }
    }

    protected List<StreamMessage<String, String>> popItems() {
        return redisConnection.commands().xreadgroup(consumer, XReadArgs.Builder.block(100),
                XReadArgs.StreamOffset.lastConsumed(Keys.QUEUE));
    }

    @Override
    public void close() {
        this.redisConnection.close();
    }
}
