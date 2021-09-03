package dev.jarcadia;

import dev.jarcadia.redis.RedisConnection;
import dev.jarcadia.redis.RedisFactory;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

class QueuePopperRepository implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(QueuePopperRepository.class);

    private final RedisConnection redisConnection;
    private final Consumer<String> consumer;

    protected QueuePopperRepository(RedisFactory redisFactory, String consumerName) {
        this.redisConnection = redisFactory.openConnection();
        this.consumer = Consumer.from(Keys.CONSUMER_GROUP, consumerName);
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
