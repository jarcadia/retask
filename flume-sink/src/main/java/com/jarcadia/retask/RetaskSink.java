package com.jarcadia.retask;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetaskSink extends AbstractSink implements Configurable {

    private final Logger logger = LoggerFactory.getLogger(RetaskSink.class);

    private String host;
    private String port;
    private String database;
    private String list;
    private String route;
    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> commands;

    public void configure(Context context) {
        this.host = context.getString("host", "localhost");
        this.port = context.getString("port", "6379");
        this.database = context.getString("database", "0");
        this.list = context.getString("list", "tasks");
        this.route = context.getString("route", "flume");
    }

    public void start() {
        this.client = RedisClient.create(
                String.format("redis://%s:%s/%s", new Object[] { this.host, this.port, this.database }));
        this.connection = this.client.connect();
        this.commands = this.connection.sync();
    }

    public void stop() {
        this.connection.close();
        this.connection = null;
        this.commands = null;
    }

    public Sink.Status process() throws EventDeliveryException {
        Sink.Status status = null;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            if (event != null) {
                String id = UUID.randomUUID().toString();
                Map<String, String> data = new HashMap<>();
                data.put("routingKey", this.route);
                data.put("params", new String(event.getBody(), StandardCharsets.UTF_8));
                this.commands.multi();
                this.commands.hmset(id, data);
                this.commands.rpush(this.list, new String[] { id });
                this.commands.exec();
            }
            txn.commit();
            status = Sink.Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            this.logger.error("Failed to take event from channel", t);
            status = Sink.Status.BACKOFF;
            if (t instanceof Error) throw (Error) t;
        } finally {
            txn.close();
        }
        return status;
    }
}
