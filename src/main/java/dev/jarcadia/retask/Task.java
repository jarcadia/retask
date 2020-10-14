package dev.jarcadia.retask;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dev.jarcadia.redao.Dao;
import dev.jarcadia.redao.DaoValue;

public class Task {

    private final String id;
    private Long targetTimestamp;
    private String recurKey;
    private String authorityKey;
    private Long recurInterval;
    private final Map<String, String> metadata;
    private final Map<String, Object> params;
    
    public static Task create(String routingKey) {
        return new Task(UUID.randomUUID().toString(), routingKey);
    }

    private Task(String id, String routingKey) {
        this.id = id;
        this.metadata = new HashMap<>();
        if (routingKey != null) {
            metadata.put("routingKey", routingKey);
        }
        this.params = new HashMap<>();
    }

    public Task in(long duration, TimeUnit unit) {
        return at(System.currentTimeMillis() + unit.toMillis(duration));
    }

    public Task at(long timestamp) {
        this.targetTimestamp = timestamp;
        metadata.put("targetTimestamp", String.valueOf(targetTimestamp));
        return this;
    }

    public Task recurEvery(String recurKey, long interval, TimeUnit unit) {
        this.recurKey = recurKey;
        this.recurInterval = unit.toMillis(interval);
        this.authorityKey = UUID.randomUUID().toString();
        metadata.put("recurKey", this.recurKey);
        metadata.put("recurInterval", String.valueOf(recurInterval));
        metadata.put("authorityKey", this.authorityKey);
        return this;
    }

    public Task requirePermit(String permitKey) {
        metadata.put("permitKey", permitKey);
        return this;
    }
    
    public Task param(String key, Object value) {
        params.put(key, value);
        return this;
    }
    
    public void dropIn(TaskBucket bucket) {
    	bucket.add(this);
    }

    protected Task shouldPublishResponse() {
        this.metadata.put("publishResponse", String.valueOf(true));
        return this;
    }

    protected Task forChangedValue(Dao dao, String field, DaoValue before, DaoValue after) {
    	this.params.put("object", dao);
    	this.params.put("field", field);
        this.metadata.put("before", before.getRawValue());
        this.metadata.put("after", after.getRawValue());
        return this;
    }

    protected String getId() {
        return this.id;
    } 

    protected Map<String, String> getMetadata() {
        return this.metadata;
    }

    protected boolean hasParams() {
        return !this.params.isEmpty();
    }

    protected Map<String, Object> getParams() {
        return this.params;
    }
}