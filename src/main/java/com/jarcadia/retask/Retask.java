package com.jarcadia.retask;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.rcommando.RedisValue;

public class Retask {

    private final String name;
    private Long scheduledTimestamp;
    private String recurKey;
    private String authorityKey;
    private Long recurInterval;
    private boolean triggerManually;
    private final Map<String, String> metadata;
    private final Map<String, Object> params;

    public static Retask create(String routingKey) {
        return new Retask(UUID.randomUUID().toString(), routingKey);
    }
    
    private Retask(String name, String routingKey) {
        this.name = name;
        this.metadata = new HashMap<>();
        if (routingKey != null) {
            metadata.put("routingKey", routingKey);
        }
        this.params = new HashMap<>();
    }

    public Retask in(long duration, TimeUnit unit) {
        return at(System.currentTimeMillis() + unit.toMillis(duration));
    }
    
    public Retask at(long timestamp) {
        this.scheduledTimestamp = timestamp;
        metadata.put("targetTimestamp", String.valueOf(scheduledTimestamp));
        return this;
    }

    public Retask triggerManually() {
        this.triggerManually = true;
        return this;
    }

    public Retask recurEvery(String recurKey, long interval, TimeUnit unit) {
        this.recurKey = recurKey;
        this.recurInterval = unit.toMillis(interval);
        this.authorityKey = UUID.randomUUID().toString();
        metadata.put("recurKey", recurKey);
        metadata.put("authorityKey", authorityKey);
        metadata.put("recurInterval", String.valueOf(recurInterval));
        return this;
    }

    public Retask permit(String permitKey) {
        metadata.put("permitKey", permitKey);
        return this;
    }
    
    public Retask param(String key, Object value) {
        params.put(key, value);
        return this;
    }

    public Retask objParam(String mapKey, String id) {
        params.put("objectMapKey", mapKey);
        params.put("objectId", id);
        return this;
    }

    public Retask objParam(RedisObject object) {
        params.put("objectMapKey", object.getMapKey());
        params.put("objectId", object.getId());
        return this;
    }

    protected Retask forInsertedObject(String id) {
        this.params.put("objectId", id);
        return this;
    }
    protected Retask forDeletedObject(String id) {
        this.params.put("objectId", id);
        return this;
    }

    protected Retask forChangedValue(String id, RedisValue before, RedisValue after) {
        this.params.put("objectId", id);
        this.metadata.put("before", before.getRawValue());
        this.metadata.put("after", after.getRawValue());
        return this;
    }

    protected String getName() {
        return this.name;
    } 

    protected boolean isScheduled() {
        return scheduledTimestamp != null;
    }

    protected long getScheduledTimestamp() {
        return scheduledTimestamp;
    }

    protected boolean isRecurring() {
        return recurInterval != null;
    }

    protected String getRecurKey() {
        return recurKey;
    }

    protected String getAuthorityKey() {
        return authorityKey;
    }

    protected long getRecurInterval() {
        return recurInterval;
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

    protected boolean isTriggeredManually() {
        return this.triggerManually;
    }

}