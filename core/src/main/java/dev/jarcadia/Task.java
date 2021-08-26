package dev.jarcadia;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Task {

    private final String route;

    protected Task(String route) {
        this.route = route;
    }

    public String getRoute() {
        return route;
    }

    public static Builder create(String route) {
        return new Task.Builder(route);
    }

    public static class Builder {

        private final String route;
        private Long targetTimestamp;
        private String recurKey;
        private Long recurInterval;
        private String permitKey;
        private String respondTo;
        private final Map<String, Object> fields;


        protected Builder(String route) {
            this.route = route;
            this.fields = new HashMap<>();
        }

        public Builder delay(long duration, TimeUnit unit) {
            return scheduledAt(System.currentTimeMillis() + unit.toMillis(duration));
        }

        public Builder scheduledAt(long timestamp) {
            this.targetTimestamp = timestamp;
            return this;
        }

        public Builder recurEvery(String recurKey, long interval, TimeUnit unit) {
            return this.recurEvery(recurKey, interval, unit, false);
        }

        public Builder recurEvery(String recurKey, long interval, TimeUnit unit, boolean randomDelay) {
            this.recurKey = recurKey;
            this.recurInterval = unit.toMillis(interval);
            return randomDelay ? this.delay((long) (recurInterval * Math.random()), TimeUnit.MILLISECONDS) : this;
        }

        public Builder requirePermit(String permitKey) {
            this.permitKey = permitKey;
            return this;
        }

        public Builder addField(String key, Object value) {
            fields.put(key, value);
            return this;
        }

        public Builder addFields(Map<String, ? extends Object> fieldMap) {
            fields.putAll(fieldMap);
            return this;
        }

        protected void setRespondTo(String respondTo) {
            this.respondTo = respondTo;
        }

        protected String getRoute() {
            return route;
        }

        protected Long getTargetTimestamp() {
            return targetTimestamp;
        }

        protected String getRecurKey() {
            return recurKey;
        }

        protected Long getRecurInterval() {
            return recurInterval;
        }

        protected String getPermitKey() {
            return permitKey;
        }

        public String getRespondTo() {
            return respondTo;
        }

        protected Map<String, Object> getFields() {
            return fields;
        }
    }
}