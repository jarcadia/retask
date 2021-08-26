//package dev.jarcadia;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import dev.jarcadia.exception.CalledTaskException;
//import io.lettuce.core.RedisClient;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.UUID;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//import java.util.concurrent.atomic.AtomicReference;
//
//public class SimpleIntegrationTests {
//
//    private final Logger logger = LoggerFactory.getLogger(AnnotatedHandlerScanner.class);
//
//    private static Jarcadia jarcadia;
//
//    @BeforeEach
//    public void setup() {
//        jarcadia = Jarcadia.configure("redis://localhost/15")
//                .flushDatabase()
//                .build();
//        jarcadia.start();
//    }
//
//    @AfterEach
//    public void close() {
//        jarcadia.close();
//    }
//
//
//    @Test
//    public void changingTypeConfAddAndRemovesIndexesAndTrackingKey() throws CalledTaskException, TimeoutException {
//        TrackedType userTrack = new TrackedType("users");
//        TrackedIndex primary = new TrackedIndex("users@id");
//        TrackedIndex byName = new TrackedIndex("users@name");
//        TrackedIndex byAge = new TrackedIndex("users#age");
//
//        jarcadia.configureType("users")
//                .trackFields()
//                .trackPrimaryIndex()
//                .indexAlphaField("name", true)
//                .indexNumericField("age", true)
//                .apply();
//
//        Record jdoe = jarcadia.getRecord("users", "jdoe");
//        Record asmith = jarcadia.getRecord("users", "asmith");
//        Record bjohnson = jarcadia.getRecord("users", "bjohnson");
//
//        jdoe.merge("name", "John Doe", "age", 30);
//        asmith.merge("name", "Alice Smith", "age", 32);
//        bjohnson.merge("name", "Bob Johnson", "age", 31);
//
//
//        makeRoundTrip(10, TimeUnit.SECONDS);
//
//        userTrack.assertRecord("jdoe","name", "\"John Doe\"", "age", "30");
//        userTrack.assertRecord("asmith","name", "\"Alice Smith\"", "age", "32");
//        userTrack.assertRecord("bjohnson","name", "\"Bob Johnson\"", "age", "31");
//        primary.assertIndex("asmith", "bjohnson", "jdoe");
//        byName.assertIndex("asmith", "bjohnson", "jdoe");
//        byAge.assertIndex("jdoe", "bjohnson", "asmith");
//    }
//
//    private class TrackedType {
//
//        private final Map<String, Map<String, String>> map;
//        private final AtomicReference<String> timestamp;
//
//        public TrackedType(String type) {
//            this.map = Collections.synchronizedMap(new HashMap<>());
//            this.timestamp = new AtomicReference<>("0");
//
//            jarcadia.subscribe(String.format("socket.io#/#/%s#", type), update -> {
//
//                logger.info("Type update {}", update);
//                JsonNode node = jarcadia.getObjectMapper().readTree(update);
//                String prevTimestamp = node.get(0).asText();
//                String newTimestamp = node.get(1).asText();
//
//                for (Iterator<Map.Entry<String, JsonNode>> it = node.get(2).fields(); it.hasNext(); ) {
//                    Map.Entry<String, JsonNode> recordEntry = it.next();
//                    Map<String, String> recordMap = map.computeIfAbsent(recordEntry.getKey(), id ->
//                            Collections.synchronizedMap(new HashMap<>()));
//
//                    for (Iterator<Map.Entry<String, JsonNode>> iter = recordEntry.getValue().fields(); iter.hasNext(); ) {
//                        Map.Entry<String, JsonNode> fieldEntry = iter.next();
//                        recordMap.put(fieldEntry.getKey(), fieldEntry.getValue().asText());
//                    }
//                }
//
//                logger.info("the map looks like {}", map);
//
//            });
//        }
//
//        void assertRecord(String id, String... expected) {
//            Map<String, String> actual = map.get(id);
//
//            for (int i=0; i<expected.length; i+=2) {
//                String key = expected[i];
//                Assertions.assertTrue(actual.containsKey(key), "Expected key " + key);
//                Assertions.assertEquals(expected[i+1], actual.remove(key), "Mismatched value for " + key);
//            }
//            Assertions.assertTrue(actual.isEmpty(), "Unexpected additional keys " + actual.keySet());
//        }
//    }
//
//    private class TrackedIndex {
//
//        private final List<String> list;
//        private final AtomicReference<String> timestamp;
//
//        public TrackedIndex(String index) {
//            this.list = Collections.synchronizedList(new ArrayList<>());
//            this.timestamp = new AtomicReference<>("0");
//
//            jarcadia.subscribe(String.format("socket.io#/#/%s#", index), update -> {
//                JsonNode node = jarcadia.getObjectMapper().readTree(update);
//                String prevTimestamp = node.get(0).asText();
//                String newTimestamp = node.get(1).asText();
//                String id = node.get(2).asText();
//
//                if (!this.timestamp.get().equals(prevTimestamp)) {
//                    throw new RuntimeException("Bad timestamp");
//                }
//                this.timestamp.set(newTimestamp);
//
//                if (!node.get(3).isNull()) {
//                    list.remove(node.get(3).asInt());
//                }
//
//                if (!node.get(4).isNull()) {
//                    list.add(node.get(4).asInt(), id);
//                }
//            });
//        }
//
//        void assertIndex(String... expected) {
//            Assertions.assertIterableEquals(List.of(expected), this.list);
//        }
//    }
//
//
//    private static void makeRoundTrip(long timeout, TimeUnit unit) throws CalledTaskException, TimeoutException {
//        String guid = UUID.randomUUID().toString();
//        final CompletableFuture<Void> future = new CompletableFuture();
//        jarcadia.registerTaskHandler(guid, (taskId, route, attempt, permit, params) -> null);
//        jarcadia.call(Task.create(guid)).await(timeout, unit);
//    }
//
//    @Test
//    public void aFewTasksWithPermit() throws InterruptedException, ExecutionException, TimeoutException {
//        Jarcadia jarcadia = Jarcadia
//                .configure("redis://localhost/15")
//                .flushDatabase()
//                .build();
//
//        Set<Integer> expectedPermits = Collections.synchronizedSet(new HashSet<>());
//        expectedPermits.addAll(List.of(0, 1, 2));
//        CompletableFuture<Void> future = new CompletableFuture<>();
//        jarcadia.registerTaskHandler("doJob", (taskId, route, attempt, permit, params) -> {
//            expectedPermits.remove(permit);
//            if (expectedPermits.isEmpty()) {
//                future.complete(null);
//            }
//            return null;
//        });
//
//        jarcadia.setPermitCap("cores", 3);
//
//        jarcadia.start();
//        jarcadia.submit(Task.create("doJob").requirePermit("cores"));
//        jarcadia.submit(Task.create("doJob").requirePermit("cores"));
//        jarcadia.submit(Task.create("doJob")
//                .requirePermit("cores")
//                .delay(200, TimeUnit.MILLISECONDS));
//
//        future.get(1, TimeUnit.SECONDS);
//        Assertions.assertEquals(0, expectedPermits.size());
//    }
//
//    private void assertMap(String[] expected, Map<String, String> actual) {
//
//    }
//}
