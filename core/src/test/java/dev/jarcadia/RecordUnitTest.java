//package dev.jarcadia;
//
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.lettuce.core.Range;
//import io.lettuce.core.RedisClient;
//import io.lettuce.core.StreamMessage;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mockito;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//import java.util.stream.Stream;
//
//public class RecordUnitTest {
//
//    static RedisClient redisClient;
//    static ValueFormatter formatter;
//    static RedisConnection rc;
//    static RedisRecordRepository repo;
//
//    @BeforeAll
//    static void setup() {
//        ObjectMapper objectMapper = new ObjectMapper();
//        formatter = new ValueFormatter(objectMapper);
//        redisClient = RedisClient.create("redis://localhost/15");
//        rc = new RedisConnection(redisClient, formatter);
//        repo = new RedisRecordRepository(rc);
//        RecordService recordService = Mockito.mock(RecordService.class);
//        objectMapper.registerModule(new PersistJsonModule(recordService));
//    }
//
//    @BeforeEach
//    void clear() {
//        rc.commands().flushdb();
//    }
//
//    private List<String> merge(String type, String id, String... args) {
//        return merge(type, id, List.of(), args);
//    }
//
//    private List<String> merge(String type, String id, List<String> metaKeys, String... args) {
//        return merge(type, id, "0", metaKeys, args);
//    }
//
//    private List<String> merge(String type, String id, String typeConfId, List<String> metaKeys, String... args) {
//        return repo.doMerge(type, id, typeConfId, metaKeys, args);
//    }
//
//    private List<String> delete(String type, String id) {
//        return repo.delete(type, id, "0", List.of());
//    }
//
//    private List<String> delete(String type, String id, String typeConfId, List<String> typeConfKeys) {
//        return repo.delete(type, id, typeConfId, typeConfKeys);
//    }
//
//
//    private void assertIdIndex(String type, String... ids) {
//        Assertions.assertIterableEquals(List.of(ids), rc.commands().zrange(type + "@id", 0, -1));
//    }
//
//    @Test
//    void touchRecord() {
//        List<String> result = merge("objs", "abc");
//        assertInsert(result, 0);
//
//        assertKeys(Keys.QUEUE, "objs@id");
//        assertIdIndex("objs", "abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "insert", "");
//    }
//
//    @Test
//    void touchExistingRecord() {
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        assertNoop(merge("objs", "abc"));
//
//        assertKeys("objs@id");
//        assertIdIndex("objs", "abc");
//    }
//
//    @Test
//    void deleteExistingRecord() {
//        rc.commands().zadd("objs@id", 0, "abc");
//        rc.commands().hset("objs/abc", "name", "John");
//        Assertions.assertIterableEquals(List.of("1"), delete("objs", "abc"));
//        assertKeys(Keys.QUEUE);
//    }
//
//    @Test
//    void deleteRecordThatDoesNotExist() {
//        Assertions.assertIterableEquals(List.of("0"), delete("objs", "abc"));
//        assertKeys();
//    }
//
//    @Test
//    void mergeIntoEmptyField() {
//        List<String> response = merge("objs", "abc", "name", "john");
//        assertInsert(response, 1);
//        assertFieldChange(response, "name", null, "john");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "insert", "", "+name", "john");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "name", "john");
//    }
//
//    @Test
//    void mergeIntoExistingField() {
//        rc.commands().hset("objs/abc", "age", "1");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> response = merge("objs", "abc", "age", "3");
//        assertUpdate(response, 1);
//        assertFieldChange(response, "age", "1", "3");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "-age", "1", "+age", "3");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "age", "3");
//    }
//
//    @Test
//    void clearField() {
//        rc.commands().hset("objs/abc", "name", "John");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> response = merge("objs", "abc", "name", "null");
//        assertUpdate(response, 1);
//        assertFieldChange(response, "name", "John", null);
//
//        assertKeys(Keys.QUEUE, "objs@id");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "-name", "John");
//        assertIdIndex("objs", "abc");
//    }
//
//    @Test
//    void incrementEmptyField() {
//        List<String> response = merge("objs", "abc", "age", "incr:1");
//        assertInsert(response, 1);
//        assertFieldChange(response, "age", null, "1");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "insert", "", "+age", "1");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "age", "1");
//    }
//
//    @Test
//    void incrementExistingField() {
//        rc.commands().hset("objs/abc", "age", "1");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> response = merge("objs", "abc", "age", "incr:1");
//        assertUpdate(response, 1);
//        assertFieldChange(response, "age", "1", "2");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "-age", "1", "+age", "2");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "age", "2");
//    }
//
//    @Test
//    void testParallelRecordPropertyIncrement() throws InterruptedException, ExecutionException {
//        int numOps = 10000;
//
//        List<Callable<List<String>>> callables = IntStream.range(0, numOps + 1)
//                .mapToObj(i -> (Callable<List<String>>) () -> merge("objs", "abc", "val", "incr:" + i))
//                .collect(Collectors.toList());
//        Collections.shuffle(callables);
//
//        ExecutorService executorService = Executors.newFixedThreadPool(100);
//        List<Future<List<String>>> futures = executorService.invokeAll(callables);
//
//        for (Future<List<String>> future : futures) {
//            future.get();
//        }
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "val", String.valueOf(((numOps * numOps) + numOps) / 2));
//    }
//
//    @Test
//    void testAtomicGetAndSetWhenMatchNull() {
//        List<String> result = merge("objs", "abc", "field", "getset:[null,\"\\\"val1\\\"\"]");
//        assertInsert(result, 1);
//        assertFieldChange(result, "field", null, "\"val1\"");
//    }
//
//    @Test
//    void testAtomicGetAndSetWhenNotMatchNull() {
//        rc.commands().hset("objs/abc", "field", "\"val\"");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> result = merge("objs", "abc", "field", "getset:[null,\"\\\"val1\\\"\"]");
//        assertNoop(result);
//    }
//
//    @Test
//    void testAtomicGetAndSetWhenMatchValue() {
//        rc.commands().hset("objs/abc", "field", "\"val0\"");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> result = merge("objs", "abc", "field", "getset:[\"\\\"val0\\\"\",\"\\\"val1\\\"\"]");
//        assertUpdate(result, 1);
//        assertFieldChange(result, "field", "\"val0\"", "\"val1\"");
//    }
//
//    @Test
//    void testAtomicGetAndSetWhenNotMatchValue() {
//        rc.commands().hset("objs/abc", "field", "\"val0\"");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> result = merge("objs", "abc", "field", "getset:[\"\\\"val1\\\"\",\"\\\"val2\\\"\"]");
//        assertNoop(result);
//    }
//
//    @Test
//    void testAtomicGetAndSetWhenMatchValueToClear() {
//        rc.commands().hset("objs/abc", "field", "\"val0\"");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> result = merge("objs", "abc", "field", "getset:[\"\\\"val0\\\"\",null]");
//        assertUpdate(result, 1);
//        assertFieldChange(result, "field", "\"val0\"", null);
//    }
//
//    // TODO GetAndSet when existing value matches
//    // TODO GetAndSet when value does not match
//    // TODO GetAndSet to clear
//
//    @Test
//    void testRecordPropertySubMergeFromEmpty() {
//        List<String> result = merge("objs", "abc", "medals", "merge:{\"gold\":8,\"silver\":7}");
//        assertInsert(result, 1);
//        assertFieldChange(result, "medals", null, "{\"silver\":7,\"gold\":8}");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "insert", "", "+medals",
//                "{\"silver\":7,\"gold\":8}");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "medals", "{\"silver\":7,\"gold\":8}");
//    }
//
//    @Test
//    void testRecordPropertySubMergeFromExisting() {
//        rc.commands().hset("objs/abc", "medals", "{\"silver\":7,\"gold\":8}");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> result = merge("objs", "abc", "medals", "merge:{\"gold\":11}");
//        assertUpdate(result, 1);
//        assertFieldChange(result, "medals", "{\"silver\":7,\"gold\":8}",
//                "{\"silver\":7,\"gold\":11}");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "-medals",
//                "{\"silver\":7,\"gold\":8}", "+medals", "{\"silver\":7,\"gold\":11}");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "medals", "{\"silver\":7,\"gold\":11}");
//    }
//
//    @Test
//    void testRecordPropertySubMergeDeleteFromExisting() {
//        rc.commands().hset("objs/abc", "medals", "{\"silver\":7,\"gold\":8}");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> result = merge("objs", "abc", "medals", "merge:{\"gold\":null}");
//        assertUpdate(result, 1);
//        assertFieldChange(result, "medals", "{\"silver\":7,\"gold\":8}", "{\"silver\":7}");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "-medals",
//                "{\"silver\":7,\"gold\":8}", "+medals", "{\"silver\":7}");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "medals", "{\"silver\":7}");
//    }
//
//    @Test
//    void testRecordPropertySubMergeChangeAndDeleteFromExisting() {
//        rc.commands().hset("objs/abc", "medals", "{\"silver\":7,\"gold\":8}");
//        rc.commands().zadd("objs@id", 0, "abc");
//
//        List<String> result = merge("objs", "abc", "medals", "merge:{\"silver\":9,\"gold\":null}");
//        assertUpdate(result, 1);
//        assertFieldChange(result, "medals", "{\"silver\":7,\"gold\":8}", "{\"silver\":9}");
//
//        assertKeys(Keys.QUEUE, "objs@id", "objs/abc");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "abc", "-medals",
//                "{\"silver\":7,\"gold\":8}", "+medals", "{\"silver\":9}");
//        assertIdIndex("objs", "abc");
//        assertHash("objs/abc", "medals", "{\"silver\":9}");
//    }
//
//    @Test
//    void testIdIndexSorting() {
//        assertInsert(merge("objs", "c"), 0);
//        assertInsert(merge("objs", "b"), 0);
//        assertInsert(merge("objs", "a"), 0);
//
//        assertKeys("objs@id", Keys.QUEUE);
//        assertIdIndex("objs", "a", "b", "c");
//        assertStreamEntry(Keys.QUEUE, 0, "type", "objs", "id", "c", "insert", "");
//        assertStreamEntry(Keys.QUEUE, 1, "type", "objs", "id", "b", "insert", "");
//        assertStreamEntry(Keys.QUEUE, 2, "type", "objs", "id", "a", "insert", "");
//    }
//
//    @Test
//    void testAlphaIndexUpdatedOnMerge() {
//        merge("users", "user1", List.of("users@name"), "name", "john");
//        merge("users", "user2", List.of("users@name"), "name", "allison");
//        assertZset("users@name", "allison:user2", "john:user1");
//    }
//
//    @Test
//    void testNumericIndexUpdatedOnMerge() {
//        merge("users", "john", List.of("users#age"), "age", "34");
//        merge("users", "allison", List.of("users#age"), "age", "33");
//        assertZsetWithScores("users#age", "allison", "33", "john", "34");
//    }
//
//    @Test
//    void testAlphaIndexUpdatedOnDelete() {
//        rc.commands().zadd("users@id", 0,"jdoe");
//        rc.commands().hset("users/jdoe", "name", "John Doe");
//        rc.commands().zadd("users@name", 0,"John Doe:jdoe");
//
//        rc.commands().zadd("users@id", 0,"jsmith");
//        rc.commands().hset("users/jsmith", "name", "Jane Smith");
//        rc.commands().zadd("users@name", 0,"Jane Smith:jsmith");
//
//        Assertions.assertIterableEquals(List.of("1"),
//                delete("users", "jdoe", "0", List.of("users@name")));
//
//        assertKeys(Keys.QUEUE, "users@id", "users@name", "users/jsmith");
//        assertZset("users@name", "Jane Smith:jsmith");
//    }
//
//    @Test
//    void testNumericIndexUpdatedOnDelete() {
//        rc.commands().zadd("users@id", 0,"jdoe");
//        rc.commands().hset("users/jdoe", "age", "32");
//        rc.commands().zadd("users#age", 32,"jdoe");
//
//        rc.commands().zadd("users@id", 0,"jsmith");
//        rc.commands().hset("users/jsmith", "age", "31");
//        rc.commands().zadd("users#age", 31,"jsmith");
//
//        Assertions.assertIterableEquals(List.of("1"),
//                delete("users", "jdoe", "0", List.of("users#age")));
//
//        assertKeys(Keys.QUEUE, "users@id", "users#age", "users/jsmith");
//        assertZsetWithScores("users#age", "jsmith", "31");
//    }
//
//    @Test
//    void testAlphaIndexOnDeleteWithoutIndexedValue() {
//        rc.commands().zadd("users@id", 0,"jdoe");
//
//        rc.commands().zadd("users@id", 0,"jsmith");
//        rc.commands().hset("users/jsmith", "name", "Jane Smith");
//        rc.commands().zadd("users@name", 0,"Jane Smith:jsmith");
//
//        Assertions.assertIterableEquals(List.of("1"),
//                delete("users", "jdoe", "0", List.of("users@name")));
//
//        assertKeys(Keys.QUEUE, "users@id", "users@name", "users/jsmith");
//        assertZset("users@name", "Jane Smith:jsmith");
//    }
//
//    @Test
//    void testNumericIndexUpdatedOnDeleteWithoutIndexedValue() {
//        rc.commands().zadd("users@id", 0,"jdoe");
//
//        rc.commands().zadd("users@id", 0,"jsmith");
//        rc.commands().hset("users/jsmith", "age", "31");
//        rc.commands().zadd("users#age", 31,"jsmith");
//
//        Assertions.assertIterableEquals(List.of("1"),
//                delete("users", "jdoe", "0", List.of("users#age")));
//
//        assertKeys(Keys.QUEUE, "users@id", "users#age", "users/jsmith");
//        assertZsetWithScores("users#age", "jsmith", "31");
//    }
//
//    @Test
//    void testTypeTrackingOnMerge() throws InterruptedException {
//
//        String tStream = Keys.TrackingStream("users");
//        List<String> typeKeys = List.of(tStream);
//
//        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
//        SimpleSubscription sub = SimpleSubscription.create(redisClient,
//                Keys.TrackingChannel("users"), msg -> queue.add(msg));
//
//        merge("users", "john", typeKeys, "age", "34");
//        merge("users", "john", typeKeys, "age", "35");
//        merge("users", "jane", typeKeys, "age", "32", "name", "\"Jane Doe\"");
//        merge("users", "jane", typeKeys, "name", "null");
//
//        String id1 = assertStreamEntry(tStream, 0, "id", "john", "age", "34");
//        String id2 = assertStreamEntry(tStream, 1, "id", "john", "age", "35");
//        String id3 = assertStreamEntry(tStream, 2, "id", "jane", "age", "32", "name", "\"Jane Doe\"");
//        String id4 = assertStreamEntry(tStream, 3, "id", "jane", "name", "null");
//
//        String event1 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        String event2 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        String event3 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        String event4 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        sub.close();
//
//        Assertions.assertEquals("[\"0\",\"" + id1 + "\",{\"john\":{\"age\":\"34\"}}]", event1);
//        Assertions.assertEquals("[\"" + id1 + "\",\"" + id2 + "\",{\"john\":{\"age\":\"35\"}}]", event2);
//        Assertions.assertEquals("[\"" + id2 + "\",\"" + id3 + "\",{\"jane\":{\"age\":\"32\",\"name\":\"\\\"Jane Doe\\\"\"}}]", event3);
//        Assertions.assertEquals("[\"" + id3 + "\",\"" + id4 + "\",{\"jane\":{\"name\":null}}]", event4);
//    }
//
//    @Test
//    void testTypeTrackingOnDelete() throws InterruptedException {
//
//        rc.commands().zadd("users@id", 0,"john");
//        rc.commands().hset("users/john", "age", "34");
//
//        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
//        SimpleSubscription sub = SimpleSubscription.create(redisClient,
//                Keys.TrackingChannel("users"), msg -> queue.add(msg));
//
//        delete("users", "john", "0", List.of("users>"));
//
//        String id1 = assertStreamEntry("users>", 0, "id", "john", "null", "");
//
//        String event1 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        Assertions.assertEquals("[\"0\",\"" + id1 + "\",{\"john\":null}]", event1);
//    }
//
//    @Test
//    void testPrimaryIndexTrackingOnMerge() throws InterruptedException {
//
//        String pIndex = Keys.PrimaryIndex("users");
//        String tStream = Keys.TrackingStream(pIndex);
//        List<String> typeKeys = List.of(tStream);
//
//        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();
//        SimpleSubscription sub = SimpleSubscription.create(redisClient, formatter,
//                Keys.TrackingChannel(pIndex),
//                new TypeReference<List<String>>() {}, msg -> queue.add(msg));
//
//        merge("users", "charles", typeKeys);
//        merge("users", "alice", typeKeys);
//        merge("users", "charles", typeKeys);
//        merge("users", "bob", typeKeys);
//
//        String id1 = assertStreamEntry(tStream, 0, "id", "charles", "+", "0");
//        String id2 = assertStreamEntry(tStream, 1, "id", "alice", "+", "0");
//        String id3 = assertStreamEntry(tStream, 2, "id", "bob", "+", "1");
//
//        List<String> event1 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event2 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event3 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        sub.close();
//
//        Assertions.assertIterableEquals(listOf("0", id1, "charles", null, "0"), event1);
//        Assertions.assertIterableEquals(listOf(id1, id2, "alice", null, "0"), event2);
//        Assertions.assertIterableEquals(listOf(id2, id3, "bob", null, "1"), event3);
//
//        assertKeys(Keys.QUEUE, pIndex, tStream);
//    }
//
//    @Test
//    void testSecondaryAlphaIndexTrackingOnMerge() throws InterruptedException {
//
//        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();
//        SimpleSubscription sub = SimpleSubscription.create(redisClient, formatter,
//                Keys.TrackingChannel("users@name"),
//                new TypeReference<List<String>>() {}, msg -> queue.add(msg));
//
//        merge("users", "alice", List.of("users@name", "users@name>"));
//        merge("users", "charles", List.of("users@name", "users@name>"), "name", "Charles");
//        merge("users", "alice", List.of("users@name", "users@name>"), "name", "Alice");
//        merge("users", "bob", List.of("users@name", "users@name>"), "name", "Bob");
//        merge("users", "alice", List.of("users@name", "users@name>"), "name", "Diana");
//        merge("users", "bob", List.of("users@name", "users@name>"), "name", "null");
//        merge("users", "alice", List.of("users@name", "users@name>"), "name", "Alice");
//
//        String id1 = assertStreamEntry("users@name>", 0, "id", "charles", "+", "0");
//        String id2 = assertStreamEntry("users@name>", 1, "id", "alice", "+", "0");
//        String id3 = assertStreamEntry("users@name>", 2, "id", "bob", "+", "1");
//        String id4 = assertStreamEntry("users@name>", 3, "id", "alice", "-", "0", "+", "2");
//        String id5 = assertStreamEntry("users@name>", 4, "id", "bob", "-", "0");
//        String id6 = assertStreamEntry("users@name>", 5, "id", "alice", "-", "1", "+", "0");
//
//        List<String> event1 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event2 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event3 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event4 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event5 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event6 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        sub.close();
//
//        Assertions.assertIterableEquals(listOf("0", id1, "charles", null, "0"), event1);
//        Assertions.assertIterableEquals(listOf(id1, id2, "alice", null, "0"), event2);
//        Assertions.assertIterableEquals(listOf(id2, id3, "bob", null, "1"), event3);
//        Assertions.assertIterableEquals(listOf(id3, id4, "alice", "0", "2"), event4);
//        Assertions.assertIterableEquals(listOf(id4, id5, "bob", "0", null), event5);
//        Assertions.assertIterableEquals(listOf(id5, id6, "alice", "1", "0"), event6);
//    }
//
//    @Test
//    void testSecondaryAlphaIndexTrackingOnDelete() throws InterruptedException {
//
//        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();
//        SimpleSubscription sub = SimpleSubscription.create(redisClient, formatter,
//                Keys.TrackingChannel("users@name"),
//                new TypeReference<List<String>>() {}, msg -> queue.add(msg));
//
//        merge("users", "alice", "0", List.of("users@name", "users@name>"), "name", "Alice");
//        merge("users", "charles", "0", List.of("users@name", "users@name>"), "name", "Charles");
//        delete("users", "alice", "0", List.of("users@name", "users@name>"));
//        delete("users", "charles", "0", List.of("users@name", "users@name>"));
//
//        String id1 = assertStreamEntry("users@name>", 0, "id", "alice", "+", "0");
//        String id2 = assertStreamEntry("users@name>", 1, "id", "charles", "+", "1");
//        String id3 = assertStreamEntry("users@name>", 2, "id", "alice", "-", "0");
//        String id4 = assertStreamEntry("users@name>", 3, "id", "charles", "-", "0");
//
//        List<String> event1 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event2 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event3 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event4 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        sub.close();
//
//        Assertions.assertIterableEquals(listOf("0", id1, "alice", null, "0"), event1);
//        Assertions.assertIterableEquals(listOf(id1, id2, "charles", null, "1"), event2);
//        Assertions.assertIterableEquals(listOf(id2, id3, "alice", "0", null), event3);
//        Assertions.assertIterableEquals(listOf(id3, id4, "charles", "0", null), event4);
//
//        assertKeys(Keys.QUEUE, "users@name>");
//    }
//
//    @Test
//    void testSecondaryNumericIndexTrackingOnMerge() throws InterruptedException {
//        String index = Keys.NumericIndex("users", "age");
//        String tStream = Keys.TrackingStream(index);
//        List<String> typeKeys = List.of(index, tStream);
//
//        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();
//        SimpleSubscription sub = SimpleSubscription.create(redisClient, formatter,
//                Keys.TrackingChannel(index),
//                new TypeReference<List<String>>() {}, msg -> queue.add(msg));
//
//        merge("users", "alice", typeKeys);
//        merge("users", "charles", typeKeys, "age", "35");
//        merge("users", "alice", typeKeys, "age", "30");
//        merge("users", "bob", typeKeys, "age", "32");
//        merge("users", "alice", typeKeys, "age", "40");
//        merge("users", "bob", typeKeys, "age", "null");
//        merge("users", "alice", typeKeys, "age", "30");
//
//        String id1 = assertStreamEntry(tStream, 0, "id", "charles", "+", "0");
//        String id2 = assertStreamEntry(tStream, 1, "id", "alice", "+", "0");
//        String id3 = assertStreamEntry(tStream, 2, "id", "bob", "+", "1");
//        String id4 = assertStreamEntry(tStream, 3, "id", "alice", "-", "0", "+", "2");
//        String id5 = assertStreamEntry(tStream, 4, "id", "bob", "-", "0");
//        String id6 = assertStreamEntry(tStream, 5, "id", "alice", "-", "1", "+", "0");
//
//        List<String> event1 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event2 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event3 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event4 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event5 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event6 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        sub.close();
//
//        Assertions.assertIterableEquals(listOf("0", id1, "charles", null, "0"), event1);
//        Assertions.assertIterableEquals(listOf(id1, id2, "alice", null, "0"), event2);
//        Assertions.assertIterableEquals(listOf(id2, id3, "bob", null, "1"), event3);
//        Assertions.assertIterableEquals(listOf(id3, id4, "alice", "0", "2"), event4);
//        Assertions.assertIterableEquals(listOf(id4, id5, "bob", "0", null), event5);
//        Assertions.assertIterableEquals(listOf(id5, id6, "alice", "1", "0"), event6);
//    }
//
//    @Test
//    void testSecondaryNumericIndexTrackingOnDelete() throws InterruptedException {
//        String index = Keys.NumericIndex("users", "age");
//        String tStream = Keys.TrackingStream(index);
//        List<String> typeKeys = List.of(index, tStream);
//
//        BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>();
//        SimpleSubscription sub = SimpleSubscription.create(redisClient, formatter,
//                Keys.TrackingChannel(index),
//                new TypeReference<List<String>>() {}, msg -> queue.add(msg));
//
//        merge("users", "charles", typeKeys, "age", "35");
//        merge("users", "alice", typeKeys, "age", "30");
//        delete("users", "alice", "0", typeKeys);
//        delete("users", "charles", "0", typeKeys);
//
//        String id1 = assertStreamEntry(tStream, 0, "id", "charles", "+", "0");
//        String id2 = assertStreamEntry(tStream, 1, "id", "alice", "+", "0");
//        String id3 = assertStreamEntry(tStream, 2, "id", "alice", "-", "0");
//        String id4 = assertStreamEntry(tStream, 3, "id", "charles", "-", "0");
//
//        List<String> event1 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event2 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event3 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        List<String> event4 = queue.poll(1000, TimeUnit.MILLISECONDS);
//        sub.close();
//
//        Assertions.assertIterableEquals(listOf("0", id1, "charles", null, "0"), event1);
//        Assertions.assertIterableEquals(listOf(id1, id2, "alice", null, "0"), event2);
//        Assertions.assertIterableEquals(listOf(id2, id3, "alice", "0", null), event3);
//        Assertions.assertIterableEquals(listOf(id3, id4, "charles", "0", null), event4);
//    }
//
//
//    @Test
//    void testMergeWithInvalidTypeConfId() {
//        rc.commands().xadd("users", "", "[\"users@age\", \"users@age>\"]");
//        String tcId = assertStreamEntry("users", 0, "", "[\"users@age\", \"users@age>\"]");
//
//        List<String> response = merge("users", "bob", "0", List.of("users@age", "users@age>"),
//                "age", "30");
//        Assertions.assertIterableEquals(List.of("-1", tcId, "[\"users@age\", \"users@age>\"]"), response);
//    }
//
//    @Test
//    void testMergeWithValidTypeConfId() {
//        rc.commands().xadd("users", "", "[\"users@age\", \"users@age>\"]");
//        String tcId = assertStreamEntry("users", 0, "", "[\"users@age\", \"users@age>\"]");
//
//        List<String> response = merge("users", "bob", tcId, List.of("users@age", "users@age>"), "age", "30");
//
//        assertInsert(response, 1);
//        assertKeys(Keys.QUEUE, "users", "users/bob", "users@id", "users@age", "users@age>");
//    }
//
//    static void assertNoop(List<String> result) {
//        Assertions.assertEquals(1, result.size(), "Size of response should be 1 for noop");
//        Assertions.assertEquals("0", result.get(0), "First element of result should be 0 for noop");
//    }
//
//    static void assertInsert(List<String> result, int numChanges) {
//        Assertions.assertEquals("1", result.get(0), "First element of result should be 1 for insert");
//        Assertions.assertEquals(numChanges, (result.size() - 1) / 2, "Expected " + numChanges + " changes");
//    }
//
//    static void assertUpdate(List<String> result, int numChanges) {
//        Assertions.assertEquals("0", result.get(0), "First element of result should be 0 for update");
//        Assertions.assertEquals(numChanges, (result.size() - 1) / 2, "Expected " + numChanges + " changes");
//    }
//
//    static void assertFieldChange(List<String> result, String field, String expectedBefore, String expectedAfter) {
//        String before = null;
//        String after = null;
//        for (int i=1; i<result.size(); i+=3) {
//            if (result.get(i).equals(field)) {
//                before = result.get(i+1);
//                after = result.get(i+2);
//            }
//        }
//        Assertions.assertEquals(expectedBefore, before, "Before value for " + field + " matches");
//        Assertions.assertEquals(expectedAfter, after, "After value for " + field + " matches");
//    }
//
//    private void assertKeys(String... keys) {
//        Assertions.assertIterableEquals(List.of(keys).stream().sorted().collect(Collectors.toList()),
//                rc.commands().keys("*").stream().sorted().collect(Collectors.toList()));
//    }
//
//    private String assertStreamEntry(String streamKey, int index, String... keyVals) {
//        List<StreamMessage<String, String>> messages = rc.commands().xrange(streamKey, Range.create("-", "+"));
//        StreamMessage<String, String> message = messages.get(index);
//        assertMap(keyVals, message.getBody());
//        return message.getId();
//    }
//
//    private void assertMap(String[] expected, Map<String, String> actual) {
//        for (int i=0; i<expected.length; i+=2) {
//            String key = expected[i];
//            Assertions.assertTrue(actual.containsKey(key), "Expected key " + key);
//            Assertions.assertEquals(expected[i+1], actual.remove(key), "Mismatched value for " + key);
//        }
//        Assertions.assertTrue(actual.isEmpty(), "Unexpected additional keys " + actual.keySet());
//    }
//
//    private void assertHash(String hashKey, String... keyVals) {
//        Map<String, String> hash = rc.commands().hgetall(hashKey);
//        for (int i=0; i<keyVals.length; i+=2) {
//            Assertions.assertEquals(keyVals[i+1], hash.get(keyVals[i]), "Field " + keyVals[i] + " matches");
//        }
//    }
//
//    public void assertZset(String zsetKey, String... values) {
//        Assertions.assertIterableEquals(listOf(values), rc.commands().zrange(zsetKey, 0, -1));
//    }
//
//    public void assertZsetWithScores(String zsetKey, String... valsAndScores) {
//        List<String> expected = rc.commands().zrangeWithScores(zsetKey, 0, -1).stream()
//                .flatMap(sv -> Stream.of(sv.getValue(), String.valueOf((int) sv.getScore())))
//                .collect(Collectors.toList());
//        Assertions.assertIterableEquals(expected, listOf(valsAndScores));
//    }
//
//    private List<String> listOf(String... values) {
//        List<String> list = new ArrayList<>();
//        for (String s : values) {
//            list.add(s);
//        }
//        return list;
//    }
//}