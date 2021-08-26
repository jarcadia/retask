//package dev.jarcadia;
//
//import java.util.Arrays;
//import java.util.Optional;
//import java.util.Set;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.stream.Collectors;
//
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//
//import io.lettuce.core.RedisClient;
//
//public class PersistorUnitTest {
//
//    static RedisClient redisClient;
//
//    @BeforeAll
//    public static void setup() {
//        redisClient = RedisClient.create("redis://localhost/15");
//    }
//
//    @Test
//    void basicKeyValue() {
//        Persistor rcommando = new Persistor(redisClient, null);
//        rcommando.core().flushdb();
//
//        rcommando.core().set("hello", "world");
//        Assertions.assertEquals("world", rcommando.core().get("hello"), "Key value is read correctly");
//    }
//
//    @Test
//    void testPubSub() throws InterruptedException, ExecutionException {
//        Persistor rcommando = new Persistor(redisClient, null);
//        rcommando.core().flushdb();
//        RecordSet objs = rcommando.getRecordSet("objs");
//
//        final AtomicReference<String> ref = new AtomicReference<>();
//        Subscription subscription = rcommando.subscribe("test-channel", (channel, val) -> {
//            System.out.println(Thread.currentThread().getName());
//            ref.set(val);
//        });
//        rcommando.core().publish("test-channel", "hello world");
//        subscription.close();
//        Assertions.assertEquals("hello world", ref.get());
//    }
//
//    @Test
//    void testCdlWithSingleValue() {
//        Persistor rcommando = new Persistor(redisClient, null);
//        rcommando.core().flushdb();
//        RecordSet objs = rcommando.getRecordSet("objs");
//
//        CountDownLatch cdl = rcommando.hla().getCountDownLatch("abc");
//        cdl.init(3, "John Doe");
//
//        Optional<Value> result;
//
//        result = cdl.decrement();
//        Assertions.assertFalse(result.isPresent());
//
//        result = cdl.decrement();
//        Assertions.assertFalse(result.isPresent());
//
//        result = cdl.decrement();
//        Assertions.assertTrue(result.isPresent());
//
//        Value value = result.get();
//        Assertions.assertNotNull(result.get());
//        Assertions.assertEquals("John Doe", value.asString());
//    }
//
//    @Test
//    void testMergeIntoSetIfDistinct() {
//        Persistor rcommando = new Persistor(redisClient, null);
//        rcommando.core().flushdb();
//        RecordSet objs = rcommando.getRecordSet("objs");
//
//        rcommando.core().sadd("test", "a", "b", "c");
//        Set<String> duplicates = rcommando.mergeIntoSetIfDistinct("test", Arrays.asList("d", "e"));
//        Assertions.assertEquals(0, duplicates.size());
//        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c", "d", "e"), rcommando.core().smembers("test").stream().sorted().collect(Collectors.toList()));
//    }
//
//    @Test
//    void testMergeIntoSetIfDistinctWithDuplicates() {
//        Persistor rcommando = new Persistor(redisClient, null);
//        rcommando.core().flushdb();
//        RecordSet objs = rcommando.getRecordSet("objs");
//
//        rcommando.core().sadd("test", "a", "b", "c", "d");
//        Set<String> duplicates = rcommando.mergeIntoSetIfDistinct("test", Arrays.asList("b", "d"));
//        Assertions.assertEquals(2, duplicates.size());
//        Assertions.assertIterableEquals(Arrays.asList("b", "d"), duplicates.stream().sorted().collect(Collectors.toList()));
//        Assertions.assertIterableEquals(Arrays.asList("a", "b", "c", "d"), rcommando.core().smembers("test").stream().sorted().collect(Collectors.toList()));
//    }
//}
