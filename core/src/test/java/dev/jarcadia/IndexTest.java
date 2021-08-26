//package dev.jarcadia;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.lettuce.core.RedisClient;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class IndexTest {
//
//    @Test
//    public void testSet() {
//        RedisClient client = RedisClient.create("redis://localhost/15");
//        ObjectMapper objectMapper = new ObjectMapper();
//        ValueFormatter formatter = new ValueFormatter(objectMapper);
//        RedisConnection redis = new RedisConnection(client, formatter);
//
//        RedisRecordRepository2 repo = new RedisRecordRepository2(redis);
//        RecordService service = new RecordService(formatter, repo, new ProxyFactory2(objectMapper, repo));
//
//        redis.commands().flushdb();
//
//        boolean metadata = service.predefineTypeConf("obj")
//                .trackFields()
//                .trackPrimaryIndex()
//                .indexAlphaField("name", true)
//                .indexNumericField("age", true)
//                .apply();
//
//        MergeResult result = service.merge("obj", "xyz", "name", "John Doe", "age", 30);
//
////        assertResponse(response, "name", null, "\"John Doe\"", "age", null, "30");
//        assertHash(redis, "obj/xyz","name", "\"John Doe\"", "age", "30");
//        assertZset(redis, "obj@id", "xyz");
//        assertZsetWithScores(redis, "obj#age", "xyz", "30");
//    }
//
////    @Test
////    public void testScheduledPop() {
////        RedisClient client = RedisClient.create("redis://localhost/15");
////        ObjectMapper objectMapper = new ObjectMapper();
////        ValueFormatter formatter = new ValueFormatter(objectMapper);
////        ProxyFactory factory = new ProxyFactory(objectMapper);
////        RecordOperationEntryHandler recordOperationEntryHandler = new RecordOperationEntryHandler(executorService, formatter, recordService);
////
////        Redis redis = new Redis(client, formatter, factory, recordOperationEntryHandler);
////
////        redis.commands().flushdb();
////
////        redis.commands().hset("schedule", "runJob.interval", "1000");
////        redis.commands().hset("schedule", "runJob.params", "{}");
////        redis.commands().zadd("scheduledQ", 5000, "runJob");
////
////        redis.eval()
////                .cachedScript(Scripts.SCHEDULED_TASK_POP)
////                .addKeys("schedule", "scheduledQ", "taskQ")
////                .addArgs("5000", "100")
////                .returnInt();
//
////        assertResponse(response, "name", null, "\"John Doe\"", "age", null, "30");
////        assertHash(redis.commands().hgetall("obj/xyz"), "name", "\"John Doe\"", "age", "30");
////        assertZset(redis.commands().zrange("obj@id", 0, -1), "xyz");
////        assertZsetWithScores(redis.commands().zrangeWithScores("obj#age", 0, -1), "xyz", "30");
////    }
//
//
////    @Test
////    public void testQueueRecurringTaskWithPermitNotAvailable() {
////        ObjectMapper objectMapper = new ObjectMapper();
////        ValueFormatter formatter = new ValueFormatter(objectMapper);
////        RedisConn redis = new RedisConn(RedisClient.create("redis://localhost/15"), formatter);
////        Procrastinator procrastinator = Mockito.mock(Procrastinator.class);
////        Mockito.when(procrastinator.getCurrentTimeMillis()).thenReturn(5000L);
////
////        redis.commands().flushdb();
////
////        TaskQueuingRepository repo = new TaskQueuingRepository(redis, formatter, procrastinator);
////
////        repo.queueTask(Task.create("doJob")
////                .requirePermit("cores")
////                .param("key", "value")
////                .param("age", 23)
////                .recurEvery(1, TimeUnit.SECONDS));
////
////        assertList(redis, "cores.backlog", "doJob", "{\"key\":\"value\",\"age\":23}");
////        assertHash(redis, "recur", "doJob.interval", "1000", "doJob.params", "{\"key\":\"value\",\"age\":23}");
////        assertZsetWithScores(redis, "scheduleq", "doJob", "6000");
//
////        assertResponse(response, "name", null, "\"John Doe\"", "age", null, "30");
////        assertHash(redis.commands().hgetall("obj/xyz"), "name", "\"John Doe\"", "age", "30");
////        assertZset(redis.commands().zrange("obj@id", 0, -1), "xyz");
////        assertZsetWithScores(redis.commands().zrangeWithScores("obj#age", 0, -1), "xyz", "30");
////    }
//
//    public static void assertResponse(List<String> response, String... expected) {
//        Assertions.assertIterableEquals(listOf(expected), response);
//    }
//
//    public static void assertHash(RedisConnection redis, String hashKey, String... keyVals) {
//        Map<String, String> hash = redis.commands().hgetall(hashKey);
//        for (int i=0; i<keyVals.length; i+=2) {
//            Assertions.assertEquals(keyVals[i + 1], hash.get(keyVals[i]));
//        }
//    }
//
//    public static void assertList(RedisConnection redis, String listKey, String... values) {
//        Assertions.assertIterableEquals(listOf(values), redis.commands().lrange(listKey, 0, -1));
//    }
//
//    public static void assertZset(RedisConnection redis, String zsetKey, String... values) {
//        Assertions.assertIterableEquals(redis.commands().zrange(zsetKey, 0, -1),
//                listOf(values));
//    }
//
//    public static void assertZsetWithScores(RedisConnection redis, String zsetKey, String... valsAndScores) {
//        List<String> expected = redis.commands().zrangeWithScores(zsetKey, 0, -1).stream()
//                .flatMap(sv -> Stream.of(sv.getValue(), String.valueOf((int) sv.getScore())))
//                .collect(Collectors.toList());
//        Assertions.assertIterableEquals(expected, listOf(valsAndScores));
//    }
//
//    public static List<String> listOf(String... values) {
//        List<String> list = new ArrayList<>();
//        for (String s : values) {
//            list.add(s);
//        }
//        return list;
//    }
//}
