//package dev.jarcadia;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.lettuce.core.RedisClient;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//
//public class RecordIndexTest {
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
////        RecordService recordService = Mockito.mock(RecordService.class);
////        objectMapper.registerModule(new JarcadiaJson(recordService));
//    }
//
//    @BeforeEach
//    void clear() {
//        rc.commands().flushdb();
//    }
//
////    @Test
////    public void testIterateIndex() {
////        IntStream.range(1, 4).forEach(i -> rc.commands().zadd("users@id", 0, "user" + i));
////        Assertions.assertIterableEquals(List.of("user1", "user2", "user3"), repo.fetchPrimaryIndex("users"));
////    }
//}
