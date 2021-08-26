//package dev.jarcadia;
//
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.lettuce.core.Range;
//import io.lettuce.core.RedisClient;
//import io.lettuce.core.StreamMessage;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class TypeConfRepoUnitTest {
//
//    static RedisClient redisClient;
//    static ValueFormatter formatter;
//    static RedisConnection rc;
//    static TypeConfRepository repo;
//
//    @BeforeAll
//    static void setup() {
//        ObjectMapper objectMapper = new ObjectMapper();
//        formatter = new ValueFormatter(objectMapper);
//        redisClient = RedisClient.create("redis://localhost/15");
//        rc = new RedisConnection(redisClient, formatter);
//        repo = new TypeConfRepository(rc);
//    }
//
//    @BeforeEach
//    void clear() {
//        rc.commands().flushdb();
//    }
//
//    @Test
//    void testDefineTypeConfAlreadyDefinedAndUnchanged() {
//        rc.commands().xadd("users", "keys", "[\"users@name\", \"users@name>\"]");
//        List<String> res = repo.configureType("users", "[\"users@name\", \"users@name>\"]");
//        Assertions.assertIterableEquals(List.of("0"), res);
//        assertKeys("users");
//        assertStreamEntry("users", 0, "keys", "[\"users@name\", \"users@name>\"]");
//    }
//
//
//   @Test
//    void testDefineTypeConfWhenUndefinedWithNoRecords() {
//        List<String> res = repo.configureType("users", "[\"users#age\", \"users#age>\"]");
//        Assertions.assertIterableEquals(List.of("0"), res);
//        assertKeys("users");
//        assertStreamEntry("users", 0, "keys", "[\"users#age\", \"users#age>\"]");
//    }
//
//    @Test
//    void testDefineTypeConfWhenDefinedWithNoRecord() {
//        rc.commands().xadd("users", "", "[\"users@name\", \"users@name>\"]");
//        List<String> res = repo.configureType("users", "[\"users#age\", \"users#age>\"]");
//        Assertions.assertIterableEquals(List.of("0"), res);
//        assertKeys("users");
//        assertStreamEntry("users", 0, "keys", "[\"users#age\", \"users#age>\"]");
//    }
//
//    @Test
//    void testDefineTypeConfWhenUndefinedWithRecords() {
//        rc.commands().zadd(Keys.PrimaryIndex("users"), 0, "user1");
//        List<String> res = repo.configureType("users", "[\"users#age\", \"users#age>\"]");
//        Assertions.assertIterableEquals(List.of("1", "users#age"), res);
//        assertKeys("users", "users@id");
//        assertStreamEntry("users", 0, "keys", "[\"users#age\", \"users#age>\"]",
//                "locked", "true");
//    }
//
//    @Test
//    void testConfigureAddIndexWhenDefinedWithRecords() {
//        rc.commands().zadd(Keys.PrimaryIndex("users"), 0, "user1");
//        rc.commands().xadd("users", "keys", "[\"users@name\", \"users@name>\"]");
//        List<String> res = repo.configureType("users", "[\"users#age\", \"users#age>\"]");
//        Assertions.assertIterableEquals(List.of("1", "users#age", "users@name", "users@name>"), res);
//        assertKeys("users", "users@id");
//        assertStreamEntry("users", 0, "keys", "[\"users#age\", \"users#age>\"]",
//                "locked", "true");
//    }
//
//    @Test
//    void testConfigureRemoveIndexWithRecords() {
//        rc.commands().zadd(Keys.PrimaryIndex("users"), 0, "user1");
//        rc.commands().xadd("users", "keys", "[\"users@name\", \"users@name>\"]");
//
//        List<String> res = repo.configureType("users", "[]");
//        Assertions.assertIterableEquals(List.of("0", "users@name", "users@name>"), res);
//        assertKeys("users", "users@id");
//        assertStreamEntry("users", 0, "keys", "[]",
//                "locked", "true");
//    }
//
//    @Test
//    void buildOneRecordAlphaIndex() {
//        rc.commands().zadd("users@id", 0, "jdoe");
//        rc.commands().hset("users/jdoe", "name", "John Doe");
//        repo.buildIndexes("users", List.of("users@name"));
//
//        assertKeys("users@id", "users/jdoe", "users@name");
//        assertZset("users@name", "John Doe\\jdoe");
//    }
//
//    @Test
//    void buildOneRecordNumericIndex() {
//        rc.commands().zadd("users@id", 0, "jdoe");
//        rc.commands().hset("users/jdoe", "age", "32");
//        repo.buildIndexes("users", List.of("users#age"));
//
//        assertKeys("users@id", "users/jdoe", "users#age");
//        assertZsetWithScores("users#age", "jdoe", "32");
//    }
//
//    @Test
//    void buildMultiRecordAlphaIndex() {
//        rc.commands().zadd("users@id", 0, "jdoe");
//        rc.commands().hset("users/jdoe", "name", "John Doe");
//        rc.commands().zadd("users@id", 0, "asmith");
//        rc.commands().hset("users/asmith", "name", "Alice Smith");
//        repo.buildIndexes("users", List.of("users@name"));
//
//        assertKeys("users@id", "users/jdoe", "users/asmith", "users@name");
//        assertZset("users@name", "Alice Smith\\asmith", "John Doe\\jdoe");
//    }
//
//    @Test
//    void buildMultiRecordNumericIndex() {
//        rc.commands().zadd("users@id", 0, "jdoe");
//        rc.commands().hset("users/jdoe", "age", "32");
//        rc.commands().zadd("users@id", 0, "asmith");
//        rc.commands().hset("users/asmith", "age", "30");
//        repo.buildIndexes("users", List.of("users#age"));
//
//        assertKeys("users@id", "users/jdoe", "users/asmith", "users#age");
//        assertZsetWithScores("users#age", "asmith", "30", "jdoe", "32");
//    }
//
//    @Test
//    void buildMultiIndexesFromMultiRecord() {
//        rc.commands().zadd("users@id", 0, "jdoe");
//        rc.commands().hset("users/jdoe", Map.of("name", "John Doe", "age", "32"));
//        rc.commands().zadd("users@id", 0, "asmith");
//        rc.commands().hset("users/asmith", Map.of("name", "Alice Smith", "age", "30"));
//        repo.buildIndexes("users", List.of("users@name", "users#age"));
//
//        assertKeys("users@id", "users/jdoe", "users/asmith", "users@name", "users#age");
//        assertZset("users@name", "Alice Smith\\asmith", "John Doe\\jdoe");
//        assertZsetWithScores("users#age", "asmith", "30", "jdoe", "32");
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
//
//
//}
