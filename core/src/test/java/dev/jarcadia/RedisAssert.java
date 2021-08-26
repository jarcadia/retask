package dev.jarcadia;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisAssert {

    private final RedisConnection rc;

    public RedisAssert(RedisConnection rc) {
        this.rc = rc;
    }

    public void assertHash(String hashKey, String... keyVals) {
        Map<String, String> hash = rc.commands().hgetall(hashKey);
        for (int i=0; i<keyVals.length; i+=2) {
            Assertions.assertEquals(keyVals[i+1], hash.get(keyVals[i]), "Field " + keyVals[i] + " matches");
        }
    }

    public void assertKeys(String... expected) {
        List<String> keys = rc.commands().keys("*").stream().sorted().collect(Collectors.toList());
        List<String> expectedList = Stream.of(expected).sorted().collect(Collectors.toList());
        Assertions.assertIterableEquals(expectedList, keys);
    }

    public void assertList(String listKey, String... values) {
        Assertions.assertIterableEquals(listOf(values), rc.commands().lrange(listKey, 0, -1));
    }

    public void assertZset(String zsetKey, String... values) {
        Assertions.assertIterableEquals(listOf(values), rc.commands().zrange(zsetKey, 0, -1));
    }

    public void assertZsetWithScores(String zsetKey, String... valsAndScores) {
        List<String> expected = rc.commands().zrangeWithScores(zsetKey, 0, -1).stream()
                .flatMap(sv -> Stream.of(sv.getValue(), String.valueOf((int) sv.getScore())))
                .collect(Collectors.toList());
        Assertions.assertIterableEquals(expected, listOf(valsAndScores));
    }

    public String assertStreamEntry(String streamKey, int index, String... keyVals) {
        List<StreamMessage<String, String>> messages = rc.commands().xrange(streamKey, Range.create("-", "+"));
        StreamMessage<String, String> message = messages.get(index);
        assertMap(keyVals, message.getBody());
        return message.getId();
    }

    public void assertEmptyStream(String streamKey) {
        List<StreamMessage<String, String>> messages = rc.commands().xrange(streamKey, Range.create("-", "+"));
        Assertions.assertEquals(0, messages.size(), "Expected " + streamKey + " to be empty");
    }



    private void assertMap(String[] expected, Map<String, String> actual) {
        for (int i=0; i<expected.length; i+=2) {
            String key = expected[i];
            Assertions.assertTrue(actual.containsKey(key), "Expected key " + key);
            Assertions.assertEquals(expected[i+1], actual.remove(key), "Mismatched value for " + key);
        }
        Assertions.assertTrue(actual.isEmpty(), "Unexpected additional keys " + actual.keySet());
    }

    private List<String> listOf(String... values) {
        List<String> list = new ArrayList<>();
        for (String s : values) {
            list.add(s);
        }
        return list;
    }
}
