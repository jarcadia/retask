package dev.jarcadia;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class PermitRepositoryUnitTest {

    private static Stream<Arguments> args() {
        RedisClient client = RedisClient.create("redis://localhost/15");
        ObjectMapper objectMapper = new ObjectMapper();
        RedisConnection rc = new RedisConnection(client, objectMapper);
        PermitRepository permitRepository = new PermitRepository(rc);
        return Stream.of(Arguments.of(rc, permitRepository, new RedisAssert(rc)));
    }

    @ParameterizedTest @MethodSource({"args"})
    void increasingPermitCapWithoutBacklogAddsToAvailable(RedisConnection rc, PermitRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        Assertions.assertEquals(1, repo.setPermitCap("cores", 1));

        redisAssert.assertKeys(Keys.PERMIT_CAPS, Keys.AvailablePermitList("cores"));
        redisAssert.assertHash(Keys.PERMIT_CAPS, "cores", "1");
        redisAssert.assertList(Keys.AvailablePermitList("cores"), "0");

        Assertions.assertEquals(1, repo.setPermitCap("cores", 2));

        redisAssert.assertKeys(Keys.PERMIT_CAPS, Keys.AvailablePermitList("cores"));
        redisAssert.assertHash(Keys.PERMIT_CAPS, "cores", "2");
        redisAssert.assertList(Keys.AvailablePermitList("cores"), "0", "1");
    }

    @ParameterizedTest @MethodSource({"args"})
    void increasingPermitCapWithBacklogPops(RedisConnection rc, PermitRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        String futureId = rc.commands().xadd(Keys.BACKLOG, "route", "doJob", "fields", "{fields}");
        rc.commands().rpush(Keys.PermitBacklogList("cores"), futureId);

        Assertions.assertEquals(1, repo.setPermitCap("cores", 1));

        redisAssert.assertKeys(Keys.PERMIT_CAPS, Keys.BACKLOG, Keys.QUEUE);
        redisAssert.assertHash(Keys.PERMIT_CAPS, "cores", "1");
        redisAssert.assertEmptyStream(Keys.FUTURES);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob", "fields", "{fields}", "permitKey",
                "cores", "permit", "0");
    }

    @ParameterizedTest @MethodSource({"args"})
    void increasingPermitCapWithPartialBacklog(RedisConnection rc, PermitRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        String futureId = rc.commands().xadd(Keys.BACKLOG, "route", "doJob", "fields", "{fields}");
        rc.commands().rpush(Keys.PermitBacklogList("cores"), futureId);

        Assertions.assertEquals(2, repo.setPermitCap("cores", 2));

        redisAssert.assertKeys(Keys.PERMIT_CAPS, Keys.BACKLOG, Keys.QUEUE, Keys.AvailablePermitList("cores"));
        redisAssert.assertHash(Keys.PERMIT_CAPS, "cores", "2");
        redisAssert.assertEmptyStream(Keys.BACKLOG);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob", "fields", "{fields}", "permitKey",
                "cores", "permit", "0");
        redisAssert.assertList(Keys.AvailablePermitList("cores"), "1");
    }

    @ParameterizedTest @MethodSource({"args"})
    void decreasingPermitCapRemovesFromAvailable(RedisConnection rc, PermitRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        rc.commands().hset(Keys.PERMIT_CAPS, "cores", "2");
        rc.commands().rpush(Keys.AvailablePermitList("cores"), "0", "1");

        Assertions.assertEquals(-1, repo.setPermitCap("cores", 1));

        redisAssert.assertKeys(Keys.PERMIT_CAPS, Keys.AvailablePermitList("cores"));
        redisAssert.assertHash(Keys.PERMIT_CAPS, "cores", "1");
        redisAssert.assertList(Keys.AvailablePermitList("cores"), "0");

        Assertions.assertEquals(-1, repo.setPermitCap("cores", 0));

        redisAssert.assertKeys();
    }

    // TODO INCREASE CAP when tasks are backlogged, they should be queued

    @ParameterizedTest @MethodSource({"args"})
    void releasingPermitQueuesBackloggedTask(RedisConnection rc, PermitRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().hset(Keys.PERMIT_CAPS, "cores", "1");
        String backlogId = rc.commands().xadd(Keys.BACKLOG, "route", "doJob", "recurring", "", "fields", "{fields}");
        rc.commands().rpush("cores.backlog", backlogId);

        int response = repo.releasePermit("cores", 0);

        Assertions.assertEquals(1, response);
        redisAssert.assertKeys(Keys.PERMIT_CAPS, Keys.BACKLOG, Keys.QUEUE);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob",
                "fields", "{fields}", "permitKey", "cores", "permit", "0", "recurring", "");
        redisAssert.assertEmptyStream(Keys.BACKLOG);
        redisAssert.assertList("cores.backlog");
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void releasingPermitExceedingCapDestroysIt(RedisConnection rc, PermitRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().hset(Keys.PERMIT_CAPS, "cores", "1");
        String backlogId = rc.commands().xadd(Keys.BACKLOG, "route", "doJob", "recurring", "", "fields", "{fields}");
        rc.commands().rpush("cores.backlog", backlogId);

        int response = repo.releasePermit("cores", 1);

        Assertions.assertEquals(-1, response);
        redisAssert.assertKeys(Keys.PERMIT_CAPS, Keys.BACKLOG, "cores.backlog");
        redisAssert.assertStreamEntry(Keys.BACKLOG, 0, "route", "doJob", "recurring", "", "fields", "{fields}");
        redisAssert.assertList("cores.backlog", backlogId);
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void releasingPermitMakesPermitAvailable(RedisConnection rc, PermitRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().hset(Keys.PERMIT_CAPS, "cores", "1");

        int response = repo.releasePermit("cores", 0);

        Assertions.assertEquals(0, response);
        redisAssert.assertKeys(Keys.PERMIT_CAPS, "cores.available");
        redisAssert.assertList("cores.available", "0");
    }
}