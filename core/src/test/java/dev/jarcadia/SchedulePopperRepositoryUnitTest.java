package dev.jarcadia;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.redis.RedisAssert;
import dev.jarcadia.redis.RedisConnection;
import dev.jarcadia.redis.RedisFactory;
import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchedulePopperRepositoryUnitTest {

    private static Stream<Arguments> args() {
        RedisFactory rf = new RedisFactory(RedisClient.create("redis://localhost/15"), new ObjectMapper());
        RedisConnection rc = rf.openConnection();
        SchedulePopperRepository repository = new SchedulePopperRepository(rf);
        return Stream.of(Arguments.of(rc, repository, new RedisAssert(rf)));
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopWithNoScheduledTasks(RedisConnection rc, SchedulePopperRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        SchedulePopperRepository.SchedulePopResponse response = repo.schedulePop(1500, 1);
        Assertions.assertNull(response);
        redisAssert.assertKeys();
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopReturnsPlusAndMinusAtEdgesOfLimit(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*doJob");
        rc.commands().zadd(Keys.SCHEDULE, 1501L, "*anotherJob");

        Assertions.assertNull(repo.schedulePop(1500, 2));

        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*doJob");
        rc.commands().zadd(Keys.SCHEDULE, 1501L, "*anotherJob");
        rc.commands().zadd(Keys.SCHEDULE, 1502L, "*yetAnotherJob");

        assertResponse(repo.schedulePop(1500, 2), List.of(), List.of());
    }

    private static void assertResponse(SchedulePopperRepository.SchedulePopResponse response,
            List<String> permitKeys, List<String> cronExprs) {
        Assertions.assertNotNull(response);
        Assertions.assertIterableEquals(permitKeys, response.getPermitKeys().collect(Collectors.toList()));
        Assertions.assertIterableEquals(cronExprs, response.getCronRequests().collect(Collectors.toList()));

    }

    @ParameterizedTest @MethodSource({"args"})
    void schedulePopMissingPermitKeyReturnsRequiredPermitKey(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.permitKey", "cores"));

        assertResponse(repo.schedulePop(1500,1), List.of("cores"), List.of());

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "1500");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}");
    }

    @ParameterizedTest @MethodSource({"args"})
    void schedulePopMissingCronInReturnsCronReq(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.cron", "* *",
                "recur1.fields", "{fields}"));

        assertResponse(repo.schedulePop(1500, 1), List.of(), List.of("* *"));

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "1500");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.cron", "* *",
                "recur1.fields", "{fields}");
    }

    @ParameterizedTest @MethodSource({"args"})
    void schedulePopMissingPermitKeysAndCronFromSameTaskReturnsBoth(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();

        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.fields",
                "{fields}", "recur1.cron", "* *", "recur1.permitKey", "cores"));

        assertResponse(repo.schedulePop(1500, 1), List.of("cores"), List.of("* *"));

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "1500");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.cron", "* *",
                "recur1.fields", "{fields}");
    }

    /**
     * In this scenario, neither doJob or anotherJob are able to be scheduled because doJob requires permitKey 'cores'
     * to be accessible and anotherJob requires cron expression '* *' to be accessible
     */
    @ParameterizedTest @MethodSource({"args"})
    void schedulePopMissingPermitKeysAndCronFromDifferentTasksReturnsBoth(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();

        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.fields", "{fields1}",
                "recur1.interval", "1000", "recur1.permitKey", "cores"));

        rc.commands().zadd(Keys.SCHEDULE, 1501L, "*recur2");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur2.route", "doJob", "recur2.fields",
                "{fields2}", "recur2.cron", "* *"));

        assertResponse(repo.schedulePop(1500, 2), List.of("cores"), List.of("* *"));

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "1500", "*recur2", "1501");
        redisAssert.assertHash(Keys.RECUR_DATA,"recur1.route", "doJob", "recur1.fields", "{fields1}",
                "recur1.interval", "1000", "recur1.permitKey", "cores", "recur1.route", "doJob", "recur2.fields",
                "{fields2}", "recur2.cron", "* *");
    }

    /**
     * In this scenario, neither doJob or anotherJob are able to be scheduled because both require permitKey 'cores'
     * to be accessible
     */
    @ParameterizedTest @MethodSource({"args"})
    void schedulePopMissingPermitKeyFromTwoTasksReturnsThatPermitKey(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();

        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.fields", "{fields1}",
                "recur1.interval", "1000", "recur1.permitKey", "cores"));

        rc.commands().zadd(Keys.SCHEDULE, 1501L, "*recur2");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur2.route", "doJob", "recur2.fields", "{fields2}",
                "recur2.interval", "2000", "recur2.permitKey", "cores"));

        assertResponse(repo.schedulePop(1500, 2), List.of("cores"), List.of());

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "1500", "*recur2", "1501");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.fields", "{fields1}",
                "recur1.interval", "1000", "recur1.permitKey", "cores", "recur2.route", "doJob", "recur2.fields",
                "{fields2}", "recur2.interval", "2000", "recur2.permitKey", "cores");
    }

    /**
     * In this scenario, neither doJob or anotherJob are able to be scheduled because both require cron expression
     * '* *'
     */
    @ParameterizedTest @MethodSource({"args"})
    void schedulePopMissingCronFromTwoTasksReturnsThatCronExpression(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();

        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.fields",
                "{fields1}", "recur1.cron", "* *"));

        rc.commands().zadd(Keys.SCHEDULE, 1501L, "*recur2");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur2.route", "doJob", "recur2.fields",
                "{fields2}", "recur2.cron", "* *"));

        assertResponse(repo.schedulePop(1500, 2), List.of(), List.of("* *"));

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "1500", "*recur2", "1501");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.fields", "{fields1}",
                "recur1.cron", "* *", "recur2.route", "doJob", "recur2.fields", "{fields2}", "recur2.cron", "* *");
    }

    /// todo need more tests about returning cron expr AND permit keys + cron expr

    // todo need tests that leave task in schedule if cron expression not available

    // todo need tests that schedule based on cron if cronExpr is included in scheduling call

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopQueuesRecurringTaskWhenPermitAvailable(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().rpush("cores.available", "0");
        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.permitKey", "cores"));

        Assertions.assertNull(repo.schedulePop(1500, 1, Stream.of("cores"), null));

        redisAssert.assertKeys(Keys.QUEUE, Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob", "recurKey", "recur1",
                "permitKey", "cores", "permit", "0", "fields", "{fields}", "targetTs", "1500");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "2500");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob",  "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.lock", "");
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopBacklogsRecurringTaskWhenPermitUnavailable(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.permitKey", "cores"));

        Assertions.assertNull(repo.schedulePop(1500, 1, Stream.of("cores"), null));

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA, Keys.BACKLOG, Keys.PermitBacklogList("cores"));
        String backlogId = redisAssert.assertStreamEntry(Keys.BACKLOG, 0, "route", "doJob", "recurKey",
                "recur1", "fields", "{fields}", "permitKey", "cores", "targetTs", "1500");
        redisAssert.assertList(Keys.PermitBacklogList("cores"), backlogId);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "2500");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.lock", "");
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopDoesNotQueueRecurringTaskWhenLockIsUnavailable(RedisConnection rc,
            SchedulePopperRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*recur1");
        rc.commands().hmset(Keys.RECUR_DATA, Map.of("recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.lock", ""));

        Assertions.assertNull(repo.schedulePop(1500, 1, Stream.of("cores"), null));

        redisAssert.assertKeys(Keys.SCHEDULE, Keys.RECUR_DATA);
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "2500");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.lock", "");
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopDoesNotQueueRecurringTaskWhenCancelled(RedisConnection rc,
            SchedulePopperRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().zadd(Keys.SCHEDULE, 1500L, "*doJob");

        Assertions.assertNull(repo.schedulePop(1500, 1, Stream.of("cores"), null));

        redisAssert.assertKeys();
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopQueuesNonRecurringTask(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        String futureId = rc.commands().xadd(Keys.FUTURES, "route", "doJob", "fields", "{fields}");
        rc.commands().zadd(Keys.SCHEDULE, 1500L, futureId);

        Assertions.assertNull(repo.schedulePop(1500, 1, Stream.of("cores"), null));

        redisAssert.assertKeys(Keys.QUEUE, Keys.FUTURES);
        redisAssert.assertEmptyStream(Keys.FUTURES);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob", "targetTs", "1500", "fields", "{fields}");
    }

    @ParameterizedTest
    @MethodSource({"args"})
    void schedulePopQueuesNonRecurringTaskWhenPermitAvailable(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().rpush("cores.available", "0");
        String futureId = rc.commands().xadd(Keys.FUTURES, "route", "doJob", "permitKey", "cores", "fields", "{fields}");
        rc.commands().zadd(Keys.SCHEDULE, 1500L, futureId);

        SchedulePopperRepository.SchedulePopResponse response = repo.schedulePop(1500, 1, Stream.of("cores"), null);
        Assertions.assertNull(response);

        redisAssert.assertKeys(Keys.QUEUE, Keys.FUTURES);
        redisAssert.assertEmptyStream(Keys.FUTURES);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob", "targetTs", "1500", "fields",
                "{fields}", "permitKey", "cores", "permit", "0");
    }

    private static void assertEmpty(SchedulePopperRepository.SchedulePopResponse response) {
    }

    @ParameterizedTest @MethodSource({"args"})
    void schedulePopBacklogsNonRecurringTaskWhenPermitUnavailable(RedisConnection rc, SchedulePopperRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();
        String futureId = rc.commands().xadd(Keys.FUTURES, "route", "doJob", "permitKey", "cores", "fields", "{fields}");
        rc.commands().zadd(Keys.SCHEDULE, 1500L, futureId);

        Assertions.assertNull(repo.schedulePop(1500, 1, Stream.of("cores"), null));

        redisAssert.assertKeys(Keys.FUTURES, Keys.BACKLOG, Keys.PermitBacklogList("cores"));
        redisAssert.assertEmptyStream(Keys.FUTURES);
        String backlogId = redisAssert.assertStreamEntry(Keys.BACKLOG, 0, "route", "doJob", "targetTs", "1500", "fields",
                "{fields}", "permitKey", "cores");
        redisAssert.assertList(Keys.PermitBacklogList("cores"), backlogId);
    }
}