package dev.jarcadia;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskQueuingRepositoryUnitTest {

    private static Stream<Arguments> args() {
        RedisClient client = RedisClient.create("redis://localhost/15");
        ObjectMapper objectMapper = new ObjectMapper();
        RedisConnection rc = new RedisConnection(client, objectMapper);
        Procrastinator procrastinator = Mockito.mock(Procrastinator.class);
        Mockito.when(procrastinator.getCurrentTimeMillis()).thenReturn(1500L);
        TaskQueuingRepository repository = new TaskQueuingRepository(rc, procrastinator);
        return Stream.of(Arguments.of(rc, repository, new RedisAssert(rc)));
    }

    @ParameterizedTest @MethodSource({"args"})
    void taskIsQueuedWhenPermitIsAvailable(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().rpush("cores.available", "0");

        int response = repo.queueTaskWithRequiredPermit("doJob", "{fields}", "cores", null);

        Assertions.assertEquals(1, response);
        redisAssert.assertKeys(Keys.QUEUE);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob",
                "fields", "{fields}", "permitKey", "cores", "permit", "0");
    }

    @ParameterizedTest @MethodSource({"args"})
    void taskIsBackloggedWhenPermitIsUnavailable(RedisConnection rc, TaskQueuingRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();

        int response = repo.queueTaskWithRequiredPermit("doJob", "{fields}","cores", null);

        Assertions.assertEquals(0, response);
        redisAssert.assertKeys(Keys.BACKLOG, "cores.backlog");
        String backlogId = redisAssert.assertStreamEntry(Keys.BACKLOG, 0, "route", "doJob", "fields", "{fields}");
        redisAssert.assertList("cores.backlog", backlogId);
    }


    @ParameterizedTest @MethodSource({"args"})
    void respondingTaskIsQueuedWhenPermitIsAvailable(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().rpush("cores.available", "0");

        int response = repo.queueTaskWithRequiredPermit("doJob", "{fields}", "cores",
                "response-abc123");

        Assertions.assertEquals(1, response);
        redisAssert.assertKeys(Keys.QUEUE);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob",
                "fields", "{fields}", "permitKey", "cores", "permit", "0", "respondTo", "response-abc123");
    }

    @ParameterizedTest @MethodSource({"args"})
    void respondingTaskIsBackloggedWhenPermitIsUnavailable(RedisConnection rc, TaskQueuingRepository repo,
            RedisAssert redisAssert) {
        rc.commands().flushdb();

        int response = repo.queueTaskWithRequiredPermit("doJob", "{fields}","cores",
                "response-abc123");

        Assertions.assertEquals(0, response);
        redisAssert.assertKeys(Keys.BACKLOG, "cores.backlog");
        String backlogId = redisAssert.assertStreamEntry(Keys.BACKLOG, 0, "route", "doJob", "fields",
                "{fields}", "respondTo", "response-abc123");
        redisAssert.assertList("cores.backlog", backlogId);
    }

    @ParameterizedTest @MethodSource({"args"})
    void taskIsScheduled(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        repo.scheduleTask("doJob", 1500L, "{fields}",null);

        redisAssert.assertKeys(Keys.FUTURES, Keys.SCHEDULE);
        String futureId = redisAssert.assertStreamEntry(Keys.FUTURES, 0, "route", "doJob", "fields",
                "{fields}");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, futureId, "1500");
    }

    @ParameterizedTest @MethodSource({"args"})
    void respondingTaskIsScheduled(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        repo.scheduleTask("doJob", 1500L, "{fields}","res-abc123");

        redisAssert.assertKeys(Keys.FUTURES, Keys.SCHEDULE);
        String futureId = redisAssert.assertStreamEntry(Keys.FUTURES, 0, "route", "doJob", "fields",
                "{fields}", "respondTo", "res-abc123");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, futureId, "1500");
    }

    @ParameterizedTest @MethodSource({"args"})
    void taskWithPermitIsScheduled(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        repo.scheduleTaskWithPermit("doJob", 1500L, "{fields}","cores", null);

        redisAssert.assertKeys(Keys.FUTURES, Keys.SCHEDULE);
        String futureId = redisAssert.assertStreamEntry(Keys.FUTURES, 0, "route", "doJob", "fields",
                "{fields}", "permitKey", "cores");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, futureId, "1500");

        // Extra assert to ensure permit is the 2nd key/value pair
        Assertions.assertEquals("permitKey", rc.commands()
                .xrange(Keys.FUTURES, Range.create("-", "+"))
                .get(0)
                .getBody()
                .entrySet()
                .stream()
                .collect(Collectors.toList())
                .get(1)
                .getKey());
    }

    @ParameterizedTest @MethodSource({"args"})
    void respondingTaskWithPermitIsScheduled(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        repo.scheduleTaskWithPermit("doJob", 1500L, "{fields}", "cores", "res-abc123");

        redisAssert.assertKeys(Keys.FUTURES, Keys.SCHEDULE);
        String futureId = redisAssert.assertStreamEntry(Keys.FUTURES, 0, "route", "doJob", "fields",
                "{fields}", "permitKey", "cores", "respondTo", "res-abc123");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, futureId, "1500");

        // Extra assert to ensure permit is the 2nd key/value pair
        Assertions.assertEquals("permitKey", rc.commands()
                .xrange(Keys.FUTURES, Range.create("-", "+"))
                .get(0)
                .getBody()
                .entrySet()
                .stream()
                .collect(Collectors.toList())
                .get(1)
                .getKey());
    }


    @ParameterizedTest @MethodSource({"args"})
    void recurringTaskIsScheduledAndQueued(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        repo.queueRecurringTask("doJob", "recur1", "{fields}", 1000);

        redisAssert.assertKeys(Keys.QUEUE, Keys.RECUR_DATA, Keys.SCHEDULE);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob", "recurKey", "recur1",
                "fields", "{fields}");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "2500");
    }

    @ParameterizedTest @MethodSource({"args"})
    void recurringTaskIsScheduledAndQueuedWhenPermitIsAvailable(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        rc.commands().rpush("cores.available", "0");

        int response = repo.queueRecurringTaskWithRequiredPermit("doJob", "recur1", "{fields}", 1000,
                "cores");

        Assertions.assertEquals(1, response);
        redisAssert.assertKeys(Keys.QUEUE, Keys.RECUR_DATA, Keys.SCHEDULE);
        redisAssert.assertStreamEntry(Keys.QUEUE, 0, "route", "doJob", "recurKey", "recur1",
                "fields", "{fields}", "permitKey", "cores", "permit", "0");
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.permitKey", "cores");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "2500");
    }

    @ParameterizedTest @MethodSource({"args"})
    void recurringTaskIsScheduledAndBackloggedWhenPermitIsUnavailable(RedisConnection rc,
            TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();

        int response = repo.queueRecurringTaskWithRequiredPermit("doJob","recur1", "{fields}",
                1000, "cores");

        Assertions.assertEquals(0, response);
        redisAssert.assertKeys(Keys.BACKLOG, Keys.RECUR_DATA, Keys.SCHEDULE, "cores.backlog");
        String backlogId = redisAssert.assertStreamEntry(Keys.BACKLOG, 0, "route", "doJob", "recurKey",
                "recur1", "fields", "{fields}");
        redisAssert.assertList("cores.backlog", backlogId);
        redisAssert.assertHash(Keys.RECUR_DATA, "recur1.route", "doJob", "recur1.interval", "1000",
                "recur1.fields", "{fields}", "recur1.permitKey", "cores");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, "*recur1", "2500");
    }

    @ParameterizedTest @MethodSource({"args"})
    void retryTaskWithoutDelayAfterFirstAttempt(RedisConnection rc,
            TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        String taskId = rc.commands().xadd(Keys.QUEUE, "route", "doJob", "fields", "{fields}");

        repo.retryTask(taskId, 0);

        redisAssert.assertKeys(Keys.QUEUE);
        redisAssert.assertStreamEntry(Keys.QUEUE, 1, "route", "doJob", "fields",
                "{fields}", "attempt", "2");
    }

    @ParameterizedTest @MethodSource({"args"})
    void retryTaskWithoutDelayAfterSecondAttempt(RedisConnection rc,
            TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        String taskId = rc.commands().xadd(Keys.QUEUE, "route", "doJob", "fields", "{fields}", "attempt", "2");

        repo.retryTask(taskId, 0);

        redisAssert.assertKeys(Keys.QUEUE);
        redisAssert.assertStreamEntry(Keys.QUEUE, 1, "route", "doJob", "fields",
                "{fields}", "attempt", "3");
    }

    @ParameterizedTest @MethodSource({"args"})
    void retryTaskWithDelayAfterFirstAttempt(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        String taskId = rc.commands().xadd(Keys.QUEUE, "route", "doJob", "fields", "{fields}");

        repo.retryTask(taskId, 1000);

        redisAssert.assertKeys(Keys.QUEUE, Keys.SCHEDULE, Keys.FUTURES);
        String retryId = redisAssert.assertStreamEntry(Keys.FUTURES, 0, "route", "doJob", "fields",
                "{fields}", "attempt", "2");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, retryId, "2500");
    }

    @ParameterizedTest @MethodSource({"args"})
    void retryTaskWithDelayAfterSecondAttempt(RedisConnection rc, TaskQueuingRepository repo, RedisAssert redisAssert) {
        rc.commands().flushdb();
        String taskId = rc.commands().xadd(Keys.QUEUE, "route", "doJob", "fields", "{fields}", "attempt", "2");

        repo.retryTask(taskId, 1000);

        redisAssert.assertKeys(Keys.QUEUE, Keys.SCHEDULE, Keys.FUTURES);
        String retryId = redisAssert.assertStreamEntry(Keys.FUTURES, 0, "route", "doJob", "fields",
                "{fields}", "attempt", "3");
        redisAssert.assertZsetWithScores(Keys.SCHEDULE, retryId, "2500");
    }

    // 4 combos of retry: with/without existing attempt, with/without delay
}