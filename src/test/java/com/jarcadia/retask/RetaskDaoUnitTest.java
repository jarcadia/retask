package com.jarcadia.retask;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;

import io.lettuce.core.RedisClient;

public class RetaskDaoUnitTest {

    static RedisClient redisClient;
    static RedisCommando rcommando;
    static ObjectMapper objectMapper;

    @BeforeAll
    public static void setup() {
        redisClient = RedisClient.create("redis://localhost/15");
        rcommando = RedisCommando.create(redisClient);
        objectMapper = rcommando.getObjectMapper();
    }

    @BeforeEach
    public void flush() {
        rcommando.core().flushdb();
    }
    
    @Test
    void verifyTaskQueueingWorks() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        Task task = Task.create("test");
        dao.submit(task);
        Assertions.assertEquals(1, rcommando.core().hlen(task.getId()));
        Assertions.assertEquals("test", rcommando.core().hget(task.getId(), "routingKey"));
        Assertions.assertEquals(1, rcommando.core().llen(Key.TASKS));
        Assertions.assertIterableEquals(Arrays.asList(task.getId()), rcommando.core().lrange(Key.TASKS, 0, -1));
    }

    @Test
    void verifyTaskQueueingWithParams() throws JsonProcessingException {
        RetaskRepository dao = new RetaskRepository(rcommando);
        Task task = Task.create("test").param("field", "value");
        dao.submit(task);
        Assertions.assertEquals(1, rcommando.core().llen(Key.TASKS));
        Assertions.assertIterableEquals(Arrays.asList(task.getId()), rcommando.core().lrange(Key.TASKS, 0, -1));
        Assertions.assertEquals(2, rcommando.core().hlen(task.getId()));
        Assertions.assertEquals("test", rcommando.core().hget(task.getId(), "routingKey"));

        Map<String, Object> expectedParams = Collections.singletonMap("field", "value");
        String expectedParamsStr = objectMapper.writeValueAsString(expectedParams);
        Assertions.assertEquals(expectedParamsStr, rcommando.core().hget(task.getId(), "params"));
    }

    @Test
    void verifyTaskQueueingWithRecurrence() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        Task task = Task.create("test").recurEvery("abc", 1, TimeUnit.SECONDS);
        String authorityKey = task.getMetadata().get("authorityKey");
        dao.submit(task);
        Assertions.assertEquals(1, rcommando.core().llen(Key.TASKS));
        Assertions.assertIterableEquals(Arrays.asList(task.getId()), rcommando.core().lrange(Key.TASKS, 0, -1));
        
        // Confirm auth map
        Assertions.assertEquals(1, rcommando.core().hlen(Key.RECUR_AUTH_KEY));
        Assertions.assertEquals(authorityKey, rcommando.core().hget(Key.RECUR_AUTH_KEY, "abc"));
        
        // Confirm task values
        Assertions.assertEquals(4, rcommando.core().hlen(task.getId()));
        Assertions.assertEquals("test", rcommando.core().hget(task.getId(), "routingKey"));
        Assertions.assertEquals("abc", rcommando.core().hget(task.getId(), "recurKey"));
        Assertions.assertEquals(authorityKey, rcommando.core().hget(task.getId(), "authorityKey"));
        Assertions.assertEquals("1000", rcommando.core().hget(task.getId(), "recurInterval"));
    }
    
    @Test
    void verifyTaskSchedulingWorks() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        long target = System.currentTimeMillis() + 1000;
        Task task = Task.create("test").at(target);
        dao.submit(task);
        Assertions.assertEquals(2, rcommando.core().hlen(task.getId()));
        Assertions.assertEquals("test", rcommando.core().hget(task.getId(), "routingKey"));
        Assertions.assertEquals(String.valueOf(target), rcommando.core().hget(task.getId(), "targetTimestamp"));
        
        Assertions.assertEquals(1, rcommando.core().zcard(Key.SCHEDULED));
        Assertions.assertEquals(target, rcommando.core().zscore(Key.SCHEDULED, task.getId()));
        Assertions.assertIterableEquals(Arrays.asList(task.getId()), rcommando.core().zrange(Key.SCHEDULED, 0, -1));
    }
    
    @Test
    void verifyTaskSchedulingWithParamsWorks() throws JsonProcessingException {
        RetaskRepository dao = new RetaskRepository(rcommando);
        long target = System.currentTimeMillis() + 1000;
        Task task = Task.create("test").at(target).param("field", "value");
        dao.submit(task);
        Assertions.assertEquals(3, rcommando.core().hlen(task.getId()));
        Assertions.assertEquals("test", rcommando.core().hget(task.getId(), "routingKey"));
        Assertions.assertEquals(String.valueOf(target), rcommando.core().hget(task.getId(), "targetTimestamp"));
        Map<String, Object> expectedParams = Collections.singletonMap("field", "value");
        String expectedParamsStr = objectMapper.writeValueAsString(expectedParams);
        Assertions.assertEquals(expectedParamsStr, rcommando.core().hget(task.getId(), "params"));
        
        Assertions.assertEquals(1, rcommando.core().zcard(Key.SCHEDULED));
        Assertions.assertEquals(target, rcommando.core().zscore(Key.SCHEDULED, task.getId()));
        Assertions.assertIterableEquals(Arrays.asList(task.getId()), rcommando.core().zrange(Key.SCHEDULED, 0, -1));
    }
    
    @Test
    void verifyTaskSchedulingWithRecurrenceWorks() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        long target = System.currentTimeMillis() + 1000;
        Task task = Task.create("test").at(target).recurEvery("abc", 1, TimeUnit.SECONDS);
        String authorityKey = task.getMetadata().get("authorityKey");
        dao.submit(task);
        Assertions.assertEquals(5, rcommando.core().hlen(task.getId()));
        Assertions.assertEquals("test", rcommando.core().hget(task.getId(), "routingKey"));
        Assertions.assertEquals(String.valueOf(target), rcommando.core().hget(task.getId(), "targetTimestamp"));
        Assertions.assertEquals("abc", rcommando.core().hget(task.getId(), "recurKey"));
        Assertions.assertEquals(authorityKey, rcommando.core().hget(task.getId(), "authorityKey"));
        Assertions.assertEquals("1000", rcommando.core().hget(task.getId(), "recurInterval"));
        
        Assertions.assertEquals(1, rcommando.core().zcard(Key.SCHEDULED));
        Assertions.assertEquals(target, rcommando.core().zscore(Key.SCHEDULED, task.getId()));
        Assertions.assertIterableEquals(Arrays.asList(task.getId()), rcommando.core().zrange(Key.SCHEDULED, 0, -1));
        
        // Confirm auth map
        Assertions.assertEquals(1, rcommando.core().hlen(Key.RECUR_AUTH_KEY));
        Assertions.assertEquals(authorityKey, rcommando.core().hget(Key.RECUR_AUTH_KEY, "abc"));
    }

    @Test
    void verifyPermitsWorksFromEmptyDb() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        dao.setAvailablePermits("permits", 2);
        Assertions.assertEquals(Arrays.asList("2", "1"), rcommando.core().lrange("permits.available", 0, -1));
        Assertions.assertEquals(Collections.emptyList(), rcommando.core().lrange("permits.assigned", 0, -1));
    }

    @Test
    void checkPermitsWorksWhenPermitsAreAvailable() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        rcommando.core().lpush("permits.available", "1", "2");
        Assertions.assertEquals(2, dao.getAvailablePermits("permits"));
    }

    @Test
    void checkPermitsWorksWhenPermitsAreNotAvailableAndThereIsNoBacklog() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        Assertions.assertEquals(0, dao.getAvailablePermits("permits"));
    }

    @Test
    void checkPermitsWorksWhenPermitsAreNotAvailableAndThereIsBacklog() {
        RetaskRepository dao = new RetaskRepository(rcommando);
        rcommando.core().lpush("permits.backlog", "task1", "task2");
        Assertions.assertEquals(-2, dao.getAvailablePermits("permits"));
    }
}

