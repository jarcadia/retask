package com.jarcadia.retask;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;

import io.lettuce.core.RedisClient;

public class RedisCommandoUnitTest {

    static RedisClient redisClient;
    static RedisCommando rcommando;

    @BeforeAll
    public static void setup() {
        redisClient = RedisClient.create("redis://localhost/15");
        rcommando = RedisCommando.create(redisClient, new ObjectMapper());
    }

    @BeforeEach
    public void flush() {
        rcommando.core().flushdb();
    }

    @Test
    void verifyPermitsWorksFromEmptyDb() {
        RetaskDao dao = new RetaskDao(rcommando);
        dao.verifyPermits("permits", 2);
        Assertions.assertEquals(Arrays.asList("2", "1"), rcommando.core().lrange("permits.available", 0, -1));
        Assertions.assertEquals(Collections.emptyList(), rcommando.core().lrange("permits.assigned", 0, -1));
    }
    
    @Test
    void checkPermitsWorksWhenPermitsAreAvailable() {
        RetaskDao dao = new RetaskDao(rcommando);
        rcommando.core().lpush("permits.available", "1", "2");
        Assertions.assertEquals(-2, dao.checkPermits("permits"));
    }
    
    @Test
    void checkPermitsWorksWhenPermitsAreNotAvailableAndThereIsNoBacklog() {
        RetaskDao dao = new RetaskDao(rcommando);
        Assertions.assertEquals(0, dao.checkPermits("permits"));
    }
    
    @Test
    void checkPermitsWorksWhenPermitsAreNotAvailableAndThereIsBacklog() {
        RetaskDao dao = new RetaskDao(rcommando);
        rcommando.core().lpush("permits.backlog", "task1", "task2");
        Assertions.assertEquals(2, dao.checkPermits("permits"));
    }
}

