package dev.jarcadia;

import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.Test;

public class DaemonUnitTest {

    @Test
    void startsAndStopsCorrectly() throws InterruptedException {
//        RedisClient redisClient = RedisClient.create("redis://localhost/15");
//        Jarcadia jarcadia = Jarcadia.create()
//                .connectRedis(redisClient)
//                .initialize();
//        jarcadia.startDaemon();
//        jarcadia.close();
    }

    @Test
    void closesCorrectlyWhenNotStarted() {
//        RedisClient redisClient = RedisClient.create("redis://localhost/15");
//        Jarcadia jarcadia = Jarcadia.create()
//                .connectRedis(redisClient)
//                .initialize();
//        jarcadia.close();
    }
}
