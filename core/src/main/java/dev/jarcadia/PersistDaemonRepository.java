//package dev.jarcadia;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
///**
// * Responds to ping messages with some information, shows clients and other backends this backend is still alive + stats
// */
//class PersistDaemonRepository {
//
//    private final Logger logger = LoggerFactory.getLogger(PersistDaemonRepository.class);
//
//    private final Redis redis;
//
//    PersistDaemonRepository(Redis redis) {
//        this.redis = redis;
//    }
//
//    protected void connect() {
//        Subscription subscription = redis.subscribe("_ping", (channel, message) -> {
//            redis.publish("_pong", "pong");
//        });
//    }
//
//    protected void close() {
//        if (subscription != null) {
//            subscription.close();
//        }
//    }
//}
//
//
