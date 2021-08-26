//package dev.jarcadia;
//
//import dev.jarcadia.exception.PersistException;
//import io.lettuce.core.RedisCommandInterruptedException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Closeable;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
///**
// * Redao Daemon maintains proof-of-life, processes external sets and life-cycle events
// */
//public class PersistDaemon implements Runnable, Closeable {
//
//    private final Logger logger = LoggerFactory.getLogger(PersistDaemon.class);
//
//    private final Persistor rcommando;
//    private final PersistDaemonRepository daemonRepository;
//    private final Procrastinator procrastinator;
//    private final Thread thread;
//    private final CountDownLatch started;
//    private final CountDownLatch drained;
//
//    private final RedisRepository redis;
//    private Subscription subscription;
//
//    public PersistDaemon(RedisRepository redisRepository) {
//        this.redis = redisRepository;
//    }
//
//    public void start() {
//
//        this.subscription = redis.subscribe("_ping", (channel, message) -> {
//            redis.publish("_pong", "pong");
//        });
//
//
//        this.thread.start();
//        try {
//            this.started.await(1, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            throw new PersistException("Interrupted while starting persist daemon");
//        }
//    }
//
//    @Override
//    public void run() {
//        logger.info("Starting redao daemon");
//
//        rcommando.registerShutdownLatches(this.getDrainedLatch());
//        daemonRepository.connect();
//
//        started.countDown();
//
//        while (!Thread.interrupted()) {
//            try {
//                // Pop update
//                String update = daemonRepository.popUpdate();
//                if (update == null) {
//                    // Blocking timeout, no update popped
//                    continue;
//                } else {
//                    try {
//                        // Parse update and apply with rcommando
//                        logger.trace("Popped update {} {}", update);
//
//                    } catch (Throwable t) {
//
//                    }
//                }
//            } catch(RedisCommandInterruptedException ex) {
//                // This exception is thrown a blpop operation is interrupted
//                break;
//            } catch (Throwable t) {
//                logger.warn("Unexpected exception while popping task queue. Retrying in 1 second", t);
//                try {
//                    procrastinator.sleepFor(1000);
//                }
//                catch (InterruptedException e) {
//                    break;
//                }
//            }
//        }
//        // Loop has exited which means all popped updates have completed
//        daemonRepository.close();
//        drained.countDown();
//        logger.info("Exited redao daemon");
//    }
//
//    @Override
//    public void close() {
//        this.thread.interrupt();
//    }
//
//    protected CountDownLatch getDrainedLatch() {
//        return drained;
//    }
//
//    protected void awaitDrainComplete() {
//        try {
//            this.drained.await();
//        } catch (InterruptedException e) {
//            logger.error("Interrupted while waiting for updates to complete");
//        }
//    }
//
//    protected void awaitDrainComplete(long timeout, TimeUnit unit) throws TimeoutException {
//        try {
//            boolean drained = this.drained.await(timeout, unit);
//            if (!drained) {
//                throw new TimeoutException("Timeout while waiting for updates to complete");
//            }
//        } catch (InterruptedException e) {
//            logger.error("Interrupted while waiting for updates to complete");
//        }
//    }
//}
