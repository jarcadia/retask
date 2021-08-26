package dev.jarcadia;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.jarcadia.iface.DmlEventHandler;
import dev.jarcadia.iface.PubSubMessageHandler;
import dev.jarcadia.iface.StartHandler;
import dev.jarcadia.iface.TaskHandler;
import dev.jarcadia.iface.TypedPubSubMessageHandler;
import io.lettuce.core.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Retask implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(Retask.class);

    /** Rcommando has a name. This name is used to submit events, register proof of life
     * and session info. Each Rcommando instance must have a unique name. It should detect if another active instance
     * is using the same name
     *
     * REDAO DAEMON is a THING - IT IS OPTIONAL, it TAKES Redao instance and does all the below stuff
     *
     * It also starts the external set popper and the session thread.
     * Session thread push latest timestamp somewhere as PoL
     * It also generates a GUID on startup as the session ID and stores it associated with the name.
     * Every proof of life should verify that session ID (if it is clobbered, names are being reused)
     *
     * On clean shutodnw, session ID, PoL are removed. On crash, PoL will become stale
     *
     * The PoL check could return any detected Stales, and return them for cleanup
     *
     * Instant check, like CLI, can check PoL timestamps. Continuous check, like frontend will contiously verify PoL
     * as well as listening for events
     *
     *
     * Replace cloneRedao with using separate connections instead. RCommando is more than just a wrapper on
     * Lettuce. You can't clone redao. I think redao should exit if the session ID is stolen
     */

    private final ObjectMapper objectMapper;

    private final Procrastinator procrastinator;

    private final RedisClient redisClient;
    private final RedisConnection primaryRc;


    private final AtomicBoolean closing;
    private final List<java.util.concurrent.CountDownLatch> shutdownLatches;
//    private final PersistDaemon daemon;


    private final TaskQueuingService taskQueuingService;
    private final TaskQueuingRepository taskQueuingRepository;

    private final PermitRepository permitRepository;

    private final DmlEventEntryHandler recordOpHandler;
    private final TaskEntryHandler taskEntryHandler;
    private final ReturnValueHandler returnValueHandler;

    private final ExecutorService executor;

    private final QueuePopper queuePopper;
    private final SchedulePopper schedulePopper;

    public static RetaskConfig configure() {
        return new RetaskConfig();
    }

    private final Set<RedisConnection> connections;
    private final LifeCycleCallbackHandler lifeCycleCallbackHandler;


    Retask(RedisClient redisClient, ObjectMapper objectMapper, boolean flushDatabase) {
        this.objectMapper = RetaskJson.decorate(objectMapper);
//        this.formatter = new ValueFormatter(objectMapper);

        this.procrastinator = new Procrastinator();

        this.redisClient = redisClient;
        this.primaryRc = new RedisConnection(redisClient, objectMapper);
        if (flushDatabase) {
            this.primaryRc.commands().flushdb();
        }

        this.taskQueuingRepository = new TaskQueuingRepository(primaryRc,  procrastinator);
        this.taskQueuingService = new TaskQueuingService(redisClient, objectMapper, taskQueuingRepository);

        this.closing = new AtomicBoolean(false);
        this.connections = Collections.synchronizedSet(new HashSet<>());
        this.shutdownLatches = Collections.synchronizedList(new LinkedList<>());

//        this.daemon = new PersistDaemon(this, daemonRepository, new Procrastinator());

        this.executor = Executors.newCachedThreadPool();

        QueuePopperRepository popperRepo = new QueuePopperRepository(redisClient, objectMapper, "TODO");

        this.returnValueHandler = new ReturnValueHandler(taskQueuingService);
        this.recordOpHandler = new DmlEventEntryHandler(executor, objectMapper, returnValueHandler);
        this.permitRepository = new PermitRepository(primaryRc);
        TaskFinalizingRepository taskFinalizingRepository = new TaskFinalizingRepository(primaryRc, objectMapper);
        this.taskEntryHandler = new TaskEntryHandler(executor, objectMapper, primaryRc, returnValueHandler,
                taskQueuingRepository, permitRepository, taskFinalizingRepository);

        QueueEntryHandler queueEntryHandler = new QueueEntryHandler(recordOpHandler, taskEntryHandler);

        this.queuePopper = new QueuePopper(popperRepo, queueEntryHandler, procrastinator);

        SchedulePopperRepository schedulePopperRepo = new SchedulePopperRepository(redisClient, objectMapper);
        SchedulePopperService schedulePopperService = new SchedulePopperService(schedulePopperRepo, objectMapper,
            new CronService(), 100);

        schedulePopper = new SchedulePopper(schedulePopperService, procrastinator);

        this.lifeCycleCallbackHandler = new LifeCycleCallbackHandler(executor, returnValueHandler);
    }

    public void start() {

        queuePopper.start();
        schedulePopper.start();

        lifeCycleCallbackHandler.invokeStartCallbacks();
    }

    /**
     * Opens a new RedisConnection.
     */
    public RedisConnection openRedisConnection() {
        RedisConnection connection = new RedisConnection(redisClient, objectMapper);
        this.connections.add(connection);
        return connection;
    }

//    public SimpleSubscription subscribe(String channel, Consumer<String> consumer) {
//        return new SimpleSubscription(redisClient, channel, consumer);
//    }

    public ObjectMapper getObjectMapper() {
    	return this.objectMapper;
    }

    public void submit(Task.Builder task) {
        taskQueuingService.submitTask(task);
    }

    protected boolean submitDmlEvent(long eventId, String statement, String table, String data) {
        return taskQueuingService.submitDmlEvent(eventId, statement, table, data);
    }

    public CompletableFuture<String> call(Task.Builder task) {
        return taskQueuingService.callTask(task);
    }

    public <T> CompletableFuture<T> call(Task.Builder task, Class<T> type) {
        return taskQueuingService.callTask(task, type);
    }

    public <T> CompletableFuture<T> call(Task.Builder task, TypeReference<T> typeRef) {
        return taskQueuingService.callTask(task, typeRef);
    }

    public <T, V> CompletableFuture<Map<T, V>> callTaskForEach(Set<T> input,
            Function<T, Task.Builder> tasker, TypeReference<V> typeRef) {
        return taskQueuingService.callTaskForEach(input, tasker, typeRef);
    }

    public Set<String> verifyRoutes(Collection<String> routes) {
        return Set.of();
    }

    public void cancelRecurrence(String route) {
        throw new RuntimeException("TODO");
    }

    public int setPermitCap(String permitKey, int cap) {
        return permitRepository.setPermitCap(permitKey, cap);
    }

    public long getPermitBacklogCount(String permitKey) {
        return permitRepository.getPermitBacklogCount(permitKey);
    }

    public SimpleSubscription subscribe(String channel, PubSubMessageHandler handler) {
        return SimpleSubscription.create(redisClient, channel, handler);
    }

    protected <T> SimpleSubscription subscribe(String channel, Class<T> type, TypedPubSubMessageHandler<T> handler) {
        return SimpleSubscription.create(redisClient, objectMapper, channel, type, handler);
    }

    protected <T> SimpleSubscription subscribe(String channel, TypeReference<T> typeRef, TypedPubSubMessageHandler<T> handler) {
        return SimpleSubscription.create(redisClient, objectMapper, channel, typeRef, handler);
    }

    public Runnable registerStartHandler(StartHandler startHandler) {
        return lifeCycleCallbackHandler.registerStartHandler(startHandler);
    }

    public Runnable registerTaskHandler(String route, TaskHandler handler) {
        return taskEntryHandler.registerTaskHandler(route, handler);
    }

    public Runnable registerInsertHandler(String table, DmlEventHandler handler) {
        return recordOpHandler.registerInsertHandler(table, handler);
    }

    public Runnable registerUpdateHandler(String table, DmlEventHandler handler) {
        return recordOpHandler.registerUpdateHandler(table, handler);
    }

    public Runnable registerUpdateHandler(String table, String[] fields, DmlEventHandler handler) {
        return recordOpHandler.registerUpdateHandler(table, fields, handler);
    }

    public Runnable registerDeleteHandler(String table, DmlEventHandler handler) {
        return recordOpHandler.registerDeleteHandler(table, handler);
    }

    public void registerShutdownLatches(CountDownLatch... latches) {
        for (CountDownLatch latch : latches) {
            this.shutdownLatches.add(latch);
        }
    }

    @Override
    public void close() {
        // Close the daemon (even if not started)
//        this.daemon.close();

        if (closing.compareAndSet(false, true)) {
            for (java.util.concurrent.CountDownLatch blocker : shutdownLatches) {
                try {
                    blocker.await();
                } catch (InterruptedException e) {
                    new RuntimeException("Interrupted while waiting for RedisCommando shutdown latch").printStackTrace();
                }
            }

            schedulePopper.close();
            schedulePopper.awaitShutdown();

            queuePopper.close();
            queuePopper.awaitDrainComplete();
            logger.info("Queue Popper has drained");

            primaryRc.close();
            for (RedisConnection connection : connections) {
                connection.close();
            }
        }
    }
}