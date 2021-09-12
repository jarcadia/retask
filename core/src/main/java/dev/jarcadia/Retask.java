package dev.jarcadia;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.iface.DmlEventHandler;
import dev.jarcadia.iface.ReturnValueHandler;
import dev.jarcadia.iface.StartHandler;
import dev.jarcadia.iface.TaskHandler;
import dev.jarcadia.redis.RedisConnection;
import dev.jarcadia.redis.RedisFactory;
import io.lettuce.core.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class Retask implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(Retask.class);

    private final ObjectMapper objectMapper;

    private final RedisFactory redisFactory;
    private final Set<RedisConnection> connections;

    private final AtomicBoolean closing;
    private final List<java.util.concurrent.CountDownLatch> shutdownLatches;
//    private final PersistDaemon daemon;

    private final TaskQueuingService taskQueuingService;

    private final PermitRepository permitRepository;

    private final ReturnValueService returnValueService;
    private final DmlEventEntryHandler dmlEventEntryHandler;
    private final TaskEntryHandler taskEntryHandler;

    private final QueuePopper queuePopper;
    private final SchedulePopper schedulePopper;

    private final LifeCycleCallbackHandler lifeCycleCallbackHandler;

    public static RetaskConfig configure() {
        return new RetaskConfig();
    }

    Retask(RedisClient redisClient, ObjectMapper objectMapper, boolean flushDatabase) {

        this.closing = new AtomicBoolean(false);
        this.connections = Collections.synchronizedSet(new HashSet<>());
        this.shutdownLatches = Collections.synchronizedList(new LinkedList<>());

        this.objectMapper = RetaskJson.decorate(objectMapper);
        this.redisFactory = new RedisFactory(redisClient, objectMapper);

        RedisConnection primaryRc = redisFactory.openConnection();
        this.connections.add(primaryRc);
        if (flushDatabase) {
            primaryRc.commands().flushdb();
        }

        Procrastinator procrastinator = new Procrastinator();
        TaskQueuingRepository taskQueuingRepository = new TaskQueuingRepository(primaryRc, procrastinator);
        this.taskQueuingService = new TaskQueuingService(redisClient, objectMapper, taskQueuingRepository);

//        this.daemon = new PersistDaemon(this, daemonRepository, new Procrastinator());

        ExecutorService executor = Executors.newCachedThreadPool();
        this.returnValueService = new ReturnValueService();
        this.returnValueService.registerHandler(Task.Builder.class, taskQueuingService::submitTask);
        this.dmlEventEntryHandler = new DmlEventEntryHandler(executor, objectMapper, returnValueService);
        this.permitRepository = new PermitRepository(primaryRc);
        TaskFinalizingRepository taskFinalizingRepository = new TaskFinalizingRepository(primaryRc, objectMapper);
        this.taskEntryHandler = new TaskEntryHandler(executor, objectMapper, primaryRc, returnValueService,
                taskQueuingRepository, permitRepository, taskFinalizingRepository);

        QueuePopperRepository popperRepo = new QueuePopperRepository(redisFactory, "TODO");
        QueueEntryHandler queueEntryHandler = new QueueEntryHandler(dmlEventEntryHandler, taskEntryHandler);
        this.queuePopper = new QueuePopper(popperRepo, queueEntryHandler, procrastinator);

        SchedulePopperRepository schedulePopperRepo = new SchedulePopperRepository(redisFactory);
        SchedulePopperService schedulePopperService = new SchedulePopperService(schedulePopperRepo, objectMapper,
            new CronService(), 100);

        schedulePopper = new SchedulePopper(schedulePopperService, procrastinator);

        this.lifeCycleCallbackHandler = new LifeCycleCallbackHandler(executor, returnValueService);
    }

    public void start() {
        queuePopper.start();
        schedulePopper.start();
        lifeCycleCallbackHandler.invokeStartCallbacks();
    }

    public RedisFactory getRedisFactory() {
        return this.redisFactory;
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
        return taskQueuingService.callTask(task, type, null);
    }

    public <T> CompletableFuture<T> call(Task.Builder task, TypeReference<T> typeRef) {
        return taskQueuingService.callTask(task, null, typeRef);
    }

    public <T, V> CompletableFuture<Map<T, V>> callTaskForEach(Collection<T> input,
            Function<T, Task.Builder> tasker, Class<V> type) {
        return taskQueuingService.callTaskForEach(input, tasker, type, null);
    }

    public <T, V> CompletableFuture<Map<T, V>> callTaskForEach(Collection<T> input,
            Function<T, Task.Builder> tasker, TypeReference<V> typeRef) {
        return taskQueuingService.callTaskForEach(input, tasker, null, typeRef);
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

    public Runnable registerStartHandler(StartHandler startHandler) {
        return lifeCycleCallbackHandler.registerStartHandler(startHandler);
    }

    public Runnable registerTaskHandler(String route, TaskHandler handler) {
        return taskEntryHandler.registerTaskHandler(route, handler);
    }

    public Runnable registerInsertHandler(String table, DmlEventHandler handler) {
        return dmlEventEntryHandler.registerInsertHandler(table, handler);
    }

    public Runnable registerUpdateHandler(String table, DmlEventHandler handler) {
        return dmlEventEntryHandler.registerUpdateHandler(table, handler);
    }

    public Runnable registerUpdateHandler(String table, String[] fields, DmlEventHandler handler) {
        return dmlEventEntryHandler.registerUpdateHandler(table, fields, handler);
    }

    public Runnable registerDeleteHandler(String table, DmlEventHandler handler) {
        return dmlEventEntryHandler.registerDeleteHandler(table, handler);
    }

    public <T> void registerReturnValueHandler(Class<T> type, ReturnValueHandler<T> returnValueHandler) {
        returnValueService.registerHandler(type, returnValueHandler);
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

            for (RedisConnection connection : connections) {
                connection.close();
            }
        }
    }
}