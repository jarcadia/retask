package dev.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.jarcadia.redao.RedaoCommando;
import dev.jarcadia.redao.proxy.Proxy;

public class RetaskManager {
	
    private final Logger logger = LoggerFactory.getLogger(RetaskManager.class);

	private final RedaoCommando rcommando;
    private final Retask retask;
    private final RetaskRepository retaskRepository;
    private final RecruitmentResults handlers;
    private final ExecutorService executor;
    private final RetaskProcrastinator procrastinator;
    private final Set<WorkerProdvider> workerProviders;
    private final Map<Class<?>, Object> workerMap;

    private final RetaskTaskPopperRepository taskPopperPopperRepository;
    private final RetaskTaskPopper taskPopper;
    private final RetaskScheduledTaskPoller scheduledTaskPoller;
    private final AtomicBoolean shuttingDown;
    private final List<Runnable> preShutdownHooks;
    private final List<Runnable> postShutdownHooks;

    public RetaskManager(RedaoCommando rcommando, Retask retask, RetaskRepository retaskRepository,
            RecruitmentResults recruits) {
    	this.rcommando = rcommando;
    	this.retask = retask;
    	this.retaskRepository = retaskRepository;
    	this.handlers = recruits;
    	this.executor = Executors.newCachedThreadPool();
    	this.procrastinator = new RetaskProcrastinator();
    	this.workerProviders = ConcurrentHashMap.newKeySet();
    	this.workerMap = new ConcurrentHashMap<>();
    	this.shuttingDown = new AtomicBoolean();
    	this.preShutdownHooks = Collections.synchronizedList(new LinkedList<>());
        this.postShutdownHooks = Collections.synchronizedList(new LinkedList<>());

    	this.addWorker(new ExternalSetWorker());
    	
   	 	// Get handler methods by routingKey
        Map<String, List<HandlerMethod>> routes = recruits.getHandlersByRoutingKey();

        // Build delegates for each discovered route/method
        Map<String, RetaskDelegate> routeToDelegateMap = buildTaskDelegatesForRoutes(routes);

        // Create a routing delegator for the route -> delegate map
        RetaskDelegate router = new RetaskDelegateRouter(routeToDelegateMap);

        // Create a RawTaskHandler that invokes the routing delegator
        RawTaskHandler handler = new RetaskDelegatingTaskHandler(retaskRepository, router, procrastinator);

        // Create RetaskTaskPopper
        this.taskPopperPopperRepository = new RetaskTaskPopperRepository(rcommando.clone());
        this.taskPopper = new RetaskTaskPopper(taskPopperPopperRepository, executor, handler, procrastinator);

        // Create RetaskScheduledTaskPoller
        this.scheduledTaskPoller = new RetaskScheduledTaskPoller(retaskRepository, procrastinator);

        // Prevent rcommando from closing until task popping/scheduling have terminated and all tasks have completed
        rcommando.registerShutdownLatches(taskPopper.getDrainedLatch(), scheduledTaskPoller.getTerminatedLatch());
    }

    public void addWorkerProvider(WorkerProdvider provider) {
    	this.workerProviders.add(provider);
    }
    
    public void addWorker(Object worker) {
    	this.workerMap.put(worker.getClass(), worker);
    }
    
    public void addWorker(Object... workers) {
    	for (Object obj : workers) {
    		this.addWorker(obj);
    	}
    }
    
    public <T> void addWorker(Class<T> clazz, T worker) {
    	this.workerMap.put(clazz, worker);
    }

    public Set<String> verifyRoutes(Collection<String> routes) {
        return this.handlers.verifyRoutes(routes);
    }
    
    public <A extends Annotation> List<HandlerMethod> getHandlersByAnnotation(Class<A> annotationClass) {
        return this.handlers.getHandlers(annotationClass);
    }
    
    public Set<Class<? extends Proxy>> getDaoProxies() {
    	return this.handlers.getProxyClasses();
    }

    public void start(RetaskStartupCallback callback) {
    	this.start();
    	callback.onStartup(rcommando, retask);
    }
    
    public void start(Task task) {
    	this.start();
    	retask.submit(task);
    }

    public void start() {
        // Associate handler methods with instantiated worker instances at the last possible moment
    	for (HandlerMethod handler : handlers.getHandlers()) {
    		Object instance = getWorker(handler.getWorkerClass());
    		handler.setWorkerInstance(instance);
    	}

    	Runnable shutdownHook = () -> {
            try {
                shutdown(5, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                System.out.println("Timeout waiting for gracefully shutdown, retask shutdown may be unclean");
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook, "shutdown-hook"));


        // CALLING application should register the shutdown hook. It should call shutdown on RetaskManager
        // which will SYNCH finish tasks then return.
        //
        // It should
        //  Exit Popper, and Poller
        // Finish All tasks
        // Close RCommando
        //
        // ACTUALLY RetaskManager should have a disable, which will cause it to no longer poll for scheduled
        // tasks or pop new tasks.
        //
        // THEN the caller can continue to do whatever they want
        //
        // THEN the caller can call shutdown, will will ensure disable is already complete (if not it will be invoked)
        // THEN it will ensure all tasks are complete. THEN it will let RCommando close
        //
        // The CALLING application can then manually shutdown
        // LOG4j if desired, which will allowing logging of graceful shutdown messages (hopefully)
        // Seems like there's a better way than having a separate close and join method
        // Also not sure if the popper needs both closedFuture and gracefulShutdownFuture
        // ALSO is there a cleaner way of tracking than the AtomicInteger that has to poll?

        this.taskPopper.start();
        this.scheduledTaskPoller.start();
    }

    /**
     * Register a shutdown hook that will run after:
     *  1. Task popping has stopped
     *  2. Scheduled task polling has stopped
     *  3. All tasks have completed (queue is completely drained)
     *
     *  But will run before:
     *
     *  1. RedisCommando is closed
     *  2. JVM exits
     *
     * @param runnable
     */
    public void registerPreShutdownHook(Runnable runnable) {
        // Register the hook with RedisCommando to ensure runnable can run before RedisCommando is closed
        CountDownLatch cdl = new CountDownLatch(1);
        Runnable wrapper = () -> {
            runnable.run();
            cdl.countDown();
        };
        this.rcommando.registerShutdownLatches(cdl);
        this.preShutdownHooks.add(wrapper);
    }

    /**
     * Register a shutdown hook that will run after:
     *  1. RedisCommando is closed
     *
     *  But will run before:
     *
     *  1. JVM exits
     *
     * @param runnable
     */
    public void registerPostShutdownHook(Runnable runnable) {
        this.postShutdownHooks.add(runnable);
    }

    public void shutdown(long timeout, TimeUnit unit) throws TimeoutException {
        if (shuttingDown.compareAndSet(false, true)) {
            logger.info("Retask is shutting down");
            this.taskPopper.close();
            this.scheduledTaskPoller.close();
            this.scheduledTaskPoller.awaitTermination(timeout, unit);
            logger.info("Scheduled task poller successfully shutdown");
            this.taskPopper.awaitDrainComplete(timeout, unit);
            logger.info("Task popper successfully shutdown and all active tasks completed");

            for (Runnable runnable : preShutdownHooks) {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    logger.error("Error executing pre-shutdown hook", t);
                }

            }

            // Close rcommando explicitly (may or may not have already been invoked but the operation is idempotent)
            this.rcommando.close();

            logger.info("Retask successfully shutdown");

            for (Runnable runnable : postShutdownHooks) {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    logger.error("Error executing post-shutdown hook", t);
                }
            }
        } else {
            logger.info("Retask shutdown requested ignored (already shutting down)");
        }
    }
    
    private Map<String, RetaskDelegate> buildTaskDelegatesForRoutes(Map<String, List<HandlerMethod>> routes) {
        Map<String, RetaskDelegate> routeToDelegateMap = new HashMap<>();
        for (String routingKey : routes.keySet()) {
            List<HandlerMethod> workerMethods = routes.get(routingKey);
            routeToDelegateMap.put(routingKey, createDelegateForRoute(workerMethods));
        }
        return Map.copyOf(routeToDelegateMap);
    }

    private RetaskDelegate createDelegateForRoute(List<HandlerMethod> handlerMethods) {
        if (handlerMethods.size() == 1) {
            return createReflectiveTaskDelegate(handlerMethods.iterator().next());
        } else {
            return createRouteSplittingDelegate(handlerMethods);
        }
    }
    
    private RetaskDelegate createRouteSplittingDelegate(List<HandlerMethod> handlerMethods) {
        List<RetaskDelegate> delegates = new LinkedList<>();
        for (HandlerMethod handlerMethod : handlerMethods) {
            delegates.add(createReflectiveTaskDelegate(handlerMethod));
        }
        return new RouteSplittingDelegate(executor, delegates);
    }
    
    private RetaskDelegate createReflectiveTaskDelegate(HandlerMethod handlerMethod) {
        Method method = handlerMethod.getMethod();
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, retask, method.getParameters(), handlers.getProxyClasses());
        return new RetaskReflectiveTaskDelegate(handlerMethod.getWorkerInstance(), handlerMethod.getMethod(), paramsProducer);
    }
    
    private Object getWorker(Class<?> clazz) {
    	Object obj = workerMap.get(clazz);
    	if (obj != null) {
    		return obj;
    	} else {
            for (WorkerProdvider provider : workerProviders) {
            	try {
                    obj = provider.getInstance(clazz);
                    if (obj != null) {
                        return obj;
                    }
            	} catch (Exception ex) {
            		logger.warn("Instance provider threw exception", ex);
            	}
            }
    	}
    	throw new RetaskException("No instance provided for class " + clazz.getName());
    }
}
