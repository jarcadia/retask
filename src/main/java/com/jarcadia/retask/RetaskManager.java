package com.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.proxy.DaoProxy;

public class RetaskManager {
	
    private final Logger logger = LoggerFactory.getLogger(RetaskManager.class);

	private final RedisCommando rcommando;
    private final Retask retask;
    private final RetaskRepository dao;
    private final RecruitmentResults handlers;
    private final ExecutorService executor;
    private final RetaskProcrastinator procrastinator;
    private final Set<InstanceProvider> instanceProviders;
    private final Map<Class<?>, Object> instanceMap;
	
    private final RetaskTaskPopperDao taskPopperDao;
    private final RetaskTaskPopper taskPopper;
    private final RetaskScheduledTaskPoller scheduledTaskPoller;
    
    public RetaskManager(RedisCommando rcommando, Retask retask, RetaskRepository dao, RecruitmentResults recruits) {
    	this.rcommando = rcommando;
    	this.retask = retask;
    	this.dao = dao;
    	this.handlers = recruits;
    	this.executor = Executors.newCachedThreadPool();
    	this.procrastinator = new RetaskProcrastinator();
    	this.instanceProviders = ConcurrentHashMap.newKeySet();
    	this.instanceMap = new ConcurrentHashMap<>();
    	
    	this.addInstance(new ExternalSetWorker());
    	
   	 	// Get handler methods by routingKey
        Map<String, List<HandlerMethod>> routes = recruits.getHandlersByRoutingKey();

        // Build delegates for each discovered route/method
        Map<String, RetaskDelegate> routeToDelegateMap = buildTaskDelegatesForRoutes(routes);

        // Create a routing delegator for the route > delegate map
        RetaskDelegate router = new RetaskDelegateRouter(routeToDelegateMap);

        // Create a handler that delegates to the routing delegator
        RawTaskHandler handler = new RetaskDelegatingTaskHandler(dao, router, procrastinator);

        // Create RetaskTaskPopper
        this.taskPopperDao = new RetaskTaskPopperDao(rcommando.clone());
        this.taskPopper = new RetaskTaskPopper(taskPopperDao, executor, handler, procrastinator);

        // Create RetaskScheduledTaskPoller
        this.scheduledTaskPoller = new RetaskScheduledTaskPoller(dao, procrastinator);
    }

    public void addInstanceProvider(InstanceProvider provider) {
    	this.instanceProviders.add(provider);
    }
    
    public void addInstance(Object obj) {
    	this.instanceMap.put(obj.getClass(), obj);
    }
    
    public void addInstances(Object... objs) {
    	for (Object obj : objs) {
    		this.addInstance(obj);
    	}
    }
    
    public <T> void addInstance(Class<T> clazz, T worker) {
    	this.instanceMap.put(clazz, worker);
    }

    public Set<String> verifyRoutes(Collection<String> routes) {
        return this.handlers.verifyRoutes(routes);
    }
    
    public <A extends Annotation> List<HandlerMethod> getHandlersByAnnontation(Class<A> annontationClass) {
        return this.handlers.getHandlers(annontationClass);
    }
    
    public Set<Class<? extends DaoProxy>> getDaoProxies() {
    	return this.handlers.getProxyClasses();
    }

    public void start(RetaskStartupCallback callback) {
    	this.start();
    	callback.onStartup(retask);
    }
    
    public void start(Task task) {
    	this.start();
    	retask.submit(task);
    }

    public void start() {
    	for (HandlerMethod handler : handlers.getHandlers()) {
    		Object instance = getWorkerInstance(handler.getWorkerClass());
    		handler.setWorkerInstance(instance);
    	}
        this.taskPopper.start();
        this.scheduledTaskPoller.start();
    }

    public void shutdown(long timeout, TimeUnit unit) throws TimeoutException {
        this.taskPopper.close();
        this.scheduledTaskPoller.close();
        this.taskPopper.join(timeout, unit);
        this.scheduledTaskPoller.join(timeout, unit);
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
    
    private Object getWorkerInstance(Class<?> clazz) {
    	Object obj = instanceMap.get(clazz);
    	if (obj != null) {
    		return obj;
    	} else {
            for (InstanceProvider provider : instanceProviders) {
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
