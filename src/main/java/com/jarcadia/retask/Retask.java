package com.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.jarcadia.rcommando.FieldChangeCallback;
import com.jarcadia.rcommando.ObjectDeleteCallback;
import com.jarcadia.rcommando.ObjectInsertCallback;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.HandlerMethod.HandlerType;

import io.lettuce.core.RedisClient;

public class Retask {
    
    private static final Logger logger = LoggerFactory.getLogger(Retask.class);
    
    private final RetaskDao dao;
    private final RecruitmentResults recruitmentResults;

    private Retask(RetaskDao dao, RecruitmentResults recruitmentResults) {
        this.dao = dao;
        this.recruitmentResults = recruitmentResults;
    }
    
    public void submit(Task... tasks) {
        dao.submit(tasks);
    }
    
    public Future<Void> call(Task task) {
    	return dao.call(task, Void.class);
    }
    
    public <T> Future<T> call(Task task, Class<T> clazz) {
    	return dao.call(task, clazz);
    }
    
    public <T> Future<T> call(Task task, TypeReference<T> typeRef) {
    	return dao.call(task, typeRef);
    }
    
    public void revokeAuthority(String recurKey) {
        dao.revokeAuthority(recurKey);
    }

    public void setAvailablePermits(String permitKey, int numPermits) {
        dao.setAvailablePermits(permitKey, numPermits);
    }

    public int getAvailablePermits(String permitKey) {
        return this.dao.getAvailablePermits(permitKey);
    }

    public Set<String> verifyRecruits(Collection<String> requestedRoutes) {
        return this.recruitmentResults.verifyRecruits(requestedRoutes);
    }
    
    public <A extends Annotation> List<HandlerMethod<A>> getRecruitsByAnnontation(Class<A> annontationClass) {
        return this.recruitmentResults.getRecruitsFor(annontationClass);
    }
    
    public static RetaskManager init(RedisClient redis, RedisCommando rcommando, RetaskRecruiter recruiter, RetaskContext instanceProvider) {
        
        // Create internal prereqs
        ExecutorService executor = Executors.newCachedThreadPool();
        RetaskDao dao = new RetaskDao(rcommando);

        final RecruitmentResults recruitmentResults = recruiter.recruit();

        // Create public API
        Retask retask = new Retask(dao, recruitmentResults);
        
        // Setup insert handlers
        for (HandlerMethod<?> insertHandler : dedupeHandlersByRoutingKey(recruitmentResults.getRecruitsFor(HandlerType.INSERT))) {
            ObjectInsertCallback callback = (setKey, id) -> {
                Task task = Task.create(insertHandler.getRoutingKey()).param("object", Map.of("setKey", setKey, "id", id));
                dao.submit(task);
            };
            rcommando.registerObjectInsertCallback(insertHandler.getSetKey(), callback);
        }

        // Setup delete handlers
        for (HandlerMethod<?> deleteHandler : dedupeHandlersByRoutingKey(recruitmentResults.getRecruitsFor(HandlerType.DELETE))) {
            ObjectDeleteCallback callback = (setKey, id) -> {
                Task task = Task.create(deleteHandler.getRoutingKey()).param("id", id);
                dao.submit(task);
            };
            rcommando.registerObjectDeleteCallback(deleteHandler.getSetKey(), callback);
        }
        
        // Setup change handlers
        for (HandlerMethod<?> changeHandler : dedupeHandlersByRoutingKey(recruitmentResults.getRecruitsFor(HandlerType.CHANGE))) {
        	 FieldChangeCallback callback = (setKey, id, version, field, before, after) -> {
                 Task task = Task.create(changeHandler.getRoutingKey()).forChangedValue(setKey, id, before, after);
                 dao.submit(task);
                 logger.trace("Dispatched change task {}: {}.{}.{}: {} -> {}", task.getId(), setKey, id, field, before.getRawValue(), after.getRawValue());
             };
             rcommando.registerFieldChangeCallback(changeHandler.getSetKey(), changeHandler.getFieldName(), callback);
        }

        // Get handler methods by routingKey
        Map<String, List<HandlerMethod<?>>> routes = recruitmentResults.getHandlersByRoutingKey();
        
        // Build delegates for each discovered route/method
        Map<String, RetaskDelegate> routeToDelegateMap = buildTaskDelegatesForRoutes(rcommando, retask, executor, instanceProvider, routes);

        // Create a routing delegator for the route > delegate map
        RetaskDelegate router = new RetaskDelegateRouter(routeToDelegateMap);

        // Create a procrastinator for handler and poller
        RetaskProcrastinator procrastinator = new RetaskProcrastinator();
        
        // Create a handler that delegates to the routing delegator
        TaskHandler handler = new RetaskDelegatingTaskHandler(dao, router, procrastinator);

        // Create RetaskTaskPopper
        RetaskTaskPopperDao taskPopperDao = new RetaskTaskPopperDao(rcommando.clone());
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(taskPopperDao, executor, handler, procrastinator);

        // Create RetaskScheduledTaskPoller
        RetaskScheduledTaskPoller scheduledTaskPoller = new RetaskScheduledTaskPoller(dao, procrastinator);

        return new RetaskManager(taskPopper, scheduledTaskPoller, retask);
    }
    
    /**
     * There can be multiple insert, delete, or change callbacks for the same setKey/fieldName. These distinct 
     * handlers that overlap as described would share the same routingKey. On an insert/delete/change, only one task
     * needs to be created. The different handlers will be routed to automatically once the task is popped.
     * 
     * This helper method dedupes the routing keys in a list of HandlerMethods to ensure only one task is pushed for
     * each (potentially shared) routing key. Note it doesn't matter which HandlerMethod is chosen, as long as they
     * are deduped by routing key.
     */
    private static List<HandlerMethod<?>> dedupeHandlersByRoutingKey(List<HandlerMethod<?>> handlers) {
    	return handlers.stream()
                .collect(Collectors.groupingBy(HandlerMethod::getRoutingKey)).values().stream()
                .map(list -> list.stream().findAny().get())
                .collect(Collectors.toList());
    }

    private static Map<String, RetaskDelegate> buildTaskDelegatesForRoutes(RedisCommando rcommando, Retask retask, ExecutorService executor,
            RetaskContext instanceProvider, Map<String, List<HandlerMethod<?>>> routes) {
        Map<String, RetaskDelegate> routeToDelegateMap = new HashMap<>();
        for (String routingKey : routes.keySet()) {
            List<HandlerMethod<?>> workerMethods = routes.get(routingKey);
            routeToDelegateMap.put(routingKey, createDelegateForRoute(rcommando, retask, executor, instanceProvider, workerMethods));
        }
        return Collections.unmodifiableMap(routeToDelegateMap);
    }

    private static RetaskDelegate createDelegateForRoute(RedisCommando rcommando, Retask retask, ExecutorService executor, RetaskContext provider, List<HandlerMethod<?>> handlerMethods) {
        if (handlerMethods.size() == 1) {
            return createReflectiveTaskDelegate(rcommando, retask, provider, handlerMethods.iterator().next());
        } else {
            return createRouteSplittingDelegate(rcommando, retask, executor, provider, handlerMethods);
        }
    }
    
    private static RetaskDelegate createRouteSplittingDelegate(RedisCommando rcommando, Retask retask,  ExecutorService executor, RetaskContext provider, List<HandlerMethod<?>> handlerMethods) {
        List<RetaskDelegate> delegates = new LinkedList<>();
        for (HandlerMethod<?> handlerMethod : handlerMethods) {
            delegates.add(createReflectiveTaskDelegate(rcommando, retask, provider, handlerMethod));
        }
        return new RouteSplittingDelegate(executor, delegates);
    }
    
    private static RetaskDelegate createReflectiveTaskDelegate(RedisCommando rcommando, Retask retask, RetaskContext provider, HandlerMethod<?> handlerMethods) {
        Method method = handlerMethods.getMethod();
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, retask, method.getParameters());
        return new RetaskReflectiveTaskDelegate(provider, handlerMethods.getWorkerClass(), method, paramsProducer);
    }
}
