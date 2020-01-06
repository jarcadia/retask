package com.jarcadia.retask;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.CheckedDeleteHandler;
import com.jarcadia.rcommando.CheckedInsertHandler;
import com.jarcadia.rcommando.CheckedSetUpdateHandler;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisValue;

import io.lettuce.core.RedisClient;

// TODO MAKE THIS A STATIC METHOD IN RetaskService and rename RetaskService to Retask
public class RetaskInit {
    
    private static final Logger logger = LoggerFactory.getLogger(RetaskInit.class);
    
    public static RetaskService init(RedisClient redis, RedisCommando rcommando, RetaskRecruiter recruiter, RetaskWorkerInstanceProvider instanceProvider) {
        
        // Create internal prereqs
        ObjectMapper objectMapper = new ObjectMapper();
        ExecutorService executor = Executors.newCachedThreadPool();
        RetaskDao dao = new RetaskDao(rcommando);

        // Scan specified for @RetaskWorkers
        Map<String, Set<WorkerHandlerMethod>> routes = recruiter.getRecruits();
        
        // Setup insert handlers
        Set<String> insertHandlerKeys = recruiter.getInsertHandlerKeys();
        for (String mapKey : insertHandlerKeys) {
            CheckedInsertHandler handler = new CheckedInsertHandler() {
                
                @Override
                public void onInsert(String mapKey, String id) {
                    String routingKey = "insert." + mapKey;
                    Retask task = Retask.create(routingKey).forInsertedObject(id);
                    dao.submit(task);
                }
            };
            rcommando.registerCheckedInsertHandler(mapKey, handler);
        }

        // Setup delete handlers
        Set<String> deleteHandlerKeys = recruiter.getDeleteHandlerKeys();
        for (String mapKey : deleteHandlerKeys) {
            CheckedDeleteHandler handler = new CheckedDeleteHandler() {
                
                @Override
                public void onDelete(String mapKey, String id) {
                    String routingKey = "delete." + mapKey;
                    Retask task = Retask.create(routingKey).forDeletedObject(id);
                    dao.submit(task);
                }
            };
            rcommando.registerCheckedDeleteHandler(mapKey, handler);
        }
        
        // Setup change handlers
        Map<String, Set<String>> changeHandlerKeys = recruiter.getChangeHandlerKeys();
        for (String mapKey : changeHandlerKeys.keySet()) {
            for (String field : changeHandlerKeys.get(mapKey)) {
                CheckedSetUpdateHandler listener = new CheckedSetUpdateHandler() {
                    
                    @Override
                    public void onChange(String mapKey, String id, long version, String field, RedisValue before, RedisValue after) {
                        String routingKey = "change." + mapKey + "." + field;
                        Retask task = Retask.create(routingKey).forChangedValue(id, before, after);
                        dao.submit(task);
//                        logger.info("Dispatched change task {}: {}.{}.{}: {} -> {}", task.getName(), mapKey, id, field, before.getRawValue(), after.getRawValue());
                    }
                };
                rcommando.registerCheckedSetUpdateHandler(mapKey, field, listener);
            }
        }

        // Build delegates for each discovered route/method
        Map<String, RetaskDelegate> routeToDelegateMap = buildTaskDelegatesForRoutes(rcommando, objectMapper, instanceProvider, routes);

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

        // Create instance of RetaskService (exposes public API for Retask)
        RetaskService retaskService = new RetaskService(dao, taskPopper, scheduledTaskPoller);
        return retaskService;
    }

    private static Map<String, RetaskDelegate> buildTaskDelegatesForRoutes(RedisCommando rcommando, ObjectMapper objectMapper,
            RetaskWorkerInstanceProvider instanceProvider, Map<String, Set<WorkerHandlerMethod>> routes) {
        Map<String, RetaskDelegate> routeToDelegateMap = new HashMap<>();
        for (String routingKey : routes.keySet()) {
            Set<WorkerHandlerMethod> workerMethods = routes.get(routingKey);
            routeToDelegateMap.put(routingKey, createDelegateForRoute(rcommando, objectMapper, instanceProvider, workerMethods));
        }
        return Collections.unmodifiableMap(routeToDelegateMap);
    }

    private static RetaskDelegate createDelegateForRoute(RedisCommando rcommando, ObjectMapper objectMapper, RetaskWorkerInstanceProvider provider, Set<WorkerHandlerMethod> workerMethods) {
        if (workerMethods.size() == 1) {
            return createReflectiveTaskDelegate(rcommando, objectMapper, provider, workerMethods.iterator().next());
        } else {
            return createDuplicateRoutingKeyDelegate(rcommando, objectMapper, provider, workerMethods);
        }
    }
    
    private static RetaskDelegate createDuplicateRoutingKeyDelegate(RedisCommando rcommando, ObjectMapper objectMapper, RetaskWorkerInstanceProvider provider, Set<WorkerHandlerMethod> workerMethods) {
        Set<RetaskDelegate> delegates = new HashSet<>();
        for (WorkerHandlerMethod workerMethod : workerMethods) {
            delegates.add(createReflectiveTaskDelegate(rcommando, objectMapper, provider, workerMethod));
        }
        return new RetaskDuplicateRoutingKeyDelegate(delegates.toArray(new RetaskDelegate[0]));
    }
    
    private static RetaskDelegate createReflectiveTaskDelegate(RedisCommando rcommando, ObjectMapper objectMapper, RetaskWorkerInstanceProvider provider, WorkerHandlerMethod workerMethod) {
        Class<?> clazz = workerMethod.getWorkerClass();
        Method method = workerMethod.getMethod();
        if (workerMethod instanceof WorkerObjectHandlerMethod) {
            WorkerObjectHandlerMethod objectHandlerMethod = (WorkerObjectHandlerMethod) workerMethod;
            return RetaskReflectiveTaskDelegate.createObjectHandlerDelegate(rcommando, objectMapper, provider, clazz, method, objectHandlerMethod.getMapKey());
        } else {
            return RetaskReflectiveTaskDelegate.createHandlerDelegate(rcommando, objectMapper, provider, clazz, method);
        }
    }
}
