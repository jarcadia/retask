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
    
    public <A extends Annotation> List<HandlerMethod<A>> getHandlersByAnnontation(Class<A> annontationClass) {
        return this.recruitmentResults.getRecruitsFor(annontationClass);
    }
    
    public static RetaskManager init(RedisClient redis, RedisCommando rcommando, RetaskRecruiter recruiter) {
        
        // Create internal prereqs
        final RetaskDao dao = new RetaskDao(rcommando);

        // Scan for recruits
        final RecruitmentResults recruits = recruiter.recruit();
        
        // Create public API
        final Retask retask = new Retask(dao, recruits);
        
        // Setup insert handlers
        for (HandlerMethod<?> insertHandler : dedupeHandlersByRoutingKey(recruits.getRecruitsFor(HandlerType.INSERT))) {
            ObjectInsertCallback callback = (setKey, id) -> {
                Task task = Task.create(insertHandler.getRoutingKey()).param("object", Map.of("setKey", setKey, "id", id));
                dao.submit(task);
            };
            rcommando.registerObjectInsertCallback(insertHandler.getSetKey(), callback);
        }

        // Setup delete handlers
        for (HandlerMethod<?> deleteHandler : dedupeHandlersByRoutingKey(recruits.getRecruitsFor(HandlerType.DELETE))) {
            ObjectDeleteCallback callback = (setKey, id) -> {
                Task task = Task.create(deleteHandler.getRoutingKey()).param("id", id);
                dao.submit(task);
            };
            rcommando.registerObjectDeleteCallback(deleteHandler.getSetKey(), callback);
        }
        
        // Setup change handlers
        for (HandlerMethod<?> changeHandler : dedupeHandlersByRoutingKey(recruits.getRecruitsFor(HandlerType.CHANGE))) {
        	 FieldChangeCallback callback = (setKey, id, version, field, before, after) -> {
                 Task task = Task.create(changeHandler.getRoutingKey()).forChangedValue(setKey, id, before, after);
                 dao.submit(task);
                 logger.trace("Dispatched change task {}: {}.{}.{}: {} -> {}", task.getId(), setKey, id, field, before.getRawValue(), after.getRawValue());
             };
             rcommando.registerFieldChangeCallback(changeHandler.getSetKey(), changeHandler.getFieldName(), callback);
        }

        return new RetaskManager(rcommando, retask, dao, recruits);
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

  
}
