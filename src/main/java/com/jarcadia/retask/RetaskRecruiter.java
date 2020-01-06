package com.jarcadia.retask;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;

import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskDeleteHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskInsertHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

public class RetaskRecruiter {
    
    private final Map<String, Set<WorkerHandlerMethod>> recruits;
    private final Set<String> requestedInsertHandlers;
    private final Set<String> requestedDeleteHandlers;
    private final Map<String, Set<String>> requestedChangeHandlers;
    
    public RetaskRecruiter() {
        this.recruits = new HashMap<>();
        this.requestedInsertHandlers = new HashSet<>();
        this.requestedDeleteHandlers = new HashSet<>();
        this.requestedChangeHandlers = new HashMap<>();
    }

    public void recruitFromClass(Class<?> clazz) {
        if (clazz.getAnnotation(RetaskWorker.class) != null) {
            for (Method method : clazz.getMethods()) {
                RetaskHandler handlerAnnotation = method.getAnnotation(RetaskHandler.class);
                if (handlerAnnotation != null) {
                    recruits.computeIfAbsent(handlerAnnotation.value(), k -> new HashSet<>()).add(new WorkerHandlerMethod(clazz, method));
                }

                RetaskInsertHandler insertHandlerAnnontation = method.getAnnotation(RetaskInsertHandler.class);
                if (insertHandlerAnnontation != null) {
                    String routingKey = "insert." + insertHandlerAnnontation.value();
                    recruits.computeIfAbsent(routingKey, k -> new HashSet<>()).add(new WorkerObjectHandlerMethod(clazz, method, insertHandlerAnnontation.value()));
                    requestedInsertHandlers.add(insertHandlerAnnontation.value());
                }

                RetaskDeleteHandler deleteHandlerAnnontation = method.getAnnotation(RetaskDeleteHandler.class);
                if (deleteHandlerAnnontation != null) {
                    String routingKey = "delete." + deleteHandlerAnnontation.value();
                    recruits.computeIfAbsent(routingKey, k -> new HashSet<>()).add(new WorkerObjectHandlerMethod(clazz, method, deleteHandlerAnnontation.value()));
                    requestedDeleteHandlers.add(deleteHandlerAnnontation.value());
                }

                RetaskChangeHandler changeHandlerAnnontation = method.getAnnotation(RetaskChangeHandler.class);
                if (changeHandlerAnnontation != null) {
                    String routingKey = "change." + changeHandlerAnnontation.mapKey() + "." + changeHandlerAnnontation.field();
                    recruits.computeIfAbsent(routingKey, k -> new HashSet<>()).add(new WorkerObjectHandlerMethod(clazz, method, changeHandlerAnnontation.mapKey()));
                    requestedChangeHandlers.computeIfAbsent(changeHandlerAnnontation.mapKey(), k -> new HashSet<>()).add(changeHandlerAnnontation.field());
                }
            }
        }
    }

    public void recruitFromPackage(String packageName) {
        Reflections reflections = new Reflections(packageName);
        for (Class<?> clazz : reflections.getTypesAnnotatedWith(RetaskWorker.class)) {
            recruitFromClass(clazz);
        }
    }

    protected Map<String, Set<WorkerHandlerMethod>> getRecruits() {
        return this.recruits;
    }

    protected Set<String> getInsertHandlerKeys() {
        return requestedInsertHandlers;
    }

    protected Set<String> getDeleteHandlerKeys() {
        return requestedDeleteHandlers;
    }

    protected Map<String, Set<String>> getChangeHandlerKeys() {
        return requestedChangeHandlers;
    }
}
