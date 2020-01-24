package com.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class HandlerMethod<A extends Annotation> {
    
	private final HandlerType type;
    private final Class<?> workerClass;
    private final Method method;
    private final A annotation;
    private final Class<A> annontationClass;
    private final String routingKey;
    private final String setKey;
    private final String fieldName;
    
    
    public enum HandlerType {
    	TASK, CHANGE, INSERT, DELETE
    }

    public HandlerMethod(HandlerType type, Class<?> workerClass, Method method, A annotation, Class<A> annontationClass, String routingKey, String setKey, String fieldName) {
    	this.type = type;
        this.workerClass = workerClass;
        this.method = method;
        this.annotation = annotation;
        this.annontationClass = annontationClass;
        this.routingKey = routingKey;
        this.setKey = setKey;
        this.fieldName = fieldName;
    }
    
    public HandlerType getType() {
    	return type;
    }

    public Class<?> getWorkerClass() {
        return workerClass;
    }
    
    public Method getMethod() {
        return method;
    }
    
    public A getAnnontation() {
    	return annotation;
    }
    
    protected Class<A> getAnnontationClass() {
    	return annontationClass;
    }

	public String getRoutingKey() {
		return routingKey;
	}
	
	public String getSetKey() {
		return setKey;
	}
	
	public String getFieldName() {
		return fieldName;
	}
}
