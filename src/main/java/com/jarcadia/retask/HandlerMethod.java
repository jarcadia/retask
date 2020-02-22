package com.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

public class HandlerMethod {
    
	private final HandlerType type;
    private final Class<?> workerClass;
    private final Method method;
    private final Annotation annotation;
    private final Class<? extends Annotation> annontationClass;
    private final String routingKey;
    private final String setKey;
    private final String fieldName;
    private final AtomicReference<Object> workerInstanceRef;
    
    public enum HandlerType {
    	TASK, CHANGE, INSERT, DELETE
    }

    protected HandlerMethod(HandlerType type, Class<?> workerClass, Method method, Annotation annotation,
    		Class<? extends Annotation> annontationClass, String routingKey, String setKey, String fieldName) {
    	this.type = type;
        this.workerClass = workerClass;
        this.method = method;
        this.annotation = annotation;
        this.annontationClass = annontationClass;
        this.routingKey = routingKey;
        this.setKey = setKey;
        this.fieldName = fieldName;
        this.workerInstanceRef = new AtomicReference<>();
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
    
    public Annotation getAnnontation() {
    	return annotation;
    }
    
    public Class<? extends Annotation> getAnnontationClass() {
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
	
	public AtomicReference<Object> getWorkerInstance() {
		return workerInstanceRef;
	}
	
	/*
	 * Only internal retask should set this instance, on startup
	 */
	protected void setWorkerInstance(Object workerInstance) {
		this.workerInstanceRef.set(workerInstance);
	}
}
