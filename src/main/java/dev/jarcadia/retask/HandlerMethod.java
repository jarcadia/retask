package dev.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

public class HandlerMethod {
    
	private final HandlerSource source;
    private final Class<?> workerClass;
    private final Method method;
    private final Annotation annotation;
    private final Class<? extends Annotation> annontationClass;
    private final String routingKey;
    private final String type;
    private final String fieldName;
    private final AtomicReference<Object> workerInstanceRef;
    
    public enum HandlerSource {
    	TASK, CHANGE, INSERT, DELETE
    }

    protected HandlerMethod(HandlerSource source, Class<?> workerClass, Method method, Annotation annotation,
    		Class<? extends Annotation> annontationClass, String routingKey, String type, String fieldName) {
    	this.source = source;
        this.workerClass = workerClass;
        this.method = method;
        this.annotation = annotation;
        this.annontationClass = annontationClass;
        this.routingKey = routingKey;
        this.type = type;
        this.fieldName = fieldName;
        this.workerInstanceRef = new AtomicReference<>();
    }
    
    public HandlerSource getSource() {
    	return source;
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
	
	public String getType() {
		return type;
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
