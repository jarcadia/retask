package com.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.retask.HandlerMethod.HandlerType;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskDeleteHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskInsertHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

public class RetaskRecruiter {

    private final Logger logger = LoggerFactory.getLogger(RetaskRecruiter.class);	

	private final Set<RegistereHandlerAnnontation<?>> taskHandlerAnnontations;
	private final Set<Class<?>> classes;
	private final Set<String> packages;

	/**
	 * 
	 * 
	 * IDEAS
	 * when calling dao.call(Task) - i CAN look up the correct type via recruiter, as all workers must emcompass
	 * the same set of tasks. So it can be magically typed... but then does it require a cast
	 * 
	 * 
	 * 
	 */

	public RetaskRecruiter() {
		this.classes = new HashSet<>();
		this.packages = new HashSet<>();
		this.taskHandlerAnnontations = new HashSet<>();
	}
	
	public void recruitFromClass(Class<?> clazz) {
		this.classes.add(clazz);
	}
	
	public void recruitFromPackage(String packageName) {
		this.packages.add(packageName);
	}
	
	public <A extends Annotation> void registerTaskHandlerAnnontation(Class<A> annontationClass, RoutingKeyFactory<A> factory) {
		taskHandlerAnnontations.add(new RegisteredTaskHandlerAnnotation<A>(annontationClass, factory));
	}

	public <A extends Annotation> void registerChangeHandlerAnnontation(Class<A> clazz, ChangeKeyFactory<A> factory) {
		taskHandlerAnnontations.add(new RegisteredChangeHandlerAnnotation<A>(clazz, factory));
	}

	public <A extends Annotation> void registerInsertHandlerAnnontation(Class<A> clazz, SetKeyFactory<A> factory) {
		taskHandlerAnnontations.add(new RegisteredInsertHandlerAnnotation<A>(clazz, factory));
	}

	public <A extends Annotation> void registerDeleteHandlerAnnontation(Class<A> clazz, SetKeyFactory<A> factory) {
		taskHandlerAnnontations.add(new RegisteredDeleteHandlerAnnotation<A>(clazz, factory));
	}
	
	protected RecruitmentResults recruit() {
		 // Add predefined Retask Annotations to recruiter
        this.registerTaskHandlerAnnontation(RetaskHandler.class,
        		(clazz, method, annontation) -> annontation.value());
        this.registerChangeHandlerAnnontation(RetaskChangeHandler.class,
        		(clazz, method, annontation) -> new ChangeKey(annontation.setKey(), annontation.field()));
        this.registerInsertHandlerAnnontation(RetaskInsertHandler.class, 
        		(clazz, method, annontation) -> annontation.value());
        this.registerDeleteHandlerAnnontation(RetaskDeleteHandler.class, 
        		(clazz, method, annontation) -> annontation.value());
        
        
		// Scan all packages to find candidate classes
		for (String packageName : this.packages) {
			Reflections reflections = new Reflections(packageName);
			for (Class<?> clazz : reflections.getTypesAnnotatedWith(RetaskWorker.class)) {
				this.classes.add(clazz);
			}
		}

		// Scan all class methods to find HandlerMethods
		final Set<HandlerMethod<?>> handlers = new HashSet<>();
		for (Class<?> clazz : this.classes) {
			for (Method method : clazz.getMethods()) {
				for (RegistereHandlerAnnontation<?> registeredAnnontation : taskHandlerAnnontations) {
                    registeredAnnontation.check(clazz, method, handlers);
				}
			}
		}
		
        // Group handlers by type
		final Map<HandlerType, List<HandlerMethod<?>>> handlersByType = handlers.stream()
				.collect(Collectors.groupingBy(HandlerMethod::getType));
		
		// Group handlers by routing key
		final Map<String, List<HandlerMethod<?>>> handlersByRoutingKey = handlers.stream()
				.collect(Collectors.groupingBy(HandlerMethod::getRoutingKey));
		
		// Group handlers by annontation class
        final Map<Class<?>, List<HandlerMethod<?>>> handlersByAnnontationClass = handlers.stream()
        		.collect(Collectors.groupingBy(HandlerMethod::getAnnontationClass));
		
		// Output results
		handlersByType.get(HandlerType.CHANGE).forEach(h -> logger.info("Retask recruited change handler {}.{} for changes to {}.{}",
				h.getWorkerClass().getName(), h.getMethod().getName(), h.getSetKey(), h.getFieldName()));
		

		return new RecruitmentResults(handlersByType, handlersByRoutingKey, handlersByAnnontationClass);
	}

	@FunctionalInterface
	public interface RoutingKeyFactory<A extends Annotation> {
		public String createRoutingKey(Class<?> clazz, Method method, A annotation);
	}

	@FunctionalInterface
	public interface ChangeKeyFactory<A extends Annotation> {
		public ChangeKey createChangeKey(Class<?> clazz, Method method, A annotation);
	}

	@FunctionalInterface
	public interface SetKeyFactory<A extends Annotation> {
		public String createSetKey(Class<?> clazz, Method method, A annotation);
	} 

	public static class ChangeKey {
		private final String setKey;
		private final String fieldName;

		public ChangeKey(String setKey, String fieldName) {
			this.setKey = setKey;
			this.fieldName = fieldName;
		}

		public String getSetKey() {
			return this.setKey;
		}

		public String getFieldName() {
			return this.fieldName;
		}
	}
	
	private abstract class RegistereHandlerAnnontation<A extends Annotation> {
		private final Class<A> clazz;
		
		public RegistereHandlerAnnontation(Class<A> clazz) {
			this.clazz = clazz;
		}
		
		public void check(Class<?> targetClass, Method method, Set<HandlerMethod<? extends Annotation>> set) {
			A annontation = method.getAnnotation(clazz);
			if (annontation != null) {
				set.add(generate(targetClass, method, clazz, annontation));
			}
		}
		
		protected abstract HandlerMethod<A> generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation);
	}
	private class RegisteredTaskHandlerAnnotation<A extends Annotation> extends RegistereHandlerAnnontation<A> {

		private final RoutingKeyFactory<A> factory;

		public RegisteredTaskHandlerAnnotation(Class<A> clazz, RoutingKeyFactory<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod<A> generate(Class<?> targetClass, Method method, Class<A> annontationClass, A annontation) {
            String routingKey = factory.createRoutingKey(targetClass, method, annontation);
            return new HandlerMethod<>(HandlerType.TASK, targetClass, method, annontation, annontationClass, routingKey, null, null);
		}
	}

	private class RegisteredChangeHandlerAnnotation<A extends Annotation> extends RegistereHandlerAnnontation<A> {

		private final ChangeKeyFactory<A> factory;

		public RegisteredChangeHandlerAnnotation(Class<A> clazz, ChangeKeyFactory<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod<A> generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation) {
            ChangeKey changeKey = factory.createChangeKey(targetClass, method, annontation);
            String routingKey = "change." + changeKey.getSetKey() + "." + changeKey.getFieldName();
            return new HandlerMethod<A>(HandlerType.CHANGE, targetClass, method, annontation,
                    clazz, routingKey, changeKey.getSetKey(), changeKey.getFieldName());
		}
	}

	private class RegisteredInsertHandlerAnnotation<A extends Annotation> extends RegistereHandlerAnnontation<A> {

		private final SetKeyFactory<A> factory;

		public RegisteredInsertHandlerAnnotation(Class<A> clazz, SetKeyFactory<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod<A> generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation) {
            String setKey = factory.createSetKey(targetClass, method, annontation);
            String routingKey = "insert." + setKey;
            return new HandlerMethod<A>(HandlerType.INSERT, targetClass, method, annontation, clazz, routingKey, setKey, null);
		}
	}

	private class RegisteredDeleteHandlerAnnotation<A extends Annotation> extends RegistereHandlerAnnontation<A> {

		private final SetKeyFactory<A> factory;

		public RegisteredDeleteHandlerAnnotation(Class<A> clazz, SetKeyFactory<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod<A> generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation) {
            String setKey = factory.createSetKey(targetClass, method, annontation);
            String routingKey = "delete." + setKey;
            return new HandlerMethod<A>(HandlerType.DELETE, targetClass, method, annontation, clazz, routingKey, setKey, null);
		}
	}
}
