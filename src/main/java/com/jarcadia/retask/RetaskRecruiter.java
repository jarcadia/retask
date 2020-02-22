package com.jarcadia.retask;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URL;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.vfs.SystemDir;
import org.reflections.vfs.Vfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.proxy.DaoProxy;
import com.jarcadia.retask.HandlerMethod.HandlerType;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskDeleteHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskInsertHandler;

public class RetaskRecruiter {

    private final Logger logger = LoggerFactory.getLogger(RetaskRecruiter.class);	

	private final Set<RegisteredHandlerAnnontation<?>> taskHandlerAnnontations;
	private final Set<URL> sources;

	/**
	 * 
	 * 
	 * IDEAS
	 * when calling dao.call(Task) - i CAN look up the correct type via recruiter, as all workers must encompass
	 * the same set of tasks. So it can be magically typed... but then does it require a cast?? I think maybe so , is that okay?
	 * 
	 * 
	 * Should NOT?? allow duplicates on Tasks with responses??
	 * 
	 * 
	 * @Annontate a class that means 
	 * 
	 * Getters are prepopulated with values
	 * Setters call checked set and update the values)
	 * 
	 * 
	 */

	public RetaskRecruiter() {
		this.sources = new HashSet<>();
		this.taskHandlerAnnontations = new HashSet<>();
	}

	public void recruitFromClass(Class<?> clazz) {
		final String resourceName = clazz.getName().replace(".", "/") + ".class";
		final URL url = clazz.getClassLoader().getResource(resourceName);
		this.sources.add(url);
	}

	public void recruitFromPackage(String packageName) {
		this.sources.addAll(ClasspathHelper.forPackage(packageName));
	}
	
	public <A extends Annotation> void registerTaskHandlerAnnontation(Class<A> annontationClass, RoutingKeyProducer<A> factory) {
		taskHandlerAnnontations.add(new RegisteredTaskHandlerAnnotation<A>(annontationClass, factory));
	}

	public <A extends Annotation> void registerChangeHandlerAnnontation(Class<A> clazz, ChangeKeyProducer<A> factory) {
		taskHandlerAnnontations.add(new RegisteredChangeHandlerAnnotation<A>(clazz, factory));
	}

	public <A extends Annotation> void registerInsertHandlerAnnontation(Class<A> clazz, SetKeyProducer<A> factory) {
		taskHandlerAnnontations.add(new RegisteredInsertHandlerAnnotation<A>(clazz, factory));
	}

	public <A extends Annotation> void registerDeleteHandlerAnnontation(Class<A> clazz, SetKeyProducer<A> factory) {
		taskHandlerAnnontations.add(new RegisteredDeleteHandlerAnnotation<A>(clazz, factory));
	}
	
	@SuppressWarnings("unchecked")
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
        
        // Register custom URLType in order to scan exactly one class file
        Vfs.addDefaultURLTypes(new ClassFileUrlType());
        
        ConfigurationBuilder builder = new ConfigurationBuilder()
        	.addUrls(sources)
        	.addScanners(new MethodAnnotationsScanner(), new SubTypesScanner());


        Reflections reflections = new Reflections(builder);
        
		final List<HandlerMethod> handlers = new LinkedList<>();
		for (RegisteredHandlerAnnontation<?> registeredAnnontation : taskHandlerAnnontations) {
            for (Method method : reflections.getMethodsAnnotatedWith(registeredAnnontation.getAnnontationClass())) {
            	Annotation annotation = method.getAnnotation(registeredAnnontation.getAnnontationClass());
            	registeredAnnontation.generate(method.getDeclaringClass(), method, annotation, handlers);
            	logger.info("Recruited {}.{}", method.getDeclaringClass().getName(), method.getName());
            }
		}
		
		final Set<Class<? extends DaoProxy>> proxyClasses = reflections.getSubTypesOf(DaoProxy.class);
		logger.info("Recruited {} Dao Proxies: {}", proxyClasses.size(), proxyClasses);
		
        // Group handlers by type
		final Map<HandlerType, List<HandlerMethod>> handlersByType = handlers.stream()
				.collect(Collectors.groupingBy(HandlerMethod::getType));
		
		// Group handlers by routing key
		final Map<String, List<HandlerMethod>> handlersByRoutingKey = handlers.stream()
				.collect(Collectors.groupingBy(HandlerMethod::getRoutingKey));
		
		// Group handlers by annontation class
        final Map<Class<?>, List<HandlerMethod>> handlersByAnnontationClass = handlers.stream()
        		.collect(Collectors.groupingBy(HandlerMethod::getAnnontationClass));
		
		// Output results
		handlersByType.getOrDefault(HandlerType.CHANGE, List.of()).forEach(h -> logger.info("Retask recruited change handler {}.{} for changes to {}.{}",
				h.getWorkerClass().getName(), h.getMethod().getName(), h.getSetKey(), h.getFieldName()));

		return new RecruitmentResults(handlers, proxyClasses, handlersByType, handlersByRoutingKey, handlersByAnnontationClass);
	}
	
	@FunctionalInterface
	public interface RoutingKeyProducer<A extends Annotation> {
		public String createRoutingKey(Class<?> clazz, Method method, A annotation);
	}

	@FunctionalInterface
	public interface ChangeKeyProducer<A extends Annotation> {
		public ChangeKey createChangeKey(Class<?> clazz, Method method, A annotation);
	}

	@FunctionalInterface
	public interface SetKeyProducer<A extends Annotation> {
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
	
	private abstract class RegisteredHandlerAnnontation<A extends Annotation> {

		private final Class<A> annontationClass;
		
		public RegisteredHandlerAnnontation(Class<A> clazz) {
			this.annontationClass = clazz;
		}
		
		protected Class<A> getAnnontationClass() {
			return annontationClass;
		}

		@SuppressWarnings("unchecked")
		public void generate(Class<?> targetClass, Method method, Annotation annontation, List<HandlerMethod> set) {
            set.add(generate(targetClass, method, annontationClass, (A) annontation));
		}
		
		protected abstract HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation);
	}
	
	private class RegisteredTaskHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final RoutingKeyProducer<A> factory;

		public RegisteredTaskHandlerAnnotation(Class<A> clazz, RoutingKeyProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> annontationClass, A annontation) {
            String routingKey = factory.createRoutingKey(targetClass, method, annontation);
            return new HandlerMethod(HandlerType.TASK, targetClass, method, annontation, annontationClass, routingKey, null, null);
		}
	}

	private class RegisteredChangeHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final ChangeKeyProducer<A> factory;

		public RegisteredChangeHandlerAnnotation(Class<A> clazz, ChangeKeyProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation) {
            ChangeKey changeKey = factory.createChangeKey(targetClass, method, annontation);
            String routingKey = "change." + changeKey.getSetKey() + "." + changeKey.getFieldName();
            return new HandlerMethod(HandlerType.CHANGE, targetClass, method, annontation,
                    clazz, routingKey, changeKey.getSetKey(), changeKey.getFieldName());
		}
	}

	private class RegisteredInsertHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final SetKeyProducer<A> factory;

		public RegisteredInsertHandlerAnnotation(Class<A> clazz, SetKeyProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation) {
            String setKey = factory.createSetKey(targetClass, method, annontation);
            String routingKey = "insert." + setKey;
            return new HandlerMethod(HandlerType.INSERT, targetClass, method, annontation, clazz, routingKey, setKey, null);
		}
	}

	private class RegisteredDeleteHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final SetKeyProducer<A> factory;

		public RegisteredDeleteHandlerAnnotation(Class<A> clazz, SetKeyProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annontation) {
            String setKey = factory.createSetKey(targetClass, method, annontation);
            String routingKey = "delete." + setKey;
            return new HandlerMethod(HandlerType.DELETE, targetClass, method, annontation, clazz, routingKey, setKey, null);
		}
	}
	
	/**
	 * 
	 * This class serves as a workaround for the default behavior of Reflections that
	 * scans multiple classes in a JAR, even when only one class is desired
	 */
	private class ClassFileUrlType implements Vfs.UrlType  {
		
		@Override
        public boolean matches(URL url)         {
        	return url.getProtocol().equals("file") && url.getFile().endsWith(".class");
        }

		@Override
        public Vfs.Dir createDir(final URL url) {
            return new ClassFileDir(Vfs.getFile(url));
        }
	}
	
	private class ClassFileDir extends SystemDir {

		private final String baseName;

		public ClassFileDir(File file) {
			super(file.getParentFile());
			// Cache the name without .class
			this.baseName = file.getName().substring(0, file.getName().length() - 6);
		}
		
		@Override
		public Iterable<Vfs.File> getFiles() {
			return () -> StreamSupport.stream(super.getFiles().spliterator(), false)
	        .filter(f -> f.getRelativePath().startsWith(baseName))
	        .iterator();
		}
		
	}
}
