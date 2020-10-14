package dev.jarcadia.retask;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import dev.jarcadia.retask.annontations.RetaskChangeHandler;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.vfs.SystemDir;
import org.reflections.vfs.Vfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.jarcadia.redao.proxy.Proxy;
import dev.jarcadia.retask.HandlerMethod.HandlerSource;
import dev.jarcadia.retask.annontations.RetaskDeleteHandler;
import dev.jarcadia.retask.annontations.RetaskHandler;
import dev.jarcadia.retask.annontations.RetaskInsertHandler;

public class RetaskRecruiter {

    private final Logger logger = LoggerFactory.getLogger(RetaskRecruiter.class);	

	private final Set<RegisteredHandlerAnnontation<?>> taskHandlerAnnotations;
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
		this.taskHandlerAnnotations = new HashSet<>();
	}

	public void recruitFromClass(Class<?> clazz) {
		final String resourceName = clazz.getName().replace(".", "/") + ".class";
		final URL url = clazz.getClassLoader().getResource(resourceName);
		this.sources.add(url);
	}

	public void recruitFromPackage(String packageName) {
		this.sources.addAll(ClasspathHelper.forPackage(packageName));
	}
	
	public <A extends Annotation> void registerTaskHandlerAnnotation(Class<A> annotationClass, RoutingKeyProducer<A> factory) {
		taskHandlerAnnotations.add(new RegisteredTaskHandlerAnnotation<A>(annotationClass, factory));
	}

	public <A extends Annotation> void registerChangeHandlerAnnotation(Class<A> clazz, ChangeKeyProducer<A> factory) {
		taskHandlerAnnotations.add(new RegisteredChangeHandlerAnnotation<A>(clazz, factory));
	}

	public <A extends Annotation> void registerInsertHandlerAnnotation(Class<A> clazz, TypeProducer<A> factory) {
		taskHandlerAnnotations.add(new RegisteredInsertHandlerAnnotation<A>(clazz, factory));
	}

	public <A extends Annotation> void registerDeleteHandlerAnnotation(Class<A> clazz, TypeProducer<A> factory) {
		taskHandlerAnnotations.add(new RegisteredDeleteHandlerAnnotation<A>(clazz, factory));
	}
	
	@SuppressWarnings("unchecked")
	protected RecruitmentResults recruit() {
		
		 // Add predefined Retask Annotations to recruiter
        this.registerTaskHandlerAnnotation(RetaskHandler.class,
        		(clazz, method, annotation) -> annotation.value());
        this.registerChangeHandlerAnnotation(RetaskChangeHandler.class,
        		(clazz, method, annotation) -> new ChangeKey(annotation.type(), annotation.field()));
        this.registerInsertHandlerAnnotation(RetaskInsertHandler.class,
        		(clazz, method, annotation) -> annotation.value());
        this.registerDeleteHandlerAnnotation(RetaskDeleteHandler.class,
        		(clazz, method, annotation) -> annotation.value());
        
        // Register custom URLType in order to scan exactly one class file
        Vfs.addDefaultURLTypes(new ClassFileUrlType());
        
        ConfigurationBuilder builder = new ConfigurationBuilder()
        	.addUrls(sources)
        	.addScanners(new MethodAnnotationsScanner(), new SubTypesScanner());


        Reflections reflections = new Reflections(builder);
        
		final List<HandlerMethod> handlers = new LinkedList<>();
		for (RegisteredHandlerAnnontation<?> registeredAnnotation : taskHandlerAnnotations) {
            for (Method method : reflections.getMethodsAnnotatedWith(registeredAnnotation.getAnnotationClass())) {
            	Annotation annotation = method.getAnnotation(registeredAnnotation.getAnnotationClass());
            	registeredAnnotation.generate(method.getDeclaringClass(), method, annotation, handlers);
            	logger.info("Recruited {}.{}", method.getDeclaringClass().getName(), method.getName());
            }
		}
		
		final Set<Class<? extends Proxy>> proxyClasses = reflections.getSubTypesOf(Proxy.class);
		logger.info("Recruited {} Dao Proxies: {}", proxyClasses.size(), proxyClasses);
		
        // Group handlers by type
		final Map<HandlerSource, List<HandlerMethod>> handlersByType = handlers.stream()
				.collect(Collectors.groupingBy(HandlerMethod::getSource));
		
		// Group handlers by routing key
		final Map<String, List<HandlerMethod>> handlersByRoutingKey = handlers.stream()
				.collect(Collectors.groupingBy(HandlerMethod::getRoutingKey));
		
		// Group handlers by annotation class
        final Map<Class<?>, List<HandlerMethod>> handlersByAnnotationClass = handlers.stream()
        		.collect(Collectors.groupingBy(HandlerMethod::getAnnontationClass));
		
		// Output results
		handlersByType.getOrDefault(HandlerSource.CHANGE, List.of()).forEach(h -> logger.info("Retask recruited change handler {}.{} for changes to {}.{}",
				h.getWorkerClass().getName(), h.getMethod().getName(), h.getType(), h.getFieldName()));

		return new RecruitmentResults(handlers, proxyClasses, handlersByType, handlersByRoutingKey, handlersByAnnotationClass);
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
	public interface TypeProducer<A extends Annotation> {
		public String createType(Class<?> clazz, Method method, A annotation);
	} 

	public static class ChangeKey {
		private final String type;
		private final String fieldName;

		public ChangeKey(String type, String fieldName) {
			this.type = type;
			this.fieldName = fieldName;
		}

		public String getType() {
			return this.type;
		}

		public String getFieldName() {
			return this.fieldName;
		}
	}
	
	private abstract class RegisteredHandlerAnnontation<A extends Annotation> {

		private final Class<A> annotationClass;
		
		public RegisteredHandlerAnnontation(Class<A> clazz) {
			this.annotationClass = clazz;
		}
		
		protected Class<A> getAnnotationClass() {
			return annotationClass;
		}

		@SuppressWarnings("unchecked")
		public void generate(Class<?> targetClass, Method method, Annotation annotation, List<HandlerMethod> set) {
            set.add(generate(targetClass, method, annotationClass, (A) annotation));
		}
		
		protected abstract HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annotation);
	}
	
	private class RegisteredTaskHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final RoutingKeyProducer<A> factory;

		public RegisteredTaskHandlerAnnotation(Class<A> clazz, RoutingKeyProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> annotationClass, A annotation) {
            String routingKey = factory.createRoutingKey(targetClass, method, annotation);
            return new HandlerMethod(HandlerSource.TASK, targetClass, method, annotation, annotationClass, routingKey, null, null);
		}
	}

	private class RegisteredChangeHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final ChangeKeyProducer<A> factory;

		public RegisteredChangeHandlerAnnotation(Class<A> clazz, ChangeKeyProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annotation) {
            ChangeKey changeKey = factory.createChangeKey(targetClass, method, annotation);
            String routingKey = "change." + changeKey.getType() + "." + changeKey.getFieldName();
            return new HandlerMethod(HandlerSource.CHANGE, targetClass, method, annotation,
                    clazz, routingKey, changeKey.getType(), changeKey.getFieldName());
		}
	}

	private class RegisteredInsertHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final TypeProducer<A> factory;

		public RegisteredInsertHandlerAnnotation(Class<A> clazz, TypeProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annotation) {
            String setKey = factory.createType(targetClass, method, annotation);
            String routingKey = "insert." + setKey;
            return new HandlerMethod(HandlerSource.INSERT, targetClass, method, annotation, clazz, routingKey, setKey, null);
		}
	}

	private class RegisteredDeleteHandlerAnnotation<A extends Annotation> extends RegisteredHandlerAnnontation<A> {

		private final TypeProducer<A> factory;

		public RegisteredDeleteHandlerAnnotation(Class<A> clazz, TypeProducer<A> factory) {
			super(clazz);
			this.factory = factory;
		}

		@Override
		protected HandlerMethod generate(Class<?> targetClass, Method method, Class<A> clazz, A annotation) {
            String setKey = factory.createType(targetClass, method, annotation);
            String routingKey = "delete." + setKey;
            return new HandlerMethod(HandlerSource.DELETE, targetClass, method, annotation, clazz, routingKey, setKey, null);
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
