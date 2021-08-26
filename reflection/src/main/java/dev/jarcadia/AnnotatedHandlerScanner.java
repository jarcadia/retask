package dev.jarcadia;

import dev.jarcadia.iface.AnnotatedMethodHandler;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.vfs.SystemDir;
import org.reflections.vfs.Vfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class AnnotatedHandlerScanner {

    private final Logger logger = LoggerFactory.getLogger(AnnotatedHandlerScanner.class);

    private final Set<String> packages;
    private final Set<Class<?>> types;
    private final List<InstanceProvider> instanceProviders;
	private final Set<RegisteredMethodAnnotation<? extends Annotation>> registeredMethodAnnotations;

	protected AnnotatedHandlerScanner() {
		this.packages = new HashSet<>();
		this.types = new HashSet<>();
		this.instanceProviders = new ArrayList<>();
		this.registeredMethodAnnotations = new HashSet<>();

		// Register custom URLType in order to add the ability to scan exactly one class file
		Vfs.addDefaultURLTypes(new ClassFileUrlType());
	}

	protected <A extends Annotation> void registerMethodAnnotation(Class<A> annotationType, AnnotatedMethodHandler<A> registrar) {
		this.registeredMethodAnnotations.add(new RegisteredMethodAnnotation<>(annotationType, registrar));
	}

	protected void addClass(Class<?> type) {
		types.add(type);
	}

	protected void addPackage(String packageName) {
		packages.add(packageName);
	}

	protected void addInstanceProvider(InstanceProvider instanceProvider) {
	    this.instanceProviders.add(instanceProvider);
	}

	protected void scan(Jarcadia jarcadia) {

		Set<URL> urls = Stream.concat(
				packages.stream().flatMap(p -> ClasspathHelper.forPackage(p).stream()),
				types.stream().map(t -> t.getClassLoader()
						.getResource(t.getName().replace(".", "/") + ".class"))
		).collect(Collectors.toSet());

		if (urls.size() > 0) {
			logger.info("Scanning {} URLs from {} classes and {} packages", urls.size(), types.size(), packages.size());

			Reflections reflections = new Reflections(new ConfigurationBuilder()
					.addScanners(new MethodAnnotationsScanner(), new SubTypesScanner())
					.setUrls(urls));

			Map<Class<?>, Object> instanceCache = new HashMap<>();
			Function<Class<?>, Object> getInstance = type -> {
				for (InstanceProvider instanceProvider : instanceProviders) {
					Object inst = instanceProvider.getInstance(type);
					if (inst != null) {
						return inst;
					}
				}
				throw new RuntimeException("Instance not provided " + type.getSimpleName());
			};

			for (RegisteredMethodAnnotation<? extends Annotation> registered : registeredMethodAnnotations) {
				for (Method method : reflections.getMethodsAnnotatedWith(registered.getAnnotationType())) {
					logger.info("Detected @{} on {}.{}", registered.getAnnotationType().getSimpleName(),
							method.getDeclaringClass().getSimpleName(), method.getName());
					Object instance = instanceCache.computeIfAbsent(method.getDeclaringClass(), getInstance);
					registered.register(jarcadia, method, instance);
				}
			}
		}
	}

	protected static class RegisteredMethodAnnotation<A extends Annotation> {

		private final Class<A> annotationType;
		private final AnnotatedMethodHandler<A> registrar;

		public RegisteredMethodAnnotation(Class<A> annotationType,
				AnnotatedMethodHandler<A> registrar) {
			this.annotationType = annotationType;
			this.registrar = registrar;
		}

		public Class<A> getAnnotationType() {
			return annotationType;
		}

		public void register(Jarcadia jarcadia, Method method, Object instance) {
			A annotation = method.getAnnotation(annotationType);
			registrar.register(new MethodRegister(jarcadia, method, instance), annotation, method, instance);
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
