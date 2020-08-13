package com.jarcadia.retask;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.jarcadia.rcommando.proxy.Proxy;
import com.jarcadia.retask.HandlerMethod.HandlerType;

class RecruitmentResults {
	
	private final List<HandlerMethod> handlers;
	private final Set<Class<? extends Proxy>> proxyClasses;
    private final Map<HandlerType, List<HandlerMethod>> handlersByType;
    private final Map<String, List<HandlerMethod>> handlersByRoutingKey;
	private final Map<Class<?>, List<HandlerMethod>> handlersByAnnontationClass;

	public RecruitmentResults(List<HandlerMethod> handlers,
			Set<Class<? extends Proxy>> proxyClasses,
			Map<HandlerType, List<HandlerMethod>> handlersByType,
			Map<String, List<HandlerMethod>> handlersByRoutingKey,
			Map<Class<?>, List<HandlerMethod>> handlersByAnnontationClass) {
		this.handlers = List.copyOf(handlers);
		this.proxyClasses = proxyClasses;
		this.handlersByRoutingKey = Map.copyOf(handlersByRoutingKey);
		this.handlersByType = Map.copyOf(handlersByType);
		this.handlersByAnnontationClass = Map.copyOf(handlersByAnnontationClass);
	}
	
	protected Set<String> verifyRoutes(Collection<String> routes) {
		return routes.stream()
				.filter(r -> !handlersByRoutingKey.containsKey(r))
				.collect(Collectors.toSet());
	}
	
	protected List<HandlerMethod> getHandlers() {
		return handlers;
	}
	
	protected Set<Class<? extends Proxy>> getProxyClasses() {
		return proxyClasses;
	}

	protected List<HandlerMethod> getHandlers(HandlerType type) {
		return handlersByType.getOrDefault(type, List.of());
	}

	protected Map<String, List<HandlerMethod>> getHandlersByRoutingKey() {
		return handlersByRoutingKey;
	}
	
	protected List<HandlerMethod> getHandlers(Class<?> annontationClass) {
        return handlersByAnnontationClass.getOrDefault(annontationClass, List.of()).stream()
        		.collect(Collectors.toList());
	}
	
	protected List<HandlerMethod> getHandlers(String routingKey) {
		return handlersByRoutingKey.get(routingKey);
	}
}
