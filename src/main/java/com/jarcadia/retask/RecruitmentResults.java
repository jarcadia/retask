package com.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.jarcadia.retask.HandlerMethod.HandlerType;

class RecruitmentResults {
	
	private final List<HandlerMethod<?>> handlers;
    private final Map<HandlerType, List<HandlerMethod<?>>> handlersByType;
    private final Map<String, List<HandlerMethod<?>>> handlersByRoutingKey;
	private final Map<Class<?>, List<HandlerMethod<?>>> handlersByAnnontationClass;

	public RecruitmentResults(List<HandlerMethod<?>> handlers,
			Map<HandlerType, List<HandlerMethod<?>>> handlersByType,
			Map<String, List<HandlerMethod<?>>> handlersByRoutingKey,
			Map<Class<?>, List<HandlerMethod<?>>> handlersByAnnontationClass) {
		this.handlers = List.copyOf(handlers);
		this.handlersByRoutingKey = Map.copyOf(handlersByRoutingKey);
		this.handlersByType = Map.copyOf(handlersByType);
		this.handlersByAnnontationClass = Map.copyOf(handlersByAnnontationClass);
	}
	
	protected Set<String> verifyRecruits(Collection<String> requestedRoutes) {
		return requestedRoutes.stream()
				.filter(r -> !handlersByRoutingKey.containsKey(r))
				.collect(Collectors.toSet());
	}
	
	protected List<HandlerMethod<?>> getTaskHandlers() {
		return handlers;
	}

	protected List<HandlerMethod<?>> getRecruitsFor(HandlerType type) {
		return handlersByType.getOrDefault(type, List.of());
	}

	protected Map<String, List<HandlerMethod<?>>> getHandlersByRoutingKey() {
		return handlersByRoutingKey;
	}
	
	@SuppressWarnings("unchecked")
	protected <A extends Annotation> List<HandlerMethod<A>> getRecruitsFor(Class<A> annontationClass) {
        return handlersByAnnontationClass.getOrDefault(annontationClass, List.of()).stream()
        		.map(handler -> ((HandlerMethod<A>) handler))
        		.collect(Collectors.toList());
	}
}
