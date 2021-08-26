package dev.jarcadia;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.ParamsException;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

class ReflectiveTaskHandlerParamsProducer extends ReflectiveParamProducer {

	private final ObjectMapper objectMapper;
	private final Method method;
    private final int taskIdParamIndex;
    private final int routingKeyParamIndex;
    private final int attemptParamIndex;
    private final int permitParamIndex;
    private final Map<Integer, TypedParam> fieldParamsIndexMap;

    ReflectiveTaskHandlerParamsProducer(Jarcadia jarcadia, ObjectMapper objectMapper, Method method) {
        super(jarcadia, method.getParameters());
        this.objectMapper = objectMapper;
        this.method = method;

        // Discover parameters
        Parameter[] parameters = method.getParameters();
        this.taskIdParamIndex = discoverNamedParamIndex(parameters, "taskId");
        this.routingKeyParamIndex = discoverNamedParamIndex(parameters, "routingKey");
        this.attemptParamIndex = discoverNamedParamIndex(parameters, "attempt");
        this.permitParamIndex = discoverNamedParamIndex(parameters, "permit");

        // The remaining method parameters will come from the task's fields
        this.fieldParamsIndexMap = discoverFieldParameterIndexes(parameters);
    }

    protected Object[] produceParams(String taskId, String routingKey, int attempt, int permit, TaskFields fields)
            throws ParamsException{

        try {
            Object[] params = super.produceParams();
            addIfPresent(params, taskIdParamIndex, taskId);
            addIfPresent(params, routingKeyParamIndex, routingKey);
            addIfPresent(params, attemptParamIndex, attempt);
            addIfPresent(params, permitParamIndex, permit);

            if (fieldParamsIndexMap.size() > 0) {

                for (Entry<Integer, TypedParam> entry : fieldParamsIndexMap.entrySet()) {

//                    Field field = fields.get(entry.getValue().getName());
//                    if (field == null) {
//                        if (entry.getValue().isOptional) {
//                            params[entry.getKey()] = Optional.empty();
//                        } else {
//                            // TODO throw something else
//                            throw new ParamsException("Parameter " + entry.getValue().getName() +
//                                    " is null in params. Value must be specified or wrap parameter in Optional<>");
//                        }
//                    } else {
//                        Object param = field.as(entry.getValue().getType());
//                        params[entry.getKey()] = param;
//                    }
                }
            }
            return params;
        } catch (Exception ex) {
            throw new ParamsException("Unable to produce parameters for " + method.getDeclaringClass().getSimpleName()
                    + "." + method.getName(), ex);
        }
    }

    private Map<Integer, TypedParam> discoverFieldParameterIndexes(Parameter[] parameters) {
        Map<Integer, TypedParam> result = new HashMap<>();
        for (int i=0; i<parameters.length; i++) {
        	if (!accountedFor[i]) {
                result.put(i, createTypedParameter(parameters[i]));
            }
        }
        return result;
    }

    private TypedParam createTypedParameter(Parameter parameter) {
        return new TypedParam(getName(parameter), createJavaType(parameter), isOptional(parameter));
    }

    private JavaType createJavaType(Parameter parameter) {
        JavaType type = objectMapper.constructType(parameter.getParameterizedType());
        return type;
    }

    private boolean isOptional(Parameter parameter) {
    	Type type = parameter.getParameterizedType();
    	if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return Optional.class.equals(parameterizedType.getRawType());
		} else {
			return false;
		}
    }

    private static class TypedParam {

        private final String name;
        private final JavaType type;
        private final boolean isOptional;

        public TypedParam(String name, JavaType type, boolean isOptional) {
        	this.name = name;
        	this.type = type;
        	this.isOptional = isOptional;
        }

        public String getName() {
            return name;
        }

        public JavaType getType() {
            return type;
        }
    }
}
