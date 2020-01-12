package com.jarcadia.retask;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.annontations.RetaskParam;

abstract class MethodParamsProducer {

    private final int numParameters;

    MethodParamsProducer(int numParameters) {
        this.numParameters = numParameters;
    }

    protected Object[] produceParameters(String taskId, String routingKey, int attempt, int permit, String before, String after, String jsonParams) throws IOException {
        Object[] parameters = new Object[numParameters];
        produceSpecificParameters(parameters, taskId, routingKey, attempt, permit, before, after, jsonParams);
        return parameters;
    }

    protected abstract void produceSpecificParameters(Object[] params, String taskId, String routingKey, int attempt, int permit, String before, String after, String jsonParams) throws IOException;

    protected int discoverNamedParamIndex(Parameter[] parameters, String name) {
        for (int i=0; i<parameters.length; i++) {
            if (matches(parameters[i], name)) {
                return i;
            }
        }
        return -1;
    }

//    protected ChangedValueParam discoverChangedValueParamIndex(Parameter[] parameters, String name) {
//        for (int i=0; i<parameters.length; i++) {
//            if (matches(parameters[i], name)) {
//                return new ChangedValueParam(i, parameters[i].getType());
//            }
//        }
//        return null;
//    }

    protected Map<Integer, RedisMap> discoverRedisMapParameterIndexes(Parameter[] parameters) {
        Map<Integer, RedisMap> result = new HashMap<>();
        for (int i=0; i<parameters.length; i++) {
            if (RedisMap.class.equals(parameters[i].getType())) {
                RetaskParam annontation = parameters[i].getAnnotation(RetaskParam.class);
                String mapKey = annontation == null ? parameters[i].getName() : annontation.value();
//                result.put(i, rcommando.getMap(mapKey));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    protected boolean matches(Parameter parameter, String name) {
        if (name.equals(parameter.getName())) {
            return true;
        } else {
            RetaskParam annotation = parameter.getAnnotation(RetaskParam.class);
            if (annotation != null && name.equals(annotation.value())) {
                return true;
            }
        }
        return false;
    }

    protected Map<Integer, String> discoverRedisObjParameterIndexes(Parameter[] parameters) {
        Map<Integer, String> result = new HashMap<>();
        for (int i=0; i<parameters.length; i++) {
            if (RedisObject.class.equals(parameters[i].getType())) {
                RetaskParam annotation = parameters[i].getAnnotation(RetaskParam.class);
                String expectedName = annotation == null ? parameters[i].getName() : annotation.value();
                result.put(i, expectedName);
//                if (annotation != null) {
////                    int openBracketIdx = annotation.value().indexOf("[");
////                    int closeBracketIdx = annotation.value().indexOf("]");
////                    String mapKey = annotation.value().substring(0, openBracketIdx);
////                    String idParamName = annotation.value().substring(openBracketIdx + 1, closeBracketIdx);
////                    result.put(i, new RedisObjectParam(rcommando.getMap(mapKey), idParamName));
//                    
//                    
//                    
//                } else if (objectHandlerMapKey != null) {
//                    result.put(i, new RedisObjectParam(rcommando.getMap(objectHandlerMapKey), "objectId"));
//                } else {
//                    // Assume mapKey and objectId will be available in JSON parameters at run-time
//                    result.put(i, new RedisObjectParam(null, null));
//                }
            }
        }
        return Collections.unmodifiableMap(result);
    }

    protected Map<Integer, JsonParam> discoverJsonParameterIndexes(Parameter[] parameters, Set<Integer> accountedForParamIndexes) {
        Map<Integer, JsonParam> result = new HashMap<>();
        if (accountedForParamIndexes.size() < parameters.length) {
            for (int i=0; i<parameters.length; i++) {
                if (!accountedForParamIndexes.contains(i)) {
                    result.put(i, new JsonParam(parameters[i].getName(), parameters[i].getType()));
                }
            }
        }
        return Collections.unmodifiableMap(result);
    }

    class JsonParam {

        private final String name;
        private final Class<?> type;

        public JsonParam(String name, Class<?> type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public Class<?> getType() {
            return type;
        }
    }

    
}
