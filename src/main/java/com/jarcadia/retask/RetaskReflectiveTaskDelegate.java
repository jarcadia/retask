package com.jarcadia.retask;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.annontations.RetaskParam;

/**
 * This class is responsible for reflectively invoking @RetaskHandler and @RetaskChangeHandler methods. It will match task properties and params
 * to the method parameters and inject them accordingly.
 */
class RetaskReflectiveTaskDelegate implements RetaskDelegate {

    private final RedisCommando rcommando;
    private final ObjectMapper objectMapper;
    private final Method targetMethod;
    private final MethodParamsProducer paramsProducer;
    private final RetaskWorkerInstanceProvider provider;
    private final Class<?> targetClass;
    private volatile Object targetInstance;
    private volatile boolean targetUnavailable;

    static RetaskReflectiveTaskDelegate createHandlerDelegate(RedisCommando rcommando, ObjectMapper objectMapper, RetaskWorkerInstanceProvider provider, Class<?> targetClass, Method targetMethod) {
        return new RetaskReflectiveTaskDelegate(rcommando, objectMapper, provider, targetClass, targetMethod, null);
    }
    
    static RetaskReflectiveTaskDelegate createObjectHandlerDelegate(RedisCommando rcommando, ObjectMapper objectMapper, RetaskWorkerInstanceProvider provider, Class<?> targetClass, Method targetMethod, String mapKey) {
        return new RetaskReflectiveTaskDelegate(rcommando, objectMapper, provider, targetClass, targetMethod, mapKey);
    }

    private RetaskReflectiveTaskDelegate(RedisCommando rcommando, ObjectMapper objectMapper, RetaskWorkerInstanceProvider provider, Class<?> targetClass, Method targetMethod, String mapKey) {
        this.rcommando = rcommando;
        this.objectMapper = objectMapper;
        this.provider = provider;
        this.targetClass = targetClass;
        this.targetMethod = targetMethod;
        this.targetUnavailable = false;
        this.paramsProducer = initMethodParamsProducer(targetMethod.getParameters(), mapKey);
    }

    @Override
    public Object invoke(String taskId, String routingKey, int attempt, int permit, String before, String after, String params) throws Throwable {
        synchronized (this) {
            if (targetInstance == null) {
                targetInstance = provider.getInstance(targetClass);
                if (targetInstance == null || !targetClass.equals(targetInstance.getClass())) {
                    targetUnavailable = true;
                }
            }
        }
        if (targetUnavailable) {
            throw new RetaskException("No instance of worker (type " + targetClass.getName() + ") provided");
        }
        try {
            Object[] methodParameters = paramsProducer.produceParameters(taskId, routingKey, attempt, permit, before, after, params);
            return targetMethod.invoke(targetInstance, methodParameters);
        }
        catch (InvocationTargetException e) {
            throw e.getCause() == null ? e : e.getCause();
        }
        catch (IllegalAccessException | IllegalArgumentException e) {
            throw new RetaskException("Failed to invoke task handler reflectively for routing key " + routingKey + " Params: " + params, e);
        }
        catch (IOException e) {
            throw new RetaskException("Unable to deserialize parameters for routing key " + routingKey + " with params " + params, e);
        }
    }

    private class MethodParamsProducer {
        
        private final int numParameters;
        private final int taskIdParamIndex;
        private final int routingKeyParamIndex;
        private final int attemptParamIndex;
        private final int permitParamIndex;
        private final ChangedValueParam beforeParam;
        private final ChangedValueParam afterParam;
        private final Map<Integer, RedisMap> redisMapParamsIndexMap;
        private final Map<Integer, RedisObjectParam> redisObjParamsIndexMap;
        private final Map<Integer, JsonParam> jsonParamsIndexMap;

        public MethodParamsProducer(int numParameters,
                int taskIdParamIndex,
                int routingKeyParamIndex,
                int attemptParamIndex,
                int permitParamIndex,
                ChangedValueParam beforeParam,
                ChangedValueParam afterParam,
                Map<Integer, RedisMap> redisMapParamsIndexMap,
                Map<Integer, RedisObjectParam> redisObjParamsIndexMap,
                Map<Integer, JsonParam> jsonParamsIndexMap) {
            this.numParameters = numParameters;
            this.taskIdParamIndex = taskIdParamIndex;
            this.routingKeyParamIndex = routingKeyParamIndex;
            this.attemptParamIndex = attemptParamIndex;
            this.permitParamIndex = permitParamIndex;
            this.beforeParam = beforeParam;
            this.afterParam = afterParam;
            this.redisMapParamsIndexMap = redisMapParamsIndexMap;
            this.redisObjParamsIndexMap = redisObjParamsIndexMap;
            this.jsonParamsIndexMap = jsonParamsIndexMap;
        }

        public Object[] produceParameters(String taskId, String routingKey, int attempt, int permit, String before, String after, String jsonParams) throws IOException {
            Object[] params = new Object[numParameters];
            if (taskIdParamIndex >= 0) {
                params[taskIdParamIndex] = taskId;
            }
            if (routingKeyParamIndex >= 0) {
                params[routingKeyParamIndex] = routingKey;
            }
            if (attemptParamIndex >= 0) {
                params[attemptParamIndex] = attempt;
            }
            if (permitParamIndex >= 0) {
                params[permitParamIndex] = permit;
            }
            if (beforeParam != null) {
                params[beforeParam.getIndex()] = before == null ? null : objectMapper.readValue(before, beforeParam.getType());
            }
            if (afterParam != null) {
                params[afterParam.getIndex()] = after == null ? null : objectMapper.readValue(after, afterParam.getType());
            }
            for (Entry<Integer, RedisMap> entry : redisMapParamsIndexMap.entrySet()) {
                params[entry.getKey()] = entry.getValue();
            }
            if (jsonParamsIndexMap.size() + redisObjParamsIndexMap.size() > 0) {
                JsonNode root = objectMapper.readTree(jsonParams);

                // Populate the RedisObjectParams by parsing the ID field and creating the RedisObject
                for (Entry<Integer, RedisObjectParam> entry : redisObjParamsIndexMap.entrySet()) {
                    RedisMap map = entry.getValue().getMap();
                    if (map == null) {
                        // Map is dynamic, lookup map key from parameters
                        JsonNode mapKeyNode = root.get("objectMapKey");
                        if (mapKeyNode != null && !mapKeyNode.isNull()) {
                            map = rcommando.getMap(mapKeyNode.asText());
                        }
                    }
                    JsonNode idNode = root.get(entry.getValue().getIdParamName());
                    if (map != null && idNode != null && !idNode.isNull()) {
                        String id = idNode.asText();
                        params[entry.getKey()] = map.get(id);
                    }
                }

                // Populate the other regular param fields with parsed values
                for (Entry<Integer, JsonParam> entry : jsonParamsIndexMap.entrySet()) {
                    JsonNode node = root.get(entry.getValue().getName());
                    if (node != null && !node.isNull()) {
                        params[entry.getKey()] = objectMapper.treeToValue(node, entry.getValue().getType());
                    }
                }
            }
            return params;
        }
    }

    private MethodParamsProducer initMethodParamsProducer(Parameter[] parameters, String mapKey) {
        int taskIdParamIndex = discoverNamedParamIndex(parameters, "taskId");
        int routingKeyParamIndex = discoverNamedParamIndex(parameters, "routingKey");
        int attemptParamIndex = discoverNamedParamIndex(parameters, "attempt");
        int permitParamIndex = discoverNamedParamIndex(parameters, "permit");
        ChangedValueParam beforeParam = discoverChangedValueParamIndex(parameters, "before");
        ChangedValueParam afterParam = discoverChangedValueParamIndex(parameters, "after");
        Map<Integer, RedisMap> redisMapParamsIndexMap = discoverRedisMapParameterIndexes(parameters);
        Map<Integer, RedisObjectParam> redisObjParamsIndexMap = discoverRedisObjParameterIndexes(parameters, mapKey);

        // Figure out which parameters indexes are accounted for so far
        Set<Integer> accountedForParamIndexes = new HashSet<>();
        accountedForParamIndexes.addAll(redisMapParamsIndexMap.keySet());
        accountedForParamIndexes.addAll(redisObjParamsIndexMap.keySet());
        if (taskIdParamIndex != -1) accountedForParamIndexes.add(taskIdParamIndex);
        if (routingKeyParamIndex != -1) accountedForParamIndexes.add(routingKeyParamIndex);
        if (attemptParamIndex != -1) accountedForParamIndexes.add(attemptParamIndex);
        if (permitParamIndex != -1) accountedForParamIndexes.add(permitParamIndex);
        if (beforeParam != null) accountedForParamIndexes.add(beforeParam.getIndex());
        if (afterParam != null) accountedForParamIndexes.add(afterParam.getIndex());

        // Assume the rest of the method parameters will come from the task's JSON parameters
        Map<Integer, JsonParam> jsonParamsIndexMap = discoverJsonParameterIndexes(parameters, accountedForParamIndexes);
        
        // Build a MethodParameterProducer based on above analysis
        return new MethodParamsProducer(parameters.length, taskIdParamIndex, routingKeyParamIndex, attemptParamIndex,
                permitParamIndex, beforeParam, afterParam, redisMapParamsIndexMap, redisObjParamsIndexMap, jsonParamsIndexMap);
    }
    
    private int discoverNamedParamIndex(Parameter[] parameters, String name) {
        for (int i=0; i<parameters.length; i++) {
            if (matches(parameters[i], name)) {
                return i;
            }
        }
        return -1;
    }

    private ChangedValueParam discoverChangedValueParamIndex(Parameter[] parameters, String name) {
        for (int i=0; i<parameters.length; i++) {
            if (matches(parameters[i], name)) {
                return new ChangedValueParam(i, parameters[i].getType());
            }
        }
        return null;
    }

    private Map<Integer, RedisMap> discoverRedisMapParameterIndexes(Parameter[] parameters) {
        Map<Integer, RedisMap> result = new HashMap<>();
        for (int i=0; i<parameters.length; i++) {
            if (RedisMap.class.equals(parameters[i].getType())) {
                RetaskParam annontation = parameters[i].getAnnotation(RetaskParam.class);
                String mapKey = annontation == null ? parameters[i].getName() : annontation.value();
                result.put(i, rcommando.getMap(mapKey));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private boolean matches(Parameter parameter, String name) {
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

    private Map<Integer, RedisObjectParam> discoverRedisObjParameterIndexes(Parameter[] parameters, String objectHandlerMapKey) {
        Map<Integer, RedisObjectParam> result = new HashMap<>();
        for (int i=0; i<parameters.length; i++) {
            if (RedisObject.class.equals(parameters[i].getType())) {
                RetaskParam annotation = parameters[i].getAnnotation(RetaskParam.class);
                if (annotation != null) {
                    int openBracketIdx = annotation.value().indexOf("[");
                    int closeBracketIdx = annotation.value().indexOf("]");
                    String mapKey = annotation.value().substring(0, openBracketIdx);
                    String idParamName = annotation.value().substring(openBracketIdx + 1, closeBracketIdx);
                    result.put(i, new RedisObjectParam(rcommando.getMap(mapKey), idParamName));
                } else if (objectHandlerMapKey != null) {
                    result.put(i, new RedisObjectParam(rcommando.getMap(objectHandlerMapKey), "objectId"));
                } else {
                    // Assume mapKey and objectId will be available in JSON parameters at run-time
                    result.put(i, new RedisObjectParam(null, "objectId"));
                }
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private Map<Integer, JsonParam> discoverJsonParameterIndexes(Parameter[] parameters, Set<Integer> accountedForParamIndexes) {
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

    private class RedisObjectParam {

        private final RedisMap map;
        private final String idParamName;

        public RedisObjectParam(RedisMap map, String idParamName) {
            this.map = map;
            this.idParamName = idParamName;
        }

        public RedisMap getMap() {
            return map;
        }

        public String getIdParamName() {
            return idParamName;
        } 
    }

    private class JsonParam {

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

    private class ChangedValueParam {

        private final int index;
        private final Class<?> type;

        public ChangedValueParam(int index, Class<?> type) {
            this.index = index;
            this.type = type;
        }

        public int getIndex() {
            return index;
        }

        public Class<?> getType() {
            return type;
        }
    }
}
