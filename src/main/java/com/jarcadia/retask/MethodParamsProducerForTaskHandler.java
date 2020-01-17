package com.jarcadia.retask;

import java.io.IOException;
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

class MethodParamsProducerForTaskHandler extends MethodParamsProducer {
    
    private final RedisCommando rcommando;
    private final ObjectMapper objectMapper;
    private final int taskIdParamIndex;
    private final int routingKeyParamIndex;
    private final int attemptParamIndex;
    private final int permitParamIndex;
    private final Map<Integer, RedisMap> redisMapParamsIndexMap;
    private final Map<Integer, String> redisObjParamsIndexMap;
    private final Map<Integer, JsonParam> jsonParamsIndexMap;

    MethodParamsProducerForTaskHandler(RedisCommando rcommando, ObjectMapper objectMapper, Parameter[] parameters) {
        super(parameters.length);
        this.rcommando = rcommando;
        this.objectMapper = objectMapper;
        this.taskIdParamIndex = discoverNamedParamIndex(parameters, "taskId");
        this.routingKeyParamIndex = discoverNamedParamIndex(parameters, "routingKey");
        this.attemptParamIndex = discoverNamedParamIndex(parameters, "attempt");
        this.permitParamIndex = discoverNamedParamIndex(parameters, "permit");
        this.redisMapParamsIndexMap = discoverRedisMapParameterIndexes(parameters);
        this.redisObjParamsIndexMap = discoverRedisObjParameterIndexes(parameters);

        // Figure out which parameters indexes are accounted for so far
        Set<Integer> accountedForParamIndexes = new HashSet<>();
        accountedForParamIndexes.addAll(redisMapParamsIndexMap.keySet());
        accountedForParamIndexes.addAll(redisObjParamsIndexMap.keySet());
        if (taskIdParamIndex != -1) accountedForParamIndexes.add(taskIdParamIndex);
        if (routingKeyParamIndex != -1) accountedForParamIndexes.add(routingKeyParamIndex);
        if (attemptParamIndex != -1) accountedForParamIndexes.add(attemptParamIndex);
        if (permitParamIndex != -1) accountedForParamIndexes.add(permitParamIndex);

        // Assume the rest of the method parameters will come from the task's JSON parameters
        this.jsonParamsIndexMap = discoverJsonParameterIndexes(parameters, accountedForParamIndexes);
    }

    public void produceSpecificParameters(Object[] params, String taskId, String routingKey, int attempt, int permit, String before, String after, String jsonParams) throws IOException {
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
        for (Entry<Integer, RedisMap> entry : redisMapParamsIndexMap.entrySet()) {
            params[entry.getKey()] = entry.getValue();
        }
        if (jsonParamsIndexMap.size() + redisObjParamsIndexMap.size() > 0) {
            JsonNode root = objectMapper.readTree(jsonParams);
            for (Entry<Integer, String> entry : redisObjParamsIndexMap.entrySet()) {
                RedisObject obj = produceRedisObject(root, entry.getValue());
                
                // If RedisObject not found by name see if is available as default name 'object'
                if (obj == null && redisObjParamsIndexMap.size() == 1) {
                    obj = produceRedisObject(root, "object");
                }
                params[entry.getKey()] = obj;
            }
            // Populate the other regular param fields with parsed values
            for (Entry<Integer, JsonParam> entry : jsonParamsIndexMap.entrySet()) {
                JsonNode node = root.get(entry.getValue().getName());
                if (node != null && !node.isNull()) {
                    params[entry.getKey()] = objectMapper.treeToValue(node, entry.getValue().getType());
                }
            }
        }
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

    private RedisObject produceRedisObject(JsonNode root, String paramName) {
        JsonNode redisObjNode = root.get(paramName);
        if (redisObjNode != null && redisObjNode.isObject()) {
            JsonNode mapKeyNode = redisObjNode.get("mapKey");
            JsonNode idNode = redisObjNode.get("id");
            if (mapKeyNode != null && mapKeyNode.isTextual() && idNode != null && idNode.isTextual()) {
                String mapKey = mapKeyNode.asText();
                String id = idNode.asText();
                return rcommando.getMap(mapKey).get(id);
            }
        }
        return null;
    }

}
