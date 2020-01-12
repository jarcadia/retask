package com.jarcadia.retask;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisObject;

public class MethodParamsProducerForInsertOrDeleteHandler extends MethodParamsProducer {

    private final RedisCommando rcommando;
    private final ObjectMapper objectMapper;
    private final int objectParamIndex;

    MethodParamsProducerForInsertOrDeleteHandler(RedisCommando rcommando, ObjectMapper objectMapper, Parameter[] parameters) {
        super(parameters.length);
        this.rcommando = rcommando;
        this.objectMapper = objectMapper;
        Map<Integer, String> redisObjParamsIndexMap = discoverRedisObjParameterIndexes(parameters);
        // Insert or delete value methods are assumed to have zero or one object params
        this.objectParamIndex = redisObjParamsIndexMap.isEmpty() ? -1 : redisObjParamsIndexMap.keySet().iterator().next();
    }

    @Override
    public void produceSpecificParameters(Object[] params, String taskId, String routingKey, int attempt, int permit, String before, String after, String jsonParams) throws IOException {
        if (objectParamIndex >= 0) {
            JsonNode root = objectMapper.readTree(jsonParams);
            params[objectParamIndex] = produceRedisObject(root, "object");
        }
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
