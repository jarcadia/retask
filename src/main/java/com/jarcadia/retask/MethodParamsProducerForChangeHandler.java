package com.jarcadia.retask;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisObject;

public class MethodParamsProducerForChangeHandler extends MethodParamsProducer {

    private final RedisCommando rcommando;
    private final ObjectMapper objectMapper;
    private final int objectParamIndex;
    private final ChangedValueParam beforeParam;
    private final ChangedValueParam afterParam;

    MethodParamsProducerForChangeHandler(RedisCommando rcommando, ObjectMapper objectMapper, Parameter[] parameters) {
        super(parameters.length);
        this.rcommando = rcommando;
        this.objectMapper = objectMapper;

        // Find object param index
        List<Integer> available = IntStream.range(0, parameters.length).mapToObj(i -> i).collect(Collectors.toList());
        int objIdx = -1;
        for (int i=0; i<parameters.length; i++) {
            if (RedisObject.class.equals(parameters[i].getType())) {
                available.remove(i);
                objIdx = i;
                break;
            }
        }
        this.objectParamIndex = objIdx;

        // Find before and after params
        if (available.size() == 1) {
            this.beforeParam = null;
            this.afterParam = new ChangedValueParam(available.get(0), parameters[available.get(0)].getType());
        } else if (available.size() == 2) {
            this.beforeParam = new ChangedValueParam(available.get(0), parameters[available.get(0)].getType());
            this.afterParam = new ChangedValueParam(available.get(1), parameters[available.get(1)].getType());
        } else {
            throw new RetaskException("@RetaskChangeHandler methods must have between 1 and 3 parameteres");
        }
    }

    @Override
    public void produceSpecificParameters(Object[] params, String taskId, String routingKey, int attempt, int permit, String before, String after, String jsonParams) throws IOException {
        if (objectParamIndex >= 0) {
            JsonNode root = objectMapper.readTree(jsonParams);
            params[objectParamIndex] = produceRedisObject(root, "object");
        }
        if (beforeParam != null) {
            params[beforeParam.getIndex()] = before == null ? null : objectMapper.readValue(before, beforeParam.getType());
        }
        if (afterParam != null) {
            params[afterParam.getIndex()] = after == null ? null : objectMapper.readValue(after, afterParam.getType());
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
