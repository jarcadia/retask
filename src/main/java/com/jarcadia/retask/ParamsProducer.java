package com.jarcadia.retask;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RcSet;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.annontations.RetaskParam;

class ParamsProducer {
    
	private final int numParams;
	private final RedisCommando rcommando;
	private final Retask retask;
    private final ObjectMapper objectMapper;
    private final int retaskParamIndex;
    private final int taskBucketParamIndex;
    private final int taskIdParamIndex;
    private final int routingKeyParamIndex;
    private final int attemptParamIndex;
    private final int permitParamIndex;
    private final ChangedValueParam beforeParam;
    private final ChangedValueParam afterParam;
    private final Map<Integer, RcSet> rcSetParamIndexMap; 
    private final Map<Integer, TypedParam> jsonParamsIndexMap;

    ParamsProducer(RedisCommando rcommando, Retask retask, Parameter[] parameters) {
    	this.numParams = parameters.length;
    	this.rcommando = rcommando;
        this.retask = retask;
        this.objectMapper = rcommando.getObjectMapper();
        
        // Create array to track which params are accounted for
        boolean[] accountedFor = new boolean[numParams];
        
        // Discover parameters
        this.retaskParamIndex = discoverTypedParamIndex(parameters, Retask.class, accountedFor);
        this.taskBucketParamIndex = discoverTypedParamIndex(parameters, TaskBucket.class, accountedFor);
        this.taskIdParamIndex = discoverNamedParamIndex(parameters, "taskId", accountedFor);
        this.routingKeyParamIndex = discoverNamedParamIndex(parameters, "routingKey", accountedFor);
        this.attemptParamIndex = discoverNamedParamIndex(parameters, "attempt", accountedFor);
        this.permitParamIndex = discoverNamedParamIndex(parameters, "permit", accountedFor);
        this.beforeParam = this.discoverChangedValueParam(parameters, "before", accountedFor);
        this.afterParam = this.discoverChangedValueParam(parameters, "after", accountedFor);
        this.rcSetParamIndexMap = this.discoverRcSetParameterIndexes(parameters, accountedFor);

        // The remaining method parameters will come from the task's JSON parameters
        this.jsonParamsIndexMap = discoverJsonParameterIndexes(parameters, accountedFor);
    }

    protected Object[] produceParams(String taskId, String routingKey, int attempt, int permit, String before, String after, String jsonParams, TaskBucket taskBucket) throws RetaskParamsException {
    	Object[] params = new Object[numParams];
    	if (retaskParamIndex >= 0) {
            params[retaskParamIndex] = retask;
    	}
    	if (taskBucketParamIndex >= 0) {
    		params[taskBucketParamIndex] = taskBucket;
    	}
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
        if (beforeParam != null && before != null) {
        	try {
				JsonNode beforeNode = objectMapper.readTree(before);
                params[beforeParam.getIndex()] = objectMapper.convertValue(beforeNode, beforeParam.getType());
			} catch (IOException e) {
				throw new RetaskParamsException("Unable to deserialize before parameter to " + beforeParam.getType().getTypeName() + " from " + before, e);
			}
        }
        if (afterParam != null && after != null) {
        	try {
                JsonNode afterNode = objectMapper.readTree(after);
                params[afterParam.getIndex()] = objectMapper.convertValue(afterNode, afterParam.getType());
			} catch (IOException e) {
				throw new RetaskParamsException("Unable to deserialize after parameter to " + afterParam.getType().getTypeName() + " from " + after, e);
			}
        }
        if (!rcSetParamIndexMap.isEmpty()) {
            for (Entry<Integer, RcSet> entry : rcSetParamIndexMap.entrySet()) {
            	params[entry.getKey()] = entry.getValue();
            }
        	
        }
        if (jsonParamsIndexMap.size() > 0) {
            JsonNode root;
			try {
				root = objectMapper.readTree(jsonParams);
			} catch (IOException e) {
				throw new RetaskParamsException("Unable to deserialize params " + jsonParams);
			}
            for (Entry<Integer, TypedParam> entry : jsonParamsIndexMap.entrySet()) {
                JsonNode node = root.get(entry.getValue().getName());
                if (node == null) {
                	throw new RetaskParamsException("Parameter " + entry.getValue().getName() + " not available in params");
                } else if (node.isNull()) {
                	throw new RetaskParamsException("Parameter " + entry.getValue().getName() + " is null in params");
                } else {
                    params[entry.getKey()] = objectMapper.convertValue(node, entry.getValue().getType());
                } 
            }
        }
        return params;
    }

    private int discoverTypedParamIndex(Parameter[] parameters, Class<?> clazz, boolean[] accountedFor) {
        for (int i=0; i<parameters.length; i++) {
        	if (!accountedFor[i] && clazz.equals(parameters[i].getType())) {
            	accountedFor[i] = true;
        		return i;
        	}
        }
        return -1;
    }

    private int discoverNamedParamIndex(Parameter[] parameters, String name, boolean[] accountedFor) {
        for (int i=0; i<parameters.length; i++) {
            if (!accountedFor[i] && matches(parameters[i], name)) {
            	accountedFor[i] = true;
                return i;
            }
        }
        return -1;
    }
    
    private ChangedValueParam discoverChangedValueParam(Parameter[] parameters, String name, boolean[] accountedFor) {
        for (int i=0; i<parameters.length; i++) {
            if (!accountedFor[i] && matches(parameters[i], name)) {
            	accountedFor[i] = true;
                return new ChangedValueParam(i, createJavaType(parameters[i]));
            }
        }
        return null;
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
    
    private Map<Integer, RcSet> discoverRcSetParameterIndexes(Parameter[] parameters, boolean[] accountedFor) {
    	Map<Integer, RcSet> map = new HashMap<>();
        for (int i=0; i<parameters.length; i++) {
        	if (!accountedFor[i] && RcSet.class.equals(parameters[i].getType())) {
        		accountedFor[i] = true;
        		map.put(i, rcommando.getSetOf(getName(parameters[i])));
        	}
        }
        return map;
    }
    
    private Map<Integer, TypedParam> discoverJsonParameterIndexes(Parameter[] parameters, boolean[] accountedFor) {
        Map<Integer, TypedParam> result = new HashMap<>();
        for (int i=0; i<parameters.length; i++) {
        	if (!accountedFor[i]) {
                result.put(i, createTypedParameter(parameters[i]));
            }
        }
        return result;
    }
    
    private TypedParam createTypedParameter(Parameter parameter) {
        return new TypedParam(getName(parameter), createJavaType(parameter));
    }
    
    private String getName(Parameter parameter) {
        RetaskParam annotation = parameter.getAnnotation(RetaskParam.class);
    	return annotation == null ? parameter.getName() : annotation.value();
    }
    
    private JavaType createJavaType(Parameter parameter) {
        JavaType type = objectMapper.constructType(parameter.getParameterizedType());
        return type;
    }
    
    private class ChangedValueParam {

        private final int index;
        private final JavaType type;

        public ChangedValueParam(int index, JavaType type) {
            this.index = index;
            this.type = type;
        }

        public int getIndex() {
            return index;
        }

        public JavaType getType() {
            return type;
        }
    }

    private class TypedParam {

        private final String name;
        private final JavaType type;

        public TypedParam(String name, JavaType type) {
        	this.name = name;
        	this.type = type;
        }

        public String getName() {
            return name;
        }

        public JavaType getType() {
            return type;
        }
    }
}
