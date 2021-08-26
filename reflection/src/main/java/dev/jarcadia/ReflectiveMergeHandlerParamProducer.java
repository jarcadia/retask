package dev.jarcadia;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.ParamsException;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ReflectiveMergeHandlerParamProducer extends ReflectiveParamProducer {

    private final ObjectMapper objectMapper;

    private final Method method;
//    private final int appliedMergeParamIndex;
//    private final int recordParamIndex;
//    private final Map.Entry<Integer, Class<? extends Proxy>> proxyParam;
//    private final Map<Integer, ModifiedValueParam> modifiedValueParams;
    private final String[] valueFieldNames;
//    private final Map<Integer, ValueParam> valueParams;

    protected ReflectiveMergeHandlerParamProducer(Jarcadia jarcadia, ObjectMapper objectMapper, Method method) {
        super(jarcadia, method.getParameters());

        this.method = method;
        Parameter[] parameters = method.getParameters();
        this.objectMapper = objectMapper;

//        this.modifiedValueParams = new HashMap<>();
//        this.valueParams = new LinkedHashMap<>();
//
//        this.appliedMergeParamIndex = discoverTypedParamIndex(parameters, AppliedMerge.class);
//        this.recordParamIndex = discoverTypedParamIndex(parameters, Record.class);

//        int proxyParamIndex = discoverAssignableParamIndex(parameters, Proxy.class);
//        this.proxyParam = proxyParamIndex >= 0 ? new AbstractMap.SimpleImmutableEntry<>(proxyParamIndex,
//                (Class<? extends Proxy>) parameters[proxyParamIndex].getType()) : null;

        // All other parameters will be sourced from the Modification (either from changed fields or the record directly)
        List<String> tempValueFieldNames = new LinkedList<>();
        for (int i=0; i<parameters.length; i++) {

//            if (!accountedFor[i]) {
//                Parameter parameter = parameters[i];
//                if (isClass(parameter, ModifiedField.class)) {
//                    modifiedValueParams.put(i, new ModifiedValueParam(getName(parameter), false));
//                } else if (isOptionalOf(parameter, ModifiedField.class)) {
//                    modifiedValueParams.put(i, new ModifiedValueParam(getName(parameter), true));
//                } else if (isClass(parameter, Field.class)) {
//                    tempValueFieldNames.add(getName(parameter));
//                    valueParams.put(i, new SimpleValueParam());
//                } else if (isOptional(parameter)) {
//                    tempValueFieldNames.add(getName(parameter));
//                    valueParams.put(i, new OptionalValueParam(createJavaType(parameter)));
//                } else {
//                    tempValueFieldNames.add(getName(parameter));
//                    valueParams.put(i, new TypedValueParam(createJavaType(parameter)));
//                }
//            }
        }
        this.valueFieldNames = tempValueFieldNames.toArray(new String[0]);
    }

    protected Object[] produceParams(String table, String id, Object appliedMerge) throws ParamsException {

        try {
            Object[] result = super.produceParams();

//            addIfPresent(result, appliedMergeParamIndex, appliedMerge);
//            addIfPresent(result, recordParamIndex, record);

//        if (proxyParam != null) {
//            result[proxyParam.getKey()] = mod.getRecord().as(proxyParam.getValue());
//        }

//            if (modifiedValueParams.size() > 0) {
//                for (Map.Entry<Integer, ModifiedValueParam> entry : modifiedValueParams.entrySet()) {
//                    result[entry.getKey()] = entry.getValue().yield(appliedMerge);
//                }
//            }

            if (valueFieldNames.length > 0) {
//                Iterator<Field> fields = record.getFields(valueFieldNames).iterator();
//                for (Map.Entry<Integer, ValueParam> entry : valueParams.entrySet()) {
//                    Field field = fields.next();
//                    result[entry.getKey()] = entry.getValue().yield(field);
//                }
            }
            return result;
        } catch (Exception ex) {
            throw new ParamsException("Unable to produce parameters for " + method.getDeclaringClass().getSimpleName()
                    + "." + method.getName(), ex);
        }
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

//    private class ModifiedValueParam {
//
//        private final String name;
//        private final boolean isOptional;
//
//        public ModifiedValueParam(String name, boolean isOptional) {
//            this.name = name;
//            this.isOptional = isOptional;
//        }
//
//        Object yield(AppliedMerge appliedMerge) {
//            ModifiedField modVal = appliedMerge.get(name);
//            if (isOptional) {
//                Optional<ModifiedField> empty = Optional.ofNullable(modVal);
//                return empty;
//            } else {
//                return modVal;
//            }
//        }
//    }

//    private interface ValueParam {
//        Object yield(Field field);
//    }

//    private static class SimpleValueParam implements ValueParam {
//
//        SimpleValueParam() { }
//
//        @Override
//        public Object yield(Field field) {
//            return field;
//        }
//    }
//
//    private static class TypedValueParam implements ValueParam {
//
//        private final JavaType javaType;
//
//        private TypedValueParam(JavaType javaType) {
//            this.javaType = javaType;
//        }
//
//        @Override
//        public Object yield(Field field) {
//            if (field.isPresent()) {
//                return field.as(javaType);
//            } else {
//                return null;
//            }
//        }
//    }
//
//    private static class OptionalValueParam implements ValueParam {
//
//        private final JavaType javaType;
//
//        private OptionalValueParam(JavaType javaType) {
//            this.javaType = javaType;
//        }
//
//        @Override
//        public Object yield(Field field) {
//
//            if (field.isPresent()) {
//                return field.as(javaType);
//            } else {
//                return Optional.empty();
//            }
//        }
//    }
}
