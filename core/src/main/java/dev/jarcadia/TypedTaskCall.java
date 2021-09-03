//package dev.jarcadia;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import dev.jarcadia.exception.CalledTaskException;
//
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//public class TypedTaskCall<T> {
//
//    private final ObjectMapper objectMapper;
//    private final TaskCall source;
//    private final Class<T> type;
//    private final TypeReference<T> typeRef;
//
//    protected TypedTaskCall(ObjectMapper objectMapper, TaskCall source, Class<T> type) {
//        this(objectMapper, source, type, null);
//    }
//
//    protected TypedTaskCall(ObjectMapper objectMapper, TaskCall source, TypeReference<T> typeRef) {
//        this(objectMapper, source, null, typeRef);
//    }
//
//    private TypedTaskCall(ObjectMapper objectMapper, TaskCall source, Class<T> type, TypeReference<T> typeRef) {
//        this.objectMapper = objectMapper;
//        this.source = source;
//        this.type = type;
//        this.typeRef = typeRef;
//    }
//
//    public T await(long timeout, TimeUnit unit) throws TimeoutException, CalledTaskException {
//        String response = source.await(timeout, unit);
//        return typeRef == null ? deserializeAsType(response) : deserializeAsTypeRef(response);
//    }
//
//    private T deserializeAsType(String response) throws CalledTaskException {
//        try {
//            return objectMapper.readValue(response, type);
//        } catch (JsonProcessingException ex) {
//            throw new CalledTaskException("Unable to deserialize task response as " + type.getSimpleName(), ex);
//        }
//    }
//
//    private T deserializeAsTypeRef(String response) throws CalledTaskException {
//        try {
//            return objectMapper.readValue(response, typeRef);
//        } catch (JsonProcessingException ex) {
//            throw new CalledTaskException("Unable to deserialize task response as " + typeRef.getType().getTypeName(),
//                    ex);
//        }
//    }
//
//}
