//package dev.jarcadia;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import dev.jarcadia.exception.CalledTaskException;
//
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//public class TaskCall {
//
//    private final ObjectMapper objectMapper;
//    private final Future<String> future;
//
//    TaskCall(ObjectMapper objectMapper, Future<String> future) {
//        this.objectMapper = objectMapper;
//        this.future = future;
//    }
//
//    public String await(long timeout, TimeUnit unit) throws TimeoutException, CalledTaskException {
//        try {
//            String published = future.get(timeout, unit);
//            if ('1' == published.charAt(0)) {
//                return published.substring(1);
//            } else {
//                try {
//                    Throwable cause = objectMapper.readValue(published.substring(1), Throwable.class);
//                    throw new CalledTaskException("Task failed", cause);
//                } catch (JsonProcessingException ex) {
//                    throw new CalledTaskException("Unable to deserialize task exception response", ex);
//                }
//            }
//        } catch (InterruptedException ex) {
//            throw new CalledTaskException("Interrupted while awaiting task completion", ex);
//        } catch (ExecutionException ex) {
//            throw new CalledTaskException("Exception while awaiting task completion", ex.getCause());
//        }
//    }
//}
