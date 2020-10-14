package dev.jarcadia.retask;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.redao.Subscription;
import dev.jarcadia.redao.RedaoCommando;

/**
 * This class is responsible for listening for tasks that have responses and completing tasks awaiting responses
 */
class TaskResponseListener implements BiConsumer<String, String> {

    private final Logger logger = LoggerFactory.getLogger(TaskResponseListener.class);

    private final ObjectMapper objectMapper;
    private final Map<String, PendingTaskResponse<?>> map;
    private final Subscription subscription;
    
    public TaskResponseListener(RedaoCommando rcommando) {
        this.objectMapper = rcommando.getObjectMapper();
        this.map = new ConcurrentHashMap<>();
        this.subscription = rcommando.subscribe(this);
    }

	@Override
	public void accept(String channel, String message) {
		// Incoming channel is of the form task.response.<taskId>
		String taskId = channel.substring(14);
        PendingTaskResponse<?> taskResponse = map.get(taskId);
        if (taskResponse != null) {
            taskResponse.handle(message);
        }
	}
	
    protected <T> CompletableFuture<T> await(String taskId, Class<T> clazz) {
    	return await(taskId, clazz, null);
    }
    
    protected <T> CompletableFuture<T> await(String taskId, TypeReference<T> typeRef) {
    	return await(taskId, null, typeRef);
    }
    
    private <T> CompletableFuture<T> await(String taskId, Class<T> clazz, TypeReference<T> typeRef) {
    	subscription.subscribeOnce("task.response." + taskId);
    	CompletableFuture<T> future = new CompletableFuture<>();
    	map.put(taskId, new PendingTaskResponse<T>(clazz, typeRef, future));
    	return future;
    }
    
    private class PendingTaskResponse<T> {

    	private final PendingTaskeResponseType<T> type;
    	private final CompletableFuture<T> future;

		private PendingTaskResponse(Class<T> clazz, TypeReference<T> typeRef, CompletableFuture<T> future) {
			this.type = new PendingTaskeResponseType<>(clazz, typeRef);
			this.future = future;
		}

		private void handle(String json) {
			try {
				if (json.isEmpty()) {
					future.complete(null);
				} else {
                    future.complete(type.parse(json));
				}
			} catch (IOException e) {
				future.completeExceptionally(e);
			}
		}
    }
    
    private class PendingTaskeResponseType<T> {

    	private final Class<T> clazz;
    	private final TypeReference<T> typeRef;

		public PendingTaskeResponseType(Class<T> clazz, TypeReference<T> typeRef) {
			this.clazz = clazz;
			this.typeRef = typeRef;
		}
		
		private T parse(String json) throws JsonParseException, JsonMappingException, IOException {
			if (clazz != null) {
                return objectMapper.readValue(json, clazz);
			} else if (typeRef != null) {
                return objectMapper.readValue(json, typeRef);
			} else {
				throw new UnsupportedOperationException("Unable to determine type for task response");
			}
		}
    }
}