package dev.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.jarcadia.redao.RedaoCommando;
import dev.jarcadia.redao.RedaoObjectMapper;
import dev.jarcadia.redao.callbacks.DaoDeletedCallback;
import dev.jarcadia.redao.callbacks.DaoInsertedCallback;
import dev.jarcadia.redao.callbacks.DaoValueModifiedCallback;
import dev.jarcadia.redao.proxy.Proxy;
import dev.jarcadia.retask.HandlerMethod.HandlerSource;

import io.lettuce.core.RedisClient;

public class Retask {
    
    private static final Logger logger = LoggerFactory.getLogger(Retask.class);
    
    private final RetaskRepository retaskRepository;
    private final RecruitmentResults recruitmentResults;

    private Retask(RetaskRepository retaskRepository, RecruitmentResults recruitmentResults) {
        this.retaskRepository = retaskRepository;
        this.recruitmentResults = recruitmentResults;
    }
    
    public void submit(Task... tasks) {
        retaskRepository.submit(tasks);
    }
    
    public Future<Void> call(Task task) {
    	return retaskRepository.call(task, Void.class);
    }
    
    public <T> Future<T> call(Task task, Class<T> clazz) {
    	return retaskRepository.call(task, clazz);
    }
    
    public <T> Future<T> call(Task task, TypeReference<T> typeRef) {
    	return retaskRepository.call(task, typeRef);
    }
    
    public void revokeAuthority(String recurKey) {
        retaskRepository.revokeAuthority(recurKey);
    }

    public void setAvailablePermits(String permitKey, int numPermits) {
        retaskRepository.setAvailablePermits(permitKey, numPermits);
    }

    public int getAvailablePermits(String permitKey) {
        return this.retaskRepository.getAvailablePermits(permitKey);
    }

    public Set<String> verifyRecruits(Collection<String> requestedRoutes) {
        return this.recruitmentResults.verifyRoutes(requestedRoutes);
    }
    
    public <A extends Annotation> List<HandlerMethod> getHandlersByAnnontation(Class<A> annontationClass) {
        return this.recruitmentResults.getHandlers(annontationClass);
    }
    
    public List<HandlerMethod> getHandlersByRoutingKey(String routingKey) {
        return this.recruitmentResults.getHandlers(routingKey);
    }
    
    public static RetaskManager init(RedisClient redis, RedaoCommando rcommando, RetaskRecruiter recruiter) {
        
        // Create internal prereqs
        final RetaskRepository repo = new RetaskRepository(rcommando);

        // Data integrity checks
        repo.checkForScheduledDuplicates();

        // Explicitly register external setter
        recruiter.recruitFromClass(ExternalSetWorker.class);

        // Scan for recruits
        final RecruitmentResults recruits = recruiter.recruit();
        
        // Create public API object
        final Retask retask = new Retask(repo, recruits);
        
        // Setup insert handlers
        for (HandlerMethod insertHandler : dedupeHandlersByRoutingKey(recruits.getHandlers(HandlerSource.INSERT))) {
            DaoInsertedCallback callback = (dao) -> {
                Task task = Task.create(insertHandler.getRoutingKey()).param("object", dao);
                repo.submit(task);
            };
            rcommando.registerObjectInsertCallback(insertHandler.getType(), callback);
        }

        // Setup delete handlers
        for (HandlerMethod deleteHandler : dedupeHandlersByRoutingKey(recruits.getHandlers(HandlerSource.DELETE))) {
            DaoDeletedCallback callback = (setKey, id) -> {
                Task task = Task.create(deleteHandler.getRoutingKey()).param("id", id);
                repo.submit(task);
            };
            rcommando.registerObjectDeleteCallback(deleteHandler.getType(), callback);
        }
        
        // Setup change handlers
        for (HandlerMethod changeHandler : dedupeHandlersByRoutingKey(recruits.getHandlers(HandlerSource.CHANGE))) {
        	 DaoValueModifiedCallback callback = (dao, field, before, after) -> {
                 Task task = Task.create(changeHandler.getRoutingKey()).forChangedValue(dao, field, before, after);
                 repo.submit(task);
                 logger.trace("Dispatched change task {}: {}.{}.{}: {} -> {}", task.getId(), dao.getType(), dao.getId(), field, before.getRawValue(), after.getRawValue());
             };
             rcommando.registerFieldChangeCallback(changeHandler.getType(), changeHandler.getFieldName(), callback);
        }
        
        // Register Jackson modules for RcProxy classes
        RedaoObjectMapper objectMapper = (RedaoObjectMapper) rcommando.getObjectMapper();
        for (Class<? extends Proxy> proxyClass : recruits.getProxyClasses()) {
        	objectMapper.registerProxyClass(proxyClass);
        }

        return new RetaskManager(rcommando, retask, repo, recruits);
    }

    /**
     * There can be multiple insert, delete, or change callbacks for the same setKey/fieldName. These distinct 
     * handlers that overlap as described would share the same routingKey. On an insert/delete/change, only one task
     * needs to be created. The different handlers will be routed to automatically once the task is popped.
     * 
     * This helper method dedupes the routing keys in a list of HandlerMethods to ensure only one task is pushed for
     * each (potentially shared) routing key. Note it doesn't matter which HandlerMethod is chosen, as long as they
     * are deduped by routing key.
     */
    private static List<HandlerMethod> dedupeHandlersByRoutingKey(List<HandlerMethod> handlers) {
    	return handlers.stream()
                .collect(Collectors.groupingBy(HandlerMethod::getRoutingKey)).values().stream()
                .map(list -> list.stream().findAny().get())
                .collect(Collectors.toList());
    }
}
