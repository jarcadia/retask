package dev.jarcadia;

import dev.jarcadia.iface.ReturnValueHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

class ReturnValueService {

    private static final Logger logger = LoggerFactory.getLogger(ReturnValueService.class);

    private final Map<Class<?>, Set<Wrapper<?>>> handlerMap;

    protected ReturnValueService() {
        this.handlerMap = new ConcurrentHashMap<>();
    }

    protected <T extends Object> void registerHandler(Class<T> type, ReturnValueHandler<T> rvh) {
        handlerMap.computeIfAbsent(type, t -> ConcurrentHashMap.newKeySet())
                .add(new Wrapper<>(type, rvh));
    }

    protected void handle(Object returned) {
        if (returned == null) {
            return;
        } else if (returned instanceof Iterable) {
            Iterable<? extends Object> iterable = (Iterable<? extends Object>) returned;
            for (Object child : iterable) {
                handle(child);
            }
        } else if (returned instanceof Stream) {
            Stream<? extends Object> stream = (Stream<? extends Object>) returned;
            for (Iterator<? extends Object> it = stream.iterator(); it.hasNext(); ) {
                handle(it.next());
            }
        } else {
            Set<Wrapper<?>> wrappers = handlerMap.get(returned.getClass());
            if (wrappers != null) {
                for (Wrapper<?> wrapper : wrappers) {
                    wrapper.call(returned);
                }
            } else {
                logger.info("No ReturnValueHandler registered for {}", returned.getClass().getSimpleName());
            }
        }
    }

    record Wrapper<T>(Class<T> type, ReturnValueHandler<T> handler){
        void call(Object obj) {
            try {
                handler.handle((T) obj);
            } catch (Exception ex) {
                logger.warn("Exception while handling return value " + obj, ex);
            }
        }
    }
}
