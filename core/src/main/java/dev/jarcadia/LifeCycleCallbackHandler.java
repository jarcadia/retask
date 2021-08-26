package dev.jarcadia;

import dev.jarcadia.iface.StartHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class LifeCycleCallbackHandler {

    private static final Logger logger = LoggerFactory.getLogger(LifeCycleCallbackHandler.class);

    private final ExecutorService executorService;
    private final ReturnValueHandler returnValueHandler;
    private final Set<StartHandler> startHandlers;

    public LifeCycleCallbackHandler(ExecutorService executorService,
            ReturnValueHandler returnValueHandler) {
        this.executorService = executorService;
        this.returnValueHandler = returnValueHandler;
        this.startHandlers = new HashSet<>();
    }

    protected Runnable registerStartHandler(StartHandler handler) {
        this.startHandlers.add(handler);
        return () -> deregisterStartHandler(handler);
    }

    private void deregisterStartHandler(StartHandler handler) {
        this.startHandlers.remove(handler);
    }

    protected void invokeStartCallbacks() {
        for (StartHandler startHandler : startHandlers) {
            executorService.execute(() -> {
                try {
                    Object returnValue = startHandler.run();
                    returnValueHandler.handle(returnValue);
                } catch (Throwable t) {
                    logger.warn("Exception occurred while running start callback", t);
                }
            });
        }
    }
}
