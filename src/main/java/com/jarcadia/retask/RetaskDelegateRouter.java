package com.jarcadia.retask;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetaskDelegateRouter implements RetaskDelegate {

    private final Logger logger = LoggerFactory.getLogger(RetaskDelegateRouter.class);
    
    private final Map<String, RetaskDelegate> routes;
    
    RetaskDelegateRouter(Map<String, RetaskDelegate> routes) {
        this.routes = routes;
    }

    @Override
    public Object invoke(String taskName, String routingKey, int attempt, int permit, String before, String after, String params, TaskBucket bucket) throws Throwable {
        RetaskDelegate destination = routes.get(routingKey);
        if (destination != null) {
            return destination.invoke(taskName, routingKey, attempt, permit, before, after, params, bucket);
        } else {
            logger.info("No @RetaskHandler for routingKey {} (task {})", routingKey, taskName);
            return null;
        }
    }

}
