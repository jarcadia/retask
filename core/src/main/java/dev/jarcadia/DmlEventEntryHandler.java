package dev.jarcadia;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.RetaskException;
import dev.jarcadia.iface.DmlEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;

public class DmlEventEntryHandler {

    private final Logger logger = LoggerFactory.getLogger(DmlEventEntryHandler.class);

    private final ExecutorService executorService;
    private final ObjectMapper objectMapper;
    private final Map<String, Set<DmlEventHandler>> insertHandlerMap;
    private final Map<String, Set<DmlEventHandler>> updateHandlerMap;
    private final Map<String, Map<String, Set<DmlEventHandler>>> fieldUpdateHandlerMap;
    private final Map<String, Set<DmlEventHandler>> deleteHandlerMap;
    private ReturnValueHandler returnValueHandler;

    protected DmlEventEntryHandler(ExecutorService executorService, ObjectMapper objectMapper,
            ReturnValueHandler returnValueHandler) {
        this.executorService = executorService;
        this.objectMapper = objectMapper;
        this.returnValueHandler = returnValueHandler;
        this.updateHandlerMap = new ConcurrentHashMap<>();
        this.insertHandlerMap = new ConcurrentHashMap<>();
        this.fieldUpdateHandlerMap = new ConcurrentHashMap<>();
        this.deleteHandlerMap = new ConcurrentHashMap<>();
    }

    protected Runnable registerInsertHandler(String type, DmlEventHandler handler) {
        this.insertHandlerMap.computeIfAbsent(type, k -> ConcurrentHashMap.newKeySet()).add(handler);
        return () -> deregisterInsertHandler(type, handler);
    }

    protected Runnable registerUpdateHandler(String type, DmlEventHandler handler) {
        this.updateHandlerMap.computeIfAbsent(type, k -> ConcurrentHashMap.newKeySet()).add(handler);
        return () -> deregisterUpdateHandler(type, handler);
    }

    protected Runnable registerUpdateHandler(String type, String[] fields, DmlEventHandler handler) {
        Map<String, Set<DmlEventHandler>> typeChangeHandlers = fieldUpdateHandlerMap
                .computeIfAbsent(type, k-> new ConcurrentHashMap<>());
        for (String field : fields) {
            typeChangeHandlers.computeIfAbsent(field, k -> ConcurrentHashMap.newKeySet()).add(handler);
        }
        return () -> deregisterUpdateHandler(type, handler, fields);
    }

    protected Runnable registerDeleteHandler(String table, DmlEventHandler consumer) {
        this.deleteHandlerMap.computeIfAbsent(table, k -> ConcurrentHashMap.newKeySet()).add(consumer);
        return () -> deregisterDeleteHandler(table, consumer);
    }

    private void deregisterInsertHandler(String table, DmlEventHandler consumer) {
        this.updateHandlerMap.computeIfAbsent(table, k -> ConcurrentHashMap.newKeySet()).remove(consumer);
    }

    private void deregisterUpdateHandler(String table, DmlEventHandler consumer) {
        this.updateHandlerMap.computeIfAbsent(table, k -> ConcurrentHashMap.newKeySet()).remove(consumer);
    }

    private void deregisterUpdateHandler(String table, DmlEventHandler handler, String[] fields) {
        for (String field : fields) {
            fieldUpdateHandlerMap.computeIfAbsent(table, k-> new ConcurrentHashMap<>())
                    .computeIfAbsent(field, k -> ConcurrentHashMap.newKeySet())
                    .remove(handler);
        }
    }

    private void deregisterDeleteHandler(String table, DmlEventHandler handler) {
        this.deleteHandlerMap.computeIfAbsent(table, k -> ConcurrentHashMap.newKeySet()).remove(handler);
    }

    protected void dispatchDmlEntry(String entryId, Map<String, String> body, Phaser phaser) {
        String stmt = body.get("stmt");
        boolean isUpdate = "update".equals(stmt);
        String table = body.get("table");
        Set<DmlEventHandler> handlers = getHandlers(stmt, table);

        if (handlers.size() > 0 || isUpdate) {
            Fields fields = deserializeData(body);

            if (isUpdate) {
                handlers.addAll(getFieldUpdateHandlers(table, fields.getNames()));
            }

            if (handlers.size() > 0) {
                logger.trace("Invoking {} {} handlers for {}", handlers.size(), stmt, table);
                for (DmlEventHandler handler : handlers) {
                    phaser.register();
                    executorService.execute(() -> {
                        try {
                            Object returnValue = handler.apply(table, fields);
                            returnValueHandler.handle(table, fields, returnValue);
                        } catch (Throwable t) {
                            logger.warn("Exception occurred while processing callback for {}", table, t);
                        } finally {
                            phaser.arriveAndDeregister();
                        }
                    });
                }
            }
        }
    }

    private Set<DmlEventHandler> getHandlers(String stmt, String table) {
        Map<String, Set<DmlEventHandler>> handlerMap = switch (stmt) {
            case "insert" -> insertHandlerMap;
            case "update" -> updateHandlerMap;
            case "delete" -> deleteHandlerMap;
            default -> Map.of();
        };
        Set<DmlEventHandler> handlersForTable = handlerMap.get(table);
        return handlersForTable == null ? new HashSet<>() : new HashSet<>(handlersForTable);
    }

    private Set<DmlEventHandler> getFieldUpdateHandlers(String table, Set<String> fieldNames) {
        HashSet<DmlEventHandler> handlers = new HashSet<>();

        Map<String, Set<DmlEventHandler>> fieldUpdateHandlersByTable = fieldUpdateHandlerMap.get(table);
        if (fieldUpdateHandlersByTable != null) {
            for (String changedField : fieldNames) {
                Set<DmlEventHandler> updateHandlersForField = fieldUpdateHandlersByTable.get(changedField);
                if (updateHandlersForField != null) {
                    handlers.addAll(updateHandlersForField);
                }
            }
        }

        return handlers;
    }

    private Fields deserializeData(Map<String, String> body) {
        try {
            return objectMapper.readValue(body.get("data").getBytes(StandardCharsets.UTF_8), Fields.class);
        } catch (IOException ex) {
            throw new RetaskException("Unable to deserialize dmlEvent data");
        }
    }
}
