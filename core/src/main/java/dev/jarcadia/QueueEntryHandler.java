package dev.jarcadia;

import io.lettuce.core.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Phaser;

class QueueEntryHandler {

    private final Logger logger = LoggerFactory.getLogger(QueueEntryHandler.class);

    private final DmlEventEntryHandler dmlEventEntryHandler;
    private final TaskEntryHandler taskEntryHandler;

    protected QueueEntryHandler(DmlEventEntryHandler dmlEventEntryHandler, TaskEntryHandler taskEntryHandler) {
        this.dmlEventEntryHandler = dmlEventEntryHandler;
        this.taskEntryHandler = taskEntryHandler;
    }

    protected void handle(StreamMessage<String, String> entry, Phaser phaser) {
        logger.info("Handling queue item {} - {}", entry.getId(), entry.getBody());
        Map<String, String> body = entry.getBody();

        if (body.containsKey("route")) {
            taskEntryHandler.dispatchTaskEntry(entry.getId(), body, phaser);
        } else if (body.containsKey("stmt")) {
            dmlEventEntryHandler.dispatchDmlEntry(entry.getId(), body, phaser);
        } else {
            logger.warn("No category identified for queue item {} - {}", entry.getId(), body);
        }
    }
}
