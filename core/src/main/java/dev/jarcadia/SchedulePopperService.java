package dev.jarcadia;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.SerializationException;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchedulePopperService {

    private final SchedulePopperRepository repo;
    private final ObjectMapper objectMapper;
    private final CronService cronService;
    private final int limit;

    public SchedulePopperService(SchedulePopperRepository repo, ObjectMapper objectMapper, CronService cronService,
            int limit) {
        this.repo = repo;
        this.objectMapper = objectMapper;
        this.cronService = cronService;
        this.limit = limit;
    }

    protected void schedulePop(long now) {
        SchedulePopperRepository.SchedulePopResponse response = repo.schedulePop(now, limit);
        while (response != null) {
            Stream<String> permitKeys = response.hasPermitKeys() ? response.getPermitKeys() : null;
            String serializedCronMap = response.hasCronRequests() ?
                    getSerializedCronMap(response.getCronRequests()) : null;

            response = repo.schedulePop(now, limit, permitKeys, serializedCronMap);
        }
    }

    private String getSerializedCronMap(Stream<String> cronRequests) {
        Map<String, Long> cronMap = cronRequests
                .collect(Collectors.toMap(Function.identity(), cronExpr -> cronService.nextTimestamp(cronExpr)));
        try {
            return objectMapper.writeValueAsString(cronMap);
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Unable to serialize cron map", ex);
        }
    }

    protected void shutdown() {
        repo.close();
    }
}
