package dev.jarcadia;

import dev.jarcadia.redis.RedisConnection;
import dev.jarcadia.redis.RedisEval;
import dev.jarcadia.redis.RedisFactory;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SchedulePopperRepository {

    private final RedisConnection rc;

    public SchedulePopperRepository(RedisFactory jarcadiaRedisFactory) {
        this.rc = jarcadiaRedisFactory.openConnection();
    }

    protected SchedulePopResponse schedulePop(long now, int limit) {
        return this.schedulePop(now, limit, null, null);
    }

    protected SchedulePopResponse schedulePop(long now, int limit, Stream<String> permitKeys, String serializedCronMap) {
        RedisEval eval = this.rc.eval()
                .cachedScript(Scripts.SCHEDULED_TASK_POP)
                .addKeys(Keys.QUEUE, Keys.SCHEDULE, Keys.FUTURES, Keys.RECUR_DATA)
                .addKeys()
                .addArgs(String.valueOf(now), "100", String.valueOf(limit));

        if (permitKeys != null) {
            eval.addKey(Keys.BACKLOG);
            eval.addKeys(permitKeys.flatMap(permitKey -> Stream.of(permitKey + ".available", permitKey + ".backlog")));
        }

        if (serializedCronMap != null) {
            eval.addArg(serializedCronMap);
        }

        List<String> response = eval.returnMulti();
        return parseResponse(response);
    }

    private SchedulePopResponse parseResponse(List<String> response) {
        if (response.size() == 1 && response.get(0) == null) {
            return null;
        }

        if (response.size() > 1) {
            int numPermitKeys = Integer.parseInt(response.get(1));
            return new SchedulePopResponse(response, 2 + numPermitKeys);
        } else {
            // This is essentially any empty SchedulePopResponse that indicates more elements are available
            // but no permitKeys or cronExpressions are needed
            return new SchedulePopResponse(response, 1);
        }

    }

    protected void close() {
        rc.close();
    }

    protected static class SchedulePopResponse {

        private final List<String> response;
        private final int cronReqStartIdx;

        private SchedulePopResponse(List<String> response, int cronReqStartIdx) {
            this.response = response;
            this.cronReqStartIdx = cronReqStartIdx;
        }

        protected boolean hasPermitKeys() {
            return cronReqStartIdx > 2;
        }

        protected Stream<String> getPermitKeys() {
            return IntStream.range(2, cronReqStartIdx).mapToObj(idx -> response.get(idx));
        }

        protected boolean hasCronRequests() {
            return cronReqStartIdx < response.size();
        }

        protected Stream<String> getCronRequests() {
            return IntStream.range(cronReqStartIdx, response.size()).mapToObj(idx -> response.get(idx));
        }
    }
}
