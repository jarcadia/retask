package dev.jarcadia;

import dev.jarcadia.redis.RedisConnection;

class PermitRepository {

    private final RedisConnection rc;

    protected PermitRepository(RedisConnection rc) {
        this.rc = rc;
    }

    protected int setPermitCap(String permitKey, int cap) {
        return rc.eval()
                .cachedScript(Scripts.SET_PERMIT_CAP)
                .addKeys(Keys.PERMIT_CAPS, Keys.BACKLOG)
                .addKeys(Keys.PermitLists(permitKey))
                .addKey(Keys.QUEUE)
                .addArgs(permitKey, String.valueOf(cap))
                .returnInt();
    }

    /**
     *
     * @param permitKey
     * @param permit
     * @return 0 if permit returned to available, 1 if popped backlogged task, -1 if permit exceeded cap
     */
    protected int releasePermit(String permitKey, int permit) {
        return rc.eval()
                .cachedScript(Scripts.RELEASE_PERMIT)
                .addKeys(Keys.PERMIT_CAPS, Keys.QUEUE, Keys.BACKLOG, Keys.AvailablePermitList(permitKey),
                        Keys.PermitBacklogList(permitKey))
                .addArgs(permitKey , String.valueOf(permit))
                .returnInt();
    }

    protected long getPermitBacklogCount(String permitKey) {
        return rc.commands().llen(Keys.PermitBacklogList(permitKey));
    }
}
