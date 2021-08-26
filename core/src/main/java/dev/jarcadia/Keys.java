package dev.jarcadia;

import java.util.List;

public class Keys {

    protected static final String QUEUE = "queue";
    protected static final String SCHEDULE = "schedule";
    protected static final String FUTURES = "futures";
    protected static final String BACKLOG = "backlog";
    protected static final String RECUR_DATA = "recur";
    protected static final String CONSUMER_GROUP = "jarcadia-backends";

    protected static final String PERMIT_CAPS = "permits";

    protected static String Record(String type, String id) {
        return String.format("%s/%s", type, id);
    }

    protected static String PrimaryIndex(String type) {
        return String.format("%s@id", type);
    }

    protected static String AlphaIndex(String type, String field) {
        return String.format("%s@%s", type, field);
    }

    protected static String NumericIndex(String type, String field) {
        return String.format("%s#%s", type, field);
    }

    protected static String TrackingStream(String typeOrIndex) {
        return typeOrIndex + ">";
    }

    protected static String TrackingChannel(String typeOrIndex) {
        return String.format("socket.io#/#/%s#", typeOrIndex);
    }

    protected static String AvailablePermitList(String permitKey) {
        return permitKey + ".available";
    }

    protected static String PermitBacklogList(String permitKey) {
        return permitKey + ".backlog";
    }

    protected static List<String> PermitLists(String permitKey) {
        return List.of(permitKey + ".available", permitKey + ".backlog");
    }

}
