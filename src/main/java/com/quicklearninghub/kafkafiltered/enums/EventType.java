package com.quicklearninghub.kafkafiltered.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum EventType {

    NEWM(1, "NEWM"), PREA(2, "PREA"), CANC(3, "CANC");

    private int id;
    private String event;

    private EventType(int id, String event) {
        this.id = id;
        this.event = event;
    }

    public int getEventId() {
        return this.id;
    }

    @JsonValue
    public String getEvent() {
        return this.event;
    }

}
