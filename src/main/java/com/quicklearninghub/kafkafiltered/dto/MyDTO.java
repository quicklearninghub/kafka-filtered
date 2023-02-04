package com.quicklearninghub.kafkafiltered.dto;

import com.quicklearninghub.kafkafiltered.enums.EventType;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class MyDTO {
    @NotNull(message = "Please provide a valid eventTYpe.")
    private EventType eventType;

    @NotNull(message = "Please provide a valid message.")
    private String message;

}
