package com.quicklearninghub.kafkafiltered.dto;

import com.quicklearninghub.kafkafiltered.enums.EventType;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyDTO {
    @NotNull(message = "Please provide a valid eventTYpe.")
    private EventType eventType;

    @NotNull(message = "Please provide a valid message.")
    private String message;

}
