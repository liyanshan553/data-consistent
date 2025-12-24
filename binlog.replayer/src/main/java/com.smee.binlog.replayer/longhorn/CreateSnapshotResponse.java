package com.smee.binlog.replayer.longhorn;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateSnapshotResponse {
    private String created;
    private String id;
    private String name;
    private String type;
    private String message;
}