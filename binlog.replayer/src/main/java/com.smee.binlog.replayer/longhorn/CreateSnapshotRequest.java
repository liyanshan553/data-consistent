package com.smee.binlog.replayer.longhorn;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CreateSnapshotRequest {
    private String name;
}