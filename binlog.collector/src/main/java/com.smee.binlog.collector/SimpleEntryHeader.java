package com.smee.binlog.collector;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import lombok.Data;

@Data
public class SimpleEntryHeader {
    private String journalName;
    private Long position;

    public SimpleEntryHeader(EntryPosition pos) {
        this.journalName = pos.getJournalName();
        this.position = pos.getPosition();
    }
}