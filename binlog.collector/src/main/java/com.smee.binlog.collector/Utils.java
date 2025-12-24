package com.smee.binlog.collector;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

public class Utils {
    public static String EntryPosition2String(EntryPosition pos){
        return String.format("%s|%s", pos.getJournalName(), pos.getPosition());
    }

    public static String Entry2String(Entry entry) { return Header2String(entry.getHeader()); }

    public static String Header2String(CanalEntry.Header header) {
        return String.format("%s|%s", header.getLogfileName(), header.getLogfileOffset());
    }
}