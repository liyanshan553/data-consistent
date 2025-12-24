package com.smee.binlog.collector;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.model.SqlWithPos;
import lombok.Data;

import java.util.List;

@Data
public class SinkBinlogEntry {

    private EntryPosition header;
    private List<SqlWithPos> sqlList;

}