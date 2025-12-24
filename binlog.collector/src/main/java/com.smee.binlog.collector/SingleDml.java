package com.smee.binlog.collector;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class SingleDml {
    private String database;
    private String table;
    private String type;
    private String sql;
    private String position;
    private Map<String, Object> data;
    private Map<String, Object> old;
    private List<String> pkNames;
}