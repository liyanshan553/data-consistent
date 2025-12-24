package com.smee.binlog.collector;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDBFactory;
import lombok.var;
import org.springframework.stereotype.Component;

@Component
public class HyperTableMetaTSDBFactory implements TableMetaTSDBFactory {

    private final HyperTableMetaTSDB tsdb;

    public HyperTableMetaTSDBFactory(HyperTableMetaTSDB tsdb) {
        this.tsdb = tsdb;
    }

    @Override
    public TableMetaTSDB build(String destination, String springXml) {
        return tsdb;
    }

    @Override
    public void destory(String destination) {
    }
}