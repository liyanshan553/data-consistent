package com.smee.binlog.collector;

import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import lombok.var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.net.InetSocketAddress;

@SpringBootApplication
public class ApplicationMain implements CommandLineRunner {
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationMain.class);

    public static void main(String[] args) {
        SpringApplication.run(ApplicationMain.class, args);
    }

    private final ApplicationConfig config;
    private final DefaultCanalEventSink canalEventSink;
    private final AbstractLogPositionManager binlogPositionManager;
    private final DefaultLocalBinlogEventParser parser;
    private final HyperTableMetaTSDBFactory tsdbFactory;

    public ApplicationMain(ApplicationConfig config, 
                           DefaultCanalEventSink canalEventSink, 
                           AbstractLogPositionManager binlogPositionManager, 
                           DefaultLocalBinlogEventParser parser, 
                           HyperTableMetaTSDBFactory tsdbFactory) {
        this.config = config;
        this.canalEventSink = canalEventSink;
        this.binlogPositionManager = binlogPositionManager;
        this.parser = parser;
        this.tsdbFactory = tsdbFactory;
    }

    @Override
    public void run(String... args) {
        var initialEntryPosition = config.getInitialEntryPosition();
        var defaultPosition = new EntryPosition(initialEntryPosition.getFileName(), initialEntryPosition.getPosition(), 1L);

        // WARNING: incorrect buffer size will lead to broken file buffer
        // localBinlogEventParser.setBufferSize(0);
        parser.setTransactionSize(512); // todo 设置获取events数量的上限
        parser.setRestartDelay(config.getRestartDelay());
        parser.setMasterInfo(new AuthenticationInfo(new InetSocketAddress("dummy", 3306), "", ""));
        parser.setDestination(config.getEquipment());
        parser.setMasterPosition(defaultPosition);
        parser.setDirectory(config.getBinlogLocation());
        parser.setEventSink(canalEventSink);
        parser.setLogPositionManager(binlogPositionManager);
        parser.setTableMetaTSDBFactory(tsdbFactory);
        parser.setEnableTsdb(true);
        parser.start();
    }
}