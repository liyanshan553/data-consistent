package com.smee.binlog.collector;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.ServerIdNotMatchException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.taobao.tddl.dbsync.binlog.*;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.StopLogEvent;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
public class LocalBinlogConnection implements ErosaConnection {

    private static final Logger logger = LoggerFactory.getLogger(LocalBinlogConnection.class);
    private final ApplicationConfig config;
    private BinlogFileQueue binlogs = null;
    private String directory;
    private int bufferSize = 16 * 1024;
    private boolean running = false;
    private long serverId;

    private FileParserListener parserListener;

    public LocalBinlogConnection(ApplicationConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws IOException {
        if (this.binlogs == null) {
            this.binlogs = new BinlogFileQueue(config, this.directory);
        }
        this.running = true;
    }

    @Override
    public void reconnect() throws IOException {
        disconnect();
        connect();
    }

    @Override
    public void disconnect() throws IOException {
        this.running = false;
        if (this.binlogs != null) {
            this.binlogs.destory();
        }
        this.binlogs = null;
        this.running = false;
    }

    public boolean isConnected() {
        return running;
    }

    public void seek(String binlogfilename, Long binlogPosition, String gtid, SinkFunction func) throws IOException {
    }


    public void dump(long timestampMills, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }
    @Override
    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        File current = new File(directory, binlogfilename);
        if(!current.exists()){
            throw new CanalParseException("binlog:"+binlogfilename+" is not found");
        }
        try (FileLogFetcher fetcher = new FileLogFetcher(bufferSize)) {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            fetcher.open(current, binlogPosition);
            context.setLogPosition(new LogPosition(binlogfilename, binlogPosition));
            
            while (running) {
                boolean needContinue = false;
                LogEvent event = null;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        continue;
                    }
                    
                    checkServerId(event);
                    
                    if (!func.sink(event)) {
                        needContinue = false;
                        break;
                    }
                    
                    if (event instanceof StopLogEvent 
                        || event instanceof RotateLogEvent) {
                        needContinue = true;
                        break;
                    }
                }
                
                fetcher.close(); // 关闭上一个文件
                parserFinish(current.getName());
                
                if (needContinue) { // 读取下一个
                    File nextFile = null;
                    nextFile = binlogs.getNextFile(current);
                    
                    if (nextFile == null) {
                        break;
                    }
                    
                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(nextFile.getName()));
                } else {
                    break; // 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinlogConnection dump interrupted");
        }
    }

    private void checkServerId(LogEvent event) {
        if (serverId != 0 && event.getServerId() != serverId) {
            throw new ServerIdNotMatchException("unexpected serverId " + event.getServerId() + " in binlog file !");
        }
    }

    @Override
    public void dump(String binlogfilename, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException {
        File current = new File(directory, binlogfilename);
        if (!current.exists()) {
            throw new CanalParseException("binlog: " + binlogfilename + " is not found");
        }

        try (FileLogFetcher fetcher = new FileLogFetcher(bufferSize)) {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            fetcher.open(current, binlogPosition);
            context.setLogPosition(new LogPosition(binlogfilename, binlogPosition));

            while (running) {
                boolean needContinue = false;
                LogEvent event = null;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        continue;
                    }

                    checkServerId(event);

                    if (!coprocessor.publish(event)) {
                        needContinue = false;
                        break;
                    }

                    if (event instanceof StopLogEvent 
                        || event instanceof RotateLogEvent) {
                        needContinue = true;
                        break;
                    }
                }

                fetcher.close(); // 关闭上一个文件
                parserFinish(binlogfilename);
                
                if (needContinue == true) { // 读取下一个
                    File nextFile = null;
                    nextFile = binlogs.getNextFile(current);

                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    fetcher.open(current);
                    binlogfilename = nextFile.getName();

                    if (!binlogfilename.equals(context.getLogPosition().getFileName())) {
                        context.setLogPosition(new LogPosition(binlogfilename, 4L));
                    }
                } else {
                    break; // 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinlogConnection dump interrupted");
        }
    }

    private void parserFinish(String fileName) {
        if (parserListener != null) {
            parserListener.onFinish(fileName);
        }
    }

    @Override
    public void dump(long timestampMills, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NotImplementedException();
    }
    

   @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public ErosaConnection fork() {
        LocalBinlogConnection connection = new LocalBinlogConnection(config);
        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        return connection;
    }

    @Override
    public long queryServerId() { return 0; }

    public String getDirectory() { return directory; }

    public void setDirectory(String directory) { this.directory = directory; }

    public int getBufferSize() { return bufferSize; }

    public void setBufferSize(int bufferSize) { this.bufferSize = bufferSize; }

    public long getServerId() { return serverId; }

    public void setServerId(long serverId) { this.serverId = serverId; }

    public void setParserListener(FileParserListener parserListener) { this.parserListener = parserListener; }

    public interface FileParserListener {
        void onFinish(String fileName);
    }
}