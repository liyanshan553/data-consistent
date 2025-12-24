package com.smee.binlog.collector;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import lombok.var;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BinLogFileQueue {
    private final ApplicationConfig config;
    private String baseName = "mysql-bin.";
    private File directory;
    private ReentrantLock lock = new ReentrantLock();
    private CanalParseException exception = null;
    private Pattern binlogIndexPattern = Pattern.compile(baseName + "(\\d{6})$");

    public BinLogFileQueue(ApplicationConfig config, String directory) {
        this(config, new File(directory));
    }

    public BinLogFileQueue(ApplicationConfig config, File directory) {
        this.config = config;
        this.directory = directory;

        if (!directory.canRead()) {
            throw new CanalParseException("Binlog index missing or unreadable: " + directory.getAbsolutePath());
        }
    }

    private int getFileIndex(String name) {
        Matcher matcher = binlogIndexPattern.matcher(name);
        ArrayList<String> arr = new ArrayList<>(2);
        while (matcher.find()) {
            arr.add(matcher.group(1));
        }
        return Integer.parseInt(arr.get(0));
    }

    private String getFileNameByIndex(int index) {
        return String.format("%s%06d", baseName, index);
    }

    /**
     * 根据前一个文件，获取符合条件的下一个 binlog 文件
     * @param file
     * @return
     */
    public File getNextFile(File file) throws InterruptedException {
        try {
            lock.lockInterruptibly();
            if (exception != null) {
                throw exception;
            }

            var allFiles = listBinlogFiles();
            Collections.reverse(allFiles);
            var allFilesName = allFiles
                    .stream()
                    .map(x -> x.getName())
                    .collect(Collectors.toList());
            
            var fileIndex = allFilesName.indexOf(file.getName());
            if (fileIndex < config.getBinlogFileDelayCount()) {
                return null;
            }

            int binlogIndex = getFileIndex(file.getName());
            File nextFile = new File(directory, getFileNameByIndex(binlogIndex + 1));
            return nextFile;
        } finally {
            lock.unlock();
        }
    }

    public File getBefore(File file) {
        try {
            lock.lockInterruptibly();
            if (exception != null) {
                throw exception;
            }

            int fileIndex = getFileIndex(file.getName());
            File preFile = new File(directory, getFileNameByIndex(fileIndex - 1));
            return preFile;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    public void destory() {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private List<File> listBinlogFiles() {
        List<File> files = new ArrayList<>();
        files.addAll(FileUtils.listFiles(directory, new IOFileFilter() {
            @Override
            public boolean accept(File file) {
                Matcher matcher = binlogIndexPattern.matcher(file.getName());
                return matcher.find();
            }

            @Override
            public boolean accept(File dir, String name) {
                return true;
            }
        }, null));
        
        // 排一下序列
        files.sort(Comparator.comparing(File::getName));
        return files;
    }

    public void setBaseName(String baseName) {
        this.baseName = baseName;
    }
}