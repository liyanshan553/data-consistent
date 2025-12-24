package com.smee.binlog.replayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApplicationMain implements CommandLineRunner {

    private final static Logger LOG = LoggerFactory.getLogger(ApplicationMain.class);

    public static void main(String[] args) {
        SpringApplication.run(ApplicationMain.class, args);
    }

    private static void preventExitOnException(Runnable func) {
        try {
            func.run();
        } catch (Throwable e) {
            LOG.error("An unexpected exception occurred.", e);
            while (true) {
                try {
                    Thread.currentThread().join();
                } catch (InterruptedException ignored) {

                }
            }
        }
    }

    private final ApplicationConfig config;
    private final ChangelogProcessor processor;

    public ApplicationMain(ApplicationConfig config, ChangelogProcessor processor) {
        this.config = config;
        this.processor = processor;
    }

    @Override
    public void run(String... args) {
        if (config.isExitOnError()) {
            processor.run();
        } else {
            preventExitOnException(() -> processor.run());
        }
    }
}