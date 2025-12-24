package com.smee.binlog.replayer;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.smee.binlog.replayer.longhorn.LonghornClient;
import lombok.var;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
public class BeanFactory {

    @Bean
    public ApplicationConfig configBean(ApplicationArguments arg) throws IOException {
        var mapper = new YAMLMapper();
        return mapper.readValue(new File(arg.getSourceArgs()[0]), ApplicationConfig.class);
    }

    @Bean
    public LonghornClient longhornClient(ApplicationConfig config) {
        return LonghornClient.build(config.getMetadata().getLonghorn());
    }
}