package com.smee.binlog.collector;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import lombok.var;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Collections;
import java.util.Properties;

public class CanalKafkaProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private final String topicName;
    private final Properties kafkaProperties;
    private final Producer<String, String> producer;
    private final JSONWriter.Context jsonWriterConfig;

    public CanalKafkaProducer(ApplicationConfig.KafkaConfig config) {
        this.topicName = config.getTopic().getName();

        kafkaProperties = new Properties();
        kafkaProperties.putAll(config.getProperties());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all");

        makeSureTopic(config);
        producer = new KafkaProducer<>(kafkaProperties);

        jsonWriterConfig = new JSONWriter.Context();
        jsonWriterConfig.config(JSONWriter.Feature.LargeObject, true);
    }

    private void makeSureTopic(ApplicationConfig.KafkaConfig config) {
        var ac = AdminClient.create(kafkaProperties);
        var topicExisted = false;
        try {
            var topics = ac.listTopics().names().get();
            topicExisted = topics.contains(topicName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        if (topicExisted) {
            return;
        }

        var newTopic = new NewTopic(topicName, 1, config.getTopic().getReplicationFactor());
        newTopic.configs(config.getTopic().getProperties());
        try {
            ac.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        ac.close();
    }

    public void stop() {
        try {
            LOG.info("## stop the kafka producer");
            if (producer != null) {
                producer.close();
            }
        } catch (Throwable e) {
            LOG.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            LOG.info("## kafka producer is down.");
        }
    }

    public void send(SinkBinlogEntry events) {
        Flux.just(events)
                .publishOn(Schedulers.boundedElastic())
                .map(e -> {
                    var pos = e.getHeader();
                    var k = Utils.EntryPosition2String(pos);
                    LOG.info("send: 当前发送消息sql数量: {}", e.getSqlList().size());
                    return new ProducerRecord<>(topicName, k, JSON.toJSONString(e.getSqlList(), jsonWriterConfig));
                })
                .flatMap(x -> Mono.create(emitter -> {
                    producer.send(x, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                emitter.error(e);
                            }
                            emitter.success(x);
                        }
                    });
                }))
                .publishOn(Schedulers.parallel())
                .doOnNext(__ -> {
                    producer.flush();
                })
                .blockLast();
    }
}