package com.uestc.bigdata.kafka.Config;

import com.uestc.bigdata.common.ConfigUtil;

import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 单例模式获取kafka配置信息
 */
public class KafkaConfig {

    private static final Logger LOG= LoggerFactory.getLogger(KafkaConfig.class);
    private static final String KAFKA_PROPERTIES="kafka/kafka-server-config.properties";

    private ProducerConfig producerConfig;
    private Properties properties;

    private static KafkaConfig kafkaConfig;

    private KafkaConfig() throws IOException {
        try {
            properties = ConfigUtil.getInstance().getProperties(KAFKA_PROPERTIES);
        } catch (Exception e) {
            IOException ioException = new IOException();
            ioException.addSuppressed(e);
            throw ioException;
        }
        producerConfig=new ProducerConfig(properties);
    }

    public static KafkaConfig getInstance(){
        if(kafkaConfig==null){
            synchronized (KafkaConfig.class){
                if(kafkaConfig==null){
                    try {
                        kafkaConfig=new KafkaConfig();
                    } catch (Exception e) {
                        LOG.error("实例化kafkaConfig失败", e);
                    }
                }
            }
        }
        return kafkaConfig;
    }

    public ProducerConfig getProducerConfig(){
        return producerConfig;
    }
}
