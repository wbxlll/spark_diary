package com.wbx.kafka;

import com.alibaba.fastjson2.JSON;
import com.wbx.entity.CallHistory;
import com.wbx.utils.CallHistoryFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafProducer {

    public static final String TOPIC = "test";
    private static KafkaProducer<String,String> producer;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = getProperties();
        producer = new KafkaProducer<>(properties);
        System.out.println("---------开始生产---------");
        try {
            do {
                CallHistory ch = CallHistoryFactory.build();
                send(new ProducerRecord<>(TOPIC, "", JSON.toJSONString(ch)));
                Thread.sleep(1000);
            } while (!Thread.currentThread().isInterrupted());
        }finally {
            producer.close();
        }
    }

    @NotNull
    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.137.104:9092");    // 指定 Broker
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  // 将 key 的 Java 对象转成字节数组
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 将 value 的 Java 对象转成字节数组
        properties.put("acks", "1");       // 消息至少成功发给一个副本后才返回成功
        properties.put("retries", "5");    // 消息重试 5 次
        return properties;
    }

    /**
     * 同步发送消息
     */
    public static void send(ProducerRecord record) {
        try {
            producer.send(record).get(200, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }

    }

    /**
     * 异步发送消息
     */
    public void sendAsync(ProducerRecord record, Callback callback) {
        try {
            producer.send(record, callback);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }

    }

}
