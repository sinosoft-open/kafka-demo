package com.sinosoft.mq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.Consumer;

/**
 * Created by yangming on 16/3/19.
 */
public class ConsumerTask {


    public final static String TOPIC = "com.sinosoft.topic.demo";
    public final static String HOST = "192.168.31.205";
    public final static String ZOO_PORT = "2181";
    public final static String KA_PORT = "9092";
    public final static String GROUP_ID = "mygroup";
    public final static Integer THREAD_COUNT = 20;

    private ConsumerConnector consumer;
    private ExecutorService executorService;

    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }


    public ConsumerTask(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("zookeeper.connect", HOST + ":" + ZOO_PORT);
        props.put("group.id", GROUP_ID);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig config = new ConsumerConfig(props);
        consumer = Consumer.createJavaConsumerConnector(config);
    }


    public void dealMessage() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, THREAD_COUNT);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executorService.submit(new MessageTask(stream));
            threadNumber++;
        }
    }

    public static void main(String[] args) {
        ConsumerTask task = new ConsumerTask(TOPIC);
        task.dealMessage();
    }

}
