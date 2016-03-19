package com.sinosoft.mq;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Created by yangming on 16/3/19.
 */
public class MessageTask implements Runnable {

    private KafkaStream stream;

    public MessageTask(KafkaStream stream) {
        this.stream = stream;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> mm = it.next();
            String message = new String(mm.message());
            String key = new String(mm.key());
            System.out.println("Thread " + Thread.currentThread().getId() + ": " + key + ":" + message);
        }
    }
}
