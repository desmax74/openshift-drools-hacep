package org.kie.okeanos.pubsub.consumer;

import java.util.Properties;

public class ConsumerThread<T> implements Runnable {

    private String id;
    private String groupId;
    private String topic;
    private String deserializerClass;
    private int size;
    private long duration;
    private boolean autoCommit;
    private boolean commitSync;
    private boolean subscribeMode;
    private ConsumerHandler consumerHandle;

    public ConsumerThread(
            String id,
            String groupId,
            String topic,
            String deserializerClass,
            int pollSize,
            long duration,
            boolean autoCommit,
            boolean commitSync,
            boolean subscribeMode,
            ConsumerHandler consumerHandle) {
        this.id = id;
        this.groupId = groupId;
        this.topic = topic;
        this.deserializerClass = deserializerClass;
        this.size = pollSize;
        this.duration = duration;
        this.autoCommit = autoCommit;
        this.commitSync = commitSync;
        this.subscribeMode = subscribeMode;
        this.consumerHandle = consumerHandle;
    }

    public void run() {
        Properties properties = new Properties();
        properties.setProperty("desererializerClass", deserializerClass);
        BaseConsumer<T> consumer = new BaseConsumer<>(id, properties, consumerHandle);
        if(subscribeMode) {
            consumer.subscribe(groupId,
                               topic,
                               autoCommit);
        } else {
            consumer.assign(topic,
                            null,
                            autoCommit);
        }
        consumer.poll(size, duration, commitSync);
    }

}
