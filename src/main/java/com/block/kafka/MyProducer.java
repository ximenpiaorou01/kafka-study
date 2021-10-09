package com.block.kafka;

import com.block.kafka.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author wangrongsong
 * @title: MyProducer
 * @projectName kafka-study
 * @description: TODO
 * @date 2021-10-08 15:40
 */
public class MyProducer {
    /**
     * topic名称，没有会自动创建,
     */
    private static final String TOPIC_NAME="my-replicated-topic";
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        /**
         * 配置连接服务器集群连接
         */
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.57.130:9092,192.168.57.130:9093,192.168.57.130:9094");
        /**
         * 把发送的key从字符串序列化为字节数组
         */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /**
         * 把发送消息value从字符串序列化为字节数组
         */
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        /**
         * ack=0,表示发送消息不需要broker中的leader把数据写到本地log就返回，生产端就可以继续发消息，这种方式容易丢失消息。
         * ack=1,表示消息至少需要leader写到本地log，但不需要等待所有follower是否写入到本地log,就可以继续发送下一条消息。
         * ack=-1或者all，需要等待min_insync_replicas(默认为1，推荐大于大于2)这个参数配置的副本个数都成功写入log，这种策略保证只要有
         * 一个备份存活就不会丢失数据，这是最强的数据保证，一般除非是金融级别，或者跟钱打交道的才用这种配置
         *
         */
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        /**
         * 发送失败会重试，默认重试间隔100ms,重试能保证消息发送的可靠性，但是也可能造成重复发送，比如网络抖动，
         * 所以需要在接收者那边做好消息接受的幂等性处理
         */
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        /**
         * 重试间隔 单位毫秒
         */
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,300);
        /**
         * 设置发送消息的本地缓冲区，如果设置了该缓冲区，消息会先发送到本地缓冲区，
         * 可以提高消息的发送性能，默认值是33554432，即32M
         */
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        /**
         * kafka本地线程会从缓冲区取数据，批量发送到broker
         * 设置批量发送的消息大小，默认值是16384，即16k，就是说batch满了16k就发送出去
         */
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        /**
         * 默认值是0，意思就是消息必须立即发送，但这样会影响性能
         * 一般设置10ms左右，就是说这个消息发送后会进入本地的一个batch，如果10ms内这个batch满了16k就会随着batch一起发送出去，
         * 如果10ms内，batch没满，那么也必须发送出去，不能让消息的发送延迟时间太长
         */
        properties.put(ProducerConfig.LINGER_MS_CONFIG,10);



        //发消息的客户端
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //发送5条消息
        int msgNum=5;
        final CountDownLatch countDownLatch=new CountDownLatch(msgNum);
        for (int i=1;i<=5;i++){
            Order order = new Order((long) i, i);

            //未指定发送分区，具体发送的分区计算公式：hash(key)%partitionNum
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPIC_NAME, order.getOrderId().toString(), new JSONObject(order).toString());

            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e!=null){
                        System.err.println("发送消息失败:"+e.getStackTrace());
                    }
                    if(recordMetadata!=null){
                        System.out.println("同步方式发送消息结果: "+"topic-"+recordMetadata.topic()+"|partition-"+recordMetadata.partition()+"|offset:"+recordMetadata.offset());
                    }
                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.close();

    }
}
