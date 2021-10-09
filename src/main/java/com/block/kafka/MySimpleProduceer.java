package com.block.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author wangrongsong
 * @title: MySimpleProduceer
 * @projectName kafka-study
 * @description: TODO
 * @date 2021-10-08 16:20
 */
public class MySimpleProduceer {
    /**
     * topic名称，没有会自动创建,
     */
    private static final String TOPIC_NAME="my-replicated-topic";
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        //配置连接服务器集群连接
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.57.130:9092,192.168.57.130:9093,192.168.57.130:9094");
        //把发送的key从字符串序列化为字节数组
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPIC_NAME, "mykey", "hello kafka");
        RecordMetadata recordMetadata = null;


        //同步方式
//        try {
//            //等待消息发送成功的同步阻塞方法
//            recordMetadata = producer.send(producerRecord).get();
//            System.out.println("同步方式发送消息结果: "+"topic-"+recordMetadata.topic()+"|partition-"+recordMetadata.partition()+"|offset:"+recordMetadata.offset());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            //1.记录日志，预警系统+1
//            //2.设置时间间隔1s,同步的方式再次发送，如果还不行，日志预警，人工介入
//            TimeUnit.MILLISECONDS.sleep(1000);
//            try {
//                //等待消息发送成功的同步阻塞方法
//                recordMetadata = producer.send(producerRecord).get();
//            } catch (Exception e1) {
//                e1.printStackTrace();
//                //人工介入
//            }
//
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//
        //异步方式

        producer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e!=null){
                    System.err.println("发送消息失败:"+e.getStackTrace());
                }
                if(recordMetadata!=null){
                    System.out.println("同步方式发送消息结果: "+"topic-"+recordMetadata.topic()+"|partition-"+recordMetadata.partition()+"|offset:"+recordMetadata.offset());

                }
            }
        });
        TimeUnit.SECONDS.sleep(1000);
    }
}
