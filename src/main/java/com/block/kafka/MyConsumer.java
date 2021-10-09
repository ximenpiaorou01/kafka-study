package com.block.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author wangrongsong
 * @title: MyConsumer
 * @projectName kafka-study
 * @description: TODO
 * @date 2021-10-08 18:27
 */
public class MyConsumer {

    private static final String TOPIC_NAME="my-replicated-topic";
    private static final String CONSUMER_GROUP_NAME="testGroup456";
    public static void main(String[] args) {

        Properties properties = new Properties();
        /**
         * 连接broker
         */
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.57.130:9092,192.168.57.130:9093,192.168.57.130:9094");
        /**
         * 消费组
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_GROUP_NAME);
        /**
         * 指定key反序列化
         */
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         * 指定value反序列化
         */
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        /**
         * 是否自动提交offset，默认就是true,自动提交，是消费者拉取消息后，但是还没来得及消费，就提交offset到集群,
         * 这时如果消费者挂掉，就会丢失消息风险,所以改成false变成手动提交
         *
         * warning：消费者无论是自动提交还是手动提交，都需要把所属的消费组Group+消费的某个主题Topic+消费的某个分区Partition为key，
         * offset偏移量为value的信息提交到集群的_consumer_offset主题里面。
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        /**
         * 自动提交offset的时间间隔,properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false)时就不需要
         */
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);

        /**
         * 消费分组名
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_GROUP_NAME);
        /**
         * 当消费主题是一个新的消费组，或者指定offset的消费方式，offset不存在，name应该如何消费
         * latest(默认)：只消费自己启动之后发送到主题的消息
         * earliest:第一次从头开始消费，以后按照offset记录继续消费，这个需要区别于consumer.seekBeginning(每次都从头开始消费)
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        /**
         * consumer给broker发的心跳间隔时间
         */
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,1000);
        /**
         * kafka如果超过10s没有收到消费者心跳，则会吧消费者踢出消费组，进行rebalance,把分区分配给其他消费者
         */
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,10*1000);
        /**
         * 一次性poll最大的拉取消息的条数，可以根据消费者的快慢来设置，如果1次就poll500条消息，就结束长轮询，继续向下执行
         * ，如果poll的不足500条，但没到1s(以下配置poll(1s))，就会继续poll，如果超过1s，还没到500条，就结束长轮询，继续向下执行
         */
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,500);
        /**
         * 如果2次poll的时间如果超过了30s的时间间隔，kafka会认为其消费能力过弱，将其踢出消费组，将分区分给其他消费者，
         * 继续rebalance,这样会损坏性能
         */
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,30*1000);

        
        /**
         * 创建消费者
         */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        /**
         * 消费者定于主题列表，没有指定分区
         */
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        /**
         * 指定分区消费,0号分区
         */
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));
        /**
         * 消息回溯消费
         * 表示从指定topic的指定分区的offset=0开始消费
         */
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));
//        consumer.seekToBeginning(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));

        /**
         * 指定offset消费,指定offset=10开始消费
         */
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME,0)));
//        consumer.seek(new TopicPartition(TOPIC_NAME,0),10);

        /**
         * 从指定时间点开始消费
         * 原理：根据时间找到偏移量offset，再指定offset消费
         */
        //拿到指定主题topic下的所有分区
        List<PartitionInfo> topicPartitions = consumer.partitionsFor(TOPIC_NAME);
        //从1小时前开始消费
        long fetchDateTime=System.currentTimeMillis()-1000*60*60;
        Map<TopicPartition,Long> map=new HashMap<>();
        //遍历所有分区partition
        for (PartitionInfo par : topicPartitions) {
            map.put(new TopicPartition(TOPIC_NAME,par.partition()),fetchDateTime);
        }
        //根据时间拿到偏移量offset
        Map<TopicPartition, OffsetAndTimestamp> parMap = consumer.offsetsForTimes(map);
        parMap.forEach((topicPartition, offsetAndTimestamp) -> {
            if(topicPartition!=null&&offsetAndTimestamp!=null){
                long offset = offsetAndTimestamp.offset();
                System.out.println("partition-"+topicPartition.partition()+"|offset-"+offset);
                if(offsetAndTimestamp!=null){
                    //从指定偏移量offset开始消费
                    consumer.assign(Arrays.asList(topicPartition));
                    consumer.seek(topicPartition,offset);
                }
            }
        });

        while(true){
            /**
             * poll() API是拉取消息的长轮询，如果每隔1s内没有poll到任何消息，则继续poll消息，循环往复，
             * 直到poll到消息。若超过1s，则此次长轮询结束，往下执行。
             */
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.printf("收到消息:partition = %d,offset = %d,key = %s,value = %s%n",record.partition(),record.offset(),record.key(),record.value());
            }

            //所有的信息已经消费完
            //如果有消息
            if(consumerRecords.count()>0){
                //手动同步提交offset,当前线程会阻塞直到offs提交成功
                //一般使用同步提交，因为提交之后一般页没有什么逻辑代码了
                consumer.commitSync();//======阻塞====提交成功

                //手动异步提交offset，当前线程提交offset不会则是，可以继续处理后面的逻辑代码
//                consumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
//                        if(e!=null){
//                            System.err.println("Commit failed for "+offsets);
//                            System.err.println("Commit failed exception:"+e.getStackTrace());
//                        }
//                    }
//                });
            }
        }
    }
}
