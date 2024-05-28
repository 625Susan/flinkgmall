package com.xly.gmall.realtime.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER="hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //设置kafka的读取数据的隔离级别，需要kafka支持不能读写预提交的数据
        prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        // simplestringschema序列化时要求不能处理为空的数据，而kafka里面可能存在为空的脏数据等
        //new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                //流数据没有结束
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                //判断输入的数据是不是为空，不为空的话返回值
                if (consumerRecord.value()!=null){  //ComsumerRecord是k 和v 类型的，判断数据value不为空的话，返回他的value
                    return new String(consumerRecord.value());
                }
                return null;
            }
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);  //将对象转换为字符串类型的数据
            }
        }, prop);
        return kafkaConsumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        //默认情况创建的FlinkKafkaProducer对象并不能保证生产一直性，因为semantic的值默认是AT_LEAST_ONCE
        //如果要想保证一致性，需要在创建对象的时候，指定semantic的值为EXACTLY_ONCE
        // FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("hadoop102:9092,hadoop103:9092,hadoop104:9092","dirty_data",new SimpleStringSchema());
        Properties pro = new Properties();
        pro.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        //两阶段的提交，需要设置kafka事务的超时时间大于flink的检查点间隔时间
        pro.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                "default_topic", new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String JsonStr, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]> (topic,JsonStr.getBytes());
            }
        },pro, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        return kafkaProducer;
    }
    public static String getTopic_db(String groupId){
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  `ts` STRING,\n" +
                "  `proc_time` as proctime()\n" +
                ") " + getKafkaCon("topic_db",groupId);
    }
    public static String getKafkaCon(String topic,String groupId){
        return "WITH (" +
                "                  'connector' = 'kafka'," +
                "                  'topic' = '"+topic+"'," +
                "                  'properties.bootstrap.servers' = '"+KAFKA_SERVER+"'," +
                "                  'properties.group.id' = '"+groupId+"'," +
                "                  'scan.startup.mode' = 'latest-offset'," +
                "                  'format' = 'json'" +
                "                )";
    }
    public static String getUpsertKafka(String topic){
        return  "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = '"+topic+"',\n" +
                        "  'properties.bootstrap.servers' = '"+KAFKA_SERVER+"',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")";
    }
    public static <T>FlinkKafkaProducer<T> getKafkaProducerBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        //默认情况创建的FlinkKafkaProducer对象并不能保证生产一直性，因为semantic的值默认是AT_LEAST_ONCE
        //如果要想保证一致性，需要在创建对象的时候，指定semantic的值为EXACTLY_ONCE
        // FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("hadoop102:9092,hadoop103:9092,hadoop104:9092","dirty_data",new SimpleStringSchema());
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "");

        FlinkKafkaProducer<T> kafkaProducer
                = new FlinkKafkaProducer<>("default_topic", kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return kafkaProducer;
    }
}
