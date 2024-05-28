package com.xly.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.app.func.BaseDbTableProcessFunction;
import com.xly.gmall.realtime.beans.TableProcess;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 对dwd层的处理逻辑较为简单的其他事务事实表进行动态建表
 * 思路：1.将数据从topic_db中读取出来
 *      2.将动态建表的表的内容广播出去
 *      3.将两条流进行连接，进行过滤筛选
 *      4.将过滤筛选出来的数据传递到kafka主题中去
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.检查点的相关配置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(8000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3.从topic_db中读取主流数据（定义kafka主题，消费者组）
        //FlinkKafkaconsumer维护的偏移量
        String topic = "topic_db";
        String groupId = "base_db_app";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        //TODO 4.对主流数据进行处理（如果是dwd需要创建的表的数据的话，
        // 其type不应该为bootstrap-start、bootstrap-insert、bootstrap-complete）
        //简单的etl清洗，同时将数据从jsonstr转换为jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                //转换成jsonobj，同时通过try catch 将空数据过滤掉
                try {
                    JSONObject jsonObject = JSON.parseObject(jsonStr);
                    String type = jsonObject.getString("type");
                    if (!type.startsWith("bootstrap-")){
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        //jsonObj.print(">>>>>>>");
        /*
         * {
         * "database":"gmall"
         * ,"xid":833839,
         * "data":{"create_time":"2023-04-12 12:31:29","user_id":39,"appraise":"1201","comment_txt":"评论内容：32722349252347515326898156861454527884645473412871","nick_name":"林有","sku_id":27,"id":1608723436069192345,"spu_id":9,"order_id":43768},
         * "commit":true,"
         * type":"insert",
         * "table":"comment_info",
         * "ts":1681273889}
         */
        //TODO 5.使用flinkCDC处理广播流数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database 可以同时监控多个数据库，需要在mysql中对该数据开启binlog
                .tableList("gmall_config.table_process") // set captured table
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial()) //initial第一次启动时对数据进行全表扫描
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql_Source");

        //TODO 6.将配置表中的数据进行广播，sink_type+sorce_type为key
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 7.将两条流中的数据进行关联  connect（使用非广播流关联广播流数据）
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObjDS.connect(broadcastDS);

        //TODO 8.对关联表进行处理

        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));

        //TODO 9.将关联的数据写到kafka主题中去
        realDS.print(">>>>>>");
        realDS.addSink(MyKafkaUtil.<JSONObject>getKafkaProducerBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                        String topic = jsonObj.getString("sink_table");
                        jsonObj.remove("sink_table");
                        return new ProducerRecord<byte[], byte[]>(topic,jsonObj.toJSONString().getBytes());
                    }
                }
        ));
        env.execute();
    }
}
