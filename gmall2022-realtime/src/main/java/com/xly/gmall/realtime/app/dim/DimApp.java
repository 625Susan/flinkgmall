package com.xly.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.app.func.DimSinkFunction;
import com.xly.gmall.realtime.app.func.TableProcessFunction;
import com.xly.gmall.realtime.beans.TableProcess;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class DimApp {
    public static void main(String[] args) throws Exception {
        //数据处理的流程
        //TODO 1.创建flink相应的环境
        //1.1 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度，flink数据源来自kafka的消费者主题的分区
        env.setParallelism(4);

        //TODO 2.检查点相关的配置
      /*  //2.1 开启检查点，表示每隔多久启动一次检查点，指的是检查点的开始之间的时间范围,检查点模式：数据来了先不处理，等barrier对其之后存储了检查点之后才处理数据，不会重复处理数据
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间,超时未完成会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消之后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略,每30天有3次重试的机会，数据延迟为3s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端，HashMapStateBancked是将状态保存在java heap中
        env.setStateBackend(new HashMapStateBackend());
        // 设置状态保存的位置
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //2.7 设置hadoop用户为atguigu
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        //TODO 3.从Kafka中读取数据 -- 主流数据
        //3.1 声明kafka的消费者主题和消费者组
        String topic = "topic_db";
        String groupId = "dim_app_group";
        //3.2 kafka主机配置等,使用flinkkafkaconsumer，因为其内部维护了flink向kafka读取数据之后的偏移量
        //可以将kafka作为一个工具类定义，在其中既可以定义消费方法，又可定义其生产方法和相关的配置
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 kafka中的消费数据作为数据流的来源
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        //TODO 4.对流中的数据进行类型转换以及简单的ETL 将jsonstring->json object
        //如果不明确如何将数据进行转换，直接使用process进行转换
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonstr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    // 将kafka生成的json字符串转换为json对象，但是也有可能传入的数据不是一个标准的json数据，进行try catch的不做处理
                    JSONObject jsonObj = JSONObject.parseObject(jsonstr);
                    //然后读取kafka中的数据，当type类型不为bootstrap_start和bootstrap_complete时，读取数据
                    Object type = jsonObj.get("type");
                    if (!type.equals("bootstrap-start") && !type.equals("bootstrap-complete")) {
                        collector.collect(jsonObj);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        // 所有的主流已经读取完，需要启动的进程：zk kafka hadoop maxwell
         jsonObj.print(">>>>>>>>>>>>>>");

        //TODO 5.使用FlinkCDC读取配置表mysql中的封装的维度表的数据-配置流
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
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        //mysqlDS.print(">>>>>>>>>>>>>");
        //TODO 6.将配置流进行广播，成广播流(key为表名，v为表中读取的信息封装成对象)
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);
        //TODO 7.将主流和广播流进行关联  调用非 广播流 与广播流之间的连接
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObj.connect(broadcastDS);
        //TODO 8.对关联后的数据进行处理，过滤出维度数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );

        //TODO 9.将维度数据中读取到的相关信息写入到PHENIX中
        //传递到这里的数据{"tm_name":"atguigu","sink_table":"dim_base_trademark","id":14}
        dimDS.print(">>>>>>>");
        //不能直接使用jdbcsinkfunction，因为它只能往一张表中写数据
        dimDS.addSink(new DimSinkFunction());
        env.execute();  //excute一定要执行开启
    }
}
