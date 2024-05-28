package com.xly.gmall.realtime.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/*
该案例测试的是flinkCDC
 */
public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {
        //TODO:1.配置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        // 开启检查点
        env.enableCheckpointing(3000);

        //读取数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database 可以同时监控多个数据库，需要在mysql中对该数据开启binlog
                .tableList("gmall_config.t_user") // set captured table
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial()) //initial第一次启动时对数据进行全量同步
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print(); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
