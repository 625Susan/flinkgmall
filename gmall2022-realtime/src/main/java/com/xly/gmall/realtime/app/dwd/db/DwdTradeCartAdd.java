package com.xly.gmall.realtime.app.dwd.db;

import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域加购事务事实表
 * 实现的功能：将加购操作写到Kafka主题中去
 * 从topic_db中读取数据
 * 操作的表：cart_info:insert操作  或者update操作，sku_num不为null，同时满足新的sku_num>旧的sku_num
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {
        //TODO 1.基本环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态的过期时间，本次数据处理过程中不涉及join表的相关操作
        //tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10+15*60));

        //TODO 2.检查点相关设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3.将kafka中的topic_db中的数据进行动态建表 connector-kafka
        tableEnv.executeSql(MyKafkaUtil.getTopic_db("dwd_trade_cart_add"));
        //tableEnv.executeSql("select * from topic_db").print();

        //TODO 4.筛选出表为cart_info:insert操作  或者update操作，sku_num不为null，同时满足新的sku_num>旧的sku_num
        Table addCart = tableEnv.sqlQuery("select \n" +
                "\tdata['id'] id,\n" +
                "\tdata['user_id'] user_id,\n" +
                "\tdata['sku_id'] sku_id,\n" +
                "\tif(`type`= 'insert',`data`['sku_num'],cast(cast(`data`['sku_num']as int )- cast(`old`['sku_num'] as int) as STRING )) sku_num,\n" +
                "\tdata['sku_name'] sku_name,\n" +
                "\tts\n" +
                "from topic_db where `table` = 'cart_info' and (`type` = 'insert' or \n" +
                "(`type` = 'update' and `old`['sku_num'] is not null and cast(`data`['sku_num']as int )> cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("add_cart",addCart);
//        tableEnv.executeSql("select * from add_cart").print();

        //TODO 5.将满足条件的数据写出到kafka主题中，使用upsert_kafka，先创建一张表与kafka主题中的json数据进行映射（字段名和属性要相同）
        tableEnv.executeSql("CREATE TABLE dwd_trade_cart_add (\n" +
                "  `id` STRING,\n" +
                "  `user_id` STRING,\n" +
                "  `sku_id` STRING,\n" +
                "  `sku_num` STRING,\n" +
                "  `sku_name` STRING,\n" +
                "  `ts` STRING,\n" +
                "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ")" + MyKafkaUtil.getUpsertKafka("dwd_trade_cart_add"));
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from add_cart");
    }
}
