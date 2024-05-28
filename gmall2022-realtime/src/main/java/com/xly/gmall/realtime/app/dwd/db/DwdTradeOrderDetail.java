package com.xly.gmall.realtime.app.dwd.db;

import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 交易域下单事务事实表
 * 实现业务需求：将用户下单的数据从mysql中读取出来，涉及到的表:order_info order_detail（主表） order_detail_activity order_detail_coupon
 * 实现步骤：1.将四个表中的数据分别从topic_db数据表中分出来
 *         2.order_info 和 order_detail是属于内连接，所以需要设置状态，设置状态的过期时间
 *         3.order_detail 和 order_detail_activity、order_detail_coupon之间是外连接的形式
 *         4.将查询出来的数据写入到kafka主题中去
 */
public class DwdTradeOrderDetail {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //TODO 2.检查点相关设置
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3.将topic_db中的数据读取到kafka的表中
        tableEnv.executeSql(MyKafkaUtil.getTopic_db("dwd_trade_order_detail"));

        //TODO 4.将order_info表读取到kafka中，并在表环境中注册
        Table orderInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        //TODO 5.将order_detail表读取到kafka中，并在表环境中注册
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "ts \n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        //TODO 6.将order_detail_activity表读取到kafka中，并在表环境中注册
        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        //TODO 7.将order_detail_coupon表读取到kafka中，并在表环境中注册
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        //TODO 8.将四张表进行关联读取数据
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oi.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oi.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "od.create_time,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "od.ts \n" +
                "from order_detail od \n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id");
        tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 9.写入到kafka主题中，先创建与上文字段和属性一致的表，映射写入到kafka中的json文件
        tableEnv.executeSql("create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafka("dwd_trade_order_detail"));
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from result_table");
    }
}
