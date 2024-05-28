package com.xly.gmall.realtime.app.dwd.db;

import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 交易域取消订单事务事实表
 * 实现的功能： 用户已经下单，但是后来取消了订单的数据
 * 操作的表：order_info order_detail（主表） order_detail_activity order_detail_coupon
 * 实现步骤：1.将四个表中的数据分别从topic_db数据表中分出来
 *         2.order_info 和 order_detail是属于内连接，所以需要设置状态，设置状态的过期时间，
 *         4.只有order_info的数据会因为取消订单而修改，订单状态order_status的值改为1003，并且数据类型为update
 *         3.order_detail 和 order_detail_activity、order_detail_coupon之间是外连接的形式
 *         4.将查询出来的数据写入到kafka主题中去
 */
public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) {
        //TODO 1.环境设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //TODO 2.检查点设置(lue)
        //TODO 3.将topic_db中的数据读取写入到kafka中的topic_db表中
        tableEnv.executeSql(MyKafkaUtil.getTopic_db("dwd_trade_order_cancel_detail"));

        //TODO 4.将order_detail order_info order_detail_activity order_detail_coupon从topic_db中读取出来，并注册表
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
                "data['split_coupon_amount'] split_coupon_amount \n" +
                "from `topic_db` where `table` = 'order_detail'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        Table orderCancelInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['operate_time'] operate_time,\n" +
                "ts\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and `old`['order_status'] is not null\n" +
                "and data['order_status'] = '1003'");
        tableEnv.createTemporaryView("order_cancel_info", orderCancelInfo);

        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        //TODO 5.关联数据
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oci.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oci.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(oci.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                "oci.operate_time,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "oci.ts \n" +
                "from order_detail od \n" +
                "join order_cancel_info oci\n" +
                "on od.order_id = oci.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id");
        tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 6.将关联的数据写到kafka主题中
        tableEnv.executeSql("create table dwd_trade_cancel_detail(\n" +
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
                "cancel_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts string,\n" +
                "primary key(id) not enforced" +
                ")" + MyKafkaUtil.getUpsertKafka("dwd_trade_cancel_detail"));

        tableEnv.executeSql("insert into dwd_trade_cancel_detail select * from result_table");
    }
}
