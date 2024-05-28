package com.xly.gmall.realtime.app.dwd.db;

import com.xly.gmall.realtime.utils.MyJdbcUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderRefund {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql(MyKafkaUtil.getTopic_db("dwd_trade_order_refund"));

        // TODO 4. 读取退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['refund_type'] refund_type,\n" +
                "data['refund_num'] refund_num,\n" +
                "data['refund_amount'] refund_amount,\n" +
                "data['refund_reason_type'] refund_reason_type,\n" +
                "data['refund_reason_txt'] refund_reason_txt,\n" +
                "data['create_time'] create_time,\n" +
                "proc_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'order_refund_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 5. 读取订单表数据，筛选退单数据
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "`old`\n" +
                "from topic_db\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and data['order_status']='1005'\n" +
                "and `old`['order_status'] is not null");

        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 6. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MyJdbcUtil.getMysqlTable());

        // TODO 7. 关联三张表获得退单宽表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "ri.id,\n" +
                "ri.user_id,\n" +
                "ri.order_id,\n" +
                "ri.sku_id,\n" +
                "oi.province_id,\n" +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id,\n" +
                "ri.create_time,\n" +
                "ri.refund_type,\n" +
                "type_dic.dic_name,\n" +
                "ri.refund_reason_type,\n" +
                "reason_dic.dic_name,\n" +
                "ri.refund_reason_txt,\n" +
                "ri.refund_num,\n" +
                "ri.refund_amount,\n" +
                "ri.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_refund_info ri\n" +
                "join \n" +
                "order_info oi\n" +
                "on ri.order_id = oi.id\n" +
                "join \n" +
                "base_dic for system_time as of ri.proc_time as type_dic\n" +
                "on ri.refund_type = type_dic.dic_code\n" +
                "join\n" +
                "base_dic for system_time as of ri.proc_time as reason_dic\n" +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
//        tableEnv.executeSql("select * from result_table").print();

        // TODO 8. 建立 Upsert-Kafka dwd_trade_order_refund 表
        tableEnv.executeSql("create table dwd_trade_order_refund(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "refund_type_code string,\n" +
                "refund_type_name string,\n" +
                "refund_reason_type_code string,\n" +
                "refund_reason_type_name string,\n" +
                "refund_reason_txt string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafka("dwd_trade_order_refund"));

        // TODO 9. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_trade_order_refund select * from result_table");
    }
}
