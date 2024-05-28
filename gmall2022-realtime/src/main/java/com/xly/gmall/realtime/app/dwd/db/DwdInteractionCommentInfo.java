package com.xly.gmall.realtime.app.dwd.db;

import com.xly.gmall.realtime.utils.MyJdbcUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
    实现提取评论操作生成评论表，并关联字典表中的数据，将数据写入到kafka主题中  评论表为左表，执行时去缓存中加载字典表
    互动域评论事务事实表
 */
public class DwdInteractionCommentInfo {
    public static void main(String[] args) {
        //TODO 1准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2检查点设置

        //TODO 3通过Kafka连接器将业务数据写入到表中
        tableEnv.executeSql(MyKafkaUtil.getTopic_db("dwd_interaction_comment_group"));
        //tableEnv.executeSql("select * from topic_db").print();
        //TODO 4将评论的数据从Kafka业务数据中筛选出来
        Table comTable = tableEnv.sqlQuery("select data['id'] id,\n" +
                "\t   data['user_id'] user_id,\n" +
                "\t   data['sku_id'] sku_id,  \n" +
                "\t   data['appraise'] appraise, \n" +
                "\t   data['comment_txt'] comment_txt,\n" +
                "\t   ts,\n" +
                "\t   proc_time\n" +
                "from topic_db where `table` = 'comment_info' and `type` = 'insert'");
        tableEnv.createTemporaryView("com_table",comTable);
        //tableEnv.executeSql("select * from com_table").print();
        //TODO 5将字典表数据从mysql加载到缓存表中
        tableEnv.executeSql(MyJdbcUtil.getMysqlTable());
       // tableEnv.executeSql("select * from base_dic").print(); 在flink流环境中不要连续执行多次print操作，因为是流环境，print没有结束的时候，可能导致下一个print没有办法输出数据
        //TODO 6使用lookup_join与字典表数据进行关联
        Table lookTable = tableEnv.sqlQuery("select \n" +
                "\t   ct.id,\n" +
                "\t   ct.user_id,\n" +
                "\t   ct.sku_id,\n" +
                "\t   ct.appraise,\n" +
                "\t   bd.dic_name,\n" +
                "\t   ct.comment_txt,\n" +
                "\t   ct.ts,\n" +
                "\t   ct.proc_time\n" +
                "from com_table ct join base_dic for SYSTEM_TIME AS OF ct.proc_time AS bd\n" +
                "on ct.appraise = bd.dic_code");
        tableEnv.createTemporaryView("look_table",lookTable);
//        tableEnv.executeSql("select * from look_table").print();
        //TODO 7使用upsert_kafka将关联的数据写入到kafka主题中
        tableEnv.executeSql("CREATE TABLE dwd_interaction_comment (\n" +
                "  `id` STRING,\n" +
                "  `user_id` STRING,\n" +
                "  `sku_id` STRING,\n" +
                "  `appraise` STRING,\n" +
                "  `dic_name` STRING,\n" +
                "  `comment_txt` STRING,\n" +
                "  `ts` STRING,\n" +
                "  `proc_time` TIMESTAMP,\n" +
                "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ")" + MyKafkaUtil.getUpsertKafka("dwd_interaction_comment"));
        tableEnv.executeSql("insert into dwd_interaction_comment select * from look_table");
    }
}
