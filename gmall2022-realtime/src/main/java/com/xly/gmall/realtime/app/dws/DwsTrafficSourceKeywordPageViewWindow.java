package com.xly.gmall.realtime.app.dws;

import com.xly.gmall.realtime.app.func.KeywordUDTF;
import com.xly.gmall.realtime.beans.KeywordBean;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用flinksql的形式实现以下功能设置
 * dws流量域搜索关键词粒度页面浏览各窗口汇总表数据
 * 实现步骤：1.从kafka中topic_log主题中生成的dwd_traffic_page_log主题中读取数据
 *         2.过滤搜索行为，上一个页面id为page的数据
 *         3.自定义函数对搜索内容的关键字进行分词
 *         4.统计各窗口各关键词出现的频次
 *         5.将数据写入到clickhouse中
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 2.检查点相关设置(略)
        //TODO 3.从dwd_traffic_page_log中读取页面浏览日志数据（执行时需要启动DwdTrafficBaseLogSplit应用程序）
        //kafka连接器，将数据从dwd层读取出来
        /*
          {
          "common":{"ar":"20","uid":"298","os":"Android 12.0","ch":"vivo","is_new":"0","md":"realme Neo2","mid":"mid_292","vc":"v2.0.1","ba":"realme","sid":"cd99eadb-f588-42cb-a762-7445480e40aa"},
          "page":{"page_id":"payment","item":"48457","during_time":10762,"item_type":"order_id","last_page_id":"trade"},
          "ts":1681269395000
          }
         */
        //从 Kafka 页面浏览明细主题读取数据并设置水位线（为什么要设置水位线和事件时间：因为涉及到窗口信息等，需要将数据进行按照窗口汇总聚合）
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_keyword_group";
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "  `common` map<STRING,STRING>,\n" +
                "  `page` map<STRING,STRING>,\n" +
                "  `ts` BIGINT," +
                "   rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +//事务时间数据类型必须得是时间戳格式
                "   WATERMARK  FOR rowtime AS rowtime - INTERVAL '3' SECOND" +  //乱序程度
                ")"+ MyKafkaUtil.getKafkaCon(topic,groupId));
      // tableEnv.executeSql("select * from page_log").print();

        //TODO 4.从表中过滤出用户搜索行为数据
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "page['item'] fullword,\n" +
                "rowtime\n" +
                "from page_log \n" +
                "where page['item'] is not null and page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("search_table",searchTable);
       // tableEnv.executeSql("select * from search_table").print();

        //TODO 5.使用自定义的UDTF函数对搜索的内容分词（返回一个带有关键词的集合）
        Table keywordTable = tableEnv.sqlQuery("select \n" +
                "keyword,\n" +
                "rowtime\n" +
                "from search_table,\n" +
                "lateral table (ik_analyze(fullword)) as t(keyword)");//相当于把这个函数处理的结果变成一个表，与前表进行关联，具体用法可以参考官网
        tableEnv.createTemporaryView("keyword_table",keywordTable);
       // tableEnv.executeSql("select * from split_table").print();

        //TODO 6.分组、开窗、聚合计算
     Table reduceTable = tableEnv.sqlQuery("select\n" +
             "  DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss' ) AS stt,\n" +
             "  DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss' ) AS edt,\n" +
             "  keyword,\n" +
             "  count(*) keyword_count,\n" +
             "  UNIX_TIMESTAMP()*1000 ts\n" + //处理时间转换成秒，但是时间戳是以毫秒，为了统一单位使用毫秒
             "  from keyword_table group by TUMBLE(rowtime, INTERVAL '10' second),keyword");
     tableEnv.createTemporaryView("reduce_table",reduceTable);
     //tableEnv.executeSql("select * from reduce_table").print();

        //TODO 7.将动态表转换成流数据
        DataStream<KeywordBean> keywordBeanDS = tableEnv.toDataStream(reduceTable, KeywordBean.class);
        keywordBeanDS.print(">>>>>>");

        //TODO 8.将流中的数据写入到clickhouse中
        //使用jdbcsink方法可以连接到cickhouse 中，但是一次连接只能将数据往一个表中写入
        String sql ="insert into dws_traffic_keyword_page_view_window values(?,?,?,?,?)";
        keywordBeanDS.addSink(MyClickhouseUtil.getSinkFunction(sql));
        env.execute();
    }
}
