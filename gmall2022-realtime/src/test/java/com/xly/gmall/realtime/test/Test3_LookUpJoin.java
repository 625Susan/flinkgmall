package com.xly.gmall.realtime.test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
    实现功能：将kafka主题中产生的日志数据与mysql中的表的数据进行关联，并输出到kafka主题中
 */
public class Test3_LookUpJoin {
    public static void main(String[] args) {
//        //TODO 1.基本环境准备
//        //1创建流环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //2设置并行度
//        env.setParallelism(1);
//        //3创建表执行环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        //TODO 2.检查点的相关设置
//         /*env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//         env.getCheckpointConfig().setCheckpointTimeout(60000L);
//         env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
//         env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//         env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//         env.setStateBackend(new HashMapStateBackend());
//         System.setProperty("HADOOP_USER_NAME","atguigu");*/
//        //TODO 3.将评论的内容从kafka_log主题中提取出来(这里映射的是员工信息，从first主题中读取员工数据)
//        tableEnv.executeSql("create table emp(\n" +
//                " empno integer,\n" +
//                " ename string,\n" +
//                " deptno integer,\n" +
//                " proctime as PROCTIME()\n" +
//                ")with (\n" +
//                "'connector'='kafka',\n" +
//                "'topic' = 'first',\n" +
//                "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop102:9092,hadoop104:9092',\n" +
//                "'properties.group.id' = 'testGroup',\n" +
//                "'scan.startup.mode' = 'latest-offset',\n" +
//                "'format' = 'json'\n" +
//                ")");
//        //TODO 4.从mysql中读取评论数据(映射在mysql中读取部门信息)
//        tableEnv.executeSql("CREATE TABLE dept (\n" +
//                "  deptno integer,\n" +
//                "  dname string,\n" +
//                "  PRIMARY KEY (deptno) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
//                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall1031_config',\n" +
//                "   'lookup.cache.max-rows' = '200',\n" +
//                "   'lookup.cache.ttl' = '1 hour',\n" +
//                "   'table-name' = 't_dept',\n" +
//                "   'username' = 'root',\n" +
//                "   'password' = '123456'\n" +
//                ")");
////        tableEnv.executeSql("CREATE TABLE dept (\n" +
////                "  deptno integer,\n" +
////                "  dname STRING,\n" +
////                "  PRIMARY KEY (deptno) NOT ENFORCED\n" +
////                ") WITH (\n" +
////                "   'connector' = 'jdbc',\n" +
////                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall_config',\n" +
////                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
////                "   'table-name' = 't_dept',\n" +
////                "   'lookup.partial-cache.max-rows' = '200',\n" +
////                "    'lookup.cache.ttl' = '1 hour',\n" +
////                "   'username' = 'root',\n" +
////                "   'password' = '000000'\n" +
////                ")");
//        //TODO 5.将评论的内容和评论数据进行关联，使用lookupjoin对员工信息和部门信心进行关联
//        //在原始的join中，需要设置状态，但是数据会源源不断的过来，会长时间占用时间，需要设置过期时间，但是在有的表来到时状态会失效
//        //lookup以左表作为驱动，当左表数据到来时，向右表发送关联请求
//        Table querySql = tableEnv. sqlQuery("SELECT e.empno,e.ename,d.deptno,d.dname\n" +
//                "FROM emp AS e JOIN dept FOR SYSTEM_TIME AS OF e.proctime AS d\n" +
//                "  ON e.deptno = d.deptno");
//        //"SELECT e.empno,e.ename,d.deptno,d.dname\n" +
//        //            "FROM emp AS e JOIN dept FOR SYSTEM_TIME AS OF e.proctime AS d\n" +
//        //            "  ON e.deptno = d.deptno"
//        //TODO 6.将关联的结果写到kafka主题中
//        tableEnv.executeSql("create table emp_dept (\n" +
//                "empno integer,\n" +
//                "ename string,\n" +
//                "deptno integer,\n" +
//                "dname string,\n" +
//                "PRIMARY KEY (empno) NOT ENFORCED\n" +
//                ")with(\n" +
//                "  'connector' = 'upsert-kafka',\n" +
//                "  'topic' = 'dept_emp_topic',\n" +
//                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
//                "  'key.format' = 'json',\n" +
//                "  'value.format' = 'json'\n" +
//                ")");
//
//        tableEnv.executeSql("insert into emp_dept select * from " + querySql);
//    }
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka主题中读取员工数据
        tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  empno integer,\n" +
                "  ename string,\n" +
                "  deptno integer,\n" +
                "  proctime as PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        //tableEnv.executeSql("select * from emp").print();

        //TODO 4.从mysql中读取部门数据
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                "  deptno integer,\n" +
                "  dname string,\n" +
                "  PRIMARY KEY (deptno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall_config',\n" +
                "   'lookup.cache.max-rows' = '200',\n" +
                "   'lookup.cache.ttl' = '1 hour',\n" +
                "   'table-name' = 't_dept',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '000000'\n" +
                ")");
//         tableEnv.executeSql("select * from dept").print();

        //TODO 5.将员工部门进行关联
        //使用普通的内外连接不太合适，因为没有办法设置状态的失效时间
        //我们使用lookupjoin进行关联，lookupjoin的底层实现原理和普通的内外连接完全不一样，
        //在lookupjoin的底层不会维护状态存放参与连接的表中的数据
        //它是以左表进行驱动，当左表数据来到之后，再发送请求到右表中进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT e.empno,e.ename,d.deptno,d.dname\n" +
                "FROM emp AS e JOIN dept FOR SYSTEM_TIME AS OF e.proctime AS d\n" +
                "  ON e.deptno = d.deptno");
       tableEnv.createTemporaryView("joined_table",joinedTable);


        //TODO 6.将关联的结果写到kafka主题中
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empno integer,\n" +
                "  ename string,\n" +
                "  deptno integer,\n" +
                "  dname string,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'emp_dept_topic',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");

       tableEnv.executeSql("insert into emp_dept select * from joined_table" );
    }
}
