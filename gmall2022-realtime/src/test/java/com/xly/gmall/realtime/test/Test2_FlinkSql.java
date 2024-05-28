package com.xly.gmall.realtime.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*

 */
public class Test2_FlinkSql {
    public static void main(String[] args) {
        //TODO 1.配置相关的环境(流环境 表环境)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关配置
        //TODO 3.读取员工的相关信息，并装配成表
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 8888)
                .map(
                        new MapFunction<String, Emp>() {
                            @Override
                            public Emp map(String empStr) throws Exception {
                                String[] empArr = empStr.split(",");
                                return new Emp(Integer.parseInt(empArr[0]), empArr[1], Integer.parseInt(empArr[2]), Long.parseLong(empArr[3]));
                            }
                        }
                );
//        empDS.print("emp");
        tableEnv.createTemporaryView("emp",empDS);
        //TODO 4.读取部门的相关信息，并装配成表
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String dept) throws Exception {
                        String[] deptArr = dept.split(",");
                        return new Dept(Integer.parseInt(deptArr[0]), deptArr[1], Long.parseLong(deptArr[2]));
                    }
                });
//        deptDS.print("dept");
        tableEnv.createTemporaryView("dept",deptDS);
        //TODO 5.执行sql语句
        //flink的
//        tableEnv.executeSql("select e.empno,e.ename,e.deptno,d.dname ,d.ts from " +
//                "emp e left join dept d on e.deptno = e.deptno").print();

        //TODO 6.将执行的结果输出到kafka主题中
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empno BIGINT,\n" +
                "  ename STRING,\n" +
                "  deptno BIGINT,\n" +
                "  dname STRING,\n" +
                "  ts BIGINT,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'emp_dept_topic',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
         tableEnv.executeSql("insert into emp_dept select e.empno,e.ename,e.deptno,d.dname ,d.ts from " +
                "emp e left join dept d on e.deptno = e.deptno");
    }
}
