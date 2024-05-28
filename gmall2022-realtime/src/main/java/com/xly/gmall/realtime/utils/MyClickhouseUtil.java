package com.xly.gmall.realtime.utils;

import com.xly.gmall.realtime.beans.TransientSink;
import com.xly.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *操作clkickhouse的实现类
 */
public class MyClickhouseUtil {
    /**因为我们无法知道要处理的数据类型是什么类型，所以在方法中传入的参数类型为泛型
     * @return
     * @param <T>
     */
    public static <T>SinkFunction<T> getSinkFunction(String sql){
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                //需要执行的sql语句
                sql,
                //"insert into dws_traffic_keyword_page_view_window values(?,?,?,?,?)"
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        /*
                       将流中的属性给sql语句中的?赋值处理
                         */
                        //T(obj)即为要操作的对象，通过反射来获取对象的属性
                        //getDeclaredFields()和getFields()的区别：
                        // getDeclaredFields()可以当前对象的属性包括私有属性
                        //定义一个跳过的次数的变量
                        int skipNum = 0;
                        Field[] fieldArr = obj.getClass().getDeclaredFields();
                        //遍历数据中的属性值，这个需要与sql语句中的values的值相对应
                        for (int i = 0; i < fieldArr.length; i++) {
                            Field field = fieldArr[i];
                            //判断是否需要将该属性写到clickhouse中，给原本属性加标记(如果已经被加了注解，不需要传递)
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }
                            //设置私有属性的访问权限
                            field.setAccessible(true);
                            //获取属性的值  属性对象.get(obj)       获取这个对象的属性值
                            try {
                                Object feildValue = field.get(obj);
                                //将属性的值给问号占位符(jdbc的索引是从1开始的，java中的索引从0开始)如果有数据被跳过了，要减去跳过的次数
                                ps.setObject(i + 1 - skipNum, feildValue);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                },
                //构造者模式调用静态内部类，再使用build方法返回一个需要调用的子类
                //设置每次传入clickhouse的数据什么时候触发写入clickhouse这个操作
               new JdbcExecutionOptions.Builder()
                       .withBatchSize(5)
                       .withBatchIntervalMs(3000)
                       .build(),
               //设置连接clickhouse的driver和url
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
        return sinkFunction;
    }
}
