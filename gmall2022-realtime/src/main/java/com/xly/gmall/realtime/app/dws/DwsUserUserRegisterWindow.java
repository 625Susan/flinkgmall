package com.xly.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.UserRegisterCountBean;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


/**
 * 用户域用户注册各窗口的汇总表
 * 从kafka数据dwd_user_register中读取数据作为数据源
 */
public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关设置
        //TODO 3.从kafka中读取相关数据
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);
//        kafkaDS.print("从dwd_user_register中获取数据");
        /*
        {"create_time":"2023-04-12 20:33:00","id":611,"ts":1681302780}
         */
        //TODO 4.将读取的数据jsonstr转换为jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(JSON::parseObject);
        //因为数据没有状态需要进行和历史 相对比，所以不需要进行状态编程
        //TODO 5.设置水位线和获取事件时间
        SingleOutputStreamOperator<JSONObject> watermarkDS = jsonObj.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonOb, long recordTimestamp) {
                                Long ts = jsonOb.getLong("ts");
                                return ts * 1000L;
                            }
                        })
        );
        //  watermarkDS.print("s");
        //TODO 6.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = watermarkDS.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 7.聚合数据
        SingleOutputStreamOperator<UserRegisterCountBean> aggregateDS = windowDS.aggregate(new AggregateFunction<JSONObject, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(JSONObject value, Long accumulator) {
                accumulator++;
                return accumulator;
            }

            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }

            @Override
            public Long merge(Long a, Long b) {
                return null;
            }
        }, new AllWindowFunction<Long, UserRegisterCountBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Long> values, Collector<UserRegisterCountBean> out) throws Exception {
                String stt = DataFormatUtil.toYmdHms(window.getStart());
                String edt = DataFormatUtil.toYmdHms(window.getEnd());
                for (Long value : values) {
                    UserRegisterCountBean userRegisterBean = new UserRegisterCountBean(
                            stt,
                            edt,
                            value,
                            System.currentTimeMillis()
                    );
                    out.collect(userRegisterBean);

                }
            }
        });
        aggregateDS.print("写入到clickhouse中的数据");
        //TODO 8.将数据写入进clickhouse中
        aggregateDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));
        env.execute();
    }
}
