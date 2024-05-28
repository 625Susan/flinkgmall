package com.xly.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.UserLoginBean;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 用户域用户登陆各窗口汇总表
 * 数据来源：kafka中的dwd_traffic_page_log主题
 */
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关设置

        //TODO 3.从kafka中读取数据
        String topic ="dwd_traffic_page_log";
        String groupId = "dws_user_user_login";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);
//        kafkaDS.print("从dwd_traffic_page_log中读取的数据");
        /*
         {
         "common":{"ar":"6","uid":"13","os":"Android 12.0","ch":"oppo","is_new":"0","md":"realme Neo2","mid":"mid_103","vc":"v2.1.134","ba":"realme","sid":"29021930-ec0d-4b4b-8f0d-7b7c6f43c0a0"},
         "page":{"from_pos_seq":3,"page_id":"good_detail","item":"9","during_time":5505,"item_type":"sku_id","last_page_id":"home","from_pos_id":9},
         "ts":1681289665000
         }
        */
        //TODO 4.将jsonstr转换为jsonobj，便于数据的读取
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(JSON::parseObject);
        //TODO 5.过滤（last_page_id 为空，表示新登录 或者 last_page_id为login的数据）
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObj.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                String uid = jsonObject.getJSONObject("common").getString("uid");
                return uid != null && (StringUtils.isEmpty(lastPageId) || "login".equals(lastPageId));
            }
        });
//        filterDS.print("过滤出来的数据");
        /*
        {
        "common":{"ar":"9","uid":"240","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 13","mid":"mid_247","vc":"v2.1.134","ba":"iPhone","sid":"f4f28434-69d9-48c3-b14f-d64b317b5801"},
        "page":{"page_id":"search","during_time":9760},
        "ts":1681290067000
        }
         */
        //TODO 6.设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jonOb, long recordTimestamp) {
                                return jonOb.getLong("ts");
                            }
                        })
        );
        //TODO 7.按照uid进行分组
        KeyedStream<JSONObject, String> keyedStream = watermarkDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String userid = value.getJSONObject("common").getString("uid");
                return userid;
            }
        });
        //TODO 8.设置状态（状态没有过期时间），统计七日回流用户和当日独立用户数。
        SingleOutputStreamOperator<UserLoginBean> backUniqueStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            private ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //在open方法中初始化上次的登陆状态，因为要统计七天回流用户，在这不设置过期时间，不设置的话默认是不过期
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastLoginDtState", String.class);
                lastLoginDtState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                //状态中的日期
                String lastLoginDate = lastLoginDtState.value();
                //数据中的日期
                Long ts = jsonObject.getLong("ts");
                String curDate = DataFormatUtil.toDate(ts);
                //定义一个回流用户常量和一个独立用户常量
                Long uuCt = 0L;
                Long backCt = 0L;
                if (StringUtils.isNotEmpty(lastLoginDate)){
                    //	若状态中的末次登陆日期不为 null，进一步判断。
                    //将当前时间和状态中保存的最后的登陆时间转换成时间戳的形式计算登陆的时间差
                    Long lastTs = DataFormatUtil.toTs(lastLoginDate);
                    if (!lastLoginDate.equals(curDate)){
                        //	如果末次登陆日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登陆日期更新为当日，进一步判断。
                        uuCt = 1L;
                        lastLoginDtState.update(curDate);
                        if ( (ts-lastTs)/1000/60/60/24 >7 ){
                            //	如果当天日期与末次登陆日期之差大于等于8天则回流用户数backCt置为1。
                            //	否则 backCt 置为 0。
                            backCt = 1L;
                        }
                    }
                    //	若末次登陆日期为当天，则 uuCt 和 backCt 均为 0，此时本条数据不会影响统计结果，舍弃，不再发往下游。
                }else {
                    //	如果状态中的末次登陆日期为 null，将 uuCt 置为 1，backCt 置为 0，并将状态中的末次登陆日期更新为当日。
                    uuCt = 1L;
                    lastLoginDtState.update(curDate);
                }
                //当其中有一个数据由变化时，就将这个数据封装成对象向下游传递
                if (uuCt != 0L || backCt !=0L){
                    out.collect(new UserLoginBean(
                            "",
                            "",
                            backCt,
                            uuCt,
                            ts
                    ));
                }
            }
        });
//        backUniqueStream.print("七日回流用户和当日独立用户");
        /*
        UserLoginBean(stt=, edt=, backCt=0, uuCt=1, ts=1681295740000)
         */
        //TODO 9.开窗(将数据按照窗口进行统计计算)
        AllWindowedStream<UserLoginBean, TimeWindow> windowedStream = backUniqueStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 10.聚合(数据还是按照原本的数据结构输出，所以可以直接使用reduce进行聚合)
        SingleOutputStreamOperator<UserLoginBean> reduceStream = windowedStream.reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, //通过windowfunction获取窗口的开始时间和结束时间
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                String stt =DataFormatUtil.toYmdHms(window.getStart()) ;
                String edt =DataFormatUtil.toYmdHms(window.getEnd()) ;
                for (UserLoginBean value : values) {
                    value.setStt(stt);
                    value.setEdt(edt);
                    value.setTs(System.currentTimeMillis());
                    out.collect(value);
                }
            }
        });
        reduceStream.print("写入到clickhouse中的数据");
        //TODO 11.写入数据到clickhouse中
        String sql = "insert into dws_user_user_login_window values(?,?,?,?,?)";
        reduceStream.addSink(MyClickhouseUtil.getSinkFunction(sql));
        env.execute();;
    }
}
