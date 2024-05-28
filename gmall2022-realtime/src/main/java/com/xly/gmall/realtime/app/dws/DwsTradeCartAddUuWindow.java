package com.xly.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.CartAddUuBean;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 交易域加购各窗口汇总表
 * 从dwd_trade_cart_add中读取数据，并进行开窗统计各窗口加购独立用户数，写入clickhouse中
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关设置
        //TODO 3.从dwd层的kafka主题中读取相关的数据
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);
//        kafkaDS.print("从dwd_trade_cart_add中读取数据");
        /*
       {"id":"271345","user_id":"269","sku_id":"1","sku_num":"1","sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机","ts":"1681297787"}
         */
        //TODO 4.将读取的数据从jsonstr转换为jsonobj
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(JSON::parseObject);
        //TODO 5.指定watermark以及提取事件时间
        SingleOutputStreamOperator<JSONObject> watermarkDS = jsonObj.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                                return jsonObject.getLong("ts") * 1000;
                            }
                        })
        );
        //TODO 6.按照用户id进行分组
        KeyedStream<JSONObject, String> keyedDS = watermarkDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                String userId = jsonObject.getString("user_id");
                return userId;
            }
        });
        //TODO 7.使用flink的状态编程(判断是否为独立用户)
        SingleOutputStreamOperator<JSONObject> processDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    //每日独立用户判断，维护一个一天时间内的状态，判断数据的当前时间在状态中是否包含
                    private ValueState<String> lastAddDateState;
                    //open方法是可以对状态进行初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //可以通过状态描述器设置一天数据状态的过期时间
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastAddDateState",String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastAddDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }
                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //获取状态中的数据
                        String lastAddDate = lastAddDateState.value();
                        String curDate = DataFormatUtil.toDate(jsonObject.getLong("ts") * 1000);
                        //判断，如果当前状态为空，就将状态更新为当前数据时间,如果当前状态时间不为空，并且当前状态时间不等于当前时间状态，更新状态中的时间，将数据转换为实体类对象向下游传递
                        if (StringUtils.isEmpty(lastAddDate) || !lastAddDate.equals(curDate)){
                            lastAddDateState.update(curDate);
                            out.collect(jsonObject);
                        }
                    }
                }
        );
//        processDS.print("判断用户是否为独立用户");
        /**
         * {"sku_num":"1","user_id":"360","sku_id":"4","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"271538","ts":"1681299218"}
         */
        //TODO 8.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 9.聚合
        //因为元数据为json对象，但是要转换为cartadduubean对象向下游传递，类型不同，选择用aggregate
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(new AggregateFunction<JSONObject, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(JSONObject value, Long accumulator) {
                return accumulator + 1;
            }

            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }

            @Override
            public Long merge(Long a, Long b) {
                return null;
            }
        }, new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                String stt = DataFormatUtil.toYmdHms(window.getStart());
                String edt = DataFormatUtil.toYmdHms(window.getEnd());
                for (Long value : values) {
                    out.collect(new CartAddUuBean(
                            stt,
                            edt,
                            value,
                            System.currentTimeMillis()
                    ));
                }
            }
        });
        aggregateDS.print("写入到clickhouse中的数据");
        //TODO 10.写入数据到clickhouse中
        aggregateDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values (?,?,?,?)"));
        env.execute();
    }
}
