package com.xly.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.app.func.DimAsyncFunction;
import com.xly.gmall.realtime.beans.TradeProvinceOrderBean;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 交易域省份粒度下单聚合统计
 */
public class DwsTradeProvinceOrderWindowMyTest1 {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3.从dwd层读取kafka数据 dwd_trade_order_detail
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        //TODO 4.过滤掉null数据，并进行类型转换 jsonstr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (jsonStr != null){
                    out.collect(JSON.parseObject(jsonStr));
                }
            }
        });
        //jsonObj.print("从kafka中读取数据过滤空值");
        /*
        {"create_time":"2023-04-12 19:50:46","sku_num":"1","activity_rule_id":"1","split_original_amount":"6499.0000","split_coupon_amount":"0.0",
        "sku_id":"3","date_id":"2023-04-12","user_id":"401","province_id":"1","activity_id":"1","sku_name":"小米12S","id":"14285781","order_id":"69650",
        "split_activity_amount":"500.0","split_total_amount":"5999.0","ts":"1681300246"}
         */
        //TODO 5.按照order_detail_id 进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObj.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("id");
                    }
                }
        );

        //TODO 6.去重（kafka中左表与右表关联的度量值可能为null值的数据） 状态 + 抵消（不应通过order_detail去分组统计count）
        SingleOutputStreamOperator<JSONObject> distinctDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                        //数据没必要一直存在
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());
                        lastValueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //获取状态中的数据，如果状态中有数据，就获取状态中的数据，并对下单次数，下单金额进行取反操作
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue != null){
                            String splitTotalAmount = jsonObject.getString("split_total_amount");
                            lastValue.put("order_count", -1);
                            lastValue.put("split_total_amount","-"+splitTotalAmount);
                            out.collect(lastValue);
                        }
                        jsonObject.put("order_count",1);
                        lastValueState.update(jsonObject);
                        out.collect(jsonObject);
                    }
                }
        );
       //distinctDS.print("过滤掉null值的数据");
        /*
        {"create_time":"2023-04-12 20:24:26","sku_num":"2","order_count":"+1","split_original_amount":"5798.0000","split_coupon_amount":"0.0","sku_id":"20",
        "date_id":"2023-04-12","user_id":"472","province_id":"18","sku_name":"小米电视E65X","id":"14286085","order_id":"69835","split_activity_amount":"0.0",
        "split_total_amount":"5798.0","ts":"1681302266"}
         */

        //TODO 7.封装成对象向下传递
        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceOrderBean = distinctDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                        //将数据封装给TradeProvinceOrderBean
                        TradeProvinceOrderBean provinceOrderBean = TradeProvinceOrderBean
                                .builder()
                                .orderAmount(new BigDecimal(jsonObject.getString("split_total_amount")))
                                .orderCount(jsonObject.getLong("order_count"))
                                .orderId(jsonObject.getString("order_id"))
                                .provinceId(jsonObject.getString("province_id"))
                                .ts(jsonObject.getLong("ts") * 1000L)
                                .build();
                        return provinceOrderBean;
                    }
                }
        );
        //provinceOrderBean.print("转换为provinceOrderBean对象");
        /*
        TradeProvinceOrderBean(stt=null, edt=null, provinceId=1, provinceName=, orderId=71550, orderCount=1, orderAmount=1299.0, ts=1681303831000)
        TradeProvinceOrderBean(stt=null, edt=null, provinceId=24, provinceName=, orderId=71559, orderCount=-1, orderAmount=-6499.0, ts=1681303831000)
         */
        //TODO 8.分组（按照order_id进行分组计算）
        KeyedStream<TradeProvinceOrderBean, String> orderIdKeyedStream = provinceOrderBean.keyBy(
                new KeySelector<TradeProvinceOrderBean, String>() {
                    @Override
                    public String getKey(TradeProvinceOrderBean value) throws Exception {
                        return value.getOrderId();
                    }
                }
        );
        //TODO 9.统计订单数
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = orderIdKeyedStream.reduce(new ReduceFunction<TradeProvinceOrderBean>() {
            @Override
            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
        });
//        reduceDS.print("统计订单数");
        /*
        TradeProvinceOrderBean(stt=null, edt=null, provinceId=9, provinceName=, orderId=71895, orderCount=1, orderAmount=8197.0, ts=1681304885000)
         */

        //TODO 10.提取水位线和事件时间
        SingleOutputStreamOperator<TradeProvinceOrderBean> watermarkDS = reduceDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                            @Override
                            public long extractTimestamp(TradeProvinceOrderBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );
        //TODO 11.按照省份id进行分组
        KeyedStream<TradeProvinceOrderBean, String> keyedByProIdStream = watermarkDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //TODO 12.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = keyedByProIdStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(3));

        //TODO 13.按照省份id进行分组
        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceOrderDS = windowDS.reduce(new ReduceFunction<TradeProvinceOrderBean>() {
            @Override
            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
        }, new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                long stt = window.getStart();
                long eet = window.getEnd();
                for (TradeProvinceOrderBean element : input) {
                    element.setStt(DataFormatUtil.toYmdHms(stt));
                    element.setEdt(DataFormatUtil.toYmdHms(eet));
                    element.setTs(System.currentTimeMillis());
                    out.collect(element);
                }
            }
        });
//        provinceOrderDS.print("按照省份id分组");
        /*
        TradeProvinceOrderBean(stt=2023-04-12 21:21:10, edt=2023-04-12 21:21:20, provinceId=4, provinceName=, orderId=72091, orderCount=1, orderAmount=11749.0, ts=1682428882369)
         */

         //TODO 14.关联省份id，省份名称
        SingleOutputStreamOperator<TradeProvinceOrderBean> provindDS = AsyncDataStream.unorderedWait(
                provinceOrderDS,
                new DimAsyncFunction<TradeProvinceOrderBean>("DIM_BASE_PROVINCE") {
                    @Override
                    public void getJoin(TradeProvinceOrderBean object, JSONObject dimInfo) {
                        String provinceName = dimInfo.getString("NAME");
                        object.setProvinceName(provinceName);
                    }

                    @Override
                    public String getKey(TradeProvinceOrderBean object) {
                        return object.getProvinceId();
                    }
                },
                60*5,
                TimeUnit.SECONDS
        );
        provindDS.print("写到clickhouse中的数据");
        //TODO 15.将关联的数据写到ck中去
        provindDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));
        env.execute();
    }
}
