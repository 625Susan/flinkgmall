package com.xly.gmall.realtime.app.dws;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.app.func.DimAsyncFunction;
import com.xly.gmall.realtime.beans.TradeProvinceOrderBean;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Felix
 * @date 2022/12/27
 * 交易域省份粒度下单聚合统计
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 从 Kafka dwd_trade_order_detail 读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        // TODO 4. 过滤 null 数据
        SingleOutputStreamOperator<String> filteredStream = kafkaStrDS.filter(Objects::nonNull);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<String, String> keyedStream = filteredStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String jsonStr) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                return jsonObj.getString("id");
            }
        });

        // TODO 6. 去重并转换数据结构
        SingleOutputStreamOperator<TradeProvinceOrderBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, String, TradeProvinceOrderBean>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue != null) {
                            String provinceId = lastValue.getString("province_id");
                            String orderId = lastValue.getString("order_id");
                            BigDecimal orderAmount = new BigDecimal(
                                    "-" + lastValue.getString("split_total_amount"));
                            Long ts = lastValue.getLong("ts") * 1000L;

                            out.collect(TradeProvinceOrderBean.builder()
                                    .provinceId(provinceId)
                                    .orderId(orderId)
                                    .orderAmount(orderAmount)
                                    .ts(ts)
                                    .build());
                        }
                        lastValueState.update(jsonObj);
                        String provinceId = jsonObj.getString("province_id");
                        String orderId = jsonObj.getString("order_id");
                        BigDecimal orderAmount = new BigDecimal(jsonObj.getString("split_total_amount"));
                        Long ts = jsonObj.getLong("ts") * 1000L;

                        out.collect(TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderId(orderId)
                                .orderAmount(orderAmount)
                                .ts(ts)
                                .build());
                    }
                }
        );

        // TODO 7. 按照 order_id 分组
        KeyedStream<TradeProvinceOrderBean, String> orderIdKeyedStream = processedStream.keyBy(TradeProvinceOrderBean::getOrderId);

        // TODO 8. 统计订单数
        SingleOutputStreamOperator<TradeProvinceOrderBean> withOrderCountStream = orderIdKeyedStream.process(
                new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                    ValueState<String> lastOrderIdState;

                    @Override
                    public void open(Configuration paramters) throws Exception {
                        super.open(paramters);
                        ValueStateDescriptor<String> lastOrderIdStateDescriptor =
                                new ValueStateDescriptor<>("province_last_order_id_state", String.class);
                        lastOrderIdStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.seconds(10L)).build()
                        );
                        lastOrderIdState = getRuntimeContext().getState(lastOrderIdStateDescriptor);
                    }

                    @Override
                    public void processElement(TradeProvinceOrderBean javaBean, Context context, Collector<TradeProvinceOrderBean> out) throws Exception {
                        String orderId = javaBean.getOrderId();
                        String lastOrderId = lastOrderIdState.value();
                        if (lastOrderId == null) {
                            javaBean.setOrderCount(1L);
                            out.collect(javaBean);
                            lastOrderIdState.update(orderId);
                        } else {
                            javaBean.setOrderCount(0L);//因为数据是按照order_detail来的，所以要将order_id 维护到状态中
                            out.collect(javaBean);
                        }
                    }
                }
        );

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeProvinceOrderBean> withWatermarkStream = withOrderCountStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderBean javaBean, long recordTimestamp) {
                                        return javaBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 10. 按照省份 ID 分组
        KeyedStream<TradeProvinceOrderBean, String> keyedByProIdStream =
                withWatermarkStream.keyBy(TradeProvinceOrderBean::getProvinceId);

        // TODO 11. 开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = keyedByProIdStream
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L));

        // TODO 12. 聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedStream = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TradeProvinceOrderBean> elements, Collector<TradeProvinceOrderBean> out) throws Exception {
                        for (TradeProvinceOrderBean element : elements) {
                            String stt = DataFormatUtil.toYmdHms(context.window().getStart());
                            String edt = DataFormatUtil.toYmdHms(context.window().getEnd());
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 13. 关联省份信息
        SingleOutputStreamOperator<TradeProvinceOrderBean> fullInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeProvinceOrderBean>("dim_base_province".toUpperCase()) {

                    @Override
                    public void getJoin(TradeProvinceOrderBean object, JSONObject dimInfo) {
                        String provinceName = dimInfo.getString("name".toUpperCase());
                        object.setProvinceName(provinceName);
                    }

                    @Override
                    public String getKey(TradeProvinceOrderBean javaBean) {
                        return javaBean.getProvinceId();
                    }
                },
                60 * 50, TimeUnit.SECONDS
        );

        // TODO 14. 写入到 clickhouse 数据库
        fullInfoStream.print(">>>>");
        fullInfoStream.<TradeProvinceOrderBean>addSink(MyClickhouseUtil.getSinkFunction(
                "insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"
        ));

        env.execute();
    }
}


