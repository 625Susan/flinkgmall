package com.xly.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.TrafficPageViewBean;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口轻度聚合
 * 数据处理：1.从流量域中获取dwd层的page页面相关的数据
 * 2.对页面的数据进行筛选，按照相关的值进行分组筛选
 * 3.进行开窗聚合数据（需要设置事件时间的处理和水位线设置）
 * 4.聚合逻辑
 * 5.将聚合出来的数据写入到clickhouse中
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关设置（lue）
        //TODO 3.从kafka主题中读取数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_channel_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
        //TODO 4.转换dwd_page流量域的数据结构，jsonstr->jsonobj
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonstr) throws Exception {
                return JSON.parseObject(jsonstr);
            }
        });
//        jsonObjDS.print("从dwd_traffic_page_log获取数据");
        /* {
        "common":{"ar":"33","uid":"389","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_391","vc":"v2.1.134","ba":"iPhone","sid":"b8786d28-d580-42bb-b0e0-12b74874aa15"},
        "page":{"from_pos_seq":6,"page_id":"good_detail","item":"20","during_time":9972,"item_type":"sku_id","last_page_id":"good_detail","from_pos_id":4},
        "ts":1681299855000
        }
         */
        //TODO 5.按照设备id进行分组
        //为什么要以设备id作为统计粒度？因为这个统计粒度更细，比user_id更细致些，可以统计独立用户访客数、会话数、页面浏览数、页面访问时长
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //TODO 6.统计独立用户访客数、会话数、页面浏览数、页面访问时长，并封装为实体类
        //这些都是属于维度数据，可以将其封装成一个类
        SingleOutputStreamOperator<TrafficPageViewBean> pageViewBeanDS = keyedStream.map(
                //因为是统计日活，在统计独立访客数时需要和状态中的已经保留的独立访客数据进行比对，所以需要使用富函数来获取（在open方法中）
                new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                    //定义一个值状态，用来保存独立访客每日登陆的数据
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //在open方法中定义状态描述器，获取状态内的值和状态timetolive
                        ValueStateDescriptor<String> stateProperties = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        stateProperties.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        //获取值状态中的数据内容
                        lastVisitDateState = getRuntimeContext().getState(stateProperties);
                    }

                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObject) throws Exception {
                        //维度信息
                        JSONObject common = jsonObject.getJSONObject("common");
                        String vc = common.getString("vc"); //版本号
                        String ch = common.getString("ch"); //渠道
                        String ar = common.getString("ar"); //地区
                        String isNew = common.getString("is_new"); //新老访客标记
                        JSONObject page = jsonObject.getJSONObject("page");
                        String lastPageId = page.getString("last_page_id");

                        //判断是否为独立访客，在状态中是否有与它相匹配的数据
                        Long uvCt = 0L;
                        //获取状态中的值
                        String lastVisitDt = lastVisitDateState.value();
                        //获取产生数据的值
                        Long ts = jsonObject.getLong("ts");
                        String currentData = DataFormatUtil.toDate(ts);
                        //如果状态中没有这个数据，或者状态中的时间日期和数据产生的时间日期不相等，就将数据写入进状态中
                        if (StringUtils.isEmpty(lastVisitDt) || !lastVisitDt.equals(currentData)) {
                            lastVisitDateState.update(currentData);
                            uvCt = 1L;
                        }
                        //判断是否为新的会话（判断上一个会话的页面id是不是为空，如果是空就返回1，证明它是新的会话开始）
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;
                        //获取访问时长
                        Long duringTime = page.getLong("during_time");
                        TrafficPageViewBean pageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                isNew,
                                uvCt,
                                svCt,
                                1L,//页面浏览数，出现一次，统计一次
                                duringTime,
                                ts
                        );
                        return pageViewBean;
                    }
                }
        );
//        pageViewBeanDS.print("封装数据实体类");
        /*
        TrafficPageViewBean(stt=, edt=, vc=v2.1.134, ch=oppo, ar=14, isNew=1, uvCt=0, svCt=0, pvCt=1, durSum=5989, ts=1681303479000)
         */
        //TODO 7.设置水位线（水位线，乱序程度）
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = pageViewBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();//将封装对象中的ts作为水位线传递
                            }
                        })
        );
        //TODO 8.按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedDimDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean viewBean) throws Exception {
                        return Tuple4.of(
                                viewBean.getVc(),
                                viewBean.getCh(),
                                viewBean.getAr(),
                                viewBean.getIsNew()
                        );
                    }
                }
        );

        //TODO 9.开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = keyedDimDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 10.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        String stt = DataFormatUtil.toYmdHms(window.getStart());
                        String edt = DataFormatUtil.toYmdHms(window.getEnd());
                        for (TrafficPageViewBean bean : input) {
                            bean.setStt(stt);
                            bean.setEdt(edt);
                            bean.setTs(System.currentTimeMillis());
                            out.collect(bean);
                        }
                    }
                }
        );
        reduceDS.print(">>>>");
        //TrafficPageViewBean
        // (stt=2023-04-12 21:29:00, edt=2023-04-12 21:29:10, vc=v2.1.134, ch=360, ar=19, isNew=0, uvCt=1, svCt=2, pvCt=6, durSum=49833, ts=1682083754709)
        //TODO 11.将聚合的结果写到CK中(参数类型参照trafficpageviewbean)
        reduceDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }
}
