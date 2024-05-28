package com.xly.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.TrafficHomeDetailPageViewBean;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * DWS层流量域首页、详情页页面浏览各窗口汇总表
 * 将数据从程序中写入到clickhouse
 * 执行思路：1.将数据从dwd_traffic_page_log中读取出来
 *         2.将数据从jsonstr转换为jsonobj（为了方便数据处理）
 *         3.将其中的数据进行过滤，过滤出page_id为home 或者 good_detail的数据
 *         4.对数据设置水位线（越靠近数据源越好）
 *         5.设置状态，将提取出来的page_id 为home 或者 good_detail的数据设置到状态中
 *         6.将数据和状态中的数据进行比对（日活，状态中没有相关数据或者状态中的日期和当前数据中的日期不同的数据，封装到对象中，并且更新状态）
 *         7.封装对象，向下游传递
 *         8.聚合（全局开窗聚合）
 *         9. 将聚合的数据向clickhouse传递
 */
public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关设置（lue）
        //TODO 3.读取dwd层的topic中的数据
        String topic = "dwd_traffic_page_log";
        String groupId ="dws_traffic_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);
//        kafkaDS.print("从dwd_traffic_page_log读取数据");
        /*
        {
        "common":{"ar":"16","uid":"272","os":"Android 11.0","ch":"oppo","is_new":"1","md":"vivo x90","mid":"mid_418","vc":"v2.1.134","ba":"vivo","sid":"2247a9af-87a7-4649-ad8b-c72a242a7739"},
        "page":{"from_pos_seq":0,"page_id":"good_detail","item":"9","during_time":13017,"item_type":"sku_id","last_page_id":"home","from_pos_id":9},
        "ts":1681271269000
        }
         */
        //TODO 4.将数据jsonstr转换成jsonobj(方便处理)
        SingleOutputStreamOperator<JSONObject> mappedDS = kafkaDS.map(JSON::parseObject);
        //TODO 5.过滤数据（过滤出page_id为home 或者 good_detail的数据）
        SingleOutputStreamOperator<JSONObject> filterDS = mappedDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //判断json对象中的page_id是否为home或者good_detail
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );
       // filterDS.print("过滤出page_id为home和good_detail");
        /*
        {
        "common":{"ar":"27","uid":"48","os":"Android 10.1","ch":"wandoujia","is_new":"1","md":"xiaomi 12 ultra ","mid":"mid_397","vc":"v2.1.134","ba":"xiaomi","sid":"6b3e9890-8dfa-4565-9f9f-b6185a8dcf1d"},
        "page":{"from_pos_seq":5,"page_id":"good_detail","item":"35","during_time":15170,"item_type":"sku_id","last_page_id":"trade","from_pos_id":4},
        "ts":1681271654000
        }
         */
        //TODO 6.设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //timestampassignersupplier底层会忽略对时间戳的序列化
                //serializabletimestampassigner就是继承的timestampassiger，可以对不同要素的时间戳进行转换
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //TODO 7.按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
        //keyedDS.print("keyed");
        //TODO 8.判断独立访客（状态编程）
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            //设置状态(首页一天中最早的访问的时间，商品详情页一天中最早的访问的时间)
            private ValueState<String> homeLastVisitDtstate;
            private ValueState<String> detailLastDtstate;

            //在open方法中初始化值状态（设置状态过期时间等）
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("home_last_visit_date", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                homeLastVisitDtstate=getRuntimeContext().getState(valueStateDescriptor);

                ValueStateDescriptor<String> detailStateDescriptor = new ValueStateDescriptor<>("detail_last_visit_date", String.class);
                detailStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                detailLastDtstate = getRuntimeContext().getState(detailStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                //获取状态中的上一次首页访问的时间和商品详情访问的时间
                String homeLastDt = homeLastVisitDtstate.value();
                String detailLastDt = detailLastDtstate.value();
                //获取数据中的详细信息

        /*{
        "common":{"ar":"27","uid":"48","os":"Android 10.1","ch":"wandoujia","is_new":"1","md":"xiaomi 12 ultra ","mid":"mid_397","vc":"v2.1.134","ba":"xiaomi","sid":"6b3e9890-8dfa-4565-9f9f-b6185a8dcf1d"},
        "page":{"from_pos_seq":5,"page_id":"good_detail","item":"35","during_time":15170,"item_type":"sku_id","last_page_id":"trade","from_pos_id":4},
        "ts":1681271654000
        }*/

                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                Long ts = jsonObject.getLong("ts");
                String visitDt = DataFormatUtil.toDate(ts);
                //给需要统计的数据进行初始化
                Long homeUvCt = 0L;
                Long goodDetailUvCt = 0L;
                if (pageId .equals("home")){
                    //当数据为home时的数据,判断状态中的是否有数据，或者状态中的数据的日期是否为数据产生的时间
                    if (StringUtils.isEmpty(homeLastDt) || !homeLastDt.equals(visitDt)){
                        homeUvCt = 1L;
                        homeLastVisitDtstate.update(visitDt);
                    }
                }
                if (pageId.equals("good_detail")){
                    if (StringUtils.isEmpty(detailLastDt) || !detailLastDt.equals(visitDt)){
                        goodDetailUvCt = 1L;
                        detailLastDtstate.update(visitDt);
                    }
                }
                //将数据装载到bean对象中
                if (homeUvCt != 0L || goodDetailUvCt != 0L){
                    out.collect(new TrafficHomeDetailPageViewBean(
                            "",
                            "",
                            homeUvCt,
                            goodDetailUvCt,
                            0L
                    ));
                }
            }
        });
        // processDS.print("筛选之后的数据");
        /*
         TrafficHomeDetailPageViewBean(stt=, edt=, homeUvCt=1, goodDetailUvCt=0, ts=0)
         */
        //TODO 9.开窗，是全局开窗进行统计
       AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowStream
               = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        //TODO 10.聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = windowStream.reduce(
                //将度量值读取出来进行相加
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                //获取窗口的开始时间和结束时间
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DataFormatUtil.toYmdHms(window.getStart());
                        String edt = DataFormatUtil.toYmdHms(window.getEnd());
                        for (TrafficHomeDetailPageViewBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setTs(System.currentTimeMillis());
                            out.collect(value);
                        }
                    }
                }
        );
        reduceStream.print("写入clickhouse的数据");
        //TODO 11.写入数据到clickhouse
        String sql = "insert into dws_traffic_home_detail_page_view_window values(?,?,?,?,?)";
        reduceStream.addSink(MyClickhouseUtil.getSinkFunction(sql));
         env.execute();
    }
}
