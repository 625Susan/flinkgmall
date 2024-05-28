package com.xly.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.app.func.DimAsyncFunction;
import com.xly.gmall.realtime.beans.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.utils.*;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyClickhouseUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 交易域sku粒度下单业务过程聚合统计
 * 度量：统计原始金额、活动减免金额、优惠券减免金额和订单金额(同时，还需要统计sku_id的相关信息)
 * 度量值：原始金额，最终下单金额，优惠券金额，参与活动的金额
 * 难点：从kafka读取数据的去重过滤（状态 + 定时器 / 状态 + 抵消）
 *      与phoenix的维度关联
 *      将维度数据的热数据存到redis中，同时将读取到的维度数据从phoenix中缓存到redis中，
 *      如果维度数据有变化，删除redis中的数据
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);//和kafka主题分区一致

        //TODO 2.检查点相关设置
        //TODO 3.从kafka主题中读取数据（dwd_trade_order_detail）
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_sku_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);
        //kafkaDS.print("从kafka中读取的数据");
        /*
        {"id":"14274976","order_id":"63099","user_id":"619","sku_id":"4","sku_name":"Redmi ","province_id":"16","activity_id":null,
        "activity_rule_id":null,"coupon_id":null,"date_id":"2023-04-12","create_time":"2023-04-12 16:50:57","sku_num":"1",
        "split_original_amount":"999.0000","split_activity_amount":"0.0","split_coupon_amount":"0.0","split_total_amount":"999.0","ts":"1681289457"}
         */

        //TODO 4.数据类型转换同时过滤null数据（jsonstr ->jsonobj）
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonSt, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (StringUtils.isNotEmpty(jsonSt)){
                    JSONObject jsonObject = JSON.parseObject(jsonSt);
                    out.collect(jsonObject);
                }
            }
        });

       // jsonObj.print("将null的数据过滤出去");
        /*
        {"create_time":"2023-04-12 11:57:58","sku_num":"1","activity_rule_id":"1","split_original_amount":"6499.0000",
        "split_coupon_amount":"0.0","sku_id":"3","date_id":"2023-04-12","user_id":"660","province_id":"25","activity_id":"1",
        "sku_name":"小米12S","id":"14275300","order_id":"63283","split_activity_amount":"500.0","split_total_amount":"5999.0","ts":"1681271878"}
         */
        //TODO 5.去重（两种方式去重：状态+定时器/状态+抵消） 按照key值进行去重操作key值是order_detail的主键id
        KeyedStream<JSONObject, String> orderDetailKeyedDS = jsonObj.keyBy(jso -> jso.getString("id"));
        //方式一：状态+定时器（每条数据到了都得到状态中去等待五秒时间，对时效性有影响）
        /*SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailKeyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            //方式：数据来了，将json数据保留在状态中，同时设置定时器，避免数据一直放到状态中
            private ValueState<JSONObject> lastValueState;
            //对状态进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> stringValueStateDescriptor = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                //不需要设置状态的过期时间了，因为下文要设置定时器
                lastValueState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                //获取状态中的数据
                JSONObject lastValue = lastValueState.value();
                //如果状态中的数据为空，说明状态中还没有数据，现在传入的这个数据就是最新的数据
                if (lastValue == null) {
                    //将现在产生的数据更新到状态中去，并注册一个定时器
                    lastValueState.update(value);
                    //获取当前处理时间
                    long currenTs = ctx.timerService().currentProcessingTime();
                    //注册一个五秒之后的定时器
                    ctx.timerService().registerProcessingTimeTimer(currenTs + 5000L);
                } else {
                    //如果状态中的数据不为空，说明现在到达的数据就是左表不为空，右表也不为空的数据（我们需要的数据）
                    Long ts1 = value.getLong("ts");//这里是伪代码，因为我们在dwd层没有传递处理数据的时间过来，只传递了order_detail的ts过来，不能作为判断依据
                    Long ts2 = lastValue.getLong("ts");
                    if (ts1 > ts2) {
                        lastValueState.update(value);
                    }
                }
            }
            //写定时方法，五秒时间到达之后，应该做什么样的事情
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = lastValueState.value();
                out.collect(jsonObject);
                lastValueState.clear();//清空状态中的数据
            }
        });*/
        //方式二：状态+抵消（判断如果是第二条数据到来，就将原本读取的第二条数据减掉）
        SingleOutputStreamOperator<JSONObject> processDS = orderDetailKeyedDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                lastValueState = getRuntimeContext().getState(valueStateDescriptor);
            }
            /*
             {"create_time":"2023-04-12 11:57:58","sku_num":"1","activity_rule_id":"1","split_original_amount":"6499.0000",
             "split_coupon_amount":"0.0","sku_id":"3","date_id":"2023-04-12","user_id":"660","province_id":"25","activity_id":"1",
             "sku_name":"小米12S","id":"14275300","order_id":"63283","split_activity_amount":"500.0","split_total_amount":"5999.0","ts":"1681271878"}
              */
            @Override
            public void processElement(JSONObject jsonOb, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                //获取状态中的值
                JSONObject lastValue = lastValueState.value();
                if (lastValue != null){
                    //如果状态中有值，说明当前来的数据是第二条数据，需要将消费数据金额从状态中提取出来，取反操作
                    String splitOriginalAmount = lastValue.getString("split_original_amount");
                    String splitCouponAmount = lastValue.getString("split_coupon_amount");
                    String splitActivityAmount = lastValue.getString("split_activity_amount");
                    String splitTotalAmount = lastValue.getString("split_total_amount");
                    //对状态中的数据重新取出来，如果参加了活动就取反，如果没有参加，就还是为0.0
                    lastValue.put("split_original_amount","-"+splitOriginalAmount);
                    lastValue.put("split_coupon_amount",splitCouponAmount == null ? "0.0":"-"+splitCouponAmount);
                    lastValue.put("split_activity_amount",splitActivityAmount == null ? "0.0":"-"+splitActivityAmount);
                    lastValue.put("split_total_amount","-"+splitTotalAmount);
                    out.collect(lastValue);
                }
                out.collect(jsonOb);
                lastValueState.update(jsonOb);
            }
        });
//        processDS.print("过滤掉右表为null的数据");
        /*
       {"create_time":"2023-04-12 13:42:21","sku_num":"1","activity_rule_id":"4","split_original_amount":"11999.0000","split_coupon_amount":"0.0",
       "sku_id":"14","date_id":"2023-04-12","user_id":"447","province_id":"19","activity_id":"3","sku_name":"联想（Lenovo）","id":"14275607",
       "order_id":"63471","split_activity_amount":"250.0","split_total_amount":"11749.0","ts":"1681278141"}
         */
        //TODO 6.对流中的数据进行转换（jsonobj->javaobj，封装）也可以不封装成javaobj
        SingleOutputStreamOperator<TradeSkuOrderBean> orderBeanDS = processDS.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject jsonObject) throws Exception {
                //获取json数据中的主键和度量值
                String skuId = jsonObject.getString("sku_id");
                String splitOriginalAmount = jsonObject.getString("split_original_amount");
                String splitCouponAmount = jsonObject.getString("split_coupon_amount");
                String splitActivityAmount = jsonObject.getString("split_activity_amount");
                String splitTotalAmount = jsonObject.getString("split_total_amount");
                //将获取的数据封装到TradeSkuOrderBean中
                TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                        .skuId(skuId)
                        .originalAmount(new BigDecimal(splitOriginalAmount))
                        .couponAmount(new BigDecimal(splitCouponAmount))
                        .activityAmount(new BigDecimal(splitActivityAmount))
                        .orderAmount(new BigDecimal(splitTotalAmount))
                        .ts(jsonObject.getLong("ts")*1000)
                        .build();
                return orderBean;
            }
        });
        //orderBeanDS.print("将jsonsobj转化为javaobj");
        /*
        TradeSkuOrderBean(stt=null, edt=null, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null,
         category2Name=null, category3Id=null, category3Name=null, skuId=22, skuName=null, spuId=null, spuName=null, originalAmount=39.0000,
         activityAmount=0.0, couponAmount=0.0, orderAmount=39.0, ts=1681279242000)
         */
        //TODO 7.设置水位线和提取事件时间
        SingleOutputStreamOperator<TradeSkuOrderBean> withWatermarkDS = orderBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                            @Override
                            public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );
        //TODO 8.keyby，按照sku_id分组，去重
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = withWatermarkDS.keyBy(TradeSkuOrderBean::getSkuId);

        //TODO 9.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 10聚合数据（原本的数据类型：javabean->javabean）
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                //获取两个数据的四个度量值相加的结果
                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                return value1;
            }
        },
                //获取窗口范围的数据
                new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
            @Override
            public void apply(String groupId, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                String stt = DataFormatUtil.toYmdHms(window.getStart());
                String edt = DataFormatUtil.toYmdHms(window.getEnd());
                for (TradeSkuOrderBean skuOrderBean : input) {
                    //将窗口信息和处理的当前系统时间信息放到这个javabean对象中
                    skuOrderBean.setStt(stt);
                    skuOrderBean.setEdt(edt);
                    skuOrderBean.setTs(System.currentTimeMillis());
                    out.collect(skuOrderBean);
                }
            }
        });
        //reduceDS.print("reduceDS");
        /*
    TradeSkuOrderBean(stt=2023-04-12 15:52:10, edt=2023-04-12 15:52:20, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null,
    category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=3, skuName=null, spuId=null, spuName=null,
    originalAmount=71489.0000, activityAmount=3900.0, couponAmount=0.0, orderAmount=67589.0, ts=1682322743396)
         */
        //TODO 11.关联sku的维度数据(使用map)
        //方法一：旁路缓存
       /* SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(new MapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {
                //把商品相关的维度id获取出来
                String skuId = value.getSkuId();
                //根据主键到对应的维度表中获取维度对象
                JSONObject dimInfoJsonObj = DimUtil.getDimInfo("dim_sku_info", skuId);
                //将维度对象补充到流中的对象上
                value.setTrademarkId(dimInfoJsonObj.getString("TM_ID"));
                value.setSpuId(dimInfoJsonObj.getString("SPU_ID"));
                value.setCategory3Id(dimInfoJsonObj.getString("CATEGORY3_ID"));
                value.setSkuName(dimInfoJsonObj.getString("SKU_NAME"));
                return value;
            }
        });
        withSkuInfoDS.print("从redis缓存/phoenix中获取数据");*/
        //方法二：异步IO操作
      /*  SingleOutputStreamOperator<TradeSkuOrderBean> withSkuId = AsyncDataStream.unorderedWait(
                reduceDS,
                new AsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                   //开启线程，从工具类中获取线程
                        ThreadPoolExecutor poolExecutor = ThreadPoolUtil.getInstance();
                        poolExecutor.submit(new Runnable() {
                            @Override
                            public void run() {
                                //根据流中的对象获取主键
                                String skuId = orderBean.getSkuId();
                                //通过主键查询phoenix表中的对象
                                JSONObject dimSkuInfo = DimUtil.getDimInfo("DIM_SKU_INFO", skuId);
                                //将表中的对象补全到对象属性中
                                orderBean.setSkuName(dimSkuInfo.getString("SKU_NAME"));
                                orderBean.setSpuId(dimSkuInfo.getString("SPU_ID"));
                                orderBean.setCategory3Id(dimSkuInfo.getString("CATEGORY3_ID"));
                                orderBean.setTrademarkId(dimSkuInfo.getString("TRADEMARK_ID"));
                                //将对象向下游传递
                                resultFuture.complete(Collections.singleton(orderBean));
                            }
                        });
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withSkuId.print("关联sku_id");
        *//*
        radeSkuOrderBean(stt=2023-04-12 16:08:30, edt=2023-04-12 16:08:40, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null,
         category2Id=null, category2Name=null, category3Id=61, category3Name=null, skuId=5, skuName=Redmi, spuId=2, spuName=null, originalAmount=999.0000,
         activityAmount=0.0, couponAmount=0.0, orderAmount=999.0, ts=1682410122262)
         */
        //方法优化：异步IO方法优化，使用模板设计模式（定义抽象类或者接口，不能确定子类是什么数据类型或具体处理）
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuId = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public void getJoin(TradeSkuOrderBean orderBean, JSONObject dimSkuInfo) {
                        //通过主键将数据取出，并封装到流中
                   /*     orderBean.setSkuName(dimSkuInfo.getString("sku_name".toUpperCase()));
                        orderBean.setSpuId(dimSkuInfo.getString("spu_id".toUpperCase()));
                        orderBean.setCategory3Id(dimSkuInfo.getString("category3_id".toUpperCase()));
                        orderBean.setTrademarkId(dimSkuInfo.getString("trademark_id".toUpperCase()));*/
                        orderBean.setSkuName(dimSkuInfo.getString("SKU_NAME"));
                        orderBean.setSpuId(dimSkuInfo.getString("SPU_ID"));
                        orderBean.setCategory3Id(dimSkuInfo.getString("CATEGORY3_ID"));
                        orderBean.setTrademarkId(dimSkuInfo.getString("TRADEMARK_ID"));
                    }
                    @Override
                    public String getKey(TradeSkuOrderBean orderBean) {
                        //获取主键
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
       // withSkuId.print("关联sku_id");

        //TODO 12.关联spu的维度数据
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuId = AsyncDataStream.unorderedWait(
                withSkuId,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public void getJoin(TradeSkuOrderBean object, JSONObject dimInfo) {
               /*         object.setSpuName(dimInfo.getString("spu_name".toUpperCase()));
                        object.setTrademarkId(dimInfo.getString("tm_id".toUpperCase()));*/
                        object.setSpuName(dimInfo.getString("SPU_NAME"));
                        object.setTrademarkId(dimInfo.getString("TM_ID"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean object) {
                        return object.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
       // withSpuId.print("关联spu_id");
        //TODO 13.关联trade_mark的维度数据
        SingleOutputStreamOperator<TradeSkuOrderBean> withTrade_mark = AsyncDataStream.unorderedWait(
                withSpuId,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void getJoin(TradeSkuOrderBean object, JSONObject dimInfo) {
                        /*object.setTrademarkName(dimInfo.getString("tm_name".toUpperCase()));*/
                        object.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean object) {
                        return object.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withTrade_mark.print("withTrade_mark");
        /*
        TradeSkuOrderBean(stt=2023-04-12 18:17:10, edt=2023-04-12 18:17:20, trademarkId=5, trademarkName=小米, category1Id=null, category1Name=null,
         category2Id=null, category2Name=null, category3Id=86, category3Name=null, skuId=20, skuName=小米电视E65X , spuId=6, spuName=小米电视 内置小爱 智能网络液晶平板教育电视,
         originalAmount=2899.0000, activityAmount=0.0, couponAmount=0.0, orderAmount=2899.0, ts=1682417842185)
         */
        //TODO 14.关联category3的维度数据
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3 = AsyncDataStream.unorderedWait(
                withTrade_mark,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void getJoin(TradeSkuOrderBean object, JSONObject dimInfo) {
                       /* object.setCategory3Name(dimInfo.getString("name".toUpperCase()));
                        object.setCategory2Id(dimInfo.getString("category2_id".toUpperCase()));*/
                        object.setCategory3Name(dimInfo.getString("NAME"));
                        object.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean object) {
                        return object.getCategory3Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withCategory3.print("withCategory3");
        //TODO 15.关联category2的维度数据
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2 = AsyncDataStream.unorderedWait(
                withCategory3,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public void getJoin(TradeSkuOrderBean object, JSONObject dimInfo) {
             /*           object.setCategory2Name(dimInfo.getString("name".toUpperCase()));
                        object.setCategory1Id(dimInfo.getString("category1_id".toUpperCase()));*/
                        object.setCategory2Name(dimInfo.getString("NAME"));
                        object.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean object) {
                        return object.getCategory2Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //TODO 16.关联category1的维度数据
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1 = AsyncDataStream.unorderedWait(
                withCategory2,
                new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public void getJoin(TradeSkuOrderBean object, JSONObject dimInfo) {
                        /*object.setCategory1Name(dimInfo.getString("name".toUpperCase()));*/
                        object.setCategory1Name(dimInfo.getString("NAME"));
                    }

                    @Override
                    public String getKey(TradeSkuOrderBean object) {
                        return object.getCategory1Id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withCategory1.print("完整关联的数据");
        //TODO 17.将数据写入到clickhouse中
        withCategory1.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_sku_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
