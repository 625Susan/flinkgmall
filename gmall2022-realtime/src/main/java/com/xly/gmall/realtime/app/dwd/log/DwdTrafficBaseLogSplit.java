package com.xly.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.utils.DataFormatUtil;
import com.xly.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficBaseLogSplit {
    /*
   本类解决的问题：将交易域中的数据从kafka中读取出来，通过分流处理将不同类型的日志写到不同的Kafka主题中
   本次处理的功能需求：ETL对新老访客的数据进行标记修改和修复
                    将日志数据进行分类（启动日志 页面日志 曝光日志 错误日志 动作日志紫写到不同的kafka主题中去）
     */
    public static void main(String[] args) throws Exception {
        //TODO 1基本环境准备
        //指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);

        //TODO 2检查点相关设置
        //开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //设置检查点的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //设置job取消后检查点是否保留
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //设置两个检查点之间的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//        //设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
//        //设置操作HADOOP的用户
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3从kafka中读取数据
        //声明消费的主题和消费者组
        String topic = "topic_log";
        String groupId = "dwd_traffic_base_log_split_group";
        //创建消费者对象，使用flinkkafkaconsumer，内部维护了偏移量
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //消费数据，并将消费的数据封装成流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
       // kafkaStrDS.print("----------原始日志数据输出---------");
        /*
        {"common":{"ar":"2","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_61","os":"iOS 13.3.1","sid":"49c3ed07-1a0d-45e5-84cb-f8930f92d4e0","uid":"23","vc":"v2.1.134"},
        "displays":[{"item":"18","item_type":"sku_id","pos_id":5,"pos_seq":0},{"item":"14","item_type":"sku_id","pos_id":5,"pos_seq":1},{"item":"16","item_type":"sku_id","pos_id":5,"pos_seq":2},{"item":"1","item_type":"sku_id","pos_id":5,"pos_seq":3},{"item":"7","item_type":"sku_id","pos_id":5,"pos_seq":4},{"item":"29","item_type":"sku_id","pos_id":5,"pos_seq":5}],
        "page":{"during_time":6832,"last_page_id":"good_detail","page_id":"cart"},
        "ts":1681287251000}
         */
        //TODO 4对读取的数据进行简单的ETL以及类型转换  JsonStr ->JsonOb 将脏数据放到侧输出流中，输出到kafka主题
        //定义侧输出流标签(flink侧输出流中new的对象可能会有泛型差除，使用大括号相当于new了他的实现类或者子类)
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        //ETL以及转换操作
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            //在转换的过程中，如果没有发生异常，向下游传递
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            //如果发生了异常，说明从主题中读到的数据不是完整的json数据，将数据放到侧输出流中输出数据
                            context.output(dirtyTag,jsonStr);
                        }
                    }
                }
        );
        //将侧输出流中的脏数据放到kafka脏数据主题中去
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//        jsonObjDS.print("--------主流数据--------");
//        dirtyDS.print("---------脏数据打印------");
//        FlinkKafkaProducer<String> stringFlinkKafkaProducer = new FlinkKafkaProducer<String>();默认的是调用semantic的atleastonce，不能保证精准一次性，需要
        dirtyDS.addSink(MyKafkaUtil.getKafkaProducer("dirty_data"));
        //TODO 5将所有产生的数据清洗后按照mid进行分组处理，将用户之前登陆的信息维护到状态中，
         /*
        {"common":{"ar":"2","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_61","os":"iOS 13.3.1","sid":"49c3ed07-1a0d-45e5-84cb-f8930f92d4e0","uid":"23","vc":"v2.1.134"},
        "displays":[{"item":"18","item_type":"sku_id","pos_id":5,"pos_seq":0},{"item":"14","item_type":"sku_id","pos_id":5,"pos_seq":1},{"item":"16","item_type":"sku_id","pos_id":5,"pos_seq":2},{"item":"1","item_type":"sku_id","pos_id":5,"pos_seq":3},{"item":"7","item_type":"sku_id","pos_id":5,"pos_seq":4},{"item":"29","item_type":"sku_id","pos_id":5,"pos_seq":5}],
        "page":{"during_time":6832,"last_page_id":"good_detail","page_id":"cart"},
        "ts":1681287251000}
         */
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 6使用flink的状态编程，对新老访客的标记进行修复，如果is_new 为0同时状态中也没有之前访问的数据，说明是新访客，将今天访问的日期存放到状态中
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                //数据输入的是jsonobj，而传出到Kafka主题的时候也是使用jsonobj
                new RichMapFunction<JSONObject, JSONObject>() {
                    //声明状态-》先声明一个值状态
                    private ValueState<String> lastVisitDateState ;
                    //给状态初始化，在open方法中，是开始的时候开启状态，直接在属性中赋值的话，不能使状态在第一时间创建出来
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);  //通过状态名称来分辨是不是同一个状态
                        this.lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取is_new的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //获取上次设备访问的日期（维护在状态中）
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前设备的本次访问时间，并将它进行格式化
                        Long ts = jsonObj.getLong("ts");
                        //封装一个dataformat的工具类，线程更安全
                        String curVisitDate = DataFormatUtil.toDate(ts);
                        if ("1".equals(isNew)){
                            //再判断状态中是否有这个mid信息，如果状态中是空，就将当前访问的日期更新到状态中
                            if (StringUtils.isEmpty(lastVisitDate)){
                                lastVisitDateState.update(curVisitDate);
                            }else {
                            //判断如果状态中有数据，而且记录的日期不是当前日期，说明是老用户，将is_new的值变成0
                                if (!lastVisitDate.equals(curVisitDate)){
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new",isNew);
                                }
                            }
                        }else {
                            //如果is_new的值为0，并且状态中没有维护这个日期状态，说明访问的是老用户，但本次是他首次进入程序，可以将今天之前的访问数据维护到状态中
                            String yesterDay = DataFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
                            lastVisitDateState.update(yesterDay);
                        }
                        return jsonObj;
                    }
                }
        );
       // fixedDS.print("---------------将is_new的标记修复--------------");
        //TODO 7使用flink的侧输出流实现分流操作
        //定义侧输出流标（将不同的数据类型输出到不同的侧输出流中）
        /*  日志之间的关系
                 错误日志
                     启动日志
                     页面日志
                         曝光日志
                         动作日志
         */
        OutputTag<String> errTag=new OutputTag<String>("errTag"){};
        OutputTag<String> startTag=new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag=new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag=new OutputTag<String>("actionTag"){};
        //将不同的日志放到不同的侧输出流中
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        //错误日志处理
                        JSONObject errJsonObj = jsonObject.getJSONObject("err");  //err是基本所有的类都可能存在的数据
                        if (errJsonObj != null){
                            //如果不为空，说明它是错误日志数据，将错误日志写入到错误输出流errTag中
                            context.output(errTag,jsonObject.toJSONString());
                            jsonObject.remove("err");
                        }
                        //判断启动和页面数据
                        JSONObject startJsonObj = jsonObject.getJSONObject("start");
                        if (startJsonObj != null){
                            //是启动日志的话，将启动日志中的数据封装到侧输出流中进行输出
                            context.output(startTag,jsonObject.toJSONString());
                        }else {
                            //如果是页面日志，判断是曝光日志还是动作日志
                            //页面日志一定要有common ts 和page字段
                            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");
                            //判断页面中是否存在曝光信息数据，曝光信息是个数据json对象
                            JSONArray displayArr = jsonObject.getJSONArray("displays");
                            if(displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    //遍历数组得到曝光的数据
                                    JSONObject displayJson = displayArr.getJSONObject(i);
                                    //创建一个json对象用于接收每条曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJson);
                                    newDisplayJsonObj.put("ts", ts);
                                    //将曝光信息写到曝光侧输出流
                                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObject.remove("displays");
                            }
                            // //判断页面是否存在着动作数据
                            JSONArray  actionArr = jsonObject.getJSONArray("actions");
                            if(actionArr != null && actionArr.size() > 0){
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common",commonJsonObj);
                                    newActionJsonObj.put("page",pageJsonObj);
                                    newActionJsonObj.put("action",actionJsonObj);
                                    //将动作信息写到动作侧输出流
                                    context.output(actionTag,newActionJsonObj.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                }
        );
        DataStream<String> errDS = pageDS.getSideOutput(errTag);
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print(">>>>>");
        errDS.print("###");
        startDS.print("@@@");
        displayDS.print("$$$");
        actionDS.print("^^^^");

        //TODO 8将不同的数据写入到不同的kafka主题中
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_page_log"));
        errDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_err_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_display_log"));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_action_log"));
        env.execute();
    }
}
