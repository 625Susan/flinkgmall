package com.xly.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.common.GmallConfig;
import com.xly.gmall.realtime.utils.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collection;
import java.util.Set;

//将读取出来的属于维度表中的数据写入到phenix中
//该类主要继承sinkfunction，拼接写入数据
public class DimSinkFunction implements SinkFunction<JSONObject> {
    //重写invoke方法
/*
写入数据的语句：upsert into 库名.表名 (字段，字段) values('字段值','字段值')
 */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //传递到这里的数据{"tm_name":"atguigu","sink_table":"dim_base_trademark","id":14}
        //获取表名
        String sinkTable = value.getString("sink_table");
        //将除了表名之外的其他数据封装成一个json对象，直接通过其中的key值提取他的value
        value.remove("sink_table");
        //将移除了的数据的key值和value值取出来
        //json底层就是一个map，其实质就是去map中获取key(keyset)值和value(map.value)值
        Set<String> key = value.keySet();
        Collection<Object> values = value.values();
        //拼接语句
        String upsertSql = "upsert into "+ GmallConfig.PHOENIX_SCHEMA +"."+sinkTable+" (" +
                StringUtils.join(key,",") +
                ")" + " values" +"( '"+StringUtils.join(values,"','") + "')";  //因为在phenix中的数据我们定义的都是varchar类型，所以数字要用单引号拼接
        System.out.println("向phenix表中插入的语句为"+upsertSql);
        //执行语句
        PhoenixUtil.excuteSql(upsertSql);
    }
}
