package com.xly.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;
    //定义一个属性，将配置表中的数据加载到这个map中来
    private Map<String,TableProcess> configmap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    //由于广播数据中的数据一定要比主流中的数据先到，否则主流中的数据没办法和其他数据进行关联
    //将广播中装载的表数据在程序启动时先加载到内存中

    @Override
    public void open(Configuration parameters) throws Exception {
        //JDBC连接
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_config?" +
                "user=root&password=000000&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        String sql = "select * from gmall_config.table_process where sink_type ='dwd'";
        PreparedStatement pre = conn.prepareStatement(sql);
        ResultSet rs = pre.executeQuery();
        //获取元数据信息，里面包含有这个结果中共有多少列，以及列名，表名，数据类型
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            //定义一个json对象，用于封装查询出来的每一条记录
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = rs.getString(i);
                jsonObj.put(columnName,columnValue);
            }
            //将json对象转换为实体类对象
            TableProcess tableProcess = JSON.toJavaObject(jsonObj, TableProcess.class);
            //将转换好的数据放到map集合中保存数据
            String sourceTable = tableProcess.getSourceTable();
            String sourceType = tableProcess.getSourceType();
            String key = sourceTable + ":" + sourceType;
            configmap.put(key,tableProcess);
        }
        rs.close();
        pre.close();
        conn.close();
    }

    //对主流数据进行处理，提取流中的表名，与广播流中的表名进行关联，如果关联上的话，就往下游传递
    /*{
    "database":"gmall",
     "xid":113462,
     "data":{"tm_name":"DIOR","create_time":"2021-12-14 00:00:00","logo_url":"/static/default.jpg","id":12},
     "old":{},
     "commit":true,
     "type":"update",
     "table":"base_trademark",
     "ts":1681276128
     }*/
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        String key = table + ":" +type;
        //通过上下文对象获取广播状态中的数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess =null;
        //如果主流中的数据和广播流中的数据匹配上了
        if ((tableProcess =  broadcastState.get(key) )!=null || (tableProcess = configmap.get(key))!=null){
            JSONObject dataJson = jsonObj.getJSONObject("data");
            //过滤掉不需要往下传递的属性（因为主流中有的数据在广播状态数据中可能没有，并且不会使用，占用不必要的空间，所以将其进行筛选过滤掉）
            String sinkColumns = tableProcess.getSinkColumns();
            FilterColum(dataJson,sinkColumns);
            //补充表名和ts字段，补充下游传递的目的地和时间戳
            String sinkTable = tableProcess.getSinkTable();
            dataJson.put("sink_table",sinkTable);
            dataJson.put("ts",jsonObj.getLong("ts"));
            //过滤完了数据之后，将数据向下游传递
            out.collect(dataJson);
        }
    }

    private void FilterColum(JSONObject dataJson, String sinkColumns) {
        String[] sinkColumn = sinkColumns.split(",");
        List<String> columns = Arrays.asList(sinkColumn);
        Set<Map.Entry<String, Object>> entrySet = dataJson.entrySet();
        entrySet.removeIf(entry->!columns.contains(entry.getKey()));
    }

    /*
        "op":"r": {"before":null,"after":{"source_table":"activity_rule","source_type":"ALL","sink_table":"dim_activity_rule","sink_type":"dim","sink_columns":"id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681285918409,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1681285918409,"transaction":null}
        "op":"c"：{"before":null,"after":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681286000000,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11302896,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1681285999620,"transaction":null}
        "op":"u"：{"before":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c","sink_pk":"a","sink_extend":null},"after":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c,d","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681286086000,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11303262,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1681286085644,"transaction":null}
        "op":"d"：{"before":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c,d","sink_pk":"a","sink_extend":null},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681286126000,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11303656,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1681286125806,"transaction":null}*/
    //对广播流数据进行处理，将监控到的gmall_config中的table_process表中的变化放到广播流中
    @Override
    public void processBroadcastElement(String JsonStr, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //将json字符串转换成Json对象
        JSONObject jsonObj = JSON.parseObject(JsonStr);
        //获取json对象中的op对应的类型
        String op = jsonObj.getString("op");
        //获取上下文的broadcast中的数据
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //做判断，如果是d，获取before字段中的数据，在状态数据中删除这个数据
        if ("d".equals(op)){
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            //因为只对dwd层的数据进行动态建表，所以没必要将dim层的数据也加载到数据中，需要对这个数据进行过滤掉
            String sinkType = before.getSinkType();
            if ("dwd".equals(sinkType)){
                //source_table 和 source_type作为key,获取before中的数据，其他的tableprocess的数据存储为value
                String sourceTable = before.getSourceTable();
                String sourceType = before.getSourceType();
                String key = sourceTable + ":" + sourceType;
                broadcastState.remove(key);
                configmap.remove(key);
            }
        }else {
            //如果是除了这个字母之外的其他操作，就将数据put到广播状态中去，获取after字段的数据，将其封装put到广播状态中去
            TableProcess after = jsonObj.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dwd".equals(sinkType)){
                //source_table 和 source_type作为key,获取before中的数据，其他的tableprocess的数据存储为value
                String sourceTable = after.getSourceTable();
                String sourceType = after.getSourceType();
                String key = sourceTable + ":" + sourceType;
                broadcastState.put(key,after);
                configmap.put(key,after);
            }
        }
    }
}
