package com.xly.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.TableProcess;
import com.xly.gmall.realtime.common.GmallConfig;
import com.xly.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/*
由于全部将代码写在DimApp的话，代码量较大，且臃肿，建议另外写一个类来实现两个流之间的结合
在类中对dim关联的主流和广播流进行处理,过滤出维度数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject>{
    private Map<String,TableProcess> configmap = new HashMap<>();
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    //因为有两条流，我们无法保证两条流哪条先到，所以使用open方法将配置流中的表放到open中，避免主流先到了之后无法匹配到配置流中的数据

    @Override
    public void open(Configuration parameters) throws Exception {
        // 读取gmall_config中的数据
        // 注册表驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_config?" +
                "user=root&password=000000&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        //创建sql语句
        String sql = " select * from gmall_config.table_process where sink_type = 'dim'";
        //获取执行平台
        PreparedStatement pre = conn.prepareStatement(sql);
        //执行sql语句
        ResultSet resultSet = pre.executeQuery();
        //获取元数据信息(为什么要获取元数据信息？)可以通过元数据查询出当前数据的字段个数，和数据中有哪些字段信息，便于下一步对集合数据进行遍历
        ResultSetMetaData metaData = resultSet.getMetaData();
        //处理结果集，遍历查询出来的结果集进行判
        //ObjectRelationMap 表中的字段和对象属性对应，表中的字段名 字段值和对象的属性名和属性值相对应
        while (resultSet.next()){
            //jdbc中的索引都是从1开始的，Java中的索引是从0开始的
            JSONObject jsonO = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                //alibabaJson将数据的_转换为驼峰数据
                String columnName = metaData.getColumnName(i);
                //获取数据中的字段，并将字段的key和value维护到json中
                Object columnValue = resultSet.getObject(i);
                jsonO.put(columnName,columnValue);
            }
            //将json对象转换为java对象数据
            TableProcess tableProcess = jsonO.toJavaObject(TableProcess.class);
            //封装一个configmap的集合来存储key为表名，value为tableprocess对象的属性值
            configmap.put(tableProcess.getSourceTable(),tableProcess);
        }
        //关闭连接
        resultSet.close();
        pre.close();
        conn.close();
    }

    //处理主流数据
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        /*{"database":"gmall","xid":113462,"data":{"tm_name":"DIOR","create_time":"2021-12-14 00:00:00","logo_url":"/static/default.jpg","id":12},
        "old":{},"commit":true,"type":"update","table":"base_trademark","ts":1681276128}
        */
        //处理主流业务数据，将业务数据中的表名与广播流中的表名进行比对，如果业务中的数据能和广播流中的数据匹配上，就是维度数据，往下传递
        System.out.println(jsonObject);
        //获取json中的表名
        String tableName = jsonObject.getString("table");
        //获取广播状态中的表名
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = null;
        //如果主流中的数据能和广播流中的数据匹配上，获取data中的数据封装成json对象进行传递
        if ((tableProcess = broadcastState.get(tableName)) != null || (tableProcess = configmap.get(tableName))!=null){
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            //在向下游传递时，将不需要的数据进行传递，同时将传递的目的地（表名）放进去
            //"data":{"tm_name":"DIOR","create_time":"2021-12-14 00:00:00","logo_url":"/static/default.jpg","id":12}
            String columns = tableProcess.getSinkColumns();
            filterColum(dataJsonObj,columns);
            //向下游输出数据，将表名装载进去
            String sinkTable = tableProcess.getSinkTable();
            dataJsonObj.put("sink_table",sinkTable);
            collector.collect(dataJsonObj);
        }
    }

    private void filterColum(JSONObject dataJsonObj, String columns) {
        //将字段中的值全部都取出来，进行遍历将不需要的数据删除掉（广播流中的数据）
        String[] columnArr = columns.split(",");
        //将数据转换成集合?因为集合才能调用contains方法，判断集合中包含不包含columns
        List<String> fieldList = Arrays.asList(columnArr);
        //遍历json对象中的数据 data中的对象数据（主流中的数据）
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        //将entryset中（主流的数据）和fieldlist（广播流数据）进行匹配，将匹配不上的数据remove掉
        //remove底层是用的迭代器进行遍历数据，增强for不支持对set集合进行删除操作
        entrySet.removeIf(entry->!fieldList.contains(entry.getKey())
        );
    }

    //广播流中flinkcdc中的数据来源格式
   /*
    "op":"r": {"before":null,"after":{"source_table":"activity_rule","source_type":"ALL","sink_table":"dim_activity_rule","sink_type":"dim","sink_columns":"id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681285918409,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1681285918409,"transaction":null}
    "op":"c"：{"before":null,"after":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681286000000,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11302896,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1681285999620,"transaction":null}
    "op":"u"：{"before":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c","sink_pk":"a","sink_extend":null},"after":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c,d","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681286086000,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11303262,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1681286085644,"transaction":null}
    "op":"d"：{"before":{"source_table":"aa","source_type":"ALL","sink_table":"dim_aa","sink_type":"dim","sink_columns":"a,bc,c,d","sink_pk":"a","sink_extend":null},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1681286126000,"snapshot":"false","db":"gmall1031_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11303656,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1681286125806,"transaction":null}*/
    @Override
    public void processBroadcastElement(String jsonstr, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //处理广播流数据，广播流监控的数据是gmall_config中的数据
        //为了操作方便，将jsonstr转换为json对象
        JSONObject jsonObj = JSON.parseObject(jsonstr);
        String op = jsonObj.getString("op");
        //两种情况，删除操作和除了删除之外的其他操作(map操作，修改或者增加的数据会自动去重)
        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        if ("d".equals(op)){
            //如果是删除操作，在广播状态中删除对应json对象的配置信息,将beforeJSON中的数据封装成一个对象
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            //判断是否删除的是维度属性
            String sinkType = before.getSinkType();
            if ("dim".equals(sinkType)){
                //将对应的数据从广播状态中删除  -->根据表名来删除广播状态中的数据
                String sourceTable = before.getSourceTable();
                broadcastState.remove(sourceTable);
                configmap.remove(sourceTable);
            }
        }else {
            //如果不是删除，将读取的配置放到广播状态中 修改后的数据放到after中
            TableProcess after = jsonObj.getObject("after", TableProcess.class);
            String sinkType = after.getSinkType();
            if ("dim".equals(sinkType)){
                //在将数据放到广播流之前，提前在phenix中创建表
                //获取sink表名
                String sinkTable = after.getSinkTable();
                //获取字段数据
                String sinkColumns = after.getSinkColumns();
                //获取主键
                String pk = after.getSinkPk();
                //获取扩展的数据
                String ext = after.getSinkExtend();
                checkTable(sinkTable,sinkColumns,pk,ext);
                //将对应的维度数据放到广播状态中
                String sourceTable = after.getSourceTable();
                broadcastState.put(sourceTable,after); //广播状态底层维护的是一个map集合，在类型为i时放入的key值数据类型是什么，删除等操作也是这个key值类型
                configmap.put(sourceTable,after);  //在预加载数据中也加载
            }
        }
    }

    private void checkTable(String sinkTable, String sinkColumns, String pk,String ext) {
        //原本的数据中有的数据没有主键，给数据的id作为默认主键
        if (pk == null){
            pk = "id";
        }
        if (ext == null){
            ext="";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + sinkTable + " (");
        //将读取到的colums中的数据进行切割
        String[] columnArr = sinkColumns.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String column =columnArr[i];
            //判断这个字段是否为主键
            if (pk.equals(column)){
                createSql.append(column + " varchar primary key");
            }else {
                createSql.append(column + " varchar");
            }
            //拼接逗号
            if (i < columnArr.length-1){
                createSql.append(",");
            }
        }
        createSql.append(" )" + ext);
        System.out.println("在phenix中的建表语句为："+ createSql);
        PhoenixUtil.excuteSql(createSql.toString());
    }
}
