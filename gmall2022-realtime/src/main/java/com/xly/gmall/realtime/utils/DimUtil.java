package com.xly.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

//查询维度的工具类
public class DimUtil {
    public static JSONObject getDimInfo(String tableName,String id){
        return getDimInfo(tableName,Tuple2.of("id",id));
    }
    //没有旁路缓存优化的查询方法
    public static JSONObject getDimInfo(String tableName, Tuple2<String,String>...colunmNameAndValues){
        //拼接从Redis中查询数据的key:dim+ 表名 + id
        StringBuilder redisKey = new StringBuilder("dim:"+tableName.toLowerCase()+":");
        //拼接sql查询语句
        StringBuilder selectSql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
        for (int i = 0; i < colunmNameAndValues.length; i++) {
            Tuple2<String, String> colunmNameAndValue = colunmNameAndValues[i];
            String columName = colunmNameAndValue.f0;
            String columValue = colunmNameAndValue.f1;
            redisKey.append(columValue);
            selectSql.append(columName + " = '" + columValue+"'");
            if (i < colunmNameAndValues.length-1){
                selectSql.append(" and ");
                redisKey.append("_");
            }
        }
        Jedis jedis =null;
        String dimStr = null;
        JSONObject dimJsonObj = null;
        //从redis缓存中读取数据
        try {
            jedis = RedisUtil.getJedis();
            dimStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (StringUtils.isNotEmpty(dimStr)){
            //如果缓存中找到了对应的维度，直接将缓存中的维度作为返回值返回
            dimJsonObj = JSON.parseObject(dimStr);
        }else {
            //如果在缓存中没有找到数据，发送请求到phoenix表中去查询维度
            System.out.println("在phoenix表中查询维度数据的sql:"+selectSql);
            //将从phoenix中的数据封装成一个list集合查询出来
            List<JSONObject> resList = PhoenixUtil.queryList(selectSql.toString(), JSONObject.class);
            if (resList != null && resList.size()>0 ){
                //根据维度的主键查询到了相应的维度数据，但是只会有一条数据
                dimJsonObj = resList.get(0);
                //将查询的结果缓存起来，方便下次查询使用
                if (jedis!=null){
                    jedis.setex(redisKey.toString(),3600*24,dimJsonObj.toJSONString());
                }
            }else {
                System.out.println("在phoenix表中没有查询到对应的维度数据");
            }
        }
        //释放资源
        if (jedis !=null){
            System.out.println("关闭jedis客户端");
            jedis.close();
        }
        return dimJsonObj;
    }
    //从redis中删除缓存的数据
    public  static void delCached(String tableName,String id){
        String redisKey = "dim:"+tableName.toLowerCase()+":"+id;
        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (jedis != null){
                System.out.println("删除维度数据后，关闭jedis客户端");
                jedis.close();
            }
        }
    }
    public static void main(String[] args) {
        System.out.println(getDimInfo("DIM_BASE_PROVINCE","1"));
    }
}
