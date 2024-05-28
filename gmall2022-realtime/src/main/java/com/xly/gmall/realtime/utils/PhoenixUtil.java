package com.xly.gmall.realtime.utils;

import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    public static void excuteSql(String sql){
        //执行建表语句，获取druid的连接池进行操作，为什么不用jdbc是因为jdbc会频繁的创建和关闭连接，会很重
        Connection conn=null;
        PreparedStatement ps =null;
        try {
            conn = DruidDSUtil.getConnection();//通过自己添加的druid连接池去连接phoenix数据库的数据
            //获取操作库对象
            ps = conn.prepareStatement(sql);
            //执行sql语句
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
            //释放资源
        }finally {
            if (ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    //从phoenix数据库表中查询数据，因为是一个工具类查询，所以返回的数据类型不能确定是哪一个数据类型，所以使用泛型
    public static <T>List<T> queryList(String sql,Class<T>clz){
        ArrayList<T> resList = new ArrayList<>();
        //获取phoenix的连接对象
        Connection conn =null;
        PreparedStatement ps =null;
        ResultSet rs =null;
        try {
            conn = DruidDSUtil.getConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            //从phoenix表中查询数据
            // +-----+-------+------------+------------+-----------+-------------+
            // | ID  | NAME  | REGION_ID  | AREA_CODE  | ISO_CODE  | ISO_3166_2  |
            // +-----+-------+------------+------------+-----------+-------------+
            // | 1   | 北京    | 1          | 110000     | CN-11     | CN-BJ      |
            // | 10  | 福建    | 2          | 350000     | CN-35     | CN-FJ      |
            //获取元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            //处理结果集
            while (rs.next()){
                //定义一个对象，用于封装查询出来的每一条记录
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    //因为是jdbc，jdbc的索引都是从1开始的
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                resList.add(obj);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return resList;
    }
}
