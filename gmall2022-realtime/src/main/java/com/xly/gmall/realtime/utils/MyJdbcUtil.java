package com.xly.gmall.realtime.utils;

public class MyJdbcUtil {
    public static String getMysqlTable(){
        return "CREATE TABLE base_dic (\n" +
                "  dic_code STRING,\n" +
                "  dic_name STRING,\n" +
                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + getJdbc("base_dic");
    }
    public static String getJdbc(String tablename){
        return "WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                        "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        "   'username' = 'root',\n" +
                        "   'password' = '000000',\n" +
                        "   'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                        "   'lookup.cache.max-rows' = '200',\n" +
                        "   'lookup.cache.ttl' = '1 hour',\n" +
                        "   'table-name' = '"+tablename+"'\n" +
                        ")";
    }
}
