package com.xly.gmall.realtime.common;

public class GmallConfig {
//设置phenix的驱动参数等
// Phoenix库名
public static final String PHOENIX_SCHEMA = "GMALL_REALTIME";
    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix连接参数，在zk中的地址
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";
}
