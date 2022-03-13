package com.zhang.gmall.common;

public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME_2022";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    //Mysql驱动
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    //Mysql连接参数
    public static final String MYSQL_SERVER = "jdbc:mysql://hadoop102:3306";

    //Phoenix库名
    public static final String MYSQL_SCHEMA = "gmall_realtime";



}