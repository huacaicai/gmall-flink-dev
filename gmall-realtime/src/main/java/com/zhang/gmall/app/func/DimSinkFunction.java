package com.zhang.gmall.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhang.gmall.common.GmallConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @title:
 * @author: zhang
 * @date: 2022/3/10 16:59
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //Phoenix默认关闭自动提交事务，需要手动开启
        connection.setAutoCommit(true);
        //设置连接schema
        connection.setSchema(GmallConfig.HBASE_SCHEMA);
    }

    @Override
    public void invoke(JSONObject value, Context context){
        PreparedStatement statement = null;
        try {
            //获取维度表
            String sinkTable = value.getString("sinkTable");
            //获取维度数据
            JSONObject data = value.getJSONObject("data");
            //拼接sql
            String upsertSQL = genUpsertSql(data,sinkTable);

            statement = connection.prepareStatement(upsertSQL);
            statement.executeUpdate();
            //注意：再向Phoenix表中，需要手动提交事务,或者开启自动提交
            //事务只针对DML，DQL、DDL是没有事务的
            //connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("向Phoenix中插入数据发生异常");
        } finally {
            if (statement!=null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //upsert into student(id,name,sex) values('1001','zhangsan','beijing');
    private String genUpsertSql(JSONObject data, String sinkTable) {
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();
        StringBuilder upsertSQL = new StringBuilder("upsert into ")
                .append(sinkTable)
                .append("(")
                .append(StringUtils.join(keys,","))
                .append(")")
                .append(" values('")
                .append(StringUtils.join(values,"','"))
                .append("')");
        return upsertSQL.toString();
    }


    @Override
    public void close() throws Exception {
        connection.close();
    }

}
