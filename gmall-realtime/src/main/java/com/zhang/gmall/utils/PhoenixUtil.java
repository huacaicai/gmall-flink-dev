package com.zhang.gmall.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.zhang.gmall.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @title: Phoenix连接工具
 * @author: zhang
 * @date: 2022/3/10 16:28
 */
public class PhoenixUtil {
    private static Connection connection;

    private static void initConnection() throws Exception {
        //注册驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //获取连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //设置连接schema
        connection.setSchema(GmallConfig.HBASE_SCHEMA);
    }

    public static <T> List<T> queryList(String sql, Class<T> T, boolean underScoreToCamel) {
        //定义返回List
        ArrayList<T> arrayList = new ArrayList<>();
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            if (connection == null) {
                synchronized (PhoenixUtil.class) {
                    if (connection == null) {
                        initConnection();
                    }
                }
            }

            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery(sql);
            //封装结果集
            /*
            +-----+----------+
            | ID  | TM_NAME  |
            +-----+----------+
            | 12  | zyf      |
            +-----+----------+
             */
            //获取结果集元数据集｜ ID  | TM_NAME  |
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                T t = T.newInstance();
                for (int i = 1; i < metaData.getColumnCount() + 1; i++) {
                    String columnName = metaData.getColumnName(i);
                    //判断是否需要转换为驼峰命名 tm_name->tmName
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }
                    Object value = resultSet.getObject(i);
                    //封装对象
                    BeanUtils.setProperty(t, columnName, value);
                }
                arrayList.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Phoenix表数据查询失败～～～");
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return arrayList;
    }

    public static void main(String[] args) {

        String sql = "select * from GMALL_REALTIME_2022.DIM_BASE_TRADEMARK";
        for (JSONObject jsonObject : queryList(sql, JSONObject.class, true)) {
            System.out.println(jsonObject);
        }
    }

}
