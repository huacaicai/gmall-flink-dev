package com.zhang.gmall.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @title: Phoenix维度查询工具类
 * @author: zhang
 * @date: 2022/3/10 20:43
 */
public class DimUtil {

    public static JSONObject getDimInfo(String tableName,String id){
        return getDimInfo(tableName,Tuple2.of("id",id));
    }

    //根据维度主键，获取维度对象 (引入缓存)
    //select * from GMALL_REALTIME_2022.DIM_BASE_TRADEMARK where id ='14';
    //redis Key:  DIM:SKU_INFO:ID_SEX
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnAndValues) {
        //拼接redis key
        StringBuilder redisKey = new StringBuilder("DIM:" + tableName + ":");
        //拼接SQL
        StringBuilder selectSQL = new StringBuilder("select * from " + tableName + " where ");
        for (int i = 0; i < columnAndValues.length; i++) {
            String columnName = columnAndValues[i].f0;
            String columnValue = columnAndValues[i].f1;
            selectSQL.append(columnName + " = '" + columnValue + "' ");
            redisKey.append(columnValue);
            if (i < columnAndValues.length - 1) {
                selectSQL.append("and ");
                redisKey.append("_");
            }
        }

        //TODO 1.先去查询redis查询
        Jedis jedis = null;
        String dimInfo = null;
        JSONObject dimJson = null;
        try {
            System.out.println("-------开启redis客户端-------");
            jedis = JedisUtil.getJedis();
            dimInfo = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("从redis中查询维度数据发生异常");
        }
        //TODO 2.如果命中缓存，直接返回
        if (dimInfo != null && dimInfo.length() > 0) {
            dimJson = JSON.parseObject(dimInfo);
            //重制过期时间
            jedis.expire(redisKey.toString(), 24 * 60 * 60);
        } else {
            //TODO 3.如果没有命中缓存，就去Phoenix查询,并把结果写入redis再返回
            List<JSONObject> jsonObjects = PhoenixUtil.queryList(selectSQL.toString(), JSONObject.class, false);
            if (jsonObjects != null && jsonObjects.size() > 0) {
                dimJson = jsonObjects.get(0);
                dimInfo = dimJson.toJSONString();
                if (jedis!=null){
                    jedis.setex(redisKey.toString(), 24 * 60 * 60, dimInfo);
                }
            } else {
                //从Phoenix表中没有查询到维度数据
                System.out.println("在维度表中没有该维度数据" + selectSQL);
            }
        }

        if (jedis != null) {
            System.out.println("-------关闭redis客户端-------");
            jedis.close();
        }

        return dimJson;
    }


    //根据维度主键，获取维度对象 (没有引入缓存)
    //select * from GMALL_REALTIME_2022.DIM_BASE_TRADEMARK where id ='14';
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... columnAndValues) {
        //拼接SQL
        StringBuilder selectSQL = new StringBuilder("select * from " + tableName + " where ");
        for (int i = 0; i < columnAndValues.length; i++) {
            String columnName = columnAndValues[i].f0;
            String columnValue = columnAndValues[i].f1;
            selectSQL.append(columnName + " = '" + columnValue + "' ");
            if (i < columnAndValues.length - 1) {
                selectSQL.append(" and ");
            }
        }

        System.out.println("从Phoenix查询的SQL:" + selectSQL);
        //从Phoenix表中查询数据，底层调用的还是PhoenixUtil
        List<JSONObject> jsonObjects = PhoenixUtil.queryList(selectSQL.toString(), JSONObject.class, false);
        JSONObject dimInfo = null;
        if (jsonObjects != null && jsonObjects.size() > 0) {
            dimInfo = jsonObjects.get(0);
        }

        return dimInfo;
    }


    public static void main(String[] args) {
        //System.out.println(getDimInfo("DIM_BASE_TRADEMARK", Tuple2.of("id", "13")));
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "13"));
        //deleteCached("DIM_BASE_TRADEMARK","12");
    }
}
