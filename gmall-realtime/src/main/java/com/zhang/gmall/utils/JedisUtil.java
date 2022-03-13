package com.zhang.gmall.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @title: redis工具类
 * @author: zhang
 * @date: 2022/3/10 21:04
 */
public class JedisUtil {

    private static JedisPool pool;

    private static void initJedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(1000);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000L);
        jedisPoolConfig.setMaxTotal(5);
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setTestOnBorrow(true);
        pool = new JedisPool(jedisPoolConfig,"hadoop103",6379);
        System.out.println("------开辟连接池------");
    }

    public static Jedis getJedis() {
        if (pool == null) {
            synchronized (Jedis.class) {
                if (pool == null) {
                   initJedisPool();
                }
            }
        }
        return pool.getResource();
    }


    public static void main(String[] args) {
        //测试
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
    }
}
