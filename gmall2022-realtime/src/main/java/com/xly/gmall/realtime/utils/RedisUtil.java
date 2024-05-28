package com.xly.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

//操作redis的工具类
public class RedisUtil {
    private static JedisPool jedispool;
    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedispool = new JedisPool(poolConfig,"hadoop102",6379,10000);
    }
    public static Jedis getJedis(){
        System.out.println("~~~创建jedis客户端~~~");
        Jedis jedis = jedispool.getResource();
        return jedis;
    }
}
