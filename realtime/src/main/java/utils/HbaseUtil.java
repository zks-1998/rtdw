package utils;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.List;



public class HbaseUtil{

    public static JSONObject getHbaseInfo(Connection connection,String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 查询Hbase前查询redis
        Jedis jedis = RedisUtil.getJedis();

        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfo = jedis.get(redisKey);
        // 查到了维度信息
        if(dimInfo != null){
            // 重置过期时间
            jedis.expire(redisKey,24 * 60 * 60);
            jedis.close();
            // 返回维度信息
            return JSONObject.parseObject(dimInfo);
        }

        // 拼接Hbase查询语句
        String querySql = "select * from RTDW_HBASE"+ "." + tableName + " where id = '" + id + "'";

        List<JSONObject> queryList = JDBCUtils.queryList(connection, querySql, JSONObject.class, true);
        JSONObject dimData = queryList.get(0);
        // 查到Hbase数据后写入到redis  {"id":1,"orderAddress":"gz"}
        jedis.set(redisKey,dimData.toJSONString());
        // 设置过期时间
        jedis.expire(redisKey,24 * 60 * 60);

        jedis.close();
        return queryList.get(0);
    }

    public static void delRedisDimInfo(String tableName,String id){
        Jedis jedis = RedisUtil.getJedis();

        String redisKey = "DIM:" + tableName + ":" + id;

        jedis.del(redisKey);

        jedis.close();
    }

}
