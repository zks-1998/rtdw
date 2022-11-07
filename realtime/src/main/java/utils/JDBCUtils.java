package utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtils {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean toCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<T> res = new ArrayList<>();
        // 预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        // 从结果中获取列的个数
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 解析 resultSet.next()是一行数据
        while(resultSet.next()){
            // 创建泛型对象
            T instance = clz.newInstance();

            for(int i = 1;i < columnCount + 1;i++){
                // 获取列名
                String columnName = metaData.getColumnName(i);
                // 判断列名是否进行驼峰转换 guava依赖
                if(toCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }
                // 获取列值
                Object value = resultSet.getObject(i);

                // 给泛型对象赋值
                BeanUtils.setProperty(instance,columnName,value);
            }
            res.add(instance);
        }
        return res;
    }
}
