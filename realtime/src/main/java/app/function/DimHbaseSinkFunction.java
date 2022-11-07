package app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import utils.HbaseUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

public class DimHbaseSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    // open 并行子任务都会执行一次open、close 常用来执行初始化操作
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
            Properties props = new Properties();
            props.put("phoenix.schema.isNamespaceMappingEnabled","true");
            connection = DriverManager.getConnection(url,props);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // 自定义输出 value 数据格式  sinkTable(hbase),database(mysql),tableName(mysql),before,after(数据),type(mysql)
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取要插入的数据 {id:1,name:kun}
        JSONObject after = value.getJSONObject("after");
        // 获取key 作为hbase的列
        Set<String> keySet = after.keySet();
        // 获取values 赋值
        Collection<Object> values = after.values();

        PreparedStatement ps = null;

        // 获取SQL语句
        String sql = "upsert into RTDW_HBASE" +"."+ value.getString("sinkTable")+
                "("+ StringUtils.join(keySet,",")+")" +
                "values('" + StringUtils.join(values,"','")
                + "')";

        System.out.println(sql);

        try{
            connection.setAutoCommit(false);
            // 预编译SQL
            ps = connection.prepareStatement(sql);

            //如果Hbase是更新操作，redis如果有数据就要删除
            if("update".equals(value.getString("type"))){
                HbaseUtil.delRedisDimInfo(value.getString("sinkTable").toUpperCase(),after.getString("id"));
            }
            // 执行插入修改操作
            ps.executeUpdate();
            connection.commit();
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
