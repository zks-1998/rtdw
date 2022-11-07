package app.function;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import utils.HbaseUtil;
import utils.ThreadPoolUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> {
    private Connection connection;
    ThreadPoolExecutor threadPoolExecutor;

    // 查询Hbase哪张表
    String tableName;
    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化Hbase连接
        try {
            String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
            Properties props = new Properties();
            props.put("phoenix.schema.isNamespaceMappingEnabled","true");
            connection = DriverManager.getConnection(url,props);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // 初始化连接池
        threadPoolExecutor = ThreadPoolUtil.getPool();
    }

    public abstract String getKey(T input);

    public abstract void join(T input,JSONObject hbaseInfo);

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // 查询id，id在input里，写一个抽象方法，让实现类来实现，获取id
                String id = getKey(input);
                // 查询维度信息
                JSONObject hbaseInfo = HbaseUtil.getHbaseInfo(connection, tableName, id);
                // 补充维度信息，无法给T赋值，写一个抽象方法，让实现类来确定类型
                if(hbaseInfo != null){
                    join(input,hbaseInfo);
                }
                // 收集结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("数据请求超时 => "+input);
    }
}
