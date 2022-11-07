package utils;

import common.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getSink(String sql){
        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                Field[] fields = t.getClass().getDeclaredFields();
                int offset = 0;
                for (int i = 0; i < fields.length; i++) {
                    // 获取字段
                    Field field = fields[i];
                    // 设置私有属性可访问
                    field.setAccessible(true);

                    TransientSink annotation = field.getAnnotation(TransientSink.class);
                    if(annotation != null){
                        offset++;
                        continue;
                    }
                    // 获取字段值
                    Object value = null;
                    try {
                        value = field.get(t);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }

                    preparedStatement.setObject(i + 1 - offset,value);
                }
            }
        },new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://hadoop102:8123/default")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build());
    }
}
