package com.gzhu.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.通过Flink CDC 构建MySQL SOURCE
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("175.178.154.194")
                .port(3306)
                .username("root")
                .password("zks123456")
                .databaseList("rtdw-flink")
                .tableList("rtdw-flink.base_sale_attr")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3.打印数据
        streamSource.print();

        // 4.启动任务
        env.execute("CDC TASK ");

    }
}
