package app.ods;

import app.function.CusDeserialization;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.MyKafkaUtil;

public class ReadDataByCDC {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 2.通过Flink CDC 读取MySQL 业务数据的新增变化情况
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("175.178.154.194")
                .port(3306)
                .username("root")
                .password("zks123456")
                .databaseList("rtdw-flink")
                .deserializer(new CusDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 3.打印数据
        streamSource.print("ReadDataByCDC = ");
        String topic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(topic));

        // 4.启动任务
        env.execute("CDC Read MySQL Data Task");
    }
}
