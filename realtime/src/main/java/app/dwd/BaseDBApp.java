package app.dwd;

import app.function.CusBroadcastProcessFunction;
import app.function.DimHbaseSinkFunction;
import app.ods.ReadConfByCDC;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.MyKafkaUtil;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.消费kafka的主题ods_base_db 创建主流 主流是所有表的变化情况 after中是最新的数据
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("ods_base_db", "base_db_app_0807"));

        // 3.每行数据转换成JSONObject 过滤掉主流的delete数据
        SingleOutputStreamOperator<JSONObject> dataDS = kafkaDS.map(JSON::parseObject).filter(line -> {
            String type = line.getString("type");
            return !Objects.equals(type, "delete");
        });

        // 4.读取配置表 after有用的数据
        DebeziumSourceFunction<String> sourceFunction = ReadConfByCDC.getConfCDC();
        SingleOutputStreamOperator<JSONObject> confDS = env.addSource(sourceFunction).map(JSON::parseObject);

        // 创建广播流（广播流要有map-state描述器） 根据Key找到Value String类型
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class,TableProcess.class);
        BroadcastStream<JSONObject> broadcastConfStream = confDS.broadcast(mapStateDescriptor);

        // 5.连接两条流
        BroadcastConnectedStream<JSONObject,JSONObject> connectedStream = dataDS.connect(broadcastConfStream);

        // 6.处理数据分流 事实数据 => kafka 主流   维度数据DIM => Hbase 侧输出流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag"){};
        // 主流数据 事实数据
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new CusBroadcastProcessFunction(hbaseTag));
        // 侧输出流数据 维度数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        kafka.print("kafka =>");
        hbase.print("hbase =>");


        // 事实数据写入kafka
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("after").getBytes(StandardCharsets.UTF_8));
            }
        }));

        // 维度数据写入到Hbase
        hbase.addSink(new DimHbaseSinkFunction());

        // 7.启动任务
        env.execute();
    }
}
