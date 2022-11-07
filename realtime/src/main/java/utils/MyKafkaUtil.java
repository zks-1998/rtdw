package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Nullable;
import java.util.Properties;

public class  MyKafkaUtil {

    private static String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(brokers,topic,
                new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        return new FlinkKafkaProducer<T>("default_topic",
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.NONE);
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        // 没有offset，从新提交的数据开始消费 默认5s自动提交一次offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

       return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }
}
