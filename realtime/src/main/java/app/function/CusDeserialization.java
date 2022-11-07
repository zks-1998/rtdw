package app.function;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CusDeserialization implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 创建JSON对象
        JSONObject res = new JSONObject();
        // 获取topic 里面有数据库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        // 1.获取表名和数据库名
        String database = fields[1];
        String tableName = fields[2];

        // 2.获取value 里面会有before（修改数据会有）和after
        Struct value = (Struct)sourceRecord.value();
        // 获取before的结构
        Struct beforeStruct = value.getStruct("before");
        // 将before对象放到JSON对象
        JSONObject beforeJSON = new JSONObject();
        // 非修改数据是不会有before的，所以要判断
        if(beforeStruct != null){
            // 获取元数据
            Schema beforeSchema = beforeStruct.schema();
            // 通过元数据获取字段名
            List<Field> fieldList = beforeSchema.fields();
            for (Field field : fieldList) {
                // 获取字段值
                Object beforeValue = beforeStruct.get(field);
                // 放入JSON对象
                beforeJSON.put(field.name(),beforeValue);
            }
        }

        // 3.获取after
        Struct after = value.getStruct("after");
        JSONObject afterJSON = new JSONObject();
        if(after != null){
            Schema afterSchema = after.schema();
            List<Field> fieldList = afterSchema.fields();
            for (Field field : fieldList) {
                Object afterValue = after.get(field);
                afterJSON.put(field.name(),afterValue);
            }
        }

        // 4.获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){  // 类型crud的全称
            type = "insert";
        }

        // 5.数据字段写入JSON
        res.put("database",database);
        res.put("tableName",tableName);
        res.put("before",beforeJSON);
        res.put("after",afterJSON);
        res.put("type",type);

        // 6.发送数据至下游
        collector.collect(res.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
