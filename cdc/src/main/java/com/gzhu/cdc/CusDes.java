package com.gzhu.cdc;

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

public class CusDes implements DebeziumDeserializationSchema<String> {
    // 仅测试git是否可用
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        // 创建JSON对象
        JSONObject res = new JSONObject();
        // 获取topic 里面有数据库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        // 1.获取表名和数据库名
        String database = fields[1];
        String tableName = fields[2];

        // 2.获取before对象  修改的数据会有before，也就是以前的数据
        Struct value = (Struct)sourceRecord.value();

        Struct before = value.getStruct("before");
        // 将before对象放到JSON对象
        JSONObject beforeJSON = new JSONObject();
        // 非修改数据是不会有before的 所以要判断
        if(before != null){
            Schema beforeSchema = before.schema();
            List<Field> fieldList = beforeSchema.fields();
            for (Field field : fieldList) {
                Object beforeValue = before.get(field);
                beforeJSON.put(field.name(),beforeValue);
            }
        }

        // 3.获取after
        Struct after = value.getStruct("after");
        // 将before对象放到JSON对象
        JSONObject afterJSON = new JSONObject();
        // 非修改数据是不会有before的 所以要判断
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
        if("create".equals(type)){
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
