package app.function;

import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class CusBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {
       MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class,TableProcess.class);
       OutputTag<JSONObject> tag;
       private Connection connection;

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

    public CusBroadcastProcessFunction(OutputTag<JSONObject> tag){
          this.tag = tag;
       }
         /*
            主流数据格式 {"database":"" ,"tableName":"", "before":"", "after":"", "type":""}
            1.读取广播状态
              状态key是表名 + 操作类型 tableName+type = (sourceTable + operateType) = 广播流的key
              广播流的key sourceTable + operateType
              广播流的value 是一个对象 sourceTable operateType sinkType sinkTable sinkColumns sinkPk sinkExtend
            2.过滤数据 哪些字段需要，过滤掉不需要的字段
            3.分流 维度数据 => Hbase 事实数据 => kafka
         */
        @Override
        public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
            // 1.根据key读取广播状态
            ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            TableProcess tableProcess = broadcastState.get(value.getString("tableName") + "-" + value.getString("type"));

            if(tableProcess != null){
                // 2.获取主流的数据
                JSONObject after = value.getJSONObject("after");
                // 根据广播状态中的sinkColumns过滤数据
                String sinkColumns = tableProcess.getSinkColumns();
                filterColumn(after,sinkColumns);

                // 3.分流  根据sinkType分流 并添加sinkTable信息 指明数据写到哪张表/哪个主题
                value.put("sinkTable",tableProcess.getSinkTable());
                String sinkType = tableProcess.getSinkType();
                if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                    // kafka数据进主流
                    out.collect(value);
                }else if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                    // 进侧输出流，写入hbase
                    ctx.output(tag,value);
                }
            }else{
                System.out.println("tableProcess为空，配置信息不完善，请尽快完善，避免数据丢失！");
            }
        }
        // 假如data是{“id”:"1","name":"joe","address":"sh"} 而我们只需要写入id和name字段 id,name
        public void filterColumn(JSONObject data,String sinkColumns){
            String[] fields = sinkColumns.split(",");
            List<String> columns = Arrays.asList(fields);

            // 遍历删除我们不需要的字段
            Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry<String, Object> next = iterator.next();
                if(!columns.contains(next.getKey())){
                    iterator.remove();
                }
            }
        }

        /*
             广播流数据格式 {"database":"" ,"tableName":"", "before":"", "after":"", "type":""}
             1.解析数据 after {sourceTable operateType sinkType sinkTable sinkColumns sinkPk sinkExtend} => TableProcess
             2.检查Hbase表是否存在并创建
             3.写入广播状态
         */
        @Override
        public void processBroadcastElement(JSONObject confObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
            // 数据是after
            String after = confObject.getString("after");
            // 将表转换成对象
            TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
            // 没有hbase表建立表
            if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                createTable(tableProcess.getSinkTable(),
                        tableProcess.getSinkColumns(),
                        tableProcess.getSinkPk(),
                        tableProcess.getSinkExtend()
                );
            }
            // 写入广播状态
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String key = tableProcess.getSourceTable() + "-" +tableProcess.getOperateType();
            broadcastState.put(key,tableProcess);
        }

        private void createTable(String sinkTable,String sinkColumns,String sinkPk,String sinkExtend){

            PreparedStatement ps = null;

            try{
                if(sinkPk == null){
                    sinkPk = "id";
                }
                if(sinkExtend == null){
                    sinkExtend = "";
                }

                StringBuffer create_table_sql = new StringBuffer("create table if not exists ")
                        .append("RTDW_HBASE")
                        .append(".")
                        .append(sinkTable)
                        .append("(");

                String[] fields = sinkColumns.split(",");

                for (int i = 0; i < fields.length; i++) {
                    String field = fields[i];

                    if(sinkPk.equals(field)){
                        create_table_sql.append(field).append(" VARCHAR primary key");
                    }else {
                        create_table_sql.append(field).append(" VARCHAR");
                    }

                    if(i < fields.length - 1){
                        create_table_sql.append(",");
                    }
                }

                create_table_sql.append(")");
                create_table_sql.append(sinkExtend);
                // 打印
                System.out.println(create_table_sql);
                // 执行建表语句
                ps = connection.prepareStatement(create_table_sql.toString());
                ps.execute();
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
