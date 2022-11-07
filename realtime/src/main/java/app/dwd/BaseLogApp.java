package app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;

public class BaseLogApp {

    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2.消费ods_base_log主题的数据 创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3.将JSON转换为JSON对象 格式不对的写入侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("dirty") {};

        SingleOutputStreamOperator<JSONObject> jsonobjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });

        // 4.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlag = jsonobjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    // 声明一个值状态
                    private ValueState<String> valueState;

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 获取数据中的is_new数据
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        // is_new为 0 说明肯定不是新用户  为 1 有可能是用户卸载后又重新登录标记为了新用户
                        if ("1".equals(isNew)) {
                            String state = valueState.value();
                            // 状态值不为空 说明不是新用户
                            if (state != null) {
                                // 修改is_new标记
                                jsonObject.getJSONObject("common").put("is_new", 0);
                            } else {
                                valueState.update("notNull");
                            }
                        }
                        return jsonObject;
                    }

                    // open方法里对状态赋值
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }
                });


        // 5.分流  主流：页面  侧输出流 启动 曝光
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};
        OutputTag<String> displayOutputTag = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    // 将数据写入启动侧输出流
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    // 曝光数据也属于页面数据，曝光标签不为空说明是曝光数据
                    // 将数据写入页面日志
                    out.collect(value.toJSONString());
                    JSONArray displays = value.getJSONArray("displays");
                    // 说明是曝光日志
                    if (displays != null && displays.size() > 0) {
                        String pageId = value.getJSONObject("page").getString("page_id");
                        // 对曝光的每个数据添加页面id
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            ctx.output(displayOutputTag, display.toJSONString());
                        }
                    }

                }
            }
        });

        // 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);
        // 7.将三个流写入对应的kafka
        /*startDS.print("start===>");
        pageDS.print("page===>");
        displayDS.print("display===>");
        */
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        // 8.启动任务
        environment.execute();
    }
}
