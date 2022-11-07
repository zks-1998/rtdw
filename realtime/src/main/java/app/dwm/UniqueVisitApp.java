package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.MyKafkaUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.读取页面数据 dwd_page_log
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaConsumer("dwd_page_log", "dwm_unique_visit"));

        // TODO 3.将数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaSource.map(JSON::parseObject);
        // TODO 4.根据每台手机编号分组 状态存今天的日期
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("time-state", String.class);

                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 更新类型 当创建状态、修改状态时更改失效时间，读时不可以更改
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 状态可见性，NeverReturnExpired，不要返回失效的数据
                        .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 取出上一页面信息
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");

                // 判断上一页面是否为null  不为null说明不是第一次访问app，直接过滤掉
                if (lastPage == null || lastPage.length() <= 0) {
                    String date = valueState.value();

                    // 从数据中取出今天的日期 2020-07-22
                    String curTs = simpleDateFormat.format(jsonObject.getLong("ts"));

                    // 不相等则更新 说明是新的一天或者date本来就是null
                    if (!curTs.equals(date)) {
                        valueState.update(curTs);
                        return true;
                    }
                }

                return false;
            }
        });

        // TODO 5.将数据写入kafka
       /* uvDS.print();*/
        uvDS.map(jsonObject -> jsonObject.toJSONString()).addSink(MyKafkaUtil.getKafkaProducer("dwm_unique_visit"));

        env.execute();
    }
}
