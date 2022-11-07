package app.dws;

import bean.VisitorStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;
import utils.ClickHouseUtil;
import utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Iterator;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2读取kafka
        String groupId = "visitor_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump";

        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        // TODO 3.把流处理成相同类型的格式
        // 3.1 处理uv数据 uv实际上就是一个page_log
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });
        // 3.2 处理uj数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        // 3.3处理pv
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });


        // TODO 4.Union几个流
        DataStream<VisitorStats> unionDS= visitorStatsWithUvDS.union(visitorStatsWithUjDS, visitorStatsWithPvDS);

        // TODO 5.生成水位线
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                        return visitorStats.getTs();
                    }
                }));

        // TODO 6.按照维度信息分组 常用统计的四个维度进行聚合 渠道ch、新老用户is_new、app 版本vc、省市区域ar
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWaterMarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {

            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<String, String, String, String>(
                        visitorStats.getAr(),
                        visitorStats.getIs_new(),
                        visitorStats.getCh(),
                        visitorStats.getVc()
                );
            }
        });


        // TODO 7.开窗聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> result = windowStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats v1, VisitorStats v2) throws Exception {
                v1.setUv_ct(v1.getUv_ct() + v2.getUv_ct());
                v1.setPv_ct(v1.getPv_ct() + v2.getPv_ct());
                v1.setSv_ct(v1.getSv_ct() + v2.getSv_ct());
                v1.setUj_ct(v1.getUj_ct() + v1.getUj_ct());
                v1.setDur_sum(v1.getDur_sum() + v2.getDur_sum());
                return v1;
            }
        }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                // 补充窗口信息
                long start = context.window().getStart();
                long end = context.window().getEnd();

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String startFormat = simpleDateFormat.format(start);
                String endFormat = simpleDateFormat.format(end);

                Iterator<VisitorStats> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    VisitorStats next = iterator.next();
                    next.setStt(startFormat);
                    next.setEdt(endFormat);
                    out.collect(next);
                }
            }
        });

        result.print("VisitorStats => ");
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));



        env.execute();
    }
}
