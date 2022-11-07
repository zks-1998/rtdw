package app.dwm;

import bean.OrderWide;
import bean.PaymentInfo;
import bean.PaymentWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2.读取kafka主题
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        // 读取订单宽表 转换成订单宽表对象
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(orderWide.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l;
                                }
                            }
                        }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l;
                                }
                            }
                        }));

        // TODO 3.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(new KeySelector<PaymentInfo, Long>() {
                    @Override
                    public Long getKey(PaymentInfo paymentInfo) throws Exception {
                        return paymentInfo.getOrder_id();
                    }
                }).intervalJoin(orderWideDS.keyBy(new KeySelector<OrderWide, Long>() {
                    @Override
                    public Long getKey(OrderWide orderWide) throws Exception {
                        return orderWide.getOrder_id();
                    }
                })).between(Time.minutes(-15), Time.seconds(10))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        // TODO 4.写到kafka
        paymentWideDS.print("paymentWideDS => ");
        paymentWideDS.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        env.execute();

    }
}
