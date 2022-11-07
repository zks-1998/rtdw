package app.dwm;

import app.function.DimAsyncFunction;
import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取kafka主题数据 转换成bean
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer("dwd_order_info", "order_wide_group3"))
                .map(data -> {
                    OrderInfo orderInfo = JSON.parseObject(data, OrderInfo.class);

                    String create_time = orderInfo.getCreate_time(); // 2022-07-26 12:05:55
                    String[] dataTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dataTimeArr[0]); // 2022-07-26
                    orderInfo.setCreate_hour(dataTimeArr[1].split(":")[0]);

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    // 将时间转换成时间戳
                    orderInfo.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer("dwd_order_detail", "order_wide_group3"))
                .map(data -> {
                    OrderDetail orderDetail = JSON.parseObject(data, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    // 将时间转换成时间戳
                    orderDetail.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        }));

        // TODO 3.双流JOIN 用间隔JOIN
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(new KeySelector<OrderInfo, Long>() {
                    @Override
                    public Long getKey(OrderInfo orderInfo) throws Exception {
                        return orderInfo.getId();
                    }
                }).intervalJoin(orderDetailDS.keyBy(new KeySelector<OrderDetail, Long>() {
                    @Override
                    public Long getKey(OrderDetail orderDetail) throws Exception {
                        return orderDetail.getOrder_id();
                    }
                })).between(Time.minutes(-10), Time.minutes(10)) // 生产环境个最大延迟时间保证不丢数据
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

            orderWideWithNoDimDS.print("orderWideWithNoDimDS =>");


        // TODO 4.关联维度信息
        // 4.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUser = AsyncDataStream.unorderedWait(orderWideWithNoDimDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderwide, JSONObject hbaseInfo) {
                        String gender = hbaseInfo.getString("gender");
                        orderwide.setUser_gender(gender);
                    }
                },
                1, TimeUnit.MINUTES,200);

        // 4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS =
                AsyncDataStream.unorderedWait(orderWideWithUser,
                        new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return orderWide.getProvince_id().toString();
                            }
                            @Override
                            public void join(OrderWide orderWide, JSONObject dimInfo){
                                //提取维度信息并设置进 orderWide
                                orderWide.setProvince_name(dimInfo.getString("name"));

                                orderWide.setProvince_area_code(dimInfo.getString("areaCode"));
                                orderWide.setProvince_iso_code(dimInfo.getString("isoCode"));

                                orderWide.setProvince_3166_2_code(dimInfo.getString("iso31662"));
                            }
                        }, 1, TimeUnit.MINUTES,200);

        // 4.3 关联 SKU 维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) {
                                orderWide.setSku_name(jsonObject.getString("skuName"));
                                orderWide.setCategory3_id(jsonObject.getLong("category3Id"));
                                orderWide.setSpu_id(jsonObject.getLong("spuId"));
                                orderWide.setTm_id(jsonObject.getLong("tmId"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getSku_id());
                            }
                        }, 1, TimeUnit.MINUTES,200);

        //5.4 关联 SPU 维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) {
                                orderWide.setSpu_name(jsonObject.getString("spuName"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getSpu_id());
                            }
                        }, 1, TimeUnit.MINUTES,200);

        // 4.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK")
                        {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) {
                                orderWide.setTm_name(jsonObject.getString("tmName"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getTm_id());
                            }
                        }, 1, TimeUnit.MINUTES,200);
        // 4.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS =
                AsyncDataStream.unorderedWait(
                        orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3")
                        {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject)  {
                                orderWide.setCategory3_name(jsonObject.getString("name"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getCategory3_id());
                            }
                        }, 1, TimeUnit.MINUTES,200);

        // TODO 5.写入kafka
        orderWideWithCategory3DS.print("orderWideWithCategory3DS ===");

        orderWideWithCategory3DS.map(JSONObject::toJSONString).addSink(MyKafkaUtil.getKafkaProducer("dwm_order_wide"));

        env.execute();
    }
}
