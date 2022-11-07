package app.dws;

import app.function.DimAsyncFunction;
import bean.OrderWide;
import bean.PaymentWide;
import bean.ProductStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取kafka 7个流  8个指标
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 获取(1)点击和(2)曝光
        FlinkKafkaConsumer<String> pageViewSource =
                MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic,groupId);
        DataStreamSource<String> pvSource = env.addSource(pageViewSource);

        // (3) 收藏
        FlinkKafkaConsumer<String> favorInfoSourceSource =
                MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic,
                        groupId);
        DataStreamSource<String> favSource = env.addSource(favorInfoSourceSource);

        // (4) 购物车
        FlinkKafkaConsumer<String> cartInfoSource =
                MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic,groupId);
        DataStreamSource<String> cartSource = env.addSource(cartInfoSource);

        // (5)订单信息
        FlinkKafkaConsumer<String> orderWideSource =
                MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic,groupId);
        DataStreamSource<String> orderSource = env.addSource(orderWideSource);


        // (6)支付信息
        FlinkKafkaConsumer<String> paymentWideSource =
                MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic,groupId);
        DataStreamSource<String> paySource = env.addSource(paymentWideSource);


        // (7) 退款
        FlinkKafkaConsumer<String> refundInfoSource =
                MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic,groupId);
        DataStreamSource<String> refundSource = env.addSource(refundInfoSource);

        // (8) 评价
        FlinkKafkaConsumer<String> commentInfoSource =
                MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic,groupId);
        DataStreamSource<String> commentSource = env.addSource(commentInfoSource);

        // TODO 3.7个流统一格式

        // (1)点击和(2)曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplay = pvSource.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                // 获取时间
                Long ts = jsonObject.getLong("ts");

                JSONObject page = jsonObject.getJSONObject("page");
                // 判断点击数据
                String page_id = page.getString("page_id");
                if ("good_detail".equals(page_id) && "sku_id".equals(page.getString("item_type"))) {
                    collector.collect(ProductStats.builder().sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                // 尝试获取商品曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        // 获取单条曝光数据
                        JSONObject display = displays.getJSONObject(i);

                        // 判断是否是商品数据
                        if ("sku_id".equals(display.getString("item_type"))) {
                            collector.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }

                    }
                }

            }
        });
        // (3) 收藏
        SingleOutputStreamOperator<ProductStats> favDS = favSource.map(value -> {
            JSONObject favObj = JSONObject.parseObject(value);

            String ts = favObj.getString("create_time");

            long time = dateFormat.parse(ts).getTime();

            return ProductStats.builder()
                    .sku_id(favObj.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(time)
                    .build();
        });

        // (4) 购物车
        SingleOutputStreamOperator<ProductStats> cartDS = cartSource.map(value -> {
            JSONObject favObj = JSONObject.parseObject(value);

            String ts = favObj.getString("create_time");

            long time = dateFormat.parse(ts).getTime();

            return ProductStats.builder()
                    .sku_id(favObj.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(time)
                    .build();
        });

        // (5)订单信息
        SingleOutputStreamOperator<ProductStats> orderDS = orderSource.map(value -> {
            OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

            HashSet<Long> set = new HashSet<>();
            set.add(orderWide.getOrder_id());

            String create_time = orderWide.getCreate_time();
            long time = dateFormat.parse(create_time).getTime();


            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getOrder_price())
                    .orderIdSet(set)
                    .ts(time)
                    .build();
        });
        // (6) 支付信息
        SingleOutputStreamOperator<ProductStats> payDS = paySource.map(value -> {
            PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);

            HashSet<Long> set = new HashSet<>();
            set.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getOrder_price())
                    .ts(dateFormat.parse(paymentWide.getPayment_create_time()).getTime())
                    .paidOrderIdSet(set)
                    .build();
        });

        // (7) 退单信息
        SingleOutputStreamOperator<ProductStats> refundDS = refundSource.map(value -> {
            JSONObject object = JSON.parseObject(value);

            HashSet<Long> set = new HashSet<>();
            set.add(object.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(object.getLong("sku_id"))
                    .refund_amount(object.getBigDecimal("refund_amount"))
                    .ts(dateFormat.parse(object.getString("create_time")).getTime())
                    .refundOrderIdSet(set)
                    .build();


        });

        // (8) 评价
        SingleOutputStreamOperator<ProductStats> commentDS = commentSource.map(value -> {
            JSONObject jsonObject = JSON.parseObject(value);

            String appraise = jsonObject.getString("appraise");
            Long goodCt = 0L;
            if ("1201".equals(appraise)) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .ts(dateFormat.parse(jsonObject.getString("create_time")).getTime())
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .build();
        });


        // TODO 4. Union7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplay.union(favDS, cartDS, orderDS, payDS, refundDS, commentDS);

        // TODO 5. 提取时间戳 水位线
        SingleOutputStreamOperator<ProductStats> unionDSWithWatermark = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long l) {
                        return productStats.getTs();
                    }
                }));

        // TODO 6.分组、开窗、聚合  分组按照spu，10秒滚动窗口
        SingleOutputStreamOperator<ProductStats> productStatsDstream = unionDSWithWatermark.keyBy(new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats productStats) throws Exception {
                        return productStats.getSku_id();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() +
                                stats2.getOrder_sku_num());

                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);

                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() +
                                stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>.Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        // 补充窗口信息
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        String startFormat = dateFormat.format(start);
                        String endFormat = dateFormat.format(end);
                        ProductStats productStats = elements.iterator().next();

                        productStats.setStt(startFormat);
                        productStats.setEdt(endFormat);
                    }
                });

        // TODO 7.关联维度信息

        // 7.1 关联sku
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream =
                AsyncDataStream.unorderedWait(productStatsDstream,
                        new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                                productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                                productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                                productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                                productStats.setTm_id(jsonObject.getLong("TM_ID"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSku_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        // 7.2 关联spu

        // 7.3 关联TM

        // 7.4 关联category

        productStatsWithSkuDstream.print("res ==>");

        // TODO 8.写入clickhouse

        env.execute();

    }
}