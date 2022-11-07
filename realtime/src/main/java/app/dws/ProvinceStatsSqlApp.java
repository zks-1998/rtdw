package app.dws;

import bean.OrderWide;
import bean.ProvinceStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.ClickHouseUtil;

public class ProvinceStatsSqlApp {

    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.使用DDL创建表 提取时间戳生成水位线
        // TO_TIMESTAMP(create_time) 方法 2022-08-05 18:28:20
        String createSQL = "CREATE TABLE order_wide (" +
                "province_id BIGINT," +
                "province_name STRING," +
                "province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id BIGINT," +
                "split_total_amount DECIMAL,"+
                "create_time STRING,"+
                "rt as TO_TIMESTAMP(create_time),"+
                "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND "+
                ") WITH ("+
                "'connector' = 'kafka', "+
                "'topic' = 'dwm_order_wide',"+
                "'properties.bootstrap.servers' = 'hadoop102:9092',"+
                "'properties.group.id' = 'province_stats',"+
                "'scan.startup.mode' = 'latest-offset',"+
                "'format' = 'json'"+
                ")";
        tableEnv.executeSql(createSQL);

        // TODO 3.查询数据
        String selectSQL = "select " +
                "DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "province_id,"+
                "province_name," +
                "province_area_code," +
                "province_iso_code," +
                "province_3166_2_code," +
                "count(distinct order_id) as order_count,"+
                "sum(split_total_amount) as order_amount," +
                "UNIX_TIMESTAMP() * 1000 as ts " +
                "from " +
                "order_wide " +
                "group by " +
                "province_id," +
                "province_name," +
                "province_area_code," +
                "province_iso_code," +
                "province_3166_2_code," +
                "TUMBLE(rt,INTERVAL '10' SECOND)";

        Table provinceStateTable = tableEnv.sqlQuery(selectSQL);



        // TODO 4.表转换成流
        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);



        provinceStatsDataStream.print("provinceStatsDataStream");

        provinceStatsDataStream.addSink(ClickHouseUtil.<ProvinceStats>getSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));


        env.execute();


    }

}
