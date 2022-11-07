package app.dws;

import app.function.SpiltFunction;
import bean.KeywordStats;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.ClickHouseUtil;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.DDL读取数据建立表 读取page_log JSON格式
        String createSQL = "create table page_view(common Map<STRING,STRING>," +
                "page Map<STRING,STRING>," +
                "ts BIGINT," +
                "rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND" +
                ") WITH ("+
                "'connector' = 'kafka', "+
                "'topic' = 'dwd_page_log',"+
                "'properties.bootstrap.servers' = 'hadoop102:9092',"+
                "'properties.group.id' = 'keyword_stats_app',"+
                "'scan.startup.mode' = 'latest-offset',"+
                "'format' = 'json'"+
                ")";

        tableEnv.executeSql(createSQL);
        // TODO 3.过滤数据 上一条为search 并且 搜索词不为空
        Table fullWordView = tableEnv.sqlQuery("select " +
                "   page['item'] as full_word ," +
                "   rt " +
                "from " +
                "   page_view " +
                "where " +
                "   page['last_page_id']='search' " +
                "and" +
                "   page['item'] IS NOT NULL ");

        // TODO 4.注册自定义函数 进行分词 fullWordView是Table，不可以直接使用，需要注册成临时视图或者 拼接
        tableEnv.createTemporaryFunction("spilt_words", SpiltFunction.class);

        Table wordTable = tableEnv.sqlQuery("select word,rt " +
                "from " + fullWordView +",LATERAL TABLE( spilt_words(full_word) )");

        // TODO 5.分组开窗聚合
        Table keywordStatsSearch = tableEnv.sqlQuery("select " +
                "'search' source,"+
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "word keyword," +
                "count(*) ct," +
                "UNIX_TIMESTAMP() * 1000 ts from "+ wordTable
                + " GROUP BY " + "TUMBLE(rt, INTERVAL '10' SECOND ),word");


        // TODO 6.转换成流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);

        keywordStatsDataStream.print("keywordStatsDataStream === ");

        keywordStatsDataStream.addSink(ClickHouseUtil
                .getSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));


        env.execute();
    }
}
