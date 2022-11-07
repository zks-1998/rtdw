package app.ods;

import app.function.CusDeserialization;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;

public class ReadConfByCDC {
    public static DebeziumSourceFunction<String> getConfCDC(){
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("175.178.154.194")
                .port(3306)
                .username("root")
                .password("zks123456")
                .databaseList("rtdw-conf")
                .tableList("rtdw-conf.table_process")  // cdc基于binlog 要开启binlog sudo vim /etc/my.cnf binlog在/var/lib/mysql
                .startupOptions(StartupOptions.initial())
                .deserializer(new CusDeserialization())
                .build();

        return sourceFunction;
    }
}
