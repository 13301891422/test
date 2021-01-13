
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class StreamingTopN {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        // Source DDL
        String sourceDDL = ""
                + "create table source_kafka "
                + "( "
                + "    userID String, "
                + "    eventType String, "
                + "    eventTime String, "
                + "    productID String "
                + ") with ( "
                + "    'connector.type' = 'kafka', "
                + "    'connector.version' = '0.10', "
                + "    'connector.properties.bootstrap.servers' = 'hadoop105:9092', "
                + "    'connector.properties.zookeeper.connect' = 'hadoop105:2181', "
                + "    'connector.topic' = 'test', "
                + "    'connector.properties.group.id' = 'test', "
                + "    'connector.startup-mode' = 'latest-offset', "
                + "    'format.type' = 'json' "
                + ")";

        tableEnv.sqlUpdate(sourceDDL);
        //tableEnv.toAppendStream(tableEnv.from("source_kafka"), Row.class).print();

        // Sink DDL
        String sinkDDL = ""
                + "create table sink_mysql "
                + "( "
                + "     datetime STRING, "
                + "     productID STRING, "
                + "     userID STRING, "
                + "     clickPV BIGINT "
                + ") with ( "
                + "    'connector.type' =        'jdbc', "
                + "    'connector.url' = 'jdbc:mysql://hadoop105:3306/bigdata', "
                + "    'connector.table' = 't_product_click_topn', "
                + "    'connector.username' = 'root', "
                + "    'connector.password' = '123456', "
                + "    'connector.write.flush.max-rows' = '50', "
                + "    'connector.write.flush.interval' = '2s', "
                + "    'connector.write.max-retries' = '3' "
                + ")";
        tableEnv.sqlUpdate(sinkDDL);

        // Query
        String execSQL = ""
                + "INSERT INTO sink_mysql "
                + "SELECT datetime, productID, userID, clickPV "
                + "FROM ( "
                + "  SELECT *, "
                + "     ROW_NUMBER() OVER (PARTITION BY datetime, productID ORDER BY clickPV desc) AS rownum "
                + "  FROM ( "
                + "        SELECT SUBSTRING(eventTime,1,13) AS datetime, "
                + "            productID, "
                + "            userID, "
                + "            count(1) AS clickPV "
                + "        FROM source_kafka "
                + "        GROUP BY SUBSTRING(eventTime,1,13), productID, userID "
                + "    ) a "
                + ") t "
                + "WHERE rownum <= 3";
        tableEnv.sqlUpdate(execSQL);

        tableEnv.execute(StreamingTopN.class.getSimpleName());
    }

}
