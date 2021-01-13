
//import org.apache.flink.streaming.api.scala.
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;


public class StreamingJob {
    public static void main(String[] args) {
        String SourceCsvPath = "/<your-path-to-test-csv>/source.csv";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



    }

}
