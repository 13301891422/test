import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Test {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "C:\\Users\\xuweiwei\\IdeaProjects\\test\\src\\main\\resources\\";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);


// 随便读取一些数据
        String mySql = "select * from xuweiwei";

        TableResult result = tableEnv.executeSql(mySql);
        System.out.println(result.getJobClient().get().getJobStatus());

    }
}
