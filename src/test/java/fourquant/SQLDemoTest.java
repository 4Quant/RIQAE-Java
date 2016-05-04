package fourquant;

import fourquant.imagej.ImageJSettings;
import fourquant.riqae.USBImageJSettings;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;
import scala.Function1;
import scala.Option;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

public class SQLDemoTest implements Serializable {
    final String fijiPath = "/Applications/Fiji.app/Contents/";
    final ImageJSettings ijs = new USBImageJSettings(fijiPath,false,false,false);
    final int bindPort = 8080;
    final String bindAddress = "0.0.0.0";

    //@Test
    public void testSetupSparkContext() {

        // show the spark context runs
        final SparkContext sc = new SparkContext("local[2]","SQL Demo Test");
        System.out.println(sc.appName());

    }

    @Test
    public void testCreateSQLTool() {
        final Option<String> basePathInput = Option.empty();

        SQLContext sq = fourquant.db.demo.createSQLTool(basePathInput,bindPort,bindAddress,"TestTable",false,true,
                ijs);

        // execute the command SHOW TABLES
        DataFrame allTables = sq.sql("SHOW TABLES");
        // show all of the results
        allTables.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.mkString(","));
            }
        });
    }
}