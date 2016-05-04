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
    final int bindPort = 11112;
    final String bindAddress = "localhost";
    // we don't want a directory based table at all
    final Option<String> basePathInput = Option.empty();

    final SQLContext sq = fourquant.db.demo.createSQLTool(basePathInput,bindPort,bindAddress,"TestTable",false,true,
            ijs);

    //@Test
    public void testCreateTemporaryPacs() {

        sq.sql("CREATE TEMPORARY TABLE DemoPacs\n" +
                "USING fourquant.pacs\n" +
                "OPTIONS (debug \"true\", debug_name \"Anonymous Sid 10007\")");

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

    @Test
    public void testCOPDPlugin() {

        sq.sql("CREATE TEMPORARY TABLE DownloadPacs\n" +
                "USING fourquant.pacs\n" +
                "OPTIONS (debug \"true\", debug_name \"Anonymous Sid 10007\")");


        // execute the command SHOW TABLES
        DataFrame tabDesc = sq.sql("DESCRIBE DownloadPacs");
        // show all of the results
        tabDesc.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.mkString(","));
            }
        });

        String simpleQuery = "SELECT seriesInstanceUID,fetch_dicom_imagej(studyInstanceUID,seriesInstanceUID) " +
                "FROM DownloadPacs WHERE success=true LIMIT 1";

        DataFrame simpleImages = sq.sql(simpleQuery);
        // show all of the results
        simpleImages.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.mkString(","));
            }
        });


        String fImage = "fetch_dicom_imagej(studyInstanceUID,seriesInstanceUID)";


        String segCommand = "run2("+fImage+",'"+USBImageJSettings.SegmentLung()+"','')";

        String copdQuery = "SELECT seriesInstanceUID,runrow("+segCommand+",'"+
                USBImageJSettings.StageLung()+"','') FROM " +
                "DownloadPacs WHERE success=true LIMIT 1";

        System.out.println("copdQuery:"+copdQuery);
        DataFrame allTables = sq.sql(copdQuery);
        // show all of the results
        allTables.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.mkString(","));
            }
        });
    }




}