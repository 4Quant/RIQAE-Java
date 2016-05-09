package fourquant;

import fourquant.imagej.ImageJSettings;
import fourquant.riqae.USBImageJSettings;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import scala.Option;
import scala.io.BufferedSource;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *  A set of tests to run and see the performance for a few simple applications
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PerformanceTests implements Serializable {
    final static String fijiPath = "/Applications/Fiji.app/Contents/";
    final static ImageJSettings ijs = new USBImageJSettings(fijiPath,false,false,false);
    final static int bindPort = 11112;
    final static String bindAddress = "localhost";
    // we don't want a directory based table at all
    final static Option<String> basePathInput = Option.empty();

    final static boolean use_hive = true;

    final static transient SQLContext sq = fourquant.db.demo.createSQLContext(basePathInput,bindPort,bindAddress,"",
            true,
            ijs,use_hive);

    @Before
    public void setupDatabase() {
        sq.sql("CREATE TEMPORARY TABLE DownloadPacs\n" +
                "USING fourquant.pacs\n" +
                "OPTIONS (debug \"true\", debug_name \"Anonymous Sid 10201\")");
        sq.cacheTable("DownloadPacs");
    }

    @Test
    public void testAppendingTable() {
        String simpleQuery = "SELECT patientName,fetch_dicom_imagej(studyInstanceUID,seriesInstanceUID) image " +
                "FROM DownloadPacs WHERE success=true LIMIT 1";

        DataFrame simpleImages = sq.sql(simpleQuery);
        simpleImages.
                cache().
                registerTempTable("ImageTable"); // create a table for other sql queries to use

        sq.cacheTable("ImageTable"); // cache the table for improved multiquery performance
        sq.sql("CACHE TABLE ImageTable"); // cache using standard SQL procedures

        //TODO fix test
        assertTrue(sq.isCached("ImageTable"));
    }

    @Test
    public void testBasicPacsQuery() {
        // show the tables and describe their structure
        sq.table("DownloadPacs").printSchema();
        sq.table("ImageTable").printSchema();
        // make a simple query on the pacs table
        DataFrame tabDesc = sq.sql("SELECT * FROM DownloadPacs where success=true");
        // show all of the results
        tabDesc.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.mkString(","));
            }
        });

        assertEquals(tabDesc.count(),1);
    }

    /**
     * A simple query of the image table and show the number of slices and calibration information
     */
    @Test
    public void testFirstImageQuery() {
        Row[] allRows = sq.sql("SELECT patientName,nslices(image) slice_count,showcalibration(image) calib FROM ImageTable")
                .collect();
        // show all of the results
        for (Row r : allRows) System.out.println(r.mkString(","));

        assertEquals(allRows.length,1);
    }

    /**
     * this test shows the benefits of caching the results
     */
    @Test
    public void testGImageQuery() {

        testFirstImageQuery();
    }

    protected static void checkOutputFiles(String path, int expectedCount, boolean readFiles) {
        File[] out_files = new File(path).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                // no junk files
                return !((pathname.getName().startsWith(".")) || (pathname.getName().startsWith("_")));
            }
        });
        for (File r : out_files) {
            System.out.println("Size:"+r.getTotalSpace()+", "+r.getAbsolutePath());
        }
        if (readFiles) {
            BufferedSource bi = scala.io.Source.fromFile(out_files[0], "utf-8");
            String outString = bi.getLines().mkString("\n");

            System.out.println("files->" + outString.substring(0, Math.min(200,outString.length())));
        }

        assertEquals("Output files should be "+expectedCount,out_files.length,expectedCount);
    }
    /**
     * This test shows how the dicom files can be output into a folder
     * @throws IOException if the folder cannot be created
     */
    @Test
    public void testOutputingImagesJson() throws IOException {
        String jsonOutputName = File.createTempFile("sql_output",".json").getAbsolutePath()+"_folder/";
        DataFrame df = sq.sql("SELECT patientName,toarray(image) slice_count FROM ImageTable");
        //TODO the builtin json doesn't register here so the class name is now explicit
        df.write().format("org.apache.spark.sql.json").save(jsonOutputName);
        checkOutputFiles(jsonOutputName,1,true);
    }

    /**
     * Performs the same task as
     * @throws IOException
     */
    @Test
    public void testOutputingImagesParquet() throws IOException {
        String pqtOutput = File.createTempFile("sql_output",".pqt").getAbsolutePath()+"_folder/";
        DataFrame df = sq.sql("SELECT patientName,toarray(image) slice_count FROM ImageTable");
        //TODO the builtin json doesn't register here so the class name is now explicit
        df.write().format("org.apache.spark.sql.parquet").save(pqtOutput);
        checkOutputFiles(pqtOutput,1,false);
    }

    @Test
    public void testRunCOPDAnalysis() {

        String segCommand = "run2(image,'"+USBImageJSettings.SegmentLung()+"','')";

        String copdQuery = "runrow("+segCommand+",'"+
                USBImageJSettings.StageLung()+"','')";

        Row[] allRows = sq.sql("SELECT patientName,"+copdQuery+" FROM ImageTable").collect();
        for (Row r : allRows) System.out.println(r.mkString(","));
        assertEquals(allRows.length,1);
        Map<String,Double> copdResults = allRows[0].getJavaMap(1);
        System.out.println("Study ID:"+copdResults.get("StudyID"));
        System.out.println("Study ID:"+copdResults.get("PD15"));
        assertTrue("PD15 should be less than -500",copdResults.getOrDefault("PD15",999.0)<-500);
    }

    @Test
    public void testXPortCOPDAnalysis() throws IOException {
        String jsonOutputName = File.createTempFile("copd_output",".json").getAbsolutePath()+"_folder/";
        String csvOutputName = File.createTempFile("copd_output",".csv").getAbsolutePath()+"_folder/";
        String segCommand = "run2(image,'"+USBImageJSettings.SegmentLung()+"','')";

        String copdQuery = "runrow("+segCommand+",'"+
                USBImageJSettings.StageLung()+"','')";

        DataFrame df = sq.sql("SELECT patientName,"+copdQuery+" FROM ImageTable");

        df.write().format("org.apache.spark.sql.json").save(jsonOutputName);
        checkOutputFiles(jsonOutputName,1,true);

        df.write().format("com.databricks.spark.csv").save(csvOutputName);
        checkOutputFiles(csvOutputName,1,true);
    }

    @Test
    public void testExplodeExportCOPDAnalysis() throws IOException {
        String jsonOutputName = File.createTempFile("copd_output",".json").getAbsolutePath()+"_folder/";
        String csvOutputName = File.createTempFile("copd_output",".csv").getAbsolutePath()+"_folder/";
        String segCommand = "run2(image,'"+USBImageJSettings.SegmentLung()+"','')";

        String copdQuery = "explode(runrow("+segCommand+",'"+
                USBImageJSettings.StageLung()+"','')) as (label,value)";

        DataFrame df = sq.sql("SELECT patientName,"+copdQuery+" FROM ImageTable");

        df.write().format("com.databricks.spark.csv").save(csvOutputName);
        checkOutputFiles(csvOutputName,1,true);
    }
}