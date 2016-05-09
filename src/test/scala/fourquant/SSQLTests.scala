package fourquant

import fourquant.pacs.patients.PacsCommunicationSingleton
import fourquant.riqae.USBImageJSettings
import org.scalatest.{FunSuite, Matchers}

class SSQLTests extends FunSuite with Matchers with Serializable {
    val use_hive = true
  val fijiPath = "/Applications/Fiji.app/Contents/"
  val ijs = new USBImageJSettings(fijiPath, false, false, false)
  val bindPort = PacsCommunicationSingleton.port.toInt;
  val bindAddress = PacsCommunicationSingleton.server;


  val sq = fourquant.db.demo.createSQLContext(None,bindPort,
    bindAddress,"", true, ijs,use_hive)

  test("Setup Tables") {
      sq.sql("""CREATE TEMPORARY TABLE DownloadPacs
        USING fourquant.pacs
        OPTIONS (debug "true", debug_name "Anonymous Sid 10201")""")
      sq.cacheTable("DownloadPacs")
    }

  test("Caching Tables") {
    val simpleQuery = "SELECT patientName,fetch_dicom_imagej(studyInstanceUID,seriesInstanceUID) image " +
      "FROM DownloadPacs WHERE success=true LIMIT 1"

    val simpleImages = sq.sql(simpleQuery)

    simpleImages.
      cache().
      registerTempTable("ImageTable"); // create a table for other sql queries to use

    sq.cacheTable("ImageTable"); // cache the table for improved multiquery performance
    sq.sql("CACHE TABLE ImageTable"); // cache using standard SQL procedures

    sq.isCached("ImageTable") shouldBe true
  }


}