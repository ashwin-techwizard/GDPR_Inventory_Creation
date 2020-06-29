package ashwin.gdpr.main

import ashwin.gdpr.entities.ConfigObjects
import com.emirates.helix.gdpr.entities.ConfigObjects
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import com.emirates.helix.gdpr.utils.Processor._
import org.apache.log4j.Logger

/**
  * Generic code to parse the Avro and Parquet files and generate the output report which contains the list of columns along with the data type and sample values.
  * It also does a basic classification whether data column is GDPR restricted based on the column name.
  *
  * @author Ashwin Kumar
  */
object DataDictionary {
  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("GdprGenericCustomerStatusProcessor")

  /**
    * Main method which reads the arguments and creates the the output Report
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Inventory Creation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val CmdArgs = CommandLineArgs.getCommandLineArgs(args)
    val confFile = spark.sparkContext.wholeTextFiles(CmdArgs.inventory_conf_file_path)
    val yaml = new Yaml(new Constructor(classOf[ConfigObjects]))
    val configObject = yaml.load(confFile.first._2).asInstanceOf[ConfigObjects]

    /**
      * This part defines the Schema of the output report file
      */
    val schema = StructType(
      StructField("name", StringType, true) ::
        StructField("parent", StringType, true) ::
        StructField("SAMPLE_DATA", StringType, true) ::
        StructField("DATA_TYPE", StringType, true) ::
        StructField("SATELLITE_TABLE", StringType, true) ::
        StructField("ATTRIBUTE", StringType, true) ::
        StructField("SOURCE_ELEMENT", StringType, true) ::
        StructField("MESSAGE_TYPE", StringType, true) ::
        StructField("MODELLED_PATH", StringType, true) ::
        StructField("SOURCE_SYSTEM", StringType, true) ::
        StructField("PERSONAL_DATA", StringType, true) :: Nil)

    var outputReportDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    /**
      * Go through each Yaml configuration and generate a report for each file, then combine them into one final report
      */
    configObject.files.foreach { configParams =>
      if (configParams.fileType.equals("avro") || configParams.fileType.equals("parquet")) {
        val inventory = try {
          createInventory(configObject, configParams, spark)
        }
        catch {
          case ex: org.apache.spark.sql.AnalysisException => {
            log.error(s"ERROR- Input File not found in the given Path:\n ${ex}");
            null
          }
          case ex: Exception => {
            log.error(s"[ERROR] Invalid File Type...:\n ${ex}");
            null
          }
        }
        if(inventory != null)
          outputReportDF = outputReportDF.union(inventory)
      } else {
        System.exit(1)
        log.error("[ERROR] Invalid File Type...")
      }
    }
    outputReportDF.show(10, false)
    outputReportDF.select("SATELLITE_TABLE", "MESSAGE_TYPE", "MODELLED_PATH", "SOURCE_SYSTEM", "ATTRIBUTE", "SOURCE_ELEMENT", "SAMPLE_DATA", "DATA_TYPE", "PERSONAL_DATA").repartition(1).write
      .format("com.databricks.spark.csv")
      .option("delimiter", configObject.delimiter)
      .option("header", "true")
      .option("quoteAll", "true")
      //  .option("escape", '"')
      //.option("quote", '"')
      .mode(SaveMode.Overwrite)
      .save(configObject.outputPath)
    println("------------------------------------")
    println("Report Created @" + configObject.outputPath)

  }

  /**
    * Remove the dot (.) from the given text
    *
    * @return string
    */
  def removeDot = udf((attribute: String) => {

    if (attribute.charAt(0).equals('.'))
      attribute.replaceFirst(".", "")
    else
      attribute
  })

  /**
    * Takes the last value from the dot (.) and ignore other values
    *
    * @return
    */
  def getElement = udf((attribute: String) => {
    attribute.substring(attribute.lastIndexOf(".") + 1)
  })


}
