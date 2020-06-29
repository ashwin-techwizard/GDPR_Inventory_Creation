
import ashwin.gdpr.entities.ConfigObjects
import ashwin.gdpr.utils.Processor._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}

/**
  * @author Ashwin
  */
@RunWith(classOf[JUnitRunner])
class InventoryProcessTest  extends FunSpec
  with DataFrameComparer {
  val tst="""
files:
 -
   fileType : avro
   modelledPath : resource/sample_Inventory.avro
   satelliteTable: Daas
   messageType: PNRData
   sourceSystem: MARS
   recordsToscan: 100
 -
   fileType : avro
   modelledPath : resource/sample_Inventory2.avro
   satelliteTable: Daas
   messageType: PNRData
   sourceSystem: MARS
   recordsToscan: 100
 -
   fileType : av
   modelledPath : resource/sample_Inventory2.avro
   satelliteTable: Daas
   messageType: PNRData
   sourceSystem: MARS
   recordsToscan: 100
outputPath: /data/helix/modelled/emcg_ashwin/pdi_output_emcg2
prfxSet: first,last,middle,full,pass,doc,con,pax,date,email,dob
sfxSet: nam,num,birth,email,dob
parAttSet: passport,passenger,contact,phone
delimiter: ","
            """
    .replaceAll("|","")
  val yaml = new Yaml(new Constructor(classOf[ConfigObjects]))
  val configObject = yaml.load(tst.trim).asInstanceOf[ConfigObjects]
  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger("GenericInventoryProcessor")

  val spark = SparkSession
    .builder()
    .appName("Spark Hive").master("local[*]").config("spark.testing.memory", "2147480000")
    .getOrCreate()

  val sqlContext = spark.sqlContext
  import sqlContext.implicits._
  val gdpr_table_name = "DSAR"

  val outputSchema = StructType(
    StructField("name", StringType, true) ::
      StructField("parent", StringType, true) ::
      StructField("SAMPLE_DATA", StringType, true) ::
      StructField("DATA_TYPE", StringType, true) ::
      StructField("SATELLITE_TABLE", StringType, true) ::
      StructField("SOURCE_ELEMENT", StringType, true) ::
      StructField("ATTRIBUTE", StringType, true) ::
      StructField("MESSAGE_TYPE", StringType, true) ::
      StructField("MODELLED_PATH", StringType, true) ::
      StructField("SOURCE_SYSTEM", StringType, true) ::
      StructField("PERSONAL_DATA", StringType, true) :: Nil)
  it("TEST : CASE 0 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 1 -  DATA DICTIONARY (TO PROCESS RECORDS)")

    val schema = StructType(
      StructField("name", StringType, true) ::
        StructField("parent", StringType, true) ::
        StructField("SAMPLE_DATA", StringType, true) ::
        StructField("DATA_TYPE", StringType, true) ::
        StructField("SATELLITE_TABLE", StringType, true) ::
        StructField("SOURCE_ELEMENT", StringType, true) ::
        StructField("ATTRIBUTE", StringType, true) ::
        StructField("MESSAGE_TYPE", StringType, true) ::
        StructField("MODELLED_PATH", StringType, true) ::
        StructField("SOURCE_SYSTEM", StringType, true) ::
        StructField("PERSONAL_DATA", StringType, true) :: Nil)
    val outputDataset = try {
 sqlContext.createDataFrame(createInventory(configObject, configObject.files(1), spark).rdd, schema)
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

    val expected_toProcess = null
assert(expected_toProcess==outputDataset)
    log.info("[INFO] TESTCASE PASSED : EXPECTED OUTPUT IS MATCHING WITH ACTUAL OUTPUT")
  }
  it("TEST : CASE 1 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 1 -  DATA DICTIONARY (TO PROCESS RECORDS)")

    val schema = StructType(
      StructField("name", StringType, true) ::
        StructField("parent", StringType, true) ::
        StructField("SAMPLE_DATA", StringType, true) ::
        StructField("DATA_TYPE", StringType, true) ::
        StructField("SATELLITE_TABLE", StringType, true) ::
        StructField("SOURCE_ELEMENT", StringType, true) ::
        StructField("ATTRIBUTE", StringType, true) ::
        StructField("MESSAGE_TYPE", StringType, true) ::
        StructField("MODELLED_PATH", StringType, true) ::
        StructField("SOURCE_SYSTEM", StringType, true) ::
        StructField("PERSONAL_DATA", StringType, true) :: Nil)

      val outputReportDF = createInventory(configObject, configObject.files(0), spark)

    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, schema)

    outputDataset.show(10, false)
    log.info(outputDataset.show(false))
    val expected_toProcess = Seq(

      (".UUID","","","StringType","Daas","UUID","UUID","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".TIMESTAMP","","","StringType","Daas","TIMESTAMP","TIMESTAMP","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".HOLD_ID","","","StringType","Daas","HOLD_ID","HOLD_ID","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".ETA_CALC_ID","","","StringType","Daas","ETA_CALC_ID","ETA_CALC_ID","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".HOLD_ENTRY","","","StringType","Daas","HOLD_ENTRY","HOLD_ENTRY","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".HOLD_EXIT","","","StringType","Daas","HOLD_EXIT","HOLD_EXIT","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".HOLD_AREA","","","StringType","Daas","HOLD_AREA","HOLD_AREA","PNRData","resource/sample_Inventory.avro","MARS","N")
    ).toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    outputDataset.show()
    expected_toProcess.show()
    assertSmallDataFrameEquality(expected_toProcess, outputDataset)
    log.info("[INFO] TESTCASE PASSED : EXPECTED OUTPUT IS MATCHING WITH ACTUAL OUTPUT")
  }

  it("TEST : CASE 2 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 2 -  Dataframe with StringType columns")

    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row("Rogger", "Rabbit"),
      Row("Trevor", "Philips"),
      Row("Michael", null),
      Row(null, null)
    )
    val schema = StructType(
      List(
        StructField("firstName", StringType, true),
        StructField("secondName", StringType, true)
      )
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)
    val expectedReportDF = Seq(
      (".firstName","","Rogger","StringType","Daas","firstName","firstName","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".secondName","","Rabbit","StringType","Daas","secondName","secondName","PNRData","resource/sample_Inventory.avro","MARS","Y"))
.toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    outputReportDF.show()
    expectedReportDF.show()
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)

  }

  it("TEST : CASE 3 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 3 -  Dataframe with IntTypes columns")
    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row(1930, 7),
      Row(1956, 12),
      Row(2014, null)/*
      Row(null, null)*/
    )
    // TODO: We get an exception in case it is a null value

    val schema = StructType(
      List(
        StructField("dob", org.apache.spark.sql.types.IntegerType, true),
        StructField("monthNum", org.apache.spark.sql.types.IntegerType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".dob","","1930","IntegerType","Daas","dob","dob","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".monthNum","","7","IntegerType","Daas","monthNum","monthNum","PNRData","resource/sample_Inventory.avro","MARS","N"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    outputReportDF.show()
    expectedReportDF.show()
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)

  }

  it("TEST : CASE 4 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 4 -  Dataframe with LongType columns")

    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    //TODO: it fails when the Row is null
    val data = Seq(
      Row(123456789l, 1000234000l),
      Row(13245768l, 2000234000l),
      Row(9876543l, null)
    )

    val schema = StructType(
      List(
        StructField("milesAcc", org.apache.spark.sql.types.LongType, true),
        StructField("pointsAcc", org.apache.spark.sql.types.LongType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(

      (".milesAcc","","123456789","LongType","Daas","milesAcc","milesAcc","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".pointsAcc","","1000234000","LongType","Daas","pointsAcc","pointsAcc","PNRData","resource/sample_Inventory.avro","MARS","N"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)

  }

  it("TEST : CASE 5 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 5 -  Dataframe with ArrayType columns")

    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row(Array("Paris", "Rome"), Array("London", "Dublin")),
      Row(Array("Oporto", "Cairo"), Array("Berlin", "New York")),
      Row(Array("Bangkok", "Chenai"), null),
      Row(null, null)
    )

    val schema = StructType(
      List(
        StructField("destArray", ArrayType(StringType) , true),
        StructField("origArray", ArrayType(StringType), true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".destArray","","","ArrayType(StringType,true)","Daas","destArray","destArray","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".destArray","destArray","Paris","StringType","Daas","destArray","destArray","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".origArray","","","ArrayType(StringType,true)","Daas","origArray","origArray","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".origArray","origArray","London","StringType","Daas","origArray","origArray","PNRData","resource/sample_Inventory.avro","MARS","N"))
    .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }

  it("TEST : CASE 6 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 6 -  Dataframe with MapType columns")

    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row(Map("home" -> "987122112", "mobile" -> "695123456"), Map("personal" -> "rr@gmail.com", "office" -> "rr@msf.com")),
      Row(Map("home" -> "987131415", "mobile" -> "678098765"), Map("personal" -> null, "office" -> "mk@ibm.com")),
      Row(Map("home" -> "987112233", "mobile" -> "678102938"), null),
      Row(null, null)
    )

    val schema = StructType(
      List(
        StructField("contactPhone", MapType(StringType, StringType, true), true),
        StructField("contactEmail", MapType(StringType, StringType, true), true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)
    val expectedReportDF = Seq(
      (".contactPhone","","","MapType(StringType,StringType,true)","Daas","contactPhone","contactPhone","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".contactPhone.home","contactPhone","987122112","StringType","Daas","contactPhone.home","home","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".contactPhone.mobile","contactPhone","695123456","StringType","Daas","contactPhone.mobile","mobile","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".contactEmail","","","MapType(StringType,StringType,true)","Daas","contactEmail","contactEmail","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".contactEmail.personal","contactEmail","rr@gmail.com","StringType","Daas","contactEmail.personal","personal","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".contactEmail.office","contactEmail","rr@msf.com","StringType","Daas","contactEmail.office","office","PNRData","resource/sample_Inventory.avro","MARS","Y"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }


  it("TEST : CASE 7 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 7 -  Dataframe with StructType columns")

    val schema = StructType(
      List(
        StructField("passName", StructType(
          StructField("firstName", StringType, true)::
            StructField("age", IntegerType, true) :: Nil
        ), true)
      )
    )
    val pax = Seq(
      Row(Row("Rogger", 54)),
      Row(Row("Trevor", 23)),
      Row(Row("Michael", null))
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(pax),
      schema
    )
  val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    //TODO: Bug when creating struct with Integer field, it comes with a dot
    val expectedReportDF = Seq(
      (".passName","","","StructType(StructField(firstName,StringType,true), StructField(age,IntegerType,true))","Daas","passName","passName","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".passName.firstName","passName","Rogger","StringType","Daas","passName.firstName","firstName","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".passName.age",".passName","54","IntegerType","Daas","passName.age","age","PNRData","resource/sample_Inventory.avro","MARS","Y"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }

  it("TEST : CASE 8 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 8 -  Dataframe with Null StructType columns")

    val schema = StructType(
      List(
        StructField("paxName", StructType(
          StructField("firstName", StringType, true):: StructField("lastName", StringType, true) :: Nil
        ), true)
      )
    )

    val pax = Seq(
      Row(null)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(pax),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".paxName","","","StructType(StructField(firstName,StringType,true), StructField(lastName,StringType,true))","Daas","paxName","paxName","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".firstName","paxName","","StringType","Daas","firstName","firstName","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".lastName","paxName","","StringType","Daas","lastName","lastName","PNRData","resource/sample_Inventory.avro","MARS","Y"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }

  //TODO: REvise the is null condition for Array
  it("TEST : CASE 9 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 9 -  Dataframe with empty ArrayType columns")

    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row(null,null)
    )

    val schema = StructType(
      List(
        StructField("destArray", ArrayType(StringType) , true),
        StructField("origArray", ArrayType(StringType), true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".destArray","","","ArrayType(StringType,true)","Daas","destArray","destArray","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".origArray","","","ArrayType(StringType,true)","Daas","origArray","origArray","PNRData","resource/sample_Inventory.avro","MARS","-"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }

  it("TEST : CASE 10 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 10 -  Dataframe with ArrayType columns containing complex structures")

    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row(Array(Map("home" -> "987122112", "mobile" -> "695123456"), Map("home" -> "980283746", "mobile" -> "643089128")), Array(Row("Rogger", "Rabbit"),Row("Trevor", "Philiphs")))
    )
    val schema = StructType(
      List(
        StructField("contactArray", ArrayType(MapType(StringType, StringType, true)), true),
        StructField("paxArray", ArrayType(StructType(
          StructField("firstName", StringType, true)::
            StructField("lastName", StringType, true) :: Nil
        )), true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
   val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".contactArray","","","ArrayType(MapType(StringType,StringType,true),true)","Daas","contactArray","contactArray","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".contactArray.home","contactArray","980283746","StringType","Daas","contactArray.home","home","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".contactArray.mobile","contactArray","643089128","StringType","Daas","contactArray.mobile","mobile","PNRData","resource/sample_Inventory.avro","MARS","N"),
      (".paxArray","","","ArrayType(StructType(StructField(firstName,StringType,true), StructField(lastName,StringType,true)),true)","Daas","paxArray","paxArray","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".paxArray.firstName","paxArray","Rogger","StringType","Daas","paxArray.firstName","firstName","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".paxArray.lastName","paxArray","Rabbit","StringType","Daas","paxArray.lastName","lastName","PNRData","resource/sample_Inventory.avro","MARS","Y"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }

  it("TEST : CASE 11 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 11 -  Dataframe with ArrayType columns containing complex structures but with null values")

    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row(Array(null, null), Array(null, null))
    )
    val schema = StructType(
      List(
        StructField("contactArray", ArrayType(MapType(StringType, StringType, true)), true),
        StructField("paxArray", ArrayType(StructType(
          StructField("firstName", StringType, true)::
            StructField("lastName", StringType, true) :: Nil
        )), true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".contactArray","","","ArrayType(MapType(StringType,StringType,true),true)","Daas","contactArray","contactArray","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".paxArray","","","ArrayType(StructType(StructField(firstName,StringType,true), StructField(lastName,StringType,true)),true)","Daas","paxArray","paxArray","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".paxArray.firstName","paxArray","","StringType","Daas","paxArray.firstName","firstName","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".paxArray.lastName","paxArray","","StringType","Daas","paxArray.lastName","lastName","PNRData","resource/sample_Inventory.avro","MARS","Y"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }

  it("TEST : CASE 12 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 12 -  Dataframe withStructType columns having one attribute as null")

    val schema = StructType(
      List(
        StructField("paxName", StructType(
          StructField("firstName", StringType, true):: StructField("lastName", StringType, true) :: Nil
        ), true)
      )
    )

    val pax = Seq(
      Row(Row("Rogger", null))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(pax),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".paxName","","","StructType(StructField(firstName,StringType,true), StructField(lastName,StringType,true))","Daas","paxName","paxName","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".paxName.firstName","paxName","Rogger","StringType","Daas","paxName.firstName","firstName","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".paxName.lastName","paxName","","StringType","Daas","paxName.lastName","lastName","PNRData","resource/sample_Inventory.avro","MARS","Y"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }

  it("TEST : CASE 13 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 3 -  Dataframe with DoubleType columns")
    // Creating a DataFrame with two columns of type StringType. The Rows may contain null values
    val data = Seq(
      Row(1930.00, 7),
      Row(1956.00, 12),
      Row(2014.00, null)/*
      Row(null, null)*/
    )
    // TODO: We get an exception in case it is a null value

    val schema = StructType(
      List(
        StructField("dob", org.apache.spark.sql.types.DoubleType, true),
        StructField("monthNum", org.apache.spark.sql.types.IntegerType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    val expectedReportDF = Seq(
      (".monthNum","","7","IntegerType","Daas","monthNum","monthNum","PNRData","resource/sample_Inventory.avro","MARS","N"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    outputReportDF.show()
    expectedReportDF.show()
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)

  }

  it("TEST : CASE 14 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 14 -  Dataframe with StructType columns")

    val schema = StructType(
      List(
        StructField("passport", StructType(
          StructField("firstName", StringType, true)::
            StructField("age", IntegerType, true) :: Nil
        ), true)
      )
    )
    val pax = Seq(
      Row(Row("Rogger", 54)),
      Row(Row("Trevor", 23)),
      Row(Row("Michael", null))
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(pax),
      schema
    )
    val outputReportDF = processInventroy(df, configObject, configObject.files(0), spark)
    val outputDataset = sqlContext.createDataFrame(outputReportDF.rdd, outputSchema)

    //TODO: Bug when creating struct with Integer field, it comes with a dot
    val expectedReportDF = Seq(
      (".passport","","","StructType(StructField(firstName,StringType,true), StructField(age,IntegerType,true))","Daas","passport","passport","PNRData","resource/sample_Inventory.avro","MARS","-"),
      (".passport.firstName","passport","Rogger","StringType","Daas","passport.firstName","firstName","PNRData","resource/sample_Inventory.avro","MARS","Y"),
      (".passport.age",".passport","54","IntegerType","Daas","passport.age","age","PNRData","resource/sample_Inventory.avro","MARS","N"))
      .toDF("name","parent","SAMPLE_DATA","DATA_TYPE","SATELLITE_TABLE","SOURCE_ELEMENT","ATTRIBUTE","MESSAGE_TYPE","MODELLED_PATH","SOURCE_SYSTEM","PERSONAL_DATA")
    outputDataset.show()
    expectedReportDF.show()
    assertSmallDataFrameEquality(outputDataset, expectedReportDF)
  }
  it("TEST : CASE 15 - DATA DICTIONARY (TO PROCESS RECORDS)") {

    log.info("[INFO] GDPR : CASE 1 -  DATA DICTIONARY (TO PROCESS RECORDS)")

    val schema = StructType(
      StructField("name", StringType, true) ::
        StructField("parent", StringType, true) ::
        StructField("SAMPLE_DATA", StringType, true) ::
        StructField("DATA_TYPE", StringType, true) ::
        StructField("SATELLITE_TABLE", StringType, true) ::
        StructField("SOURCE_ELEMENT", StringType, true) ::
        StructField("ATTRIBUTE", StringType, true) ::
        StructField("MESSAGE_TYPE", StringType, true) ::
        StructField("MODELLED_PATH", StringType, true) ::
        StructField("SOURCE_SYSTEM", StringType, true) ::
        StructField("PERSONAL_DATA", StringType, true) :: Nil)
    val outputDataset = try {
      sqlContext.createDataFrame(createInventory(configObject, configObject.files(2), spark).rdd, schema)
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

    val expected_toProcess = null
    assert(expected_toProcess==outputDataset)
    log.info("[INFO] TESTCASE PASSED : EXPECTED OUTPUT IS MATCHING WITH ACTUAL OUTPUT")
  }

}
