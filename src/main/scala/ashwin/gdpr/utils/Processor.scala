package ashwin.gdpr.utils

import ashwin.gdpr.entities.{ConfigObjects, ConfigParams}
import com.emirates.helix.gdpr.entities.ConfigObjects
import com.emirates.helix.gdpr.entities.ConfigParams
import com.emirates.helix.gdpr.main.DataDictionary.{getElement, log, removeDot}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StructField

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
  * Generic code for parsing the different data types and getting sample values from Dataframe
  *
  * @author Ashwin Kumar
  */
object Processor {
  LogManager.getRootLogger.setLevel(Level.WARN)
  val logger = org.apache.log4j.LogManager.getLogger("ProcessorLogger")

  /**
    * This function read the dataframe based on the provided file type
    *
    * @param confObject
    * @param configParams
    * @param spark
    * @return
    */
  def createInventory(confObject: ConfigObjects, configParams: ConfigParams, spark: SparkSession): DataFrame = {

  if (configParams.fileType.equals("avro")) {
    processInventroy(spark.read.format("com.databricks.spark.avro").load(configParams.modelledPath), confObject, configParams, spark)
  } else if (configParams.fileType.equals("parquet")) {
    processInventroy(spark.read.parquet(configParams.modelledPath), confObject, configParams, spark)
  } else {
    throw new Exception("Optional file formats are: 'avro', 'parquet'")
  }
  }

  /**
    * This process reads the dataframe and identifies the columns along with datatype and sample values
    *
    * @param dataframe
    * @param confObject
    * @param confParams
    * @param spark
    * @return
    */
  def processInventroy(dataframe: DataFrame, confObject: ConfigObjects, confParams: ConfigParams, spark: SparkSession): DataFrame = {

    val arrayRow = dataframe.take(confParams.recordsToscan)
    var metaList = mutable.MutableList[(String, String, String, String)]()
    dataframe.printSchema()
    dataframe.schema.fields.foreach { columnData =>
      var nullCheck = true
      breakable {
        for (i <- 0 to arrayRow.length - 1) {
          if (arrayRow(i).isNullAt(arrayRow(i).fieldIndex(columnData.name)) || arrayRow(i) == null) {
          } else {
            nullCheck = false
            checkDataType(arrayRow(i), metaList, columnData, "")
            break()
          }
        }
      }
      if (nullCheck)
        checkDataType(null, metaList, columnData, "")
    }

    val df = spark.createDataFrame(metaList.toSeq).toDF("name", "parent", "SAMPLE_DATA", "DATA_TYPE")
    val outputDf = df.withColumn("SATELLITE_TABLE", lit(confParams.satelliteTable))
      .withColumn("SOURCE_ELEMENT", removeDot(col("name")))
      .withColumn("ATTRIBUTE", getElement(col("name")))
      .withColumn("MESSAGE_TYPE", lit(confParams.messageType))
      .withColumn("MODELLED_PATH", lit(confParams.modelledPath))
      .withColumn("SOURCE_SYSTEM", lit(confParams.sourceSystem))
      .withColumn("PERSONAL_DATA", when(col("DATA_TYPE") === ("StringType") || col("DATA_TYPE") === ("IntegerType") || col("DATA_TYPE") === ("LongType"), Classifier.potentialPD(col("name"), lit(confObject.prfxSet), lit(confObject.sfxSet), lit(confObject.parAttSet))).otherwise(lit("-")))
    outputDf
  }

  /**
    * Checks the datatype and calls the corresponding function to process the columns
    *
    * @param rowData
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def checkDataType(rowData: Row, metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {

    if (columnData.dataType.isInstanceOf[org.apache.spark.sql.types.StringType]) {

      processString(rowData, metaList, columnData, parentAttribute)
    }
    else if (columnData.dataType.isInstanceOf[org.apache.spark.sql.types.LongType]) {

      processLong(rowData, metaList, columnData, parentAttribute)
    }
    else if (columnData.dataType.isInstanceOf[org.apache.spark.sql.types.IntegerType]) {

      processInteger(rowData, metaList, columnData, parentAttribute)
    }
    else if (columnData.dataType.isInstanceOf[org.apache.spark.sql.types.StructType]) {

      if (rowData == null) {
        processNullStruct(metaList, columnData, parentAttribute)
      } else {
        if (rowData.isNullAt(rowData.fieldIndex(columnData.name)))
          processNullStruct(metaList, columnData, parentAttribute)
        else
          processStruct(rowData.getStruct(rowData.fieldIndex(columnData.name)), metaList, columnData, parentAttribute)
      }

    }
    else if (columnData.dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType]) {

      if (rowData == null) {
        processNullArray(metaList, columnData, parentAttribute)
      } else {
        if (rowData.isNullAt(rowData.fieldIndex(columnData.name)) || rowData.getAs[scala.collection.mutable.WrappedArray[Any]](columnData.name).length == 0)
          processNullArray(metaList, columnData, parentAttribute)
        else {
          if (rowData.getAs[scala.collection.mutable.WrappedArray[Any]](columnData.name)(0) != null)
            processArray(rowData.getAs[scala.collection.mutable.WrappedArray[Any]](columnData.name), metaList, columnData, parentAttribute)
          else
            processNullArray(metaList, columnData, parentAttribute)
        }
      }

    }
    else if (columnData.dataType.isInstanceOf[org.apache.spark.sql.types.MapType]) {
      processMap(rowData, metaList, columnData, parentAttribute)
    }
    else {
      log.error("[ERROR] New DataType Found..." + columnData.name + "DataType is" + columnData.dataType)
      metaList
    }
  }

  /**
    * This part of the code is responsible to process for Map data types
    *
    * @param rowData
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processMap(rowData: Row, metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {

    metaList.+=((parentAttribute + "." + columnData.name, "", "", columnData.dataType.toString))
    val inputMap = rowData.getAs[Map[String, String]](columnData.name)
    inputMap.foreach { keyVal =>
      metaList.+=((parentAttribute + "." + columnData.name + "." + keyVal._1, columnData.name, keyVal._2, "StringType"))
    }
    metaList
  }

  /**
    * This part of the code is responsible to process for Array data types
    *
    * @param rowData
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processArray(rowData: mutable.WrappedArray[Any], metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {

    metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
    val childElement = columnData.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType]
    val childArrayRow = rowData(0)
    if (childElement.elementType.isInstanceOf[Map[String, String]] || childElement.elementType.isInstanceOf[org.apache.spark.sql.types.MapType]) {
      val metaMap = scala.collection.mutable.Map[String, String]()

      rowData.foreach { childArrayMapRow =>
        childArrayMapRow.asInstanceOf[Map[String, String]].foreach { keyVal =>
          metaMap += (parentAttribute + "." + columnData.name + "." + keyVal._1 -> keyVal._2)
        }
      }
      metaMap.foreach { keyVal =>
        metaList.+=((keyVal._1, columnData.name, keyVal._2, "StringType"))
      }
    }

    else if (childElement.elementType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
      val childStruct = childElement.elementType.asInstanceOf[org.apache.spark.sql.types.StructType]
      val childRow = childArrayRow.asInstanceOf[org.apache.spark.sql.Row]
      var structIndex = 0
      childStruct.fields.foreach { keyVal =>
        if (keyVal.dataType.isInstanceOf[org.apache.spark.sql.types.StringType]) {
          if (childRow(structIndex) != null)
            metaList.+=((parentAttribute + "." + columnData.name + "." + keyVal.name, columnData.name, childRow(structIndex).toString, keyVal.dataType.toString))
          else
            metaList.+=((parentAttribute + "." + columnData.name + "." + keyVal.name, columnData.name, "", keyVal.dataType.toString))

        } else {
          checkDataType(childRow, metaList, childStruct(structIndex), parentAttribute + "." + columnData.name)
        }
        structIndex = structIndex + 1
      }

    }
    else if (childElement.elementType.isInstanceOf[org.apache.spark.sql.types.StringType]) {

      metaList.+=((parentAttribute + "." + columnData.name, columnData.name, childArrayRow.toString, childElement.elementType.toString))

    } else {
      log.error("[ERROR] New DataType Found..." + childElement.toString)
    }

    metaList
  }

  /**
    * This part of the code is responsible to process for NULL Array data types
    *
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processNullArray(metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {

    metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
    val childElement = columnData.dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType]

    if (childElement.elementType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
      val childStruct = childElement.elementType.asInstanceOf[org.apache.spark.sql.types.StructType]
      var structIndex = 0
      childStruct.fields.foreach { keyVal =>
        if (keyVal.dataType.isInstanceOf[org.apache.spark.sql.types.StringType]) {
          metaList.+=((parentAttribute + "." + columnData.name + "." + keyVal.name, columnData.name, "", keyVal.dataType.toString))
        } else {
          checkDataType(null, metaList, childStruct(structIndex), parentAttribute + "." + columnData.name)
        }
        structIndex = structIndex + 1
      }

    }
    else if (childElement.isInstanceOf[org.apache.spark.sql.types.StringType]) {

      metaList.+=((parentAttribute + "." + columnData.name, columnData.name, "", childElement.elementType.toString))

    }
    else {

      log.error("[ERROR] New DataType Found..." + childElement.toString)

    }
    metaList
  }

  /**
    * This part of the code is responsible to process for Long data types
    *
    * @param rowData
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processLong(rowData: Row, metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {
    if (rowData == null) {
      metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
    } else {
      if (rowData.isNullAt(rowData.fieldIndex(columnData.name)))
        metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
      else
        metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, rowData.getLong(rowData.fieldIndex(columnData.name)).toString, columnData.dataType.toString))
    }
    metaList
  }

  /**
    * This part of the code is responsible to process for Integer data types
    *
    * @param rowData
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processInteger(rowData: Row, metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {
    if (rowData == null) {
      metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
    } else {
      if (rowData.isNullAt(rowData.fieldIndex(columnData.name)))
        metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
      else
        metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, rowData.getInt(rowData.fieldIndex(columnData.name)).toString, columnData.dataType.toString))
    }
    metaList
  }

  /**
    * This part of the code is responsible to process for String data types
    *
    * @param rowData
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processString(rowData: Row, metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {
    if (rowData == null) {
      metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
    } else {
      if (rowData.isNullAt(rowData.fieldIndex(columnData.name)))
        metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
      else
        metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, rowData.getAs[String](rowData.fieldIndex(columnData.name)), columnData.dataType.toString))
    }
    metaList
  }

  /**
    * This part of the code is responsible to process for NULL Struct data types
    *
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processNullStruct(metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {
    metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
    val structMetaData = columnData.dataType.asInstanceOf[org.apache.spark.sql.types.StructType]
    var structIndex = 0
    structMetaData.fields.foreach { keyVal =>
      if (keyVal.dataType.isInstanceOf[org.apache.spark.sql.types.StringType]) {

        metaList.+=((parentAttribute + "." + keyVal.name, columnData.name, "", keyVal.dataType.toString))

      } else {
        checkDataType(null, metaList, structMetaData(structIndex), parentAttribute + "." + columnData.name)
      }
      structIndex = structIndex + 1
    }
    metaList
  }

  /**
    * This part of the code is responsible to process for Struct data types
    *
    * @param rowData
    * @param metaList
    * @param columnData
    * @param parentAttribute
    * @return
    */
  def processStruct(rowData: Row, metaList: mutable.MutableList[(String, String, String, String)], columnData: StructField, parentAttribute: String): mutable.MutableList[(String, String, String, String)] = {
    metaList.+=((parentAttribute + "." + columnData.name, parentAttribute, "", columnData.dataType.toString))
    val structMetaData = columnData.dataType.asInstanceOf[org.apache.spark.sql.types.StructType]
    var structIndex = 0
    structMetaData.fields.foreach { keyVal =>
      if (keyVal.dataType.isInstanceOf[org.apache.spark.sql.types.StringType]) {
        if (rowData(structIndex) != null)
          metaList.+=((parentAttribute + "." + columnData.name + "." + keyVal.name, columnData.name, rowData(structIndex).toString, keyVal.dataType.toString))
        else
          metaList.+=((parentAttribute + "." + columnData.name + "." + keyVal.name, columnData.name, "", keyVal.dataType.toString))
      } else {
        checkDataType(rowData, metaList, structMetaData(structIndex), parentAttribute + "." + columnData.name)
      }
      structIndex = structIndex + 1
    }
    metaList
  }

}
