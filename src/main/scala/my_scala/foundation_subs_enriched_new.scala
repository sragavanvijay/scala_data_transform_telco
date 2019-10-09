package au.com.bdp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable
import scala.io.Source.fromFile


object foundation_subs_enriched_new {

  val spark = SparkSession
    .builder()
    .appName("foundation_subs_enriched")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict") // To read or write a particular partition from hive table dynamically
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]) {

    // Return error message if no arguments passed
    if (args.length == 0) throw new Exception("No command argments found")

    //Instanciate logger to capure logging info
    val logger = Logger.getRootLogger()//.getLogger(getClass.getName())

    // Arguments to pass
    // 1. run_date
    // 2. run_dateFormat (eg. yyyy-MM-dd)

    // Initialize arguments
    // Date formatting
    try {

      val log_level = args(2)

      //setting up the logging level according to the value passed
      val log_levels: mutable.HashMap[String, Level] = mutable.HashMap()
      log_levels.put("ALL", Level.ALL)
      log_levels.put("TRACE", Level.TRACE)
      log_levels.put("DEBUG", Level.DEBUG)
      log_levels.put("INFO", Level.INFO)
      log_levels.put("WARN", Level.WARN)
      log_levels.put("ERROR", Level.ERROR)
      log_levels.put("FATAL", Level.FATAL)
      log_levels.put("OFF", Level.OFF)

      //setting log level
      logger.setLevel(log_levels(log_level))

      logger.info("******************This is a start of execution*********************")

      val run_date_format = args(1)
      val run_date = LocalDate.parse(args(0), DateTimeFormatter.ofPattern(run_date_format))
      val current_date = java.time.LocalDate.now

      //Build Parameters
      val b_args: mutable.HashMap[String, String] = mutable.HashMap()
      //vw_subs
      b_args("vw_subs_scd_type") = "DELTA_SCD4"
      b_args("vw_subs_active_db") = "dev11_dp_ntgt"
      b_args("vw_subs_hist_db") = "dev11_dp_ntgt"
      b_args("vw_subs_db") = ""
      b_args("vw_subs_active_obj") = "vw_subs_active"
      b_args("vw_subs_hist_obj") = "vw_subs_hist"
      b_args("vw_subs_obj") = ""
      b_args("vw_subs_key_columns") = "subs_key"
      b_args("vw_subs_delta_timestamp_column_name") = "ecf_nsrt_ts"

      if (run_date.equals(current_date)) {
        b_args("vw_subs_obj") = b_args("vw_subs_active_obj")
        b_args("vw_subs_db") = b_args("vw_subs_active_db")
      }
      else {
        b_args("vw_subs_obj") = b_args("vw_subs_hist_obj")
        b_args("vw_subs_db") = b_args("vw_subs_hist_db")
      }
      //val vw_subs_delta_timestamp_column_format = ""

      //vw_subs_stts
      b_args("vw_subs_stts_scd_type") = "DELTA_SCD4"
      b_args("vw_subs_stts_active_db") = "dev11_dp_ntgt"
      b_args("vw_subs_stts_hist_db") = "dev11_dp_ntgt"
      b_args("vw_subs_stts_db") = ""
      b_args("vw_subs_stts_active_obj") = "vw_subs_stts_active"
      b_args("vw_subs_stts_hist_obj") = "vw_subs_stts_hist"
      b_args("vw_subs_stts_obj") = ""
      b_args("vw_subs_stts_key_columns") = "subs_stts_key"
      b_args("vw_subs_stts_delta_timestamp_column_name") = "ecf_nsrt_ts"

      if (run_date.equals(current_date)) {
        b_args("vw_subs_stts_obj") = b_args("vw_subs_stts_active_obj")
        b_args("vw_subs_stts_db") = b_args("vw_subs_stts_active_db")
      }
      else {
        b_args("vw_subs_stts_obj") = b_args("vw_subs_stts_hist_obj")
        b_args("vw_subs_stts_db") = b_args("vw_subs_stts_hist_db")
      }
      //val vw_subs_delta_timestamp_column_format = ""

      //vw_cust_type
      b_args("vw_cust_type_scd_type") = "DELTA_SCD4"
      b_args("vw_cust_type_active_db") = "dev11_dp_ntgt"
      b_args("vw_cust_type_hist_db") = "dev11_dp_ntgt"
      b_args("vw_cust_type_db") = ""
      b_args("vw_cust_type_active_obj") = "vw_cust_type_active"
      b_args("vw_cust_type_hist_obj") = "vw_cust_type_hist"
      b_args("vw_cust_type_obj") = ""
      b_args("vw_cust_type_key_columns") = "cust_type_key"
      b_args("vw_cust_type_delta_timestamp_column_name") = "ecf_nsrt_ts"

      if (run_date.equals(current_date)) {
        b_args("vw_cust_type_obj") = b_args("vw_cust_type_active_obj")
        b_args("vw_cust_type_db") = b_args("vw_cust_type_active_db")
      }
      else {
        b_args("vw_cust_type_obj") = b_args("vw_cust_type_hist_obj")
        b_args("vw_cust_type_db") = b_args("vw_cust_type_hist_db")
      }

      //val vw_subs_delta_timestamp_column_format = ""

      //target table and DB names
      val target_table_scd_type = "SNAPSHOT"
      val target_table_db = "dev11_dp_ntgt"
      val target_table_obj = "subs_enriched"
      val target_table_key_columns = "subs_key,run_date"
      val target_table_partition_columns = "run_date"

      logger.info("Dumping Build Args: " + b_args)

      logger.info("******************Reading table: vw_subs *********************")
      createTableDF("vw_subs",run_date,current_date,b_args,logger)
      logger.info("******************finished reading table*********************")

      logger.info("******************Reading table: vw_subs_stts *********************")
      createTableDF("vw_subs_stts",run_date,current_date,b_args,logger)
      logger.info("******************finished reading table*********************")

      logger.info("******************Reading table: vw_cust_type *********************")
      createTableDF("vw_cust_type",run_date,current_date,b_args,logger)
      logger.info("******************finished reading table*********************")


      logger.info("******************Start to process the result DF*********************")
      val processedSQL = "select  a.subs_key as subs_key, a.subs_stts_key as subs_stts_key, coalesce(b.subs_stts_ds,'~UNK') as subs_stts_ds,  a.cust_type_key as cust_type_key, coalesce(c.cust_type_ds,'~UNK') as cust_type_ds, " + """"""" +run_date + """"""" + " as run_date from " + b_args("vw_subs_obj") + " a left join " + b_args("vw_subs_stts_obj") + " b on a.subs_stts_key = b.subs_stts_key" + " left join " + b_args("vw_cust_type_obj") + " c on a.cust_type_key = c.cust_type_key"
      logger.info("Processed SQL is: " + processedSQL)
      val processedDF = dataProcessor(processedSQL).toDF()
      //write to target table
      // write a if statement to write according to the SCD-type
      logger.info("******************Finished process the result DF*********************")

      writeToHiveTableWithPartition(target_table_db, target_table_obj, processedDF, "overwrite", target_table_partition_columns)
      logger.info("******************Finished writing to Table*********************")
    }
    catch {
      case e: Exception =>
        val errArray = errorCatch(e)
        logger.error(errArray.mkString("\n"))
        throw e
    }

  }


  def createTableDF(table:String, run_date:LocalDate, current_date:LocalDate, b_args:mutable.HashMap[String, String], logger: Logger) = {

    // Identify SCD type and date, construct relevant SQL and execute SQL, sending results to temp views
    // create source data frames
    //////////////////////
    var processedSQL = ""

    if (b_args(table + "_scd_type") == "CURRENT_SCD1") {
      if (b_args(table + "_current_scd1_window_in_days") != ""){
        processedSQL = "SELECT * FROM " + b_args(table + "_db") + "." + b_args(table + "_obj")
        processedSQL += " WHERE to_date(" + b_args(table + "_current_scd1_timestamp_column_name") + ") >= date_add(" + """"""" + run_date + """"""" + ",-" + b_args(table + "_current_scd1_window_in_days") + ")"
      }
      else {
        processedSQL = "SELECT * FROM " + b_args(table + "_db") + "." + b_args(table + "_obj")
      }
    }
    else if (b_args(table + "_scd_type") == "DELTA_SCD4" && run_date.equals(current_date)) {
      processedSQL = "SELECT * FROM " + b_args(table + "_active_db") + "." + b_args(table + "_active_obj")
    }
    else if (b_args(table + "_scd_type") == "DELTA_SCD4") {
      processedSQL = "SELECT A.* from (SELECT *, RANK() OVER (PARTITION BY " + b_args(table + "_key_columns") + " ORDER BY " + b_args(table + "_delta_timestamp_column_name") + " desc) as rnk FROM " + b_args(table + "_hist_db") + "." + b_args(table + "_hist_obj") + " WHERE to_date(" + b_args(table + "_delta_timestamp_column_name") + ") <= " + """"""" + run_date + """"""" + ") A where A.rnk = 1"
    }
    else if (b_args(table + "_scd_type") == "SNAPSHOT") {
      processedSQL = "SELECT * FROM " + b_args(table + "_db") + "." + b_args(table + "_obj") + " WHERE " + b_args(table + "_snapshot_date_column_name") + " = " + """"""" + run_date + """""""
    }
    val extractSql = processedSQL
    logger.info("Source Reader SQL is: " + processedSQL)
    val sourceDF= spark.sql(extractSql).toDF()
    sourceDF.createOrReplaceTempView(b_args(table + "_obj"))

  }

  def dataProcessor(processSql:String):DataFrame ={

    val processedData= spark.sql(processSql).toDF()
    processedData.createOrReplaceTempView("processedData")
    return processedData
  }

  def writeToHiveTableWithPartition(tgtDbNm:String, tgtTblNm:String, resultDF:DataFrame, writeMode:String, partitionCols:String):Unit={

    val insert_sql = "insert overwrite table " + tgtDbNm + "." + tgtTblNm + " partition(" + partitionCols + ") select * from processedData"
    spark.sql(insert_sql)

    // Following code can be used with Spark 2.3+ instead of Insert Overwrite into partition (as above)
    //    resultDF.write
    //      .format("hive")
    //      //.option("path", "/pathHiveLocation") // Can add the defined path to save the files in HDFS
    //      .mode(writeMode)
    //      .partitionBy(colNames = partitionCols)
    //      //.partitionBy("dp_actv_flg","dp_year_mnth") // Can add static partition columns
    //      .saveAsTable(tgtDbNm + "." + tgtTblNm)
  }

  def writeToHiveTable(tgtDbNm:String, tgtTblNm:String, resultDF:DataFrame, writeMode:String):Unit={
    resultDF.write.format("hive").mode(writeMode).saveAsTable(tgtDbNm + "." + tgtTblNm)
  }

  def errorCatch(ex: Exception): Array[String] = {
    // Declares elements of exception as an array
    val elements = ex.getStackTrace

    // Splits exception string in order to get just exception type
    val errType = ex.toString().split(":")(0)

    // Try-catch to get message with exception. (Exceptions do not always carry messages, would lead to out of bounds).
    var errMsg = ""

    try {
      errMsg = ex.toString().split(": ")(1)
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        errMsg = "No error message"
    }

    // Use elements and remove unnecessary chars to extract location and line number
    val errLocation =  elements(1).toString().split("\\(")(0)
    val errLinNum = elements(0).toString().split(":")(1).replace(")","")

    // Return array of error info
    val errArray:Array[String] = new Array[String](4)
    errArray(0) = "Error: " + errType
    errArray(1) = "Message: " + errMsg
    errArray(2) = "In class: " + errLocation
    errArray(3) = "On line number: " + errLinNum
    return errArray
  }
}
