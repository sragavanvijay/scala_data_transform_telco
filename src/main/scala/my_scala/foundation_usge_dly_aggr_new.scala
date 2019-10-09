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



object foundation_usge_dly_aggr_new {

  val spark = SparkSession
    .builder()
    .appName("foundation_usge_dly_aggr")
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
    // 3. log level
    // 4. error catalogue file

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
      //General Build Parameters
      b_args("source_drop_duplicates_flag") = "N"  //set to Y or N

      //vw_usge
      b_args("vw_usge_scd_type") = "CURRENT_SCD1"
      b_args("vw_usge_db") = "dev11_dp_ntgt"
      b_args("vw_usge_obj") = "vw_usge"
      b_args("vw_usge_current_scd1_window_in_days") = "40"
      b_args("vw_usge_current_scd1_timestamp_column_name") = "evnt_strt_ts"
      b_args("vw_usge_delta_scd1_timestamp_column_name") = "evnt_strt_ts"

      //target table and DB names

      val target_table_scd_type = "CURRENT_SCD1"
      val target_table_db = "dev11_dp_ntgt"
      val target_table_obj = "usge_dly_aggr"
      val target_table_key_columns = "subs_key,evnt_strt"
      val target_table_aggr_grain = "DAILY"
      val target_table_partition_columns = "evnt_strt"


      logger.info("Dumping Build Args: " + b_args)

      logger.info("******************Reading table: vw_subs *********************")
      createTableDF("vw_usge",run_date,current_date,b_args,logger)
      logger.info("******************finished reading table*********************")

      // getting the proccess SQL based on the aggrigation level(Daily/Monthly/Yearly)
      var processedSQL = ""
      if (target_table_aggr_grain == "DAILY")
      {
        processedSQL = "select subs_key,   COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(DWLD_VLUM_QTY,0) END),0) AS DWLD_VLUM_QTY,  COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(UPLD_VLUM_QTY,0) END),0) AS UPLD_VLUM_QTY,  COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(ACTL_DATA_VLUM_QTY,0) END),0) AS ACTL_DATA_VLUM_QTY,  COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(RND_UNIT_CNT,0) END),0) AS DATA_RND_UNIT_QTY,to_date(evnt_strt_ts) AS aggr_grain  from " + b_args("vw_usge_obj") +"  group by subs_key, to_date(evnt_strt_ts)"
      }
      else if (target_table_aggr_grain == "MONTHLY")
      {
        processedSQL = "select subs_key,  COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(DWLD_VLUM_QTY,0) END),0) AS DWLD_VLUM_QTY,  COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(UPLD_VLUM_QTY,0) END),0) AS UPLD_VLUM_QTY,  COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(ACTL_DATA_VLUM_QTY,0) END),0) AS ACTL_DATA_VLUM_QTY,  COALESCE(SUM(CASE WHEN SERV_TYPE_CD='D' THEN coalesce(RND_UNIT_CNT,0) END),0) AS DATA_RND_UNIT_QTY,concat(substring(to_date(evnt_strt_ts),1,7) , '-01') AS aggr_grain  from " + b_args("vw_usge_obj") +"  group by subs_key, concat(substring(to_date(evnt_strt_ts),1,7), '-01')"
      }

      logger.info("******************Start to process the result DF*********************")
      logger.info("Processed SQL is: " + processedSQL)

      val processedDF = dataProcessor(processedSQL).toDF()

      logger.info("******************Finished process the result DF*********************")
      logger.info("******************Start writing to target table*********************")
      writeToHiveTableWithPartition(target_table_db, target_table_obj, processedDF, "overwrite", target_table_partition_columns)

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

    var sourceDF= spark.sql(extractSql).toDF()
    if (b_args("source_drop_duplicates_flag") == "Y") {
      sourceDF= sourceDF.dropDuplicates()
    }

    //var sourceDF= spark.sql(extractSql).toDF()
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
