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


object consumption_subs_profile_new {

  type OptionMap = Map[Symbol, Any]

  val spark = SparkSession
    .builder()
    .appName("consumption_subs_profile_load")
    //.config("spark.master", "local")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict") // To read or write a particular partition from hive table dynamically
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]) {

    // Return error message if no arguments passed
    if (args.length == 0) throw new Exception("No command argments found")

    val arglist = args.toList
    //val options = parseCLIArgs(Map(), arglist)

    val current_date = java.time.LocalDate.now
    System.setProperty("current.date",new SimpleDateFormat("yyyy-MM-dd").format(new Date()))

    //setting up logger
    val logger = Logger.getRootLogger()//.getLogger(getClass.getName())
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
    //logger.setLevel(log_level)
    logger.info("******************This is a start of execution*********************")

    // Return error message if no arguments passed
    if (args.length == 0) {
      println("Error: Please enter arguments.")
    }

    // Arguments to pass
    // 1. run_date
    // 2. run_dateFormat (eg. yyyy-MM-dd)
    // 3. log level

    val run_date_format = args(1)
    val run_date = LocalDate.parse(args(0), DateTimeFormatter.ofPattern(run_date_format))

    //Build Parameters

    val b_args: mutable.HashMap[String, String] = mutable.HashMap()
    //vw_subs
    b_args("vw_subs_scd_type") = "DELTA_SCD4"
    b_args("vw_subs_active_db") = "dev11_dp_ntgt"
    b_args("vw_subs_hist_db") = "dev11_dp_ntgt"
    b_args("vw_subs_active_obj") = "vw_subs_active"
    b_args("vw_subs_hist_obj") = "vw_subs_hist"
    b_args("vw_subs_key_columns") = "subs_key"
    b_args("vw_subs_delta_timestamp_column_name") = "ecf_nsrt_ts"
    //val vw_subs_delta_timestamp_column_format = ""
    if (run_date.equals(current_date)) {
      b_args("vw_subs_obj") = b_args("vw_subs_active_obj")
      b_args("vw_subs_db") = b_args("vw_subs_active_db")
    }
    else {
      b_args("vw_subs_obj") = b_args("vw_subs_hist_obj")
      b_args("vw_subs_db") = b_args("vw_subs_hist_db")
    }

    //subs_enriched
    b_args("subs_enriched_scd_type") = "SNAPSHOT"
    b_args("subs_enriched_db") = "dev11_dp_ntgt"
    b_args("subs_enriched_obj") = "subs_enriched"
    b_args("subs_enriched_key_columns") = "subs_key,run_date"
    b_args("subs_enriched_snapshot_date_column_name") = "run_date"
    b_args("subs_enriched_snapshot_date_column_format") = "yyyy-MM-dd"

    //usge_dly_aggr

    b_args("usge_dly_aggr_scd_type") = "CURRENT_SCD1"
    b_args("usge_dly_aggr_db") = "dev11_dp_ntgt"
    b_args("usge_dly_aggr_obj") = "usge_dly_aggr"
    b_args("usge_dly_aggr_key_columns") = "subs_key,aggr_grain"
    //b_args("usge_dly_aggr_delta_timestamp_column_name") = ""


    //vw_acct
    b_args("vw_acct_scd_type") = "DELTA_SCD4"
    b_args("vw_acct_active_db") = "dev11_dp_ntgt"
    b_args("vw_acct_hist_db") = "dev11_dp_ntgt"
    b_args("vw_acct_active_obj") = "vw_acct_active"
    b_args("vw_acct_hist_obj") = "vw_acct_hist"
    b_args("vw_acct_key_columns") = "acct_key"
    b_args("vw_acct_delta_timestamp_column_name") = "ecf_nsrt_ts"
    //val vw_subs_delta_timestamp_column_format = ""
    if (run_date.equals(current_date)) {
      b_args("vw_acct_obj") = b_args("vw_acct_active_obj")
      b_args("vw_acct_db") = b_args("vw_acct_active_db")
    }
    else {
      b_args("vw_acct_obj") = b_args("vw_acct_hist_obj")
      b_args("vw_acct_db") = b_args("vw_acct_hist_db")
    }

    //vw_cust
    b_args("vw_cust_scd_type") = "DELTA_SCD4"
    b_args("vw_cust_active_db") = "dev11_dp_ntgt"
    b_args("vw_cust_hist_db") = "dev11_dp_ntgt"
    b_args("vw_cust_active_obj") = "vw_cust_active"
    b_args("vw_cust_hist_obj") = "vw_cust_hist"
    b_args("vw_cust_key_columns") = "cust_key"
    b_args("vw_cust_delta_timestamp_column_name") = "ecf_nsrt_ts"
    if (run_date.equals(current_date)) {
      b_args("vw_cust_obj") = b_args("vw_cust_active_obj")
      b_args("vw_cust_db") = b_args("vw_cust_active_db")
    }
    else {
      b_args("vw_cust_obj") = b_args("vw_cust_hist_obj")
      b_args("vw_cust_db") = b_args("vw_cust_hist_db")
    }
    //val vw_subs_delta_timestamp_column_format = ""

    //target table and DB names

    val target_table_scd_type = "SNAPSHOT"
    val target_table_db = "dev11_dp_ntgt"
    val target_table_obj = "subs_profile"
    val target_table_key_columns = "subs_key,run_date"
    val target_table_partition_columns = "run_date"


    // Identify SCD type and date, construct relevant SQL and execute SQL, sending results to temp views
    // create source data frames
    //////////////////////
    logger.info("Dumping Build Args: " + b_args)

    logger.info("******************Reading table: vw_subs *********************")
    createTableDF("vw_subs",run_date,current_date,b_args,logger)
    logger.info("******************finished reading table*********************")

    logger.info("******************Reading table: subs_enriched *********************")
    createTableDF("subs_enriched",run_date,current_date,b_args,logger)
    logger.info("******************finished reading table*********************")

    logger.info("******************Reading table: vw_cust *********************")
    createTableDF("vw_cust",run_date,current_date,b_args,logger)
    logger.info("******************finished reading table*********************")

    logger.info("******************Reading table: vw_acct *********************")
    createTableDF("vw_acct",run_date,current_date,b_args,logger)
    logger.info("******************finished reading table*********************")

    logger.info("******************Reading table: usge_dly_aggr *********************")
    createTableDF("usge_dly_aggr",run_date,current_date,b_args,logger)
    logger.info("******************finished reading table*********************")

    logger.info("******************Started processing  result DF*********************")
    val processedSQL = "  \nselect " +
      " a.subs_key as subs_key, a.subs_id as subs_id, b.subs_stts_ds as subs_stts_ds, " +
      "a.subs_stts_key as subs_stts_key, \nb.cust_type_ds as cust_type_ds, " +
      "a.prim_rsrc_valu_txt  as SUBS_PRIM_RSRC_VALU, a.SERV_TECH_NM as SERV_TECH_NM," +
      " \na.PROD_NM as PROD_NM, a.orig_actv_ts as SUBS_ORIG_ACTV_TS, " +
      "a.subs_stts_ts  as SUBS_STTS_EFFT_TS, a.imei_id  as SOLD_IMEI_ID, " +
      "\na.prim_acct_key as  SUBS_PRIM_ACCT_KEY, a.CUST_TYPE_KEY as CUST_TYPE_KEY, " +
      "a.PROD_TYPE_KEY as PROD_TYPE_KEY, \na.main_bill_offr_key as SUBS_MAIN_OFFR_KEY, " +
      "a.PRIM_ACCT_KEY as PRIM_ACCT_KEY,  a.subs_stts_key as SUBS_STTS_RSN_KEY, " +
      "\ne.acct_key as acct_key, e.ACCT_ID as ACCT_ID, e.ACCT_STTS_KEY as ACCT_STTS_KEY, " +
      "e.XTRN_ID as XTRN_ID, \nf.CUST_KEY as CUST_KEY, f.CUST_ID as CUST_ID, " +
      "f.CUST_STAT_KEY as CUST_STAT_KEY, sum(g.dwld_vlum_qty) as dwld_vlum_qty, " +
      "\nsum(g.upld_vlum_qty) as upld_vlum_qty, sum(g.actl_data_vlum_qty) as actl_data_vlum_qty," +
      "\nsum(g.data_rnd_unit_qty) as data_rnd_unit_qty, " + """"""" + run_date + """"""" + " as run_date " +
      " from " +  b_args("vw_subs_obj") + " a " +
      "left join " +  b_args("subs_enriched_obj")  + " b " + " on a.subs_key = cast(b.subs_key as int) " +
      "left join " + b_args("vw_acct_obj") + " e " + "on a.prim_acct_key = e.acct_key \n" +
      "left join " + b_args("vw_cust_obj") + " f " + "on  a.cust_key  = f.cust_key " +
      "left join " + b_args("usge_dly_aggr_obj") + " g " + "on a.subs_key = g.subs_key \n" +
      "group by " +
      "a.subs_key,a.subs_id, b.subs_stts_ds, a.subs_stts_key,  b.cust_type_ds, a.prim_rsrc_valu_txt, a.SERV_TECH_NM, a.PROD_NM, a.orig_actv_ts,\n a.subs_stts_ts, a.imei_id, a.prim_acct_key, a.CUST_TYPE_KEY, a.PROD_TYPE_KEY, a.main_bill_offr_key, a.PRIM_ACCT_KEY, b.SUBS_STTS_DS,\n b.subs_stts_key, b.CUST_TYPE_DS, e.acct_key, e.ACCT_ID, e.ACCT_STTS_KEY, e.XTRN_ID, f.CUST_KEY, f.CUST_ID, f.CUST_STAT_KEY, g.dwld_vlum_qty,\n g.upld_vlum_qty, g.actl_data_vlum_qty,  g.data_rnd_unit_qty, g.SUBS_KEY, " + """"""" + run_date + """""""
    //val processedDF = dataProcessor1("select  a.subs_id as subs_id, a.subs_stts_id as subs_stts_id, a.subs_stts_ds as subs_stts_ds,  a.cust_type_id as cust_type_id,a.cust_type_ds as cust_type_ds, b.prim_rsrc_valu_txt  as prim_rsrc_valu_txt, a.run_date as run_date from subs_enriched a join " + b_args("vw_subs_obj") + " b on a.subs_id = b.subs_key").toDF()
    logger.info("Processed SQL is: " + processedSQL)

    val processedDF = dataProcessor(processedSQL)

    logger.info("******************Finished processing result DF*********************")
    //write to target table
    // write a if statement to write according to the SCD-type
    //logger.info("Processed SQL is: " + processedSQL)

    logger.info("******************Started writing result DF to target table*********************")

    writeToHiveTableWithPartition(target_table_db, target_table_obj, processedDF, "overwrite", target_table_partition_columns)
    logger.info("******************Finished writing result DF to target table*********************")

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

  def dataProcessor(processSql: String): DataFrame = {
    val processedData = spark.sql(processSql).toDF()
    processedData.createOrReplaceTempView("processedData")
    return processedData
  }

  def writeToHiveTableWithPartition(tgtDbNm: String, tgtTblNm: String, resultDF: DataFrame, writeMode: String, partitionCols: String): Unit = {
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


  def writeToHiveTable(tgtDbNm: String, tgtTblNm: String, resultDF: DataFrame, writeMode: String): Unit = {
    resultDF.write.format("hive").mode(writeMode).saveAsTable(tgtDbNm + "." + tgtTblNm)
  }

}