package com.fis.ecomm.validation

import org.slf4j.LoggerFactory
import org.apache.spark.sql._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import scala.collection.parallel.ForkJoinTaskSupport
import org.apache.spark.sql.SparkSession

object EcommBdaVapValidation extends Serializable with App {

case class targetSchemeTarget(gdg_position: Long, gdg_txoppos: Long, gdg_txind: String, gdg_opcode: String, gdg_timestamp: String, gdg_schema: String, gdg_table: String, id: Long, table_name: String, count_date: String, bda_count: Long, vap_count: Long, matched: String, count_diff: Long, date_column_name: String, inserted_date: String, runtime_sql: String)

  val spark = SparkSession
    .builder()
    .appName("EcommBdaVapValidation")
    .config("spark.debug.maxToStringFields", 3000)
    .config("spark.scheduler.mode", "FAIR")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  var application_id = spark.sparkContext.getConf.getAppId
  val logger = LoggerFactory.getLogger(getClass.getName)
  var exitCode=0
  var prop_location = "/tmp/Ecomm_BDA_VAP_Validation/EcommValidationConfig.properties"
  try { prop_location = args(0) }
  catch { case e: Throwable => logger.info("Use default property file: " + prop_location) }

  logger.info("Spark application Id: " + application_id)
  
  val start_time = new Timestamp(System.currentTimeMillis()).toString
  logger.info("EcommBdaVapValidation::job is started at %s", start_time)
  
  val props_rdd = spark.sparkContext.textFile(prop_location)
  val props = props_rdd.collect().toList.flatMap(x => x.split('=')).grouped(2).collect { case List(k, v) => k -> v }.toMap
  logger.info("Properties: %s", props.toString)
  val source_fil_list_Path=props("source_fil_list_Path")
  val target_path = props("target_validation_count")
  val target_schema = props("target_schema")
  val rerun_failed_days = props("rerun_failed_days")
  val num_thread = props("num_thread")
    
  val run_date = java.time.LocalDate.now.toString
  val run_date_formatted = LocalDate.parse(run_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  logger.info("Run date value: " + run_date_formatted)
  import spark.implicits._
  var accumulated_df = Seq.empty[targetSchemeTarget].toDF()

  val query_part2 =
    """_sql
           from
      WITH failedrun as (
           select table_name, count_date, runtime
           (   select upper(table_name) table_name, count_date, matched, runtime_sql, count_diff, ROW_NUMBER() OVER (PARTITION BY table_name, count_date ORDER BY extract_date desc, inserted_date desc) rn
               from """ + target_schema + """.bda_data_counts_validation
               where extract_date between date_add('""" + run_date_formatted + """',-""" + rerun_failed_days + """) and date_add('""" + run_date_formatted +
      """',-1)) a
           where a.rn = 1 and matched = 'N'),
      yes_conf as (
          select table_id, upper(table_name) table_name , date_column_name
          from """ + target_schema + """.bda_data_validation_conf
          where failed_rerun = 'Y')
          select t2.table_id, t1.table_name, t2.date_column_name, t1.count_date, t1.runtime_sql
          from failedrun t1
          inner join yes_conf t2 on t1.table_name = t2.table_name""".stripMargin

  def getParArray(list: Array[Row]) = {
    val tables = list.par
    tables.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(num_thread.toInt))
    tables
  }


  def runCount(table_id: Int, table_name: String, count_date: String, date_column_name: String, runtime_sql: String): Unit = {
    printf("\n table_id: %d - table_name: %s - count_date: %s - runtime_sql: %s ", table_id, table_name, count_date, runtime_sql)
    try {
        val df_count = spark.sql(runtime_sql)
        var bda_count=0L
        if (!df_count.head(1).isEmpty){
          val ANYbda_count = df_count.collect.toList(0).get(1)
          if (ANYbda_count != null)
            bda_count = ANYbda_count.toString.toLong
          println(bda_count)
        }

        import spark.implicits._
        var bda_count_df= Seq(targetSchemeTarget(1,1,"1","1","1","1","1", table_id, table_name, count_date,  bda_count, 0, "", 0 , date_column_name, "", runtime_sql)).toDF
        bda_count_df.createOrReplaceTempView("vw_bda_count_df")
        val compared_vap_sql = """
              WITH vaprun as (
               select table_name, target_date, record_count  as vap_count
               from
               (
                   select """+table_name+""" as table_name, target_date, record_count, ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY extract_date desc, inserted_date desc) rn
                   from """+target_schema+""".vap_to_bda_data_validation
                   where extract_date between '"""+count_date+"""' and to_date(current_date())
                   and table_name = 'CONTEXT_"""+ table_name +"""'
                   and target_date = '"""+ count_date +"""'
               ) a where a.rn  = 1 )
               Select CAST(t1.gdg_position AS bigint),
                    CAST(t1.gdg_txoppos AS bigint),
                    CAST(t1.gdg_txind AS string),
                    CAST(t1.gdg_opcode AS string),
                    CAST(t1.gdg_timestamp AS string),
                    CAST(t1.gdg_schema AS string),
                    CAST(t1.gdg_table AS string),
                    CAST(t1.id AS bigint),
                    CAST(t1.table_name AS string),
                    CAST(t1.count_date AS string),
                    CAST(t1.bda_count AS bigint),
                    CAST(t2.vap_count AS bigint),
                    CASE WHEN (NVL(t1.bda_count,0)==NVL(t2.vap_count,0)) THEN 'Y' ELSE 'N' END AS matched,
                    CAST(NVL(t1.bda_count,0) - NVL(t2.vap_count,0) AS bigint) AS count_diff,
                    CAST(t1.date_column_name AS STRING) as date_column_name,
                    CAST (now() AS STRING) AS  inserted_date,
                    CAST(t1.runtime_sql AS string)
               FROM vw_bda_count_df t1
               LEFT JOIN vaprun t2
               ON t1.table_name = t2.table_name""".stripMargin
        printf("compared_vap_sql: %s\n",compared_vap_sql)
        val vap_vs_bda_count_df = spark.sql(compared_vap_sql)
      accumulated_df = accumulated_df.union(vap_vs_bda_count_df)
    }
    catch {
      case e: Throwable =>
        println(e)
        printf("\n Errors happended for table_id: %d - table_name: %s - count_date: %s - runtime_sql: %s ", table_id, table_name, count_date, runtime_sql)
        val inserted_time = new Timestamp(System.currentTimeMillis()).toString
        import spark.implicits._
        var bda_count_failed_df= Seq(targetSchemeTarget(1,1,"1","1","1","1","1", table_id, table_name, count_date,  0, 0, "F", 0 , date_column_name, inserted_time, runtime_sql+" has failed")).toDF
        accumulated_df = accumulated_df.union(bda_count_failed_df)
        println("Save failed-record "+table_name+" into target bda_data_counts_validation is completed.")
    }

  }

  def refreshTable():Unit = {
    println("Refresh target table bda_data_counts_validation")
    val addPartitionStatmt = "ALTER TABLE "+target_schema+".bda_data_counts_validation add if not exists partition(extract_date='"+run_date_formatted.toString+"')"
    spark.sql(addPartitionStatmt)
    spark.sql("REFRESH TABLE "+target_schema+".bda_data_counts_validation")
  }

  try{

      import spark.implicits._
      val tables_array_list_p1 = spark.read.parquet(source_fil_list_Path).filter($"count_ind" === "Y").collect()
      val tables_par_array_list_p1 = getParArray(tables_array_list_p1)


      logger.info("Starting part I - today's validation:")
      tables_par_array_list_p1.foreach {
        eachrow =>
          try {
              val table_id = eachrow.getInt(1)
              val table_name = eachrow.getString(2).toUpperCase
              val date_column_name = eachrow.getString(5)
              val ANYnum_days = eachrow.get(7)
              var num_days = 0
              if (ANYnum_days != null)
                num_days = ANYnum_days.toString.toInt
              val count_date = run_date_formatted.minusDays(num_days).toString
              val runtime_sql = eachrow(9).toString.replace("{var1}", "'" + count_date + "'")
              runCount(table_id, table_name, count_date, date_column_name, runtime_sql)
          }
          catch {
              case e: Throwable =>
              println(e)
              logger.info("Exception happened in Part I with" + eachrow.toString())
          }
          finally {
            val end_time = new Timestamp(System.currentTimeMillis()).toString
            printf("BdaVapValidation::job for table (%s) is completed at %s", eachrow.toString(), end_time)
          }
      }
      logger.info("Part I: Today's validation is done")

      logger.info("Starting part II - previous days' validation:")
      val tables_array_list_p2 = spark.sql(query_part2).collect()
      val tables_par_array_list_p2 = getParArray(tables_array_list_p2)
      tables_par_array_list_p2.foreach {
        eachrow =>
          try {

            val table_id = eachrow.getInt(0)
            val table_name = eachrow.getString(1)
            val date_column_name = eachrow.getString(2)
            val count_date = eachrow.getString(3)
            val runtime_sql = eachrow(4).toString
            runCount(table_id, table_name, count_date, date_column_name, runtime_sql)
          }
          catch {
            case e: Throwable =>
              println(e)
              logger.info("Exception happened in Part I with" + eachrow.toString())
          }
          finally {
            val end_time = new Timestamp(System.currentTimeMillis()).toString
            printf("BdaVapValidation::job for table (%s) is completed at %s", eachrow.toString(), end_time)
          }
      }
      logger.info("Part II: Previous days' validation is done")

    accumulated_df.coalesce(1).write.mode("append").parquet(target_path + "/extract_date=" + run_date_formatted)
      refreshTable()
      val end_time = new Timestamp(System.currentTimeMillis()).toString
      printf("EcommBdaVapValidation::job is started at %s", end_time)

  } //2 parts
  catch {
    case e: Throwable =>
      println(e)
      exitCode=1
  }
  finally {
    spark.stop()
    System.exit(exitCode)
  }

}
