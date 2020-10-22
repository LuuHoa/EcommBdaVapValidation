package com.fis.ecomm.reporting

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession


object rptFcpsByMidMonthly {

  def main(args: Array[String]) {

    var report_period = 3
    var current_date=java.time.LocalDate.now.toString

    if (args.length == 0) {
      printf("Default values report_period = %d and run date = %s",report_period,current_date)
    }
    else if(args.length == 1){
      //this value should be the period, default run date
      report_period = args(0).toInt
      current_date=java.time.LocalDate.now.toString
    }
    else if(args.length == 2){
      report_period = args(0).toInt
      current_date=args(1)
    }
    else {
      println("Usage: YYYY-MM-DD period OR Usage: No Arguments")
      System.exit(1)
    }

    var Exitcode = 0

    val spark = SparkSession
      .builder()
      .appName("rpt_fcps_by_mid_monthly")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.scheduler.mode", "FAIR")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")


    try {

      println("Start generating report rpt_fcps_by_mid_monthly")

      //val current_date=java.time.LocalDate.now.toString
      //val current_date="2019-10-05"
      val current_date_formatted = LocalDate.parse(current_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      val to_ext_date=current_date_formatted.withDayOfMonth(1).minusDays(1)
      var from_ext_date_90=LocalDate.now()
      var from_ext_date_105=LocalDate.now()
      if(report_period==3)
      {
        from_ext_date_90=current_date_formatted.withDayOfMonth(1).minusMonths(3)
        from_ext_date_105=current_date_formatted.withDayOfMonth(1).minusMonths(4).plusDays(14)
      }
      else {
        from_ext_date_90=current_date_formatted.withDayOfMonth(1).minusMonths(13)
        from_ext_date_105=current_date_formatted.withDayOfMonth(1).minusMonths(14).plusDays(14)

      }
      //val from_ext_date_90=to_ext_date.minusMonths(12).plusDays(1)
      //val from_ext_date_105=to_ext_date.minusMonths(13).plusDays(15)
      //val report_period=3
      //val report_period=12
      val to_ext_month = to_ext_date.toString.substring(0,7)
      val to_ext_month_ts = to_ext_date.toString.substring(0,7)+ "-01"
      val target_path="/lake/ecomm/rpt_fcps_by_mid_monthly/"
      val df_tmp_rxp_in_range = spark.sql("""SELECT
       merchant_id,
       MERCHANT_NAME,
       ORGANIZATION_ID,
       ORGANIZATION_NAME,
       method_of_payment_code,
       settlement_currency_code,
       settlement_currency_desc,
       payment_id,
       settlement_amount,
       settlement_rpt_precision,
       parent_id,
       complete_date,
       action_type,
       ot_start_ts,
       ot_start_ts_ts,
       amount,
       txn_reference_number
FROM ecomm.rpt_ext_payments rxp
WHERE created_date BETWEEN '"""+from_ext_date_105+"""' AND '"""+to_ext_date+"""'
AND batch_post_day  BETWEEN '"""+from_ext_date_90+"""' AND '"""+to_ext_date+"""'
AND transaction_status = '1'
AND method_of_payment_code IN ('VI','MC')""")

      df_tmp_rxp_in_range.createOrReplaceTempView("tmp_rxp_in_range")

      val df_tmp_fe_deposit = spark.sql("""
SELECT *
  FROM tmp_rxp_in_range
 WHERE action_type IN ('D','S')""")
      df_tmp_fe_deposit.createOrReplaceTempView("tmp_fe_deposit")


      val df_tmp_fe_deposit_groupby = spark.sql("""
SELECT merchant_id,
       MERCHANT_NAME,
       ORGANIZATION_ID,
       ORGANIZATION_NAME,
       settlement_currency_code,
       settlement_currency_desc,
       method_of_payment_code,
       COUNT(payment_id) AS vol_deposit_count,
       SUM(settlement_amount * EXP(-LN(10)*settlement_rpt_precision)) AS vol_deposit_settlement_amount
  FROM tmp_fe_deposit
 WHERE complete_date IS NOT NULL
 GROUP BY merchant_id,
       MERCHANT_NAME,
       ORGANIZATION_ID,
       ORGANIZATION_NAME,
       settlement_currency_code,
       settlement_currency_desc,
       method_of_payment_code""")
      df_tmp_fe_deposit_groupby.createOrReplaceTempView("tmp_fe_deposit_groupby")


      val df_tmp_fe_refund = spark.sql("""
SELECT *
  FROM tmp_rxp_in_range
 WHERE action_type IN ('T','R')
   AND complete_date IS NOT NULL""")
      df_tmp_fe_refund.createOrReplaceTempView("tmp_fe_refund")


      val df_deposit_refund_0 = spark.sql("""SELECT
       fe_deposit.merchant_id,
       fe_deposit.settlement_currency_code,
       fe_deposit.method_of_payment_code,
       COUNT(fe_refund.payment_id)  AS excptn_refund_count,
       SUM(fe_refund.settlement_amount  * EXP(-LN(10)*fe_refund.settlement_rpt_precision))  AS excptn_refund_settlement_amount
FROM      tmp_fe_deposit fe_deposit
LEFT JOIN tmp_fe_refund fe_refund
ON
       fe_refund.parent_id = fe_deposit.payment_id
WHERE  fe_deposit.complete_date IS NOT NULL
GROUP BY  fe_deposit.merchant_id,
          fe_deposit.method_of_payment_code,
          fe_deposit.settlement_currency_code""")

      df_deposit_refund_0.createOrReplaceTempView("deposit_refund_0")


      val df_deposit_refund = spark.sql("""SELECT t1.*, t2.excptn_refund_count, t2.excptn_refund_settlement_amount
FROM      tmp_fe_deposit_groupby  t1
INNER
JOIN  deposit_refund_0 t2
ON  t1.merchant_id = t2.merchant_id
    AND t1.method_of_payment_code=t2.method_of_payment_code
    AND NVL(t1.settlement_currency_code,'')=NVL(t2.settlement_currency_code,'')""")

      df_deposit_refund.createOrReplaceTempView("deposit_refund")



      val df_tmp_cbk_cases=spark.sql("""SELECT
        deposit_id
        ,chargeback_amount
        ,method_of_payment_code
        ,reason_code
        ,issuing_bank_day
        ,issuing_bank_day_ts
  FROM ecomm.rpt_cbk_cases
 WHERE open_date BETWEEN '"""+from_ext_date_90+"""' AND '"""+to_ext_date+"""'
   AND type = 'D'
   AND current_cycle in ('FIRST_CHARGEBACK', 'REPRESENTMENT')""")

      df_tmp_cbk_cases.createOrReplaceTempView("tmp_cbk_cases")


      val df_deposit_cbk=spark.sql("""SELECT fe_deposit.merchant_id,
       fe_deposit.settlement_currency_code,
       fe_deposit.settlement_currency_desc,
       fe_deposit.method_of_payment_code,
       COUNT(cbk.deposit_id) AS excptn_cbks_count,
       SUM(cbk.chargeback_amount)  AS excptn_cbks_amount,
       SUM(
            CASE WHEN (cbk.method_of_payment_code = 'VI' AND cbk.reason_code IN ('0057', '0075', '0081', '0083'))
                 OR (cbk.method_of_payment_code = 'MC' AND cbk.reason_code IN ('4837','4840','4847','4862','4863')) THEN 1 ELSE 0 END
          ) AS excptn_fraud_cbks_count,
       SUM(
           CASE WHEN (cbk.method_of_payment_code = 'VI' AND cbk.reason_code IN ('0057','0075','0081', '0083'))
                     OR  (cbk.method_of_payment_code = 'MC' AND cbk.reason_code IN ('4837', '4840', '4847', '4862', '4863')) THEN cbk.chargeback_amount ELSE 0 END
          ) AS excptn_fraud_cbks_amount
 FROM tmp_fe_deposit fe_deposit
 JOIN tmp_cbk_cases cbk
   ON cbk.deposit_id = fe_deposit.payment_id
 GROUP BY  fe_deposit.merchant_id,
           fe_deposit.method_of_payment_code,
           fe_deposit.settlement_currency_code,
           fe_deposit.settlement_currency_desc""")

      df_deposit_cbk.createOrReplaceTempView("deposit_cbk")


      val df_deposit_refund_match=spark.sql("""SELECT fe_refund.merchant_id,
       fe_refund.settlement_currency_code,
       fe_refund.settlement_currency_desc,
       fe_refund.method_of_payment_code,
       COUNT(1) AS excptn_refund_mtch_count,
       SUM( CASE WHEN fe_refund.parent_id > 0 THEN 1 ELSE 0 END ) AS excptn_refund_mtch_count_mtchd,
       SUM( fe_refund.settlement_amount * Exp(-Ln(10)*fe_refund.settlement_rpt_precision) ) AS excptn_refund_mtch_amount,
       SUM( CASE WHEN fe_refund.parent_id > 0 THEN fe_refund.settlement_amount * Exp(-Ln(10)*fe_refund.settlement_rpt_precision) ELSE 0 END ) AS excptn_refund_mtch_amount_mtchd

 FROM  tmp_fe_refund fe_refund
 GROUP BY fe_refund.merchant_id,
          fe_refund.method_of_payment_code,
          fe_refund.settlement_currency_code,
          fe_refund.settlement_currency_desc """)

      df_deposit_refund_match.createOrReplaceTempView("deposit_refund_match")


      val df_deposit_fraud_advice=spark.sql("""SELECT fe_deposit.merchant_id,
        fe_deposit.payment_id,
        fe_deposit.settlement_currency_code,
        fe_deposit.settlement_currency_desc,
        fe_deposit.method_of_payment_code,
        fe_deposit.settlement_amount * exp(-ln(10)*fe_deposit.settlement_rpt_precision) as frd_deposit_settlement_amount,
        fraud_advice.create_date as frd_advice_date,
        fe_refund.ot_start_ts as frd_refund_date,
        fe_refund.settlement_amount * exp(-ln(10)*fe_refund.settlement_rpt_precision) as frd_refund_settlement_amount,
        cbk.issuing_bank_day as frd_cbk_date,
        cbk.chargeback_amount as frd_cbk_amount,
        case when cbk.method_of_payment_code = 'VI' and cbk.reason_code in ('0057','0075','0081','0083')
        or cbk.method_of_payment_code = 'MC' and cbk.reason_code in ('4837','4840','4847','4862','4863') then 1 else 0 end as is_fraud_cbk,
        datediff(to_date(cbk.issuing_bank_day_ts), to_date(fraud_advice.create_date_ts)) as cbk_days_after_cps_reported,
        datediff(to_date(fe_refund.ot_start_ts_ts),to_date(fraud_advice.create_date_ts)) as ref_days_after_cps_reported,
        to_date(cbk.issuing_bank_day_ts) < to_date(fraud_advice.create_date_ts) as frd_cbk_prior,
        to_date(fe_refund.ot_start_ts_ts) < to_date(fraud_advice.create_date_ts) as frd_refund_prior
   FROM ecomm.rpt_fraud_advice fraud_advice
  INNER
   JOIN tmp_fe_deposit fe_deposit
     ON fraud_advice.create_date BETWEEN '"""+from_ext_date_90+"""' AND '"""+to_ext_date+"""'
        AND fe_deposit.txn_reference_number = fraud_advice.acquirer_ref_number
   LEFT
   JOIN tmp_fe_refund fe_refund
     ON fe_refund.parent_id = fe_deposit.payment_id
     AND fe_refund.amount = fe_deposit.amount
   LEFT
   JOIN tmp_cbk_cases cbk
     ON cbk.deposit_id = fe_deposit.payment_id
  WHERE fe_deposit.complete_date is not null """)

      df_deposit_fraud_advice.createOrReplaceTempView("deposit_fraud_advice")


      val df_deposit_fraud_advice_metrics=spark.sql("""select
     merchant_id,
     settlement_currency_code,
     settlement_currency_desc,
     method_of_payment_code,
     Count(payment_id)  AS cnt_all_total,
        Sum(
            CASE
               WHEN (not(frd_cbk_date is null)) THEN 1
               ELSE 0
            END)         AS cnt_all_cbks,
        Sum(
            CASE
               WHEN (not(frd_cbk_date is null) and is_fraud_cbk=1) THEN 1
               ELSE 0
            END)         AS cnt_all_frd_cbks,

        Sum(frd_deposit_settlement_amount)  AS amnt_all_total,
        Sum(
            CASE
               WHEN (not(frd_cbk_date is null)) THEN frd_cbk_amount
               ELSE 0
            END)         AS amnt_all_cbks,
        Sum(
            CASE
               WHEN (not(frd_cbk_date is null) and is_fraud_cbk=1) THEN frd_cbk_amount
               ELSE 0
            END)         AS amnt_all_frd_cbks,


        Sum(
        CASE
           WHEN (frd_cbk_date < frd_advice_date) THEN 1
           ELSE 0
        END)             AS cnt_cbk_prior_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and frd_cbk_date < frd_advice_date) THEN 1
           ELSE 0
        END)         AS cnt_cbk_prior_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null) and frd_cbk_date < frd_advice_date and is_fraud_cbk=1) THEN 1
           ELSE 0
        END)         AS cnt_cbk_prior_frd_cbks,


        Sum(
        CASE
           WHEN (frd_cbk_date < frd_advice_date) THEN frd_deposit_settlement_amount
           ELSE 0
        END)         AS amnt_cbk_prior_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and frd_cbk_date < frd_advice_date) THEN frd_cbk_amount
           ELSE 0
        END)         AS amnt_cbk_prior_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null) and frd_cbk_date < frd_advice_date and is_fraud_cbk=1) THEN frd_cbk_amount
           ELSE 0
        END)         AS amnt_cbk_prior_frd_cbks,


        Sum(
        CASE
           WHEN (not(frd_cbk_date < frd_advice_date)  and frd_refund_date < frd_advice_date) THEN 1
           ELSE 0
        END)         AS cnt_ref_full_prior_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date < frd_advice_date ) THEN 1
           ELSE 0
        END)         AS cnt_ref_full_prior_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date < frd_advice_date and is_fraud_cbk=1) THEN 1
           ELSE 0
        END)         AS cnt_ref_full_prior_frd_cbks,


        Sum(
        CASE
           WHEN (not(frd_cbk_date < frd_advice_date)  and frd_refund_date < frd_advice_date) THEN frd_deposit_settlement_amount
           ELSE 0
        END)         AS amnt_ref_full_prior_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date < frd_advice_date ) THEN frd_cbk_amount
           ELSE 0
        END)         AS amnt_ref_full_prior_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date < frd_advice_date and is_fraud_cbk=1) THEN frd_cbk_amount
           ELSE 0
        END)         AS amnt_ref_full_prior_frd_cbks,

        Sum(
        CASE
           WHEN (not(frd_cbk_date < frd_advice_date) and frd_refund_date >= frd_advice_date) THEN 1
           ELSE 0
        END)         AS cnt_ref_full_after_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date >= frd_advice_date ) THEN 1
           ELSE 0
        END)         AS cnt_ref_full_after_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date >= frd_advice_date and is_fraud_cbk=1) THEN 1
           ELSE 0
        END)         AS cnt_ref_full_after_frd_cbks,

        Sum(
        CASE
           WHEN (not(frd_cbk_date < frd_advice_date) and frd_refund_date >= frd_advice_date) THEN frd_deposit_settlement_amount
           ELSE 0
        END)         AS amnt_ref_full_after_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date >= frd_advice_date ) THEN frd_cbk_amount
           ELSE 0
        END)         AS amnt_ref_full_after_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date >= frd_advice_date and is_fraud_cbk=1) THEN frd_cbk_amount
           ELSE 0
        END)         AS amnt_ref_full_after_frd_cbks,


        Sum(
        CASE
           WHEN (not(frd_cbk_date < frd_advice_date) and frd_refund_date is null) THEN 1
           ELSE 0
        END)         AS cnt_not_ref_full_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date is null ) THEN 1
           ELSE 0
        END)         AS cnt_not_ref_full_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date is null and is_fraud_cbk=1) THEN 1
           ELSE 0
        END)         AS cnt_not_ref_full_frd_cbks,

        Sum(
        CASE
           WHEN (not(frd_cbk_date < frd_advice_date) and frd_refund_date is null) THEN frd_deposit_settlement_amount
           ELSE 0
        END)         AS amnt_not_ref_full_total,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date is null ) THEN frd_cbk_amount
           ELSE 0
        END)         AS amnt_not_ref_full_cbks,
        Sum(
        CASE
           WHEN (not(frd_cbk_date is null)  and not(frd_cbk_date < frd_advice_date) and frd_refund_date is null and is_fraud_cbk=1) THEN frd_cbk_amount
           ELSE 0
        END)             AS amnt_not_ref_full_frd_cbks    ,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported<=-1) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_prior_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported<=-1) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_prior_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported<=-1) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_prior_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported<=-1) THEN frd_refund_settlement_amount
                  ELSE 0
               END)         AS excptn_amnt_prior_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=0) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_0_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=0) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_0_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=0) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_0_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=0) THEN frd_refund_settlement_amount
                  ELSE 0
               END)         AS excptn_amnt_0_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=1) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_1_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=1) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_1_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=1) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_1_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=1) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_1_refunds,

                         SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=2) THEN 2
                  ELSE 0
               END)         AS excptn_cnt_2_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=2) THEN 2
                  ELSE 0
               END)         AS excptn_cnt_2_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=2) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_2_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=2) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_2_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=3) THEN 3
                  ELSE 0
               END)         AS excptn_cnt_3_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=3) THEN 3
                  ELSE 0
               END)         AS excptn_cnt_3_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=3) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_3_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=3) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_3_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=4) THEN 4
                  ELSE 0
               END)         AS excptn_cnt_4_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=4) THEN 4
                  ELSE 0
               END)         AS excptn_cnt_4_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=4) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_4_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=4) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_4_refunds,
                   SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=5) THEN 5
                  ELSE 0
               END)         AS excptn_cnt_5_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=5) THEN 5
                  ELSE 0
               END)         AS excptn_cnt_5_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=5) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_5_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=5) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_5_refunds,

                   SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=6) THEN 6
                  ELSE 0
               END)         AS excptn_cnt_6_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=6) THEN 6
                  ELSE 0
               END)         AS excptn_cnt_6_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=6) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_6_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=6) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_6_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=7) THEN 7
                  ELSE 0
               END)         AS excptn_cnt_7_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=7) THEN 7
                  ELSE 0
               END)         AS excptn_cnt_7_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=7) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_7_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=7) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_7_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=8) THEN 8
                  ELSE 0
               END)         AS excptn_cnt_8_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=8) THEN 8
                  ELSE 0
               END)         AS excptn_cnt_8_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=8) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_8_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=8) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_8_refunds,
                   SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=9) THEN 9
                  ELSE 0
               END)         AS excptn_cnt_9_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=9) THEN 9
                  ELSE 0
               END)         AS excptn_cnt_9_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=9) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_9_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=9) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_9_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=10) THEN 10
                  ELSE 0
               END)         AS excptn_cnt_10_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=10) THEN 10
                  ELSE 0
               END)         AS excptn_cnt_10_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=10) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_10_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=10) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_10_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=11) THEN 11
                  ELSE 0
               END)         AS excptn_cnt_11_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=11) THEN 11
                  ELSE 0
               END)         AS excptn_cnt_11_refunds,
          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=11) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_11_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=11) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_11_refunds,
                   SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=12) THEN 12
                  ELSE 0
               END)         AS excptn_cnt_12_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=12) THEN 12
                  ELSE 0
               END)         AS excptn_cnt_12_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=12) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_12_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=12) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_12_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=13) THEN 13
                  ELSE 0
               END)         AS excptn_cnt_13_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=13) THEN 13
                  ELSE 0
               END)         AS excptn_cnt_13_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=13) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_13_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=13) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_13_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=14) THEN 14
                  ELSE 0
               END)         AS excptn_cnt_14_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=14) THEN 14
                  ELSE 0
               END)         AS excptn_cnt_14_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=14) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_14_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=14) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_14_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=15) THEN 15
                  ELSE 0
               END)         AS excptn_cnt_15_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=15) THEN 15
                  ELSE 0
               END)         AS excptn_cnt_15_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=15) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_15_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=15) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_15_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=16) THEN 16
                  ELSE 0
               END)         AS excptn_cnt_16_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=16) THEN 16
                  ELSE 0
               END)         AS excptn_cnt_16_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=16) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_16_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=16) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_16_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=17) THEN 17
                  ELSE 0
               END)         AS excptn_cnt_17_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=17) THEN 17
                  ELSE 0
               END)         AS excptn_cnt_17_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=17) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_17_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=17) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_17_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=18) THEN 18
                  ELSE 0
               END)         AS excptn_cnt_18_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=18) THEN 18
                  ELSE 0
               END)         AS excptn_cnt_18_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=18) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_18_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=18) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_18_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=19) THEN 19
                  ELSE 0
               END)         AS excptn_cnt_19_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=19) THEN 19
                  ELSE 0
               END)         AS excptn_cnt_19_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=19) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_19_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=19) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_19_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=20) THEN 20
                  ELSE 0
               END)         AS excptn_cnt_20_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=20) THEN 20
                  ELSE 0
               END)         AS excptn_cnt_20_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=20) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_20_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=20) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_20_refunds,
                    SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=21) THEN 21
                  ELSE 0
               END)         AS excptn_cnt_21_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=21) THEN 21
                  ELSE 0
               END)         AS excptn_cnt_21_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported=21) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_21_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported=21) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                           AS excptn_amnt_21_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported>21) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_above_21_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported>21) THEN 1
                  ELSE 0
               END)         AS excptn_cnt_above_21_refunds,

          SUM(
               CASE
                  WHEN (cbk_days_after_cps_reported>21) THEN frd_cbk_amount
                  ELSE 0
               END)         AS excptn_amnt_above_21_cbks,
          SUM(
               CASE
                  WHEN (ref_days_after_cps_reported>21) THEN frd_refund_settlement_amount
                  ELSE 0
               END)                            AS excptn_amnt_above_21_refunds

          from deposit_fraud_advice
          group by
     merchant_id,
     method_of_payment_code,
     settlement_currency_code,
     settlement_currency_desc""")

      df_deposit_fraud_advice_metrics.createOrReplaceTempView("deposit_fraud_advice_metrics")
//,cast('"""+to_ext_month_ts+"""' AS TIMESTAMP)   AS reporting_month_day
//,cast('"""+to_ext_month_ts+"""' AS TIMESTAMP)   AS reporting_month_day
      val df_rpt_fcps_by_mid_monthly=spark.sql("""SELECT dep.MERCHANT_ID
,dep.MERCHANT_NAME
,dep.ORGANIZATION_ID
,dep.ORGANIZATION_NAME
,dep.METHOD_OF_PAYMENT_CODE
,dep.SETTLEMENT_CURRENCY_CODE
,dep.SETTLEMENT_CURRENCY_DESC
,cast('"""+to_ext_month_ts+"""' AS TIMESTAMP)   AS reporting_month_day
,CAST( NVL(dep.vol_deposit_count, 0) AS BIGINT)                  AS  vol_deposit_count
,CAST( NVL(dep.excptn_refund_count, 0) AS BIGINT)                AS  excptn_refund_count
,CAST( NVL(dep.vol_deposit_settlement_amount, 0) AS DECIMAL(38,18))       AS  vol_deposit_settlement_amount
,CAST( NVL(dep.excptn_refund_settlement_amount, 0) AS DECIMAL(38,18))     AS  excptn_refund_settlement_amount
,CAST( NVL(cbk.excptn_cbks_count, 0) AS BIGINT)                           AS  excptn_cbks_count
,CAST( NVL(cbk.excptn_cbks_amount, 0) AS DECIMAL(38,18))                  AS  excptn_cbks_amount
,CAST( NVL(cbk.excptn_fraud_cbks_count, 0) AS BIGINT)                     AS  excptn_fraud_cbks_count
,CAST( NVL(cbk.excptn_fraud_cbks_amount, 0) AS DECIMAL(38,18))            AS  excptn_fraud_cbks_amount
,CAST( NVL(match1.excptn_refund_mtch_count, 0)  AS BIGINT)      AS  excptn_refund_mtch_count
,CAST( NVL(match1.excptn_refund_mtch_amount, 0) AS DECIMAL(38,18))        AS  excptn_refund_mtch_amount
,CAST( NVL(match1.excptn_refund_mtch_count_mtchd, 0) AS BIGINT)  AS  excptn_refund_mtch_count_mtchd
,CAST( NVL(match1.excptn_refund_mtch_amount_mtchd, 0) AS DECIMAL(38,18))  AS  excptn_refund_mtch_amount_mtchd
,CAST( NVL(advice.cnt_all_total, 0) AS BIGINT)                  AS  cnt_all_total
,CAST( NVL(advice.cnt_all_cbks, 0)  AS BIGINT)                  AS  cnt_all_cbks
,CAST( NVL(advice.cnt_all_frd_cbks, 0) AS BIGINT)                AS  cnt_all_frd_cbks
,CAST( NVL(advice.amnt_all_total, 0)    AS DECIMAL(38,18))              AS  amnt_all_total
,CAST( NVL(advice.amnt_all_cbks, 0)     AS DECIMAL(38,18))              AS  amnt_all_cbks
,CAST( NVL(advice.amnt_all_frd_cbks, 0) AS DECIMAL(38,18))              AS  amnt_all_frd_cbks
,CAST( NVL(advice.cnt_cbk_prior_total, 0)     AS BIGINT)             AS  cnt_cbk_prior_total
,CAST( NVL(advice.cnt_cbk_prior_cbks, 0)      AS BIGINT)             AS  cnt_cbk_prior_cbks
,CAST( NVL(advice.cnt_cbk_prior_frd_cbks, 0)  AS BIGINT)        AS  cnt_cbk_prior_frd_cbks
,CAST( NVL(advice.amnt_cbk_prior_total, 0)    AS DECIMAL(38,18))        AS  amnt_cbk_prior_total
,CAST( NVL(advice.amnt_cbk_prior_cbks, 0)     AS DECIMAL(38,18))        AS  amnt_cbk_prior_cbks
,CAST( NVL(advice.amnt_cbk_prior_frd_cbks, 0) AS DECIMAL(38,18))        AS  amnt_cbk_prior_frd_cbks
,CAST( NVL(advice.cnt_ref_full_prior_total, 0)   AS BIGINT)    AS  cnt_ref_full_prior_total
,CAST( NVL(advice.cnt_ref_full_prior_cbks, 0)    AS BIGINT)    AS  cnt_ref_full_prior_cbks
,CAST( NVL(advice.cnt_ref_full_prior_frd_cbks, 0)AS BIGINT)    AS  cnt_ref_full_prior_frd_cbks
,CAST( NVL(advice.amnt_ref_full_prior_total, 0)    AS DECIMAL(38,18))   AS  amnt_ref_full_prior_total
,CAST( NVL(advice.amnt_ref_full_prior_cbks, 0)     AS DECIMAL(38,18))   AS  amnt_ref_full_prior_cbks
,CAST( NVL(advice.amnt_ref_full_prior_frd_cbks, 0) AS DECIMAL(38,18))   AS  amnt_ref_full_prior_frd_cbks
,CAST( NVL(advice.cnt_ref_full_after_total, 0)     AS BIGINT)   AS  cnt_ref_full_after_total
,CAST( NVL(advice.cnt_ref_full_after_cbks, 0)      AS BIGINT)   AS  cnt_ref_full_after_cbks
,CAST( NVL(advice.cnt_ref_full_after_frd_cbks, 0)  AS BIGINT)   AS  cnt_ref_full_after_frd_cbks
,CAST( NVL(advice.amnt_ref_full_after_total, 0)    AS DECIMAL(38,18))   AS  amnt_ref_full_after_total
,CAST( NVL(advice.amnt_ref_full_after_cbks, 0)     AS DECIMAL(38,18))   AS  amnt_ref_full_after_cbks
,CAST( NVL(advice.amnt_ref_full_after_frd_cbks, 0) AS DECIMAL(38,18))   AS  amnt_ref_full_after_frd_cbks
,CAST( NVL(advice.cnt_not_ref_full_total, 0)   AS BIGINT)      AS  cnt_not_ref_full_total
,CAST( NVL(advice.cnt_not_ref_full_cbks, 0)    AS BIGINT)      AS  cnt_not_ref_full_cbks
,CAST( NVL(advice.cnt_not_ref_full_frd_cbks, 0)AS BIGINT)      AS  cnt_not_ref_full_frd_cbks
,CAST( NVL(advice.amnt_not_ref_full_total, 0)     AS DECIMAL(38,18))    AS  amnt_not_ref_full_total
,CAST( NVL(advice.amnt_not_ref_full_cbks, 0)      AS DECIMAL(38,18))    AS  amnt_not_ref_full_cbks
,CAST( NVL(advice.amnt_not_ref_full_frd_cbks, 0)  AS DECIMAL(38,18))    AS  amnt_not_ref_full_frd_cbks
,CAST( NVL(advice.excptn_cnt_prior_cbks, 0)    AS BIGINT)       AS  excptn_cnt_prior_cbks
,CAST( NVL(advice.excptn_cnt_prior_refunds, 0) AS BIGINT)       AS  excptn_cnt_prior_refunds
,CAST( NVL(advice.excptn_amnt_prior_cbks, 0)    AS DECIMAL(38,18))      AS  excptn_amnt_prior_cbks
,CAST( NVL(advice.excptn_amnt_prior_refunds, 0) AS DECIMAL(38,18))      AS  excptn_amnt_prior_refunds
,CAST( NVL(advice.excptn_cnt_0_cbks, 0)          AS BIGINT)    AS  excptn_cnt_0_cbks
,CAST( NVL(advice.excptn_cnt_0_refunds, 0)       AS BIGINT)    AS  excptn_cnt_0_refunds
,CAST( NVL(advice.excptn_amnt_0_cbks, 0)    AS DECIMAL(38,18))          AS  excptn_amnt_0_cbks
,CAST( NVL(advice.excptn_amnt_0_refunds, 0) AS DECIMAL(38,18))          AS  excptn_amnt_0_refunds
,CAST( NVL(advice.excptn_cnt_1_cbks, 0)       AS BIGINT)       AS  excptn_cnt_1_cbks
,CAST( NVL(advice.excptn_cnt_1_refunds, 0)    AS BIGINT)       AS  excptn_cnt_1_refunds
,CAST( NVL(advice.excptn_amnt_1_cbks, 0)    AS DECIMAL(38,18))          AS  excptn_amnt_1_cbks
,CAST( NVL(advice.excptn_amnt_1_refunds, 0) AS DECIMAL(38,18))          AS  excptn_amnt_1_refunds
,CAST( NVL(advice.excptn_cnt_2_cbks, 0)       AS BIGINT)        AS  excptn_cnt_2_cbks
,CAST( NVL(advice.excptn_cnt_2_refunds, 0)    AS BIGINT)        AS  excptn_cnt_2_refunds
,CAST( NVL(advice.excptn_amnt_2_cbks, 0)    AS DECIMAL(38,18))          AS  excptn_amnt_2_cbks
,CAST( NVL(advice.excptn_amnt_2_refunds, 0) AS DECIMAL(38,18))          AS  excptn_amnt_2_refunds
,CAST( NVL(advice.excptn_cnt_3_cbks, 0)       AS BIGINT)        AS  excptn_cnt_3_cbks
,CAST( NVL(advice.excptn_cnt_3_refunds, 0)    AS BIGINT)        AS  excptn_cnt_3_refunds
,CAST( NVL(advice.excptn_amnt_3_cbks, 0)     AS DECIMAL(38,18))         AS  excptn_amnt_3_cbks
,CAST( NVL(advice.excptn_amnt_3_refunds, 0)  AS DECIMAL(38,18))         AS  excptn_amnt_3_refunds
,CAST( NVL(advice.excptn_cnt_4_cbks, 0)       AS BIGINT)        AS  excptn_cnt_4_cbks
,CAST( NVL(advice.excptn_cnt_4_refunds, 0)    AS BIGINT)        AS  excptn_cnt_4_refunds
,CAST( NVL(advice.excptn_amnt_4_cbks, 0)      AS DECIMAL(38,18))        AS  excptn_amnt_4_cbks
,CAST( NVL(advice.excptn_amnt_4_refunds, 0)   AS DECIMAL(38,18))        AS  excptn_amnt_4_refunds
,CAST( NVL(advice.excptn_cnt_5_cbks, 0)       AS BIGINT)        AS  excptn_cnt_5_cbks
,CAST( NVL(advice.excptn_cnt_5_refunds, 0)    AS BIGINT)        AS  excptn_cnt_5_refunds
,CAST( NVL(advice.excptn_amnt_5_cbks, 0)    AS DECIMAL(38,18))          AS  excptn_amnt_5_cbks
,CAST( NVL(advice.excptn_amnt_5_refunds, 0) AS DECIMAL(38,18))          AS  excptn_amnt_5_refunds
,CAST( NVL(advice.excptn_cnt_6_cbks, 0)      AS BIGINT)        AS  excptn_cnt_6_cbks
,CAST( NVL(advice.excptn_cnt_6_refunds, 0)   AS BIGINT)        AS  excptn_cnt_6_refunds
,CAST( NVL(advice.excptn_amnt_6_cbks, 0)      AS DECIMAL(38,18))        AS  excptn_amnt_6_cbks
,CAST( NVL(advice.excptn_amnt_6_refunds, 0)   AS DECIMAL(38,18))        AS  excptn_amnt_6_refunds
,CAST( NVL(advice.excptn_cnt_7_cbks, 0)      AS BIGINT)         AS  excptn_cnt_7_cbks
,CAST( NVL(advice.excptn_cnt_7_refunds, 0)   AS BIGINT)         AS  excptn_cnt_7_refunds
,CAST( NVL(advice.excptn_amnt_7_cbks, 0)     AS DECIMAL(38,18))         AS  excptn_amnt_7_cbks
,CAST( NVL(advice.excptn_amnt_7_refunds, 0)  AS DECIMAL(38,18))         AS  excptn_amnt_7_refunds
,CAST( NVL(advice.excptn_cnt_8_cbks, 0)     AS BIGINT)         AS  excptn_cnt_8_cbks
,CAST( NVL(advice.excptn_cnt_8_refunds, 0)  AS BIGINT)         AS  excptn_cnt_8_refunds
,CAST( NVL(advice.excptn_amnt_8_cbks, 0)     AS DECIMAL(38,18))         AS  excptn_amnt_8_cbks
,CAST( NVL(advice.excptn_amnt_8_refunds, 0)  AS DECIMAL(38,18))         AS  excptn_amnt_8_refunds
,CAST( NVL(advice.excptn_cnt_9_cbks, 0)     AS BIGINT)          AS  excptn_cnt_9_cbks
,CAST( NVL(advice.excptn_cnt_9_refunds, 0)  AS BIGINT)          AS  excptn_cnt_9_refunds
,CAST( NVL(advice.excptn_amnt_9_cbks, 0)     AS DECIMAL(38,18))         AS  excptn_amnt_9_cbks
,CAST( NVL(advice.excptn_amnt_9_refunds, 0)  AS DECIMAL(38,18))         AS  excptn_amnt_9_refunds
,CAST( NVL(advice.excptn_cnt_10_cbks, 0)    AS BIGINT)          AS  excptn_cnt_10_cbks
,CAST( NVL(advice.excptn_cnt_10_refunds, 0) AS BIGINT)          AS  excptn_cnt_10_refunds
,CAST( NVL(advice.excptn_amnt_10_cbks, 0)    AS DECIMAL(38,18))         AS  excptn_amnt_10_cbks
,CAST( NVL(advice.excptn_amnt_10_refunds, 0) AS DECIMAL(38,18))         AS  excptn_amnt_10_refunds
,CAST( NVL(advice.excptn_cnt_11_cbks, 0)     AS BIGINT)        AS  excptn_cnt_11_cbks
,CAST( NVL(advice.excptn_cnt_11_refunds, 0)  AS BIGINT)        AS  excptn_cnt_11_refunds
,CAST( NVL(advice.excptn_amnt_11_cbks, 0)    AS DECIMAL(38,18))         AS  excptn_amnt_11_cbks
,CAST( NVL(advice.excptn_amnt_11_refunds, 0) AS DECIMAL(38,18))         AS  excptn_amnt_11_refunds
,CAST( NVL(advice.excptn_cnt_12_cbks, 0)    AS BIGINT)          AS  excptn_cnt_12_cbks
,CAST( NVL(advice.excptn_cnt_12_refunds, 0) AS BIGINT)          AS  excptn_cnt_12_refunds
,CAST( NVL(advice.excptn_amnt_12_cbks, 0)    AS DECIMAL(38,18))         AS  excptn_amnt_12_cbks
,CAST( NVL(advice.excptn_amnt_12_refunds, 0) AS DECIMAL(38,18))         AS  excptn_amnt_12_refunds
,CAST( NVL(advice.excptn_cnt_13_cbks, 0)    AS BIGINT)          AS  excptn_cnt_13_cbks
,CAST( NVL(advice.excptn_cnt_13_refunds, 0) AS BIGINT)          AS  excptn_cnt_13_refunds
,CAST( NVL(advice.excptn_amnt_13_cbks, 0)    AS DECIMAL(38,18))         AS  excptn_amnt_13_cbks
,CAST( NVL(advice.excptn_amnt_13_refunds, 0) AS DECIMAL(38,18))         AS  excptn_amnt_13_refunds
,CAST( NVL(advice.excptn_cnt_14_cbks, 0)       AS BIGINT)       AS  excptn_cnt_14_cbks
,CAST( NVL(advice.excptn_cnt_14_refunds, 0)    AS BIGINT)       AS  excptn_cnt_14_refunds
,CAST( NVL(advice.excptn_amnt_14_cbks, 0)     AS DECIMAL(38,18))        AS  excptn_amnt_14_cbks
,CAST( NVL(advice.excptn_amnt_14_refunds, 0)  AS DECIMAL(38,18))        AS  excptn_amnt_14_refunds
,CAST( NVL(advice.excptn_cnt_15_cbks, 0)       AS BIGINT)       AS  excptn_cnt_15_cbks
,CAST( NVL(advice.excptn_cnt_15_refunds, 0)    AS BIGINT)       AS  excptn_cnt_15_refunds
,CAST( NVL(advice.excptn_amnt_15_cbks, 0)      AS DECIMAL(38,18))       AS  excptn_amnt_15_cbks
,CAST( NVL(advice.excptn_amnt_15_refunds, 0)   AS DECIMAL(38,18))       AS  excptn_amnt_15_refunds
,CAST( NVL(advice.excptn_cnt_16_cbks, 0)       AS BIGINT)       AS  excptn_cnt_16_cbks
,CAST( NVL(advice.excptn_cnt_16_refunds, 0)    AS BIGINT)       AS  excptn_cnt_16_refunds
,CAST( NVL(advice.excptn_amnt_16_cbks, 0)      AS DECIMAL(38,18))       AS  excptn_amnt_16_cbks
,CAST( NVL(advice.excptn_amnt_16_refunds, 0)   AS DECIMAL(38,18))       AS  excptn_amnt_16_refunds
,CAST( NVL(advice.excptn_cnt_17_cbks, 0)       AS BIGINT)       AS  excptn_cnt_17_cbks
,CAST( NVL(advice.excptn_cnt_17_refunds, 0)    AS BIGINT)       AS  excptn_cnt_17_refunds
,CAST( NVL(advice.excptn_amnt_17_cbks, 0)      AS DECIMAL(38,18))       AS  excptn_amnt_17_cbks
,CAST( NVL(advice.excptn_amnt_17_refunds, 0)   AS DECIMAL(38,18))       AS  excptn_amnt_17_refunds
,CAST( NVL(advice.excptn_cnt_18_cbks, 0)       AS BIGINT)       AS  excptn_cnt_18_cbks
,CAST( NVL(advice.excptn_cnt_18_refunds, 0)    AS BIGINT)       AS  excptn_cnt_18_refunds
,CAST( NVL(advice.excptn_amnt_18_cbks, 0)      AS DECIMAL(38,18))       AS  excptn_amnt_18_cbks
,CAST( NVL(advice.excptn_amnt_18_refunds, 0)   AS DECIMAL(38,18))       AS  excptn_amnt_18_refunds
,CAST( NVL(advice.excptn_cnt_19_cbks, 0)       AS BIGINT)       AS  excptn_cnt_19_cbks
,CAST( NVL(advice.excptn_cnt_19_refunds, 0)    AS BIGINT)       AS  excptn_cnt_19_refunds
,CAST( NVL(advice.excptn_amnt_19_cbks, 0)      AS DECIMAL(38,18))       AS  excptn_amnt_19_cbks
,CAST( NVL(advice.excptn_amnt_19_refunds, 0)   AS DECIMAL(38,18))       AS  excptn_amnt_19_refunds
,CAST( NVL(advice.excptn_cnt_20_cbks, 0)       AS BIGINT)       AS  excptn_cnt_20_cbks
,CAST( NVL(advice.excptn_cnt_20_refunds, 0)    AS BIGINT)       AS  excptn_cnt_20_refunds
,CAST( NVL(advice.excptn_amnt_20_cbks, 0)      AS DECIMAL(38,18))       AS  excptn_amnt_20_cbks
,CAST( NVL(advice.excptn_amnt_20_refunds, 0)   AS DECIMAL(38,18))       AS  excptn_amnt_20_refunds
,CAST( NVL(advice.excptn_cnt_21_cbks, 0)      AS BIGINT)        AS  excptn_cnt_21_cbks
,CAST( NVL(advice.excptn_cnt_21_refunds, 0)   AS BIGINT)        AS  excptn_cnt_21_refunds
,CAST( NVL(advice.excptn_amnt_21_cbks, 0)     AS DECIMAL(38,18))       AS  excptn_amnt_21_cbks
,CAST( NVL(advice.excptn_amnt_21_refunds, 0)  AS DECIMAL(38,18))       AS  excptn_amnt_21_refunds
,CAST( NVL(advice.excptn_cnt_above_21_cbks, 0)    AS BIGINT)    AS  excptn_cnt_above_21_cbks
,CAST( NVL(advice.excptn_cnt_above_21_refunds, 0) AS BIGINT)    AS  excptn_cnt_above_21_refunds
,CAST( NVL(advice.excptn_amnt_above_21_cbks, 0)    AS DECIMAL(38,18))   AS  excptn_amnt_above_21_cbks
,CAST( NVL(advice.excptn_amnt_above_21_refunds, 0) AS DECIMAL(38,18))   AS  excptn_amnt_above_21_refunds
from deposit_refund dep
left join deposit_cbk cbk
       on dep.merchant_id=cbk.merchant_id
       and dep.method_of_payment_code=cbk.method_of_payment_code
       and NVL(dep.settlement_currency_code,'')=NVL(cbk.settlement_currency_code,'')
left join deposit_refund_match match1
       on dep.merchant_id=match1.merchant_id
       and dep.method_of_payment_code=match1.method_of_payment_code
       and NVL(dep.settlement_currency_code,'')=NVL(match1.settlement_currency_code,'')
left join deposit_fraud_advice_metrics advice
       on dep.merchant_id=advice.merchant_id
       and dep.method_of_payment_code=advice.method_of_payment_code
       and NVL(dep.settlement_currency_code,'')=NVL(advice.settlement_currency_code,'')""")

      df_rpt_fcps_by_mid_monthly.write.mode("overwrite").parquet(target_path+"reporting_month="+to_ext_month+"/reporting_period="+report_period)

      spark.catalog.dropTempView("tmp_rxp_in_range")
      spark.catalog.dropTempView("tmp_fe_deposit")
      spark.catalog.dropTempView("tmp_fe_deposit_groupby")
      spark.catalog.dropTempView("tmp_fe_refund")
      spark.catalog.dropTempView("deposit_refund")
      spark.catalog.dropTempView("tmp_cbk_cases")
      spark.catalog.dropTempView("deposit_cbk")
      spark.catalog.dropTempView("deposit_refund_match")
      spark.catalog.dropTempView("deposit_fraud_advice")
      spark.catalog.dropTempView("deposit_fraud_advice_metrics")

      println("Info : rpt_fcps_by_mid_monthly Ingestion is complete")


    } catch {
      case e: Throwable =>
        println(e)
        Exitcode = 1
    } finally {
      spark.stop()
      System.exit(Exitcode)
    }
  }
}
