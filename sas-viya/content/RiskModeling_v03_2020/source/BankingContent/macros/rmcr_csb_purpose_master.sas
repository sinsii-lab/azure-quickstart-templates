%macro rmcr_csb_purpose_master;

proc sql;
insert into &lib_apdm..PURPOSE_MASTER (
PURPOSE_SK ,PURPOSE_CD , MODEL_ANALYSIS_TYPE_SK, CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER , PURPOSE_DESC, PURPOSE_SHORT_NM
)
VALUES( 101, "PD", 1, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PURPOSE_MASTER.PURPOSE_DS1.1, noquote))"
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PURPOSE_MASTER.PURPOSE_SM1.1, noquote))")

VALUES( 102, "LGD", 2, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PURPOSE_MASTER.PURPOSE_DS2.1, noquote))"
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PURPOSE_MASTER.PURPOSE_SM2.1, noquote))")

VALUES( 103, "CCF", 2, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PURPOSE_MASTER.PURPOSE_DS3.1, noquote))"
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PURPOSE_MASTER.PURPOSE_SM3.1, noquote))")
;
quit;

%mend rmcr_csb_purpose_master;
