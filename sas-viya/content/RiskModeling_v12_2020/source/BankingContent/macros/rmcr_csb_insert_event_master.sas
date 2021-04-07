%macro rmcr_csb_insert_event_master;

proc sql;
insert into &lib_apdm..EVENT_MASTER (
EVENT_SK ,EVENT_DATE_COLUMN_NM ,EVENT_DATE_COLUMN_DATA_TYPE_SK,CALC_ALL_VAR_WRT_SAME_DT_FLG, CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,EVENT_SHORT_NM ,EVENT_DESC 
)
VALUES( 1, 'EVENT_APPLICATION_DT',4 ,"N", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_SM1.1, noquote))","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_DS1.1, noquote))")/* i18nOK:Line */
VALUES( 2, 'EVENT_DEFAULT_DT',4,"N", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_SM2.1, noquote))","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_DS2.1, noquote))")/* i18nOK:Line */

VALUES( 3, 'EVENT_APPLICATION_DT', 4,"Y", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_SM3.1, noquote))","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_DS3.1, noquote))")/* i18nOK:Line */
VALUES( 4, 'EVENT_DEFAULT_DT',4,"Y","%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_SM4.1, noquote))","%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EVENT_MASTER.EVENT_DS4.1, noquote))")/* i18nOK:Line */

;
quit;

%mend rmcr_csb_insert_event_master;