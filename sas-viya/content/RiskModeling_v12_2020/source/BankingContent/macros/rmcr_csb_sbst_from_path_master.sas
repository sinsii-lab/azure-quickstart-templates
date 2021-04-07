%macro rmcr_csb_sbst_from_path_master;

proc sql;
insert into &lib_apdm..SUBSET_FROM_PATH_MASTER (
SUBSET_FROM_PATH_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,FROM_PATH_DESC ,FROM_PATH_SHORT_NM 
)
VALUES( 4, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS4.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM4.1, noquote))")
VALUES( 5, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS5.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM5.1, noquote))")
VALUES( 6, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS6.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM6.1, noquote))")
VALUES( 7, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS7.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM7.1, noquote))")
VALUES( 8, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS8.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM8.1, noquote))")
VALUES( 9, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS9.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM9.1, noquote))")
VALUES( 10, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS10.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM10.1, noquote))")
VALUES( 11, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS11.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM11.1, noquote))")
VALUES( 12, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS12.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM12.1, noquote))")
;
quit;

%mend rmcr_csb_sbst_from_path_master;
