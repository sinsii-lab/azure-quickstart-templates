%macro rmcr_insrt_sbst_from_path_master;

proc sql;
insert into &lib_apdm..SUBSET_FROM_PATH_MASTER (
SUBSET_FROM_PATH_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,FROM_PATH_DESC ,FROM_PATH_SHORT_NM 
)
VALUES( 1, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS1.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM1.1, noquote))")
VALUES( 2, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS2.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM2.1, noquote))")
VALUES( 3, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_DS3.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBSET_FROM_PATH_MASTER.FROM_PATH_SM3.1, noquote))")
;
quit;

%mend rmcr_insrt_sbst_from_path_master;
