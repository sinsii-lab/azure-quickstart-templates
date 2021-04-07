%macro rmcr_insert_script_level_master;

proc sql;
insert into &lib_apdm..LEVEL_MASTER (
LEVEL_SK ,MASTER_SOURCE_TABLE_SK, SUBSET_RK_TABLE_NM, SUBSET_SK_TABLE_NM, DATE_FOR_AVG_CALC_COLUMN_SK, AVAILABLE_FOR_PROJECT_DFN_FLG ,LEVEL_CD, ACTIVE_FLG, CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LEVEL_DESC ,LEVEL_SHORT_NM, LEVEL_KEY_COLUMN_NM, LEVEL_TYPE_SK 
)
VALUES( 1,62,"I_ACC_RK","I_ACC_SK", 2880,"Y", "A", "Y", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LEVEL_MASTER.LEVEL_DS1.1, noquote))","%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LEVEL_MASTER.LEVEL_SM1.1, noquote))","ACCOUNT_RK",2) /* i18NOK:LINE */
VALUES( 2,63,"I_CUS_RK","I_CUS_SK",2886,"Y", "I", "Y", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid","%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LEVEL_MASTER.LEVEL_DS2.1, noquote))","%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LEVEL_MASTER.LEVEL_SM2.1, noquote))","CUSTOMER_RK",2) /* i18NOK:LINE */
;
quit;

%mend rmcr_insert_script_level_master;