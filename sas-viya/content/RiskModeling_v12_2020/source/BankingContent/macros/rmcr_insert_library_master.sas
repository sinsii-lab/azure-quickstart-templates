%macro rmcr_insert_library_master;

proc sql;
insert into &lib_apdm..LIBRARY_MASTER (
LIBRARY_SK ,LIBRARY_REFERENCE ,LIBNAME_STATEMENT ,LIBRARY_TYPE_CD, IM_METADATA_PATH, CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LIBRARY_SHORT_NM ,LIBRARY_DESC 
)
VALUES( 1, "inpbase", '', "SRC_TBL","", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_SM1.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_DS1.1, noquote))") 
VALUES( 2, "DIM", '', "SRC_TBL","", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_SM2.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_DS2.1, noquote))") 
VALUES( 3, "DETAIL", '', "SRC_TBL","", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_SM3.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_DS3.1, noquote))") 
VALUES( 4, "BRIDGE", '', "SRC_TBL","", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_SM4.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_DS4.1, noquote))") 
VALUES( 5, "BNKF_IM", '', "SRC_IM","/Products/SAS Credit Scoring For Banking Content/Input Structures/Infomaps",  /* i18NOK:LINE */
"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", 
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_SM5.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,LIBRARY_MASTER.LIBRARY_DS5.1, noquote))") 
;
quit;

%mend rmcr_insert_library_master;
