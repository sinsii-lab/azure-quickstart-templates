%macro rmcr_insrt_src_table_master;

proc sql;
insert into &lib_apdm..SOURCE_TABLE_MASTER (
SOURCE_TABLE_SK ,SOURCE_TABLE_NM ,SOURCE_TABLE_CD ,LIBRARY_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,SOURCE_TABLE_DESC ,SOURCE_TABLE_SHORT_NM 
)
VALUES( 1, "ACCOUNT_SNAPSHOT_BASE", "SNP", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS51.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM51.1, noquote))")
VALUES( 2, "ACCOUNT_TRANSACTION_BASE", "TR", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS52.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM52.1, noquote))")
VALUES( 3, "INQUIRY_FACT_BASE", "INQ", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS53.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM53.1, noquote))")
VALUES( 6, "CONTACTS_FACT_BASE", "CNT", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS54.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM54.1, noquote))")
VALUES( 7, "OUTBOUND_COMM_FACT_BASE", "OC", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS55.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM55.1, noquote))")
VALUES( 13, "IND_CUSTOMER_DIM", "IDM", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS56.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM56.1, noquote))")
VALUES( 17, "ACCOUNT_DETAIL", "ADT", 3, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS57.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM57.1, noquote))")
VALUES( 18, "IND_ACCOUNT_DIM", "ADM", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS58.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM58.1, noquote))")
VALUES( 45, "IND_ACCOUNT_DIM", "IAD", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS61.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM61.1, noquote))")
VALUES( 19, "ACCOUNT_TRANSACTION_DETAIL", "TDT", 3, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS59.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM59.1, noquote))")
VALUES( 41, "BANK_CARD_DTL_BASE", "BCD", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS60.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM60.1, noquote))")
VALUES( 47, "ACCOUNT_OTHER_INFO_BASE", "ALB", 1, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS62.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM62.1, noquote))")
VALUES( 62, "ACCOUNT_DIM", "AD", 2, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS69.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM69.1, noquote))")
VALUES( 63, "CUSTOMER_DIM", "CD", 2, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS68.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM68.1, noquote))")
VALUES( 64, "IND_CUSTOMER_X_ACCOUNT", "CXA", 5, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS63.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM63.1, noquote))")
;
quit;

%mend rmcr_insrt_src_table_master;