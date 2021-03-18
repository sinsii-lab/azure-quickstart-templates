%macro rmcr_csb_subject_group_master;

proc sql;
insert into &lib_apdm..SUBJECT_GROUP_MASTER (
SUBJECT_GROUP_SK ,SUBJECT_GROUP_CD ,TARGET_QUERY_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,SUBJECT_GROUP_DESC ,SUBJECT_GROUP_SHORT_NM 
)
VALUES( 7, 'P_AP_LON', 93, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS1.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM1.1, noquote))")
VALUES( 9, 'P_AP_CCD', 95, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS2.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM2.1, noquote))")
VALUES( 8, 'P_AP_MTG', 97, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS3.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM3.1, noquote))")

/*
VALUES( 11, 'L_IA_LON', 103, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS4.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM4.1, noquote))")

VALUES( 12, 'L_IA_MTG', 111, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS5.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM5.1, noquote))")

VALUES( 13, 'L_IA_CCD', 114, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS6.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM6.1, noquote))")

VALUES( 14, 'C_IA_CCD', 116, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS7.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM7.1, noquote))")

VALUES( 15, 'L_CA_LON', 120, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS8.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM8.1, noquote))")

VALUES( 16, 'L_CA_MTG', 122, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS9.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM9.1, noquote))")
*/

VALUES( 18, 'L_CF_LOC', 126, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS10.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM10.1, noquote))")
VALUES( 19, 'L_CF_WC', 128, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS11.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM11.1, noquote))")
VALUES( 20, 'L_CF_CC', 130, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS12.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM12.1, noquote))")
VALUES( 21, 'P_CP_LON', 132, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS13.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM13.1, noquote))")
VALUES( 22, 'P_CP_MTG', 134, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS14.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM14.1, noquote))")
VALUES( 24, 'P_CA_LON', 138, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS15.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM15.1, noquote))")
VALUES( 25, 'P_CA_MTG', 140, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS16.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM16.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..SUBJECT_GROUP_MASTER (
SUBJECT_GROUP_SK ,SUBJECT_GROUP_CD ,TARGET_QUERY_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,SUBJECT_GROUP_DESC ,SUBJECT_GROUP_SHORT_NM 
)
VALUES( 1, "P_IC_LON", 81, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS1.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM1.1, noquote))")
VALUES( 2, "P_IC_CCD", 83, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS2.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM2.1, noquote))")
VALUES( 4, "P_IC_COR", 85, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS3.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM3.1, noquote))")
VALUES( 3, "P_IC_MTG", 87, "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_DS4.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_bnkfdn_banking,SUBJECT_GROUP_MASTER.SUBJECT_GROUP_SM4.1, noquote))")
;
quit;

%mend rmcr_csb_subject_group_master;