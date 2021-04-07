%macro rmcr_csb_modeling_abt_master;

proc sql;
insert into &lib_apdm..MODELING_ABT_MASTER (
ABT_SK ,PROJECT_SK ,LEVEL_SK ,MODELING_ABT_STATUS_SK ,SOURCE_DATA_TIME_GRAIN_SK ,TARGET_PERIOD_CNT ,TARGET_PERIOD_TIME_FREQ_SK ,APPLY_BLD_DT_CAP_OUTCM_VAR_FLG ,APPLY_OC_BSD_PST_IMPL_SBST_FLG ,ABT_TABLE_NM ,PUBLISH_TEMPLATE_FLG ,EVENT_SK ,CREATED_DTTM ,
CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LAST_BUILT_RUN_DTTM ,LAST_BUILT_STATUS_SK ,LAST_BUILT_STACKED_TYPE_FLG ,ABT_SHORT_NM ,ABT_DESC 
)
VALUES( 1, ., 3, 1, 1, 12, 1, 'Y', 'N', 'ABT_PDA_IMPLICIT_VAR_MDL', 'N', 1, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM1.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS1.1, noquote))")
VALUES( 5, ., 3, 1, 1, 12, 1, 'Y', 'N', 'ABT_PDA_IND_APP_OUTCOME_VAR', 'Y', 1, 1610894750.8, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM2.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS2.1, noquote))")
VALUES( 6, ., 8, 1, 1, 12, 1, 'Y', 'N', 'ABT_CRP_APP_IMPLICIT_VAR_MDL', 'N', 1, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", 1610902864.9, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM3.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS3.1, noquote))")
VALUES( 7, ., 1, 1, 1, 12, 1, 'Y', 'Y', 'ABT_PDO_IND_ACC_IMPLICIT_VAR_M', 'N', ., "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM4.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS4.1, noquote))")
VALUES( 9, ., 1, 1, 1, 12, 1, 'Y', 'Y', 'ABT_PDO_IND_ACC_OUTCOME_VAR', 'Y', ., "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM5.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS5.1, noquote))")
VALUES( 11, ., 7, 1, 1, 12, 1, 'Y', 'Y', 'PDO_CRP_ACC_IMPLICIT_VAR_MDL', 'N', ., "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM6.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS6.1, noquote))")
VALUES( 13, ., 2, 1, 1, 12, 1, 'Y', 'Y', 'ABT_PDO_IND_CUS_OUTCOME_VAR', 'Y', ., "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM7.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS7.1, noquote))")
VALUES( 14, ., 5, 1, 1, 12, 1, 'Y', 'Y', 'ABT_PDO_CRP_CUS_OUTCOME_VAR', 'Y', ., "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM8.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS8.1, noquote))")
VALUES( 21, ., 1, 1, 1, ., 1, 'N', 'N', 'ABT_CCF_IND_ACC_OUTCOME_VAR', 'Y', 2, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM14.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS14.1, noquote))")
VALUES( 22, ., 7, 1, 1, ., 1, 'N', 'N', 'ABT_CCF_CRP_ACC_OUTCOME_VAR', 'Y', 2, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM15.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS15.1, noquote))")
VALUES( 23, ., 1, 1, 1, ., 1, 'N', 'N', 'ABT_CCF_IND_ACC_IMPLICIT_VAR_M', 'N', 2, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM16.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS16.1, noquote))")
VALUES( 24, ., 7, 1, 1, ., 1, 'N', 'N', 'ABT_CCF_CRP_ACC_IMPLICIT_VAR_M', 'N', 2, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM17.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS17.1, noquote))")
VALUES( 26, ., 8, 1, 1, 12, 1, 'Y', 'N', 'ABT_PDA_CRP_APP_OUTCOME_VAR', 'Y', 1, "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM18.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS18.1, noquote))")
VALUES( 27, ., 7, 1, 1, 12, 1, 'Y', 'Y', 'ABT_PDO_CRP_ACC_OUTCOME_VAR', 'Y', ., "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM19.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS19.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_MASTER (
ABT_SK ,PROJECT_SK ,LEVEL_SK ,MODELING_ABT_STATUS_SK ,SOURCE_DATA_TIME_GRAIN_SK ,TARGET_PERIOD_CNT ,TARGET_PERIOD_TIME_FREQ_SK ,APPLY_BLD_DT_CAP_OUTCM_VAR_FLG ,APPLY_OC_BSD_PST_IMPL_SBST_FLG ,
ABT_TABLE_NM ,PUBLISH_TEMPLATE_FLG ,EVENT_SK ,CREATED_DTTM ,
CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LAST_BUILT_RUN_DTTM ,LAST_BUILT_STATUS_SK ,LAST_BUILT_STACKED_TYPE_FLG ,
ABT_SHORT_NM ,ABT_DESC 
)
VALUES( 30, ., 1, 1, 1, ., 1, 'Y', 'N', /* I18NOK:LINE */
'RETAIL_ACC_LGD_EXT_VAR', 'Y', 2, /* I18NOK:LINE */
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM21.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS21.1, noquote))")


VALUES( 31, ., 7, 1, 1, ., 1, 'Y', 'N', /* I18NOK:LINE */
'CORP_ACC_LGD_EXT_VAR', 'Y', 2, /* I18NOK:LINE */
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM22.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS22.1, noquote))")


VALUES( 32, ., 4, 1, 1, ., 1, 'Y', 'N', /* I18NOK:LINE */
'CRD_FAC_LGD_EXT_VAR', 'Y', 2, /* I18NOK:LINE */
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", ., ., '',/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_SM23.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_MASTER.ABT_DS23.1, noquote))")
;
quit;

%mend rmcr_csb_modeling_abt_master;