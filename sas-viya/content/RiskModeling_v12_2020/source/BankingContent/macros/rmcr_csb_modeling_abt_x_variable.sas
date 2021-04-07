%macro rmcr_csb_modeling_abt_x_variable;
proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 5, 32, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS1.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM1.1, noquote))")
VALUES( 9, 57, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS2.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM2.1, noquote))")
VALUES( 26, 183, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS3.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM3.1, noquote))")
VALUES( 27, 243, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS4.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM4.1, noquote))")
VALUES( 13, 84, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS5.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM5.1, noquote))")
VALUES( 14, 118, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS6.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM6.1, noquote))")
VALUES( 5, 27, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS7.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM7.1, noquote))")
VALUES( 9, 51, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS8.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM8.1, noquote))")
VALUES( 14, 113, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS9.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM9.1, noquote))")
VALUES( 26, 178, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS10.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM10.1, noquote))")
VALUES( 27, 238, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS11.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM11.1, noquote))")
VALUES( 13, 79, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS12.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM12.1, noquote))")
VALUES( 5, 33, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS13.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM13.1, noquote))")
VALUES( 9, 58, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS14.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM14.1, noquote))")
VALUES( 14, 119, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS15.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM15.1, noquote))")
VALUES( 27, 244, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS16.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM16.1, noquote))")
VALUES( 13, 85, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS17.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM17.1, noquote))")
VALUES( 26, 184, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS18.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM18.1, noquote))")
VALUES( 5, 28, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS19.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM19.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 9, 52, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS20.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM20.1, noquote))")
VALUES( 14, 114, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS21.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM21.1, noquote))")
VALUES( 26, 179, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS22.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM22.1, noquote))")
VALUES( 27, 239, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS23.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM23.1, noquote))")
VALUES( 13, 80, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS24.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM24.1, noquote))")
VALUES( 5, 29, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS25.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM25.1, noquote))")
VALUES( 9, 54, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS26.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM26.1, noquote))")
VALUES( 26, 180, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS27.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM27.1, noquote))")
VALUES( 27, 240, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS28.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM28.1, noquote))")
VALUES( 13, 81, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS29.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM29.1, noquote))")
VALUES( 14, 115, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS30.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM30.1, noquote))")
VALUES( 5, 22, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS31.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM31.1, noquote))")
VALUES( 9, 48, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS32.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM32.1, noquote))")
VALUES( 14, 110, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS33.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM33.1, noquote))")
VALUES( 26, 175, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS34.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM34.1, noquote))")
VALUES( 27, 235, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS35.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM35.1, noquote))")
VALUES( 13, 76, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS36.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM36.1, noquote))")
VALUES( 5, 30, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS37.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM37.1, noquote))")
VALUES( 9, 55, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS38.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM38.1, noquote))")
VALUES( 26, 181, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS39.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM39.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 27, 241, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS40.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM40.1, noquote))")
VALUES( 13, 82, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS41.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM41.1, noquote))")
VALUES( 14, 116, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS42.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM42.1, noquote))")
VALUES( 5, 23, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS43.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM43.1, noquote))")
VALUES( 9, 49, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS44.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM44.1, noquote))")
VALUES( 14, 111, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS45.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM45.1, noquote))")
VALUES( 26, 176, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS46.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM46.1, noquote))")
VALUES( 27, 236, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS47.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM47.1, noquote))")
VALUES( 13, 77, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS48.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM48.1, noquote))")
VALUES( 5, 31, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS49.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM49.1, noquote))")
VALUES( 9, 56, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS50.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM50.1, noquote))")
VALUES( 26, 182, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS51.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM51.1, noquote))")
VALUES( 27, 242, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS52.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM52.1, noquote))")
VALUES( 13, 83, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS53.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM53.1, noquote))")
VALUES( 14, 117, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS54.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM54.1, noquote))")
VALUES( 5, 26, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS55.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM55.1, noquote))")
VALUES( 9, 50, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS56.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM56.1, noquote))")
VALUES( 14, 112, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS57.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM57.1, noquote))")
VALUES( 26, 177, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS58.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM58.1, noquote))")
VALUES( 27, 237, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS59.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM59.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 13, 78, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS60.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM60.1, noquote))")
VALUES( 1, 1, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS61.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM61.1, noquote))")
VALUES( 5, 12, 'Y', 'N', 'Y',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS62.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM62.1, noquote))")
VALUES( 6, 46, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS63.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM63.1, noquote))")
VALUES( 26, 164, 'Y', 'N', 'Y', "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS64.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM64.1, noquote))")
VALUES( 21, 143, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS65.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM65.1, noquote))")
VALUES( 22, 147, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS66.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM66.1, noquote))")
VALUES( 11, 154, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS67.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM67.1, noquote))")
VALUES( 24, 160, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS69.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM69.1, noquote))")
VALUES( 13, 97, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS70.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM70.1, noquote))")
VALUES( 1, 150, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS71.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM71.1, noquote))")
VALUES( 6, 151, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS72.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM72.1, noquote))")
VALUES( 7, 152, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS73.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM73.1, noquote))")
VALUES( 11, 153, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS74.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM74.1, noquote))")
VALUES( 23, 158, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS77.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM77.1, noquote))")
VALUES( 24, 159, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS78.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM78.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 5, 44, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS80.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM80.1, noquote))")
VALUES( 27, 248, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS81.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM81.1, noquote))")
VALUES( 9, 72, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS82.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM82.1, noquote))")
VALUES( 26, 187, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS83.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM83.1, noquote))")
VALUES( 14, 123, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS84.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM84.1, noquote))")
VALUES( 13, 98, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS85.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM85.1, noquote))")
VALUES( 5, 45, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS86.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM86.1, noquote))")
VALUES( 27, 249, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS87.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM87.1, noquote))")
VALUES( 9, 73, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS88.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM88.1, noquote))")
VALUES( 26, 188, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS89.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM89.1, noquote))")
VALUES( 14, 124, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS90.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM90.1, noquote))")
VALUES( 13, 99, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS91.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM91.1, noquote))")
VALUES( 14, 122, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS95.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM95.1, noquote))")
VALUES( 27, 247, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS96.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM96.1, noquote))")
VALUES( 9, 65, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS97.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM97.1, noquote))")
VALUES( 13, 91, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS98.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM98.1, noquote))")
VALUES( 13, 89, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS99.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM99.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 14, 120, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS100.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM100.1, noquote))")
VALUES( 27, 228, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS104.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM104.1, noquote))")
VALUES( 9, 64, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS105.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM105.1, noquote))")
VALUES( 13, 88, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS106.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM106.1, noquote))")
VALUES( 27, 226, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS107.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM107.1, noquote))")
VALUES( 14, 103, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS108.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM108.1, noquote))")
VALUES( 9, 60, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS109.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM109.1, noquote))")
VALUES( 5, 35, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS110.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM110.1, noquote))")
VALUES( 26, 168, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS111.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM111.1, noquote))")
VALUES( 13, 87, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS112.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM112.1, noquote))")
VALUES( 27, 227, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS113.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM113.1, noquote))")
VALUES( 14, 102, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS114.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM114.1, noquote))")
VALUES( 9, 59, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS115.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM115.1, noquote))")
VALUES( 5, 34, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS116.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM116.1, noquote))")
VALUES( 26, 167, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS117.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM117.1, noquote))")
VALUES( 13, 86, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS118.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM118.1, noquote))")
VALUES( 27, 224, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS119.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM119.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 14, 101, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS120.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM120.1, noquote))")
VALUES( 9, 53, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS121.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM121.1, noquote))")
VALUES( 5, 20, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS122.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM122.1, noquote))")
VALUES( 26, 166, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS123.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM123.1, noquote))")
VALUES( 13, 75, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS124.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM124.1, noquote))")
VALUES( 27, 225, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS125.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM125.1, noquote))")
VALUES( 14, 100, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS126.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM126.1, noquote))")
VALUES( 9, 47, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS127.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM127.1, noquote))")
VALUES( 5, 13, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS128.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM128.1, noquote))")
VALUES( 26, 165, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS129.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM129.1, noquote))")
VALUES( 13, 74, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS130.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM130.1, noquote))")
VALUES( 21, 140, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS131.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM131.1, noquote))")
VALUES( 22, 145, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS132.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM132.1, noquote))")

;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 5, 43, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS144.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM144.1, noquote))")
VALUES( 26, 174, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS145.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM145.1, noquote))")
VALUES( 27, 229, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS146.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM146.1, noquote))")
VALUES( 14, 104, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS147.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM147.1, noquote))")
VALUES( 9, 71, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS148.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM148.1, noquote))")
VALUES( 21, 142, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS149.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM149.1, noquote))")
VALUES( 22, 148, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS150.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM150.1, noquote))")
VALUES( 21, 141, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS151.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM151.1, noquote))")
VALUES( 22, 146, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS152.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM152.1, noquote))")
VALUES( 27, 230, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS153.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM153.1, noquote))")
VALUES( 14, 108, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS154.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM154.1, noquote))")
VALUES( 5, 41, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS155.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM155.1, noquote))")
VALUES( 26, 171, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS156.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM156.1, noquote))")
VALUES( 9, 69, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS157.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM157.1, noquote))")
VALUES( 13, 95, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS158.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM158.1, noquote))")
VALUES( 27, 231, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS159.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM159.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 14, 109, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS160.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM160.1, noquote))")
VALUES( 5, 42, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS161.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM161.1, noquote))")
VALUES( 26, 170, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS162.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM162.1, noquote))")
VALUES( 9, 70, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS163.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM163.1, noquote))")
VALUES( 13, 96, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS164.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM164.1, noquote))")
VALUES( 27, 232, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS165.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM165.1, noquote))")
VALUES( 14, 105, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS166.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM166.1, noquote))")
VALUES( 9, 66, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS167.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM167.1, noquote))")
VALUES( 5, 38, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS168.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM168.1, noquote))")
VALUES( 26, 169, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS169.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM169.1, noquote))")
VALUES( 13, 92, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS170.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM170.1, noquote))")
VALUES( 27, 234, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS171.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM171.1, noquote))")
VALUES( 14, 106, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS172.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM172.1, noquote))")
VALUES( 26, 172, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS173.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM173.1, noquote))")
VALUES( 9, 67, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS174.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM174.1, noquote))")
VALUES( 5, 39, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS175.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM175.1, noquote))")
VALUES( 13, 93, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS176.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM176.1, noquote))")
VALUES( 27, 233, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS177.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM177.1, noquote))")
VALUES( 14, 107, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS178.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM178.1, noquote))")
VALUES( 26, 173, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS179.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM179.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE (
ABT_SK ,VARIABLE_SK ,PART_OF_ABT_FLG ,OUTCOME_VARIABLE_FLG ,IMPLICIT_VARIABLE_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,VARIABLE_DESC ,VARIABLE_SHORT_NM 
)
VALUES( 9, 68, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS180.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM180.1, noquote))")
VALUES( 5, 40, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS181.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM181.1, noquote))")
VALUES( 13, 94, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS182.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM182.1, noquote))")
VALUES( 21, 144, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS184.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM184.1, noquote))")
VALUES( 22, 149, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS185.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM185.1, noquote))")
VALUES( 5, 37, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS186.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM186.1, noquote))")
VALUES( 26, 186, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS187.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM187.1, noquote))")
VALUES( 27, 246, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS188.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM188.1, noquote))")
VALUES( 14, 121, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS189.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM189.1, noquote))")
VALUES( 9, 63, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS190.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM190.1, noquote))")
VALUES( 5, 36, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS191.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM191.1, noquote))")
VALUES( 26, 185, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS192.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM192.1, noquote))")
VALUES( 27, 245, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS193.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM193.1, noquote))")
VALUES( 9, 61, 'Y', 'N', 'N',  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",/* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS194.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM194.1, noquote))")
;
quit;

proc sql;
insert into &lib_apdm..MODELING_ABT_X_VARIABLE(
abt_sk ,variable_sk ,part_of_abt_flg ,outcome_variable_flg ,implicit_variable_flg ,
variable_short_nm ,variable_desc ,
created_dttm ,created_by_user ,last_processed_dttm ,last_processed_by_user 
)

VALUES(30,322,"Y","N","Y",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM138.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS138.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(30,323,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM102.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS102.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(30,324,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM92.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS92.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(30,325,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM141.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS141.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(30,326,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM135.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS135.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES( 30, 351, 'Y', 'N', 'N',  /* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS75.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM75.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(30,368,"Y","N","Y",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS195.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM195.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(30,369,"Y","N","Y",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS198.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM198.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(31,329,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM135.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS135.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(31,330,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM92.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS92.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

VALUES(31,331,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM141.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS141.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(31,332,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM138.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS138.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(31,333,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM102.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS102.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES( 31, 352, 'Y', 'N', 'N', /* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS75.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM75.1, noquote))"
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(31,381,"Y","N","Y",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS195.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM195.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(31,382,"Y","N","Y",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS198.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM198.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,346,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM92.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS92.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,347,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM141.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS141.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,348,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM135.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS135.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,349,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM102.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS102.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,350,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM138.1, noquote))", 
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS138.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES( 32, 353, 'Y', 'N', 'N', /* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS75.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM75.1, noquote))"
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,356,"Y","N","Y",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS195.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM195.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,357,"Y","N","Y",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS198.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM198.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

VALUES(32,383,"Y","N","N",/* I18NOK:LINE */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS68.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM68.1, noquote))",
"%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid")

;
quit;



proc sql noprint;

update &lib_apdm..modeling_abt_x_variable
set outcome_variable_flg = "Y"/* i18NOK:LINE */
where variable_sk in  
(

22,23,26,27,28,29,30,31,32,33,36,37,44,45,48,49,50,51,52,54,55,56,57,58,61,63,65,72,73,76,77,78,79,80,
81,82,83,84,85,89,91,97,98,99,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,144,149,175,
176,177,178,179,180,181,182,183,184,185,186,187,188,235,236,237,238,239,240,241,242,243,244,245,246,24
7,248,249
)
;

quit;

%mend rmcr_csb_modeling_abt_x_variable;