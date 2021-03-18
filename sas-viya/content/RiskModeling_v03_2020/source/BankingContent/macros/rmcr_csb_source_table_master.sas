%macro rmcr_csb_source_table_master;

proc sql;
insert into &lib_apdm..SOURCE_TABLE_MASTER (
SOURCE_TABLE_SK ,SOURCE_TABLE_NM ,SOURCE_TABLE_CD ,LIBRARY_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,SOURCE_TABLE_DESC ,SOURCE_TABLE_SHORT_NM 
)
VALUES( 4, 'ALL_APPLICANT_DIM', 'APC', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS1.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM1.1, noquote))")
VALUES( 8, 'RECOVERY_FACT_BASE', 'RC', 6,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS3.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM3.1, noquote))")
VALUES( 9, 'CORPORATE_FINANCIAL_DETAIL', 'CDT', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS4.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM4.1, noquote))")
VALUES( 11, 'CORP_CUSTOMER_DIM', 'CDM', 6,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS5.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM5.1, noquote))")
VALUES( 12, 'GUR_IND_APPLICANT_DIM', 'AGR', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS6.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM6.1, noquote))")
VALUES( 14, 'NOM_IND_APPLICANT_DIM', 'ANM', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS7.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM7.1, noquote))")
VALUES( 15, 'PRI_IND_APPLICANT_DIM', 'APR', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS8.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM8.1, noquote))")
VALUES( 16, 'SEC_IND_APPLICANT_DIM', 'ASC', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS9.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM9.1, noquote))")
VALUES( 20, 'ALL_APPLICATION_DIM', 'PDM', 6,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS10.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM10.1, noquote))")
VALUES( 21, 'CORPORATE_OWNER_DETAIL', 'ODT', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS11.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM11.1, noquote))")
VALUES( 23, 'CREDIT_FACILITY_DIM', 'FDM', 2,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS12.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM12.1, noquote))")
VALUES( 26, 'ACCOUNT_COUNTRY_DETAIL', 'ACM', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS13.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM13.1, noquote))")
VALUES( 27, 'CREDIT_FACILITY_COUNTRY_DETAIL', 'CFC', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS14.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM14.1, noquote))")
VALUES( 28, 'ACCOUNT_DEFAULT_DIM', 'ADF', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS15.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM15.1, noquote))")
VALUES( 31, 'CREDIT_FACILITY_DEFAULT_DIM', 'CFD', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS18.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM18.1, noquote))")
VALUES( 32, 'CORP_ACCOUNT_DIM', 'CAD', 6,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS19.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM19.1, noquote))")
;
quit;


proc sql;
insert into &lib_apdm..SOURCE_TABLE_MASTER (
SOURCE_TABLE_SK ,SOURCE_TABLE_NM ,SOURCE_TABLE_CD ,LIBRARY_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,SOURCE_TABLE_DESC ,SOURCE_TABLE_SHORT_NM 
)
VALUES( 33, 'ALL_APPLICATION_DIM', 'CPD', 6,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS20.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM20.1, noquote))")
VALUES( 34, 'ACCOUNT_SCORE_DETAIL', 'ASD', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS21.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM21.1, noquote))")
VALUES( 35, 'APPLICATION_SCORE_DETAIL', 'APD', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS22.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM22.1, noquote))")
VALUES( 36, 'CORPORATE_CUSTOMER_QUAL_DETAIL', 'CQD', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS23.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM23.1, noquote))")
VALUES( 37, 'CREDIT_BUREAU_DTL_BASE', 'CBD', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS24.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM24.1, noquote))")
VALUES( 38, 'CREDIT_FACILITY_SCORE_DETAIL', 'CFS', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS25.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM25.1, noquote))")
VALUES( 39, 'CUSTOMER_SCORE_DETAIL', 'CSD', 3,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS26.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM26.1, noquote))")
VALUES( 40, 'ALL_APPLICATION_DIM', 'CAS', 6,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS27.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM27.1, noquote))")
VALUES( 43, 'CORP_CUSTOMER_PRI_OWNER_DETAIL', 'CCP', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS29.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM29.1, noquote))")
VALUES( 44, 'CORP_CUSTOMER_SEC_OWNER_DETAIL', 'CCS', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS30.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM30.1, noquote))")
VALUES( 46, 'CORP_ACCOUNT_DIM', 'CAC', 6,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS31.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM31.1, noquote))")
VALUES( 48, 'GUR_CORP_APPLICANT_DIM', 'GCA', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS32.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM32.1, noquote))")
VALUES( 49, 'NOM_CORP_APPLICANT_DIM', 'NCA', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS33.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM33.1, noquote))")
VALUES( 50, 'PRI_CORP_APPLICANT_DIM', 'PCA', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS34.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM34.1, noquote))")
VALUES( 51, 'SEC_CORP_APPLICANT_DIM', 'SCA', 1,  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid",  "%sysfunc(datetime(),datetime.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS35.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM35.1, noquote))")
;
quit;

proc sql;
insert into &lib_apdm..SOURCE_TABLE_MASTER (
SOURCE_TABLE_SK ,SOURCE_TABLE_NM ,SOURCE_TABLE_CD ,LIBRARY_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,SOURCE_TABLE_DESC ,SOURCE_TABLE_SHORT_NM 
)
VALUES(52,'CORP_ACCT_COUNTRY_SNAPSHOT','CAN',1, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS36.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM36.1, noquote))")
VALUES(53,'CORP_APPL_COUNTRY_SNAPSHOT','CLN',1, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS37.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM37.1, noquote))")
VALUES(54,'IND_ACCT_COUNTRY_SNAPSHOT','IAN',1, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS38.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM38.1, noquote))")
VALUES(55,'IND_APPL_COUNTRY_SNAPSHOT','ILN',1, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS39.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM39.1, noquote))")
VALUES(56,'CREDIT_FACILITY_CNTRY_SNAPSHOT','ICF',1, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS40.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM40.1, noquote))")
VALUES(57,'IND_CUST_COUNTRY_SNAPSHOT','ICN',1, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS41.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM41.1, noquote))")
VALUES(58,'CORP_CUST_COUNTRY_SNAPSHOT','CCN',1, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS42.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM42.1, noquote))")
VALUES(65,'CORP_ACCOUNT_X_CUSTOMER ','CAX',6, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS45.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM45.1, noquote))")
VALUES(66,'CORP_CRD_FACILITY_X_CUSTOMER','CXC',6, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS46.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM46.1, noquote))")
VALUES(67,'APPLICATION_DIM','APP',2, "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", "%sysfunc(datetime(),datetime.)"dt,"&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_DS50.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,SOURCE_TABLE_MASTER.SOURCE_TABLE_SM50.1, noquote))");
quit;

%mend rmcr_csb_source_table_master;