%Macro rmcr_csb_insert_library_master;

proc sql;
insert into &lib_apdm..LIBRARY_MASTER (
LIBRARY_SK ,LIBRARY_REFERENCE ,LIBNAME_STATEMENT ,LIBRARY_TYPE_CD, IM_METADATA_PATH, CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LIBRARY_SHORT_NM ,LIBRARY_DESC 
)
VALUES( 6, "CSB_IM", '', "SRC_IM","/Products/SAS Credit Scoring For Banking Content/Input Structures/Infomaps", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", /* i18nOK:Line */
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,LIBRARY_MASTER.LIBRARY_SM1.1, noquote))",
"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,LIBRARY_MASTER.LIBRARY_DS1.1, noquote))")
;
quit;

%mend rmcr_csb_insert_library_master;

