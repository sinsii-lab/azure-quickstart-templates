/************************************************************************************
sample code to execute this:
%rmcr_apply_server_localisation();
=================================================================
This macro updates the apdm table MM_REPORT_CATEGORY_MASTER with the locale language
************************************************************************************/

%macro rmcr_apply_server_localisation()/minoperator;

/**********************************************************
Check for current locale.If english cahnge is not required
***********************************************************/
%let m_locale=%scan(%sysfunc(getpxLocale()),1,'_');	/* i18NOK:LINE */
%let m_locale_full=%sysfunc(getpxLocale());
%put &m_locale;

%let m_locale=&m_locale;
%let m_locale_full=&m_locale_full;

%if &m_locale in (es ja ko ru) or &m_locale_full in (pt_BR zh_CN zh_HK zh_TW) %then %do;
/**************************************************************************************
              Table containg list of apdm tables and columns to be updated with their locale language
****************************************************************************************/
              proc sql;

              CREATE TABLE work.table_list (
                                                                                                                                                   TABLE_NM         CHARACTER(255) FORMAT=$255. INFORMAT=$255. LABEL='TABLE NAME', /* i18NOK:LINE */
                                                                                                                                                   COLUMN_NM               CHARACTER(255) FORMAT=$255. INFORMAT=$255. LABEL='COLUMN TO BE UPDATED', /* i18NOK:LINE */
                                                                                                                                                   COLUMN_SK_NM      CHARACTER(255) FORMAT=$255. INFORMAT=$255. LABEL='COLUMN TO BE JOINED' /* i18NOK:LINE */
                                                                                                                                                  
                                                                                                                                  );
                                                                                                                      
              quit;

              PROC SQL;
              INSERT INTO TABLE_LIST(TABLE_NM,COLUMN_NM,COLUMN_SK_NM)
VALUES("MM_REPORT_CATEGORY_MASTER","report_category_desc","report_category_sk") /* i18NOK:LINE */
              ;
              QUIT;


              proc sql;
              select * from table_list;
              quit;

              %let count_rec=&sqlobs;
/***********************************************************************
              Create replica of above apdm tables in work library with no data 
************************************************************************/
              %do i=1 %to &count_rec;

                             data _null_;
                             obs=&i;
                             set table_list point=obs;
                             call symputx("m_table_nm",table_nm);  /* i18NOK:LINE */
                             stop;
                             run;
                             
                             proc sql;
                             create table &m_table_nm as
                             select * from &lib_apdm..&m_table_nm 
                             where 1=2;
                             quit;

              %end;

/******************************************************************************************
              Call the macro to insert values in tables present in work library according to their locale
*******************************************************************************************/

              %let lib_apdm=work;
              
              options LRECL=5000;

              options NOQUOTELENMAX;

              proc sql;

              insert into &lib_apdm..MM_REPORT_CATEGORY_MASTER ( report_category_sk ,report_category_cd ,report_category_desc )

              VALUES( 1, "ONG", "%sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports,MM_REPORT_CATEGORY_MASTER.CATEGORY_DESC1.1, noquote))" )  /* i18NOK:LINE*/

              VALUES( 2, "BCK", "%sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports,MM_REPORT_CATEGORY_MASTER.CATEGORY_DESC2.1, noquote))" )  /* i18NOK:LINE*/

              VALUES( 3, "DD", "%sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports,MM_REPORT_CATEGORY_MASTER.CATEGORY_DESC3.1, noquote))" )  /* i18NOK:LINE*/

              ;
              quit;      
              


/***************************************************************************************
              Update columns of apdm tables with those of column of work tables that have locale values
****************************************************************************************/
              %let lib_apdm=apdm;
              %do i=1 %to &count_rec;

                             data _null_;
                               obs=&i;
                                           set table_list point=obs;
                                                          call symputx("m_table_nm",table_nm);		/* i18NOK:LINE */
                                                          call symputx("m_column_nm",column_nm);	/* i18NOK:LINE */
                                                          call symputx("m_column_sk_nm",column_sk_nm);	/* i18NOK:LINE */
                                                          stop;
                             run;
                             
                             proc sql;
                                           update &lib_apdm..&m_table_nm apdm_tbl 
                                           set &m_column_nm = (select &m_column_nm from &m_table_nm work_tbl where apdm_tbl.&m_column_sk_nm = work_tbl.&m_column_sk_nm    );
                             quit;

              %end;
%end;/*end for locale check*/
%else %if "&m_locale" eq "en" %then    %put Current locale is English. Localisation is not required to be applied.;	/* i18NOK:LINE */

%else %put Current locale is &m_locale_full. Localisation is not supported for this locale.;

%mend rmcr_apply_server_localisation;
/****************************************************************************************************************************************************/