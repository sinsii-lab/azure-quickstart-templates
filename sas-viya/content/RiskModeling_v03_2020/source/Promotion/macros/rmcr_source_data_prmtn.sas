/********************************************************************************************************
   	Module		:  rmcr_source_data_prmtn

   	Function	:  This macro will be used to import source data to cas libraries.

	Called-by	:  rmcr_81_import_wrppr
	Calls		:  None
					
   	Parameters	:  None
	
	Author		:  CSB Team 
				   
*********************************************************************************************************/
%macro rmcr_source_data_prmtn()/minoperator;

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;

%let scr_xpt_path=&m_staging_area_path/cs63_export_pkg/source_data;
%let scr_tbl_xpt_path=&scr_xpt_path/source_table;
%let scr_vw_xpt_path=&scr_xpt_path/source_view;
%let scr_im_xpt_path=&scr_xpt_path/source_im;

%let path_to_unzip_source_data_xpt=;
%dabt_make_work_area(dir=&scr_xpt_path, create_dir=unzip_src, path=path_to_unzip_source_data_xpt);

%let path_to_unzip_source_data_xpt=&path_to_unzip_source_data_xpt;

libname unzipsl "&path_to_unzip_source_data_xpt";


/*********************************************
Import source data to cas lib BANKCRFM
*********************************************/

data UPDATED_LIB_MAP;
length FROM_LIB $50. TO_LIB $50.;
infile "&scr_xpt_path./library_mapping.csv" dlm=',' dsd firstobs=2;     /* i18nOK:LINE */
input FROM_LIB $ TO_LIB $;
run;

%let bnk_cnt_appl=;
proc sql;
		select count(*) into :bnk_cnt_appl from &lib_apdm..CS_CR_INFO     /* i18nOK:LINE */
		where cr_unique_cd='BANKING_BASEL' and status_sk=1 ;    /* i18nOK:LINE */
quit;
%let bnk_cnt_appl=&bnk_cnt_appl;

%if &bnk_cnt_appl ge 1 %then %do;
	proc sql;
	update UPDATED_LIB_MAP set TO_LIB="BANKCRFM"      /* i18nOK:LINE */
	where kupcase(FROM_LIB) in ('CSB_IM','BNKF_IM','DETAIL','BRIDGE','DIM','INPBASE');  /* i18nOK:LINE */
	quit;
	
	proc sql;
	insert into UPDATED_LIB_MAP
	values ('BNKF_IM_RMCR','BANKCRFM')    /* i18nOK:LINE */
	values ('CSB_IM_RMCR','BANKCRFM')   /* i18nOK:LINE */
	values ('inpbase_rmcr','BANKCRFM');;  /* i18nOK:LINE */
	quit;
%end;

proc sql;
select * from UPDATED_LIB_MAP;
quit;

%let cnt=&sqlobs;


%dabt_initiate_cas_session(cas_session_ref=load_src_data_cas);

caslib src_data datasource=(srctype="path") path="&path_to_unzip_source_data_xpt";   /* i18nOK:LINE */

%if &cnt ge 1 %then %do;
	/*macro to read xpt files from source_table,source_view and source_im folders and load to cas lib*/
	%macro export_source_data(src_type_xpt_path=);
		%do lib_cnt=1 %to &cnt;
			data _null_;
			obs=&lib_cnt;
			set UPDATED_LIB_MAP point=obs;
			call symputx('from_lib',FROM_LIB);    /* i18nOK:LINE */
			call symputx('to_lib',TO_LIB);    /* i18nOK:LINE */
			stop;
			run;

			%let source_xpt_exist=%sysfunc(fileexist(&src_type_xpt_path/&from_lib..xpt));
			
			%if &source_xpt_exist eq 1 %then %do;

				proc cimport infile="&src_type_xpt_path/&from_lib..xpt" library=unzipsl;    /* i18nOK:LINE */
				run;
				
				proc sql noprint;
				create table files as 
					select memname as name from dictionary.tables
					where upcase(libname) eq "UNZIPSL" ;      /* i18nOK:LINE */
				quit;
			
				%local m_cnt m_name ;
				%let m_cnt = 0;
				proc sql noprint;
					select count(*) , name  /* i18nOK:Line */
						into :m_cnt, 
							:m_name separated by '~'  /* i18nOK:Line */
					from files;
				quit;
					
				%let m_name = &m_name.;
				%let m_cnt = &m_cnt.;
					
				%if &m_cnt. gt 0 %then %do;
					%do i=1 %to &m_cnt;
						%let current_nm = %scan(&m_name,&i.,~);    /* i18nOK:LINE */
						%let op_nm = %scan(&current_nm,1);   /* i18nOK:LINE */
						
							%let etls_tableExist = %eval(%sysfunc(exist(&to_lib..&current_nm., DATA)));
				
							%if &etls_tableExist. eq 0 %then %do;
							
								proc casutil ; 
									load casdata="&current_nm..sas7bdat" incaslib="src_data"     /* i18nOK:LINE */
									casout="&current_nm." outcaslib="&to_lib."  
									importoptions=(filetype="basesas" dataTransferMode="parallel") promote  ;     /* i18nOK:LINE */
								quit ; 	
								
								%dabt_save_cas_table(m_in_cas_lib_ref=&to_lib., m_in_cas_table_nm=&current_nm.);
							
							%end;
						
						
					%end;
				%end;
				
				%if &from_lib eq DETAIL and &bnk_cnt_appl ge 1 %then %do;
					%dabt_drop_table(m_table_nm=bankcrfm.ACCOUNT_SCORE_DETAIL, m_cas_flg=Y);
					%dabt_drop_table(m_table_nm=bankcrfm.APPLICATION_SCORE_DETAIL, m_cas_flg=Y);
					%dabt_drop_table(m_table_nm=bankcrfm.CREDIT_FACILITY_SCORE_DETAIL, m_cas_flg=Y);
					%dabt_drop_table(m_table_nm=bankcrfm.CUSTOMER_SCORE_DETAIL, m_cas_flg=Y);
					
					proc sql ;
					create table work.WRITEBACK_ALL_MODEL 
					(SUBJECT_RK num (8) "SUBJECT_RK ", 	/* i18nOK:LINE */
					SUBJECT_ANALYSIS_TYPE_CD num (8) "  ",
					SUBJECT_ANALYSIS_NAME char (480) "  ",
					LEVEL_KEY_TYPE char (800) "  ",
					PURPOSE_TYPE_CD char (12) "  ",
					PURPOSES_NAME char (480) "  ",
					MODEL_ANALYSIS_TYPE_CD char (48) "  ",
					MODEL_ANALYSIS_TYPE_NAME char (480) "  ",
					MODEL_TYPE_CD char (12) "  ",
					MODEL_PRODUCT_TYPE_CD char (12) "  ",
					SUBJECT_GROUP_CD char (32) "  ",
					SUBJECT_GROUP_SHORT_NM char (480) "  ",
					MODEL_ID char (120) "  ",
					MODEL_NAME char (480) "  ",
					LATEST_SCORE_DTTM DATE FORMAT=NLDATM.,
					LATEST_SCORE_POINTS_NO num (8) "  ",
					LATEST_SCORE_NO num (8) "  ",
					CREATED_BY char (28) "  ",
					CREATED_DTTM DATE FORMAT=datetime25.6 ,	/* I18NOK:LINE */
					PROCESSED_DTTM DATE FORMAT=datetime25.6 )	/* I18NOK:LINE */
					;
					quit;

					/* Insert each table one by one */

					/* Insert ACCOUNT_SCORE_DETAIL */

					PROC SQL;	
					INSERT INTO WORK.WRITEBACK_ALL_MODEL (
						SUBJECT_RK,SUBJECT_ANALYSIS_TYPE_CD,SUBJECT_ANALYSIS_NAME,LEVEL_KEY_TYPE,PURPOSE_TYPE_CD,PURPOSES_NAME
						,MODEL_ANALYSIS_TYPE_CD,MODEL_ANALYSIS_TYPE_NAME,MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,SUBJECT_GROUP_CD,SUBJECT_GROUP_SHORT_NM
						,MODEL_ID,MODEL_NAME,LATEST_SCORE_DTTM,LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM)

					SELECT ACCOUNT_RK,.,'','ACCOUNT_RK','','','','',MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,'','',put(MODEL_RK, NLBEST.),'',LATEST_SCORE_DTTM, /* i18nOK:LINE */
						LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM
					FROM unzipsl.ACCOUNT_SCORE_DETAIL;
					QUIT;

					/* Insert APPLICATION SCORE DETAIL */

					PROC SQL;	
					INSERT INTO WORK.WRITEBACK_ALL_MODEL (
						SUBJECT_RK,SUBJECT_ANALYSIS_TYPE_CD,SUBJECT_ANALYSIS_NAME,LEVEL_KEY_TYPE,PURPOSE_TYPE_CD,PURPOSES_NAME
						,MODEL_ANALYSIS_TYPE_CD,MODEL_ANALYSIS_TYPE_NAME,MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,SUBJECT_GROUP_CD,SUBJECT_GROUP_SHORT_NM
						,MODEL_ID,MODEL_NAME,LATEST_SCORE_DTTM,LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM)

					SELECT APPLICATION_RK,.,'','APPLICATION_RK','','','','',MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,'','',put(MODEL_RK, NLBEST.),'',LATEST_SCORE_DTTM,  /* i18nOK:LINE */
						LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM
					FROM unzipsl.APPLICATION_SCORE_DETAIL;
					QUIT;

					/* Insert CREDIT FACILITY SCORE DETAIL */

					PROC SQL;	
					INSERT INTO WORK.WRITEBACK_ALL_MODEL (
						SUBJECT_RK,SUBJECT_ANALYSIS_TYPE_CD,SUBJECT_ANALYSIS_NAME,LEVEL_KEY_TYPE,PURPOSE_TYPE_CD,PURPOSES_NAME
						,MODEL_ANALYSIS_TYPE_CD,MODEL_ANALYSIS_TYPE_NAME,MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,SUBJECT_GROUP_CD,SUBJECT_GROUP_SHORT_NM
						,MODEL_ID,MODEL_NAME,LATEST_SCORE_DTTM,LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM)

					SELECT CREDIT_FACILITY_RK,.,'','CREDIT_FACILITY_RK','','','','',MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,'','',put(MODEL_RK, NLBEST.),'',LATEST_SCORE_DTTM,  /* i18nOK:LINE */  
						LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM
					FROM unzipsl.CREDIT_FACILITY_SCORE_DETAIL;
					QUIT;


					/* Insert CUSTOMER SCORE DETAIL */

					PROC SQL;	
					INSERT INTO WORK.WRITEBACK_ALL_MODEL (
						SUBJECT_RK,SUBJECT_ANALYSIS_TYPE_CD,SUBJECT_ANALYSIS_NAME,LEVEL_KEY_TYPE,PURPOSE_TYPE_CD,PURPOSES_NAME
						,MODEL_ANALYSIS_TYPE_CD,MODEL_ANALYSIS_TYPE_NAME,MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,SUBJECT_GROUP_CD,SUBJECT_GROUP_SHORT_NM
						,MODEL_ID,MODEL_NAME,LATEST_SCORE_DTTM,LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM)

					SELECT CUSTOMER_RK,.,'','CUSTOMER_RK','','','','',MODEL_TYPE_CD,MODEL_PRODUCT_TYPE_CD,'','',put(MODEL_RK, NLBEST.),'',LATEST_SCORE_DTTM,    /* i18nOK:LINE */
						LATEST_SCORE_POINTS_NO,LATEST_SCORE_NO,CREATED_BY,CREATED_DTTM,PROCESSED_DTTM
					FROM unzipsl.CUSTOMER_SCORE_DETAIL;
					QUIT;

					/* Promote Work table to bankcrfm */
					%let wrtbck_tableExist = %eval(%sysfunc(exist(bankcrfm.ACCOUNT_SCORE_DETAIL, DATA)));
					%if &wrtbck_tableExist. eq 1 %then %do;
						%dabt_drop_table(m_table_nm=bankcrfm.ACCOUNT_SCORE_DETAIL, m_cas_flg=Y);
					%end;
					
					proc casutil;
								load data=WRITEBACK_ALL_MODEL casout="WRITEBACK_ALL_MODEL" outcaslib="bankcrfm" replace;     /* I18NOK:LINE */
					run;

					/* Add Dabt promote table and save table to promote and save tables in bankcrfm library */
						%dabt_save_cas_table(m_in_cas_lib_ref=bankcrfm, m_in_cas_table_nm=writeback_all_model);
						%dabt_promote_table_to_cas(input_caslib_nm =bankcrfm,input_table_nm =writeback_all_model);
						
					/* Call macro to create view */

					
				%end;
				
				proc datasets lib=unzipsl
						nolist kill;
						quit;
						run;
			
			%end;
		
		%end;
	%mend export_source_data;
	%export_source_data(src_type_xpt_path=&scr_tbl_xpt_path);
	%export_source_data(src_type_xpt_path=&scr_vw_xpt_path);
	%export_source_data(src_type_xpt_path=&scr_im_xpt_path);
%end;

caslib src_data drop;
libname unzipsl clear;
/****************Call macro to create views-start****************/	
%rmcr_writeback_views;
%rmcr_create_single_table_view();
/****************Call macro to create views-end****************/	
%dabt_terminate_cas_session(cas_session_ref=load_src_data_cas);

%mend rmcr_source_data_prmtn;
