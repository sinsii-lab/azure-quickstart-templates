/********************************************************************************************************
   	Module		:  rmcr_63_source_view_export

   	Function	:  This macro will be used to create export package for views from 63 to 81.

	Called-by	:  NONE
	Calls		:  	NONE
					
   	Parameters	:  None
	Author		:   RM Team
	
	Sample call :
					%let m_staging_area_path=/tmp;
					%let m_source_code_path=/tmp/source_code;
					%include "&m_source_code_path/rmcr_63_source_view_export.sas";
					%CSBINIT;
					%rmcr_63_source_view_export();
				   
*********************************************************************************************************/

%macro rmcr_63_source_view_export();

	%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
	%end;
	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;
	
	/****** Creating macro variable m_staging_area which holds the staging path ******/
	
	%let m_staging_area=&m_staging_area_path;
	
	/****** Creating a timestamp macro variable that will be used for logs and txt files ******/	
	data _null_;   
	   call symputx('timestamp', put(datetime(),datetime20.));    /* i18nOK:LINE */
	run;
	%let timestamp=%sysfunc(ktranslate(&timestamp,'.',':'));   /* i18nOK:LINE */
	
	/****** path_to_migration is a macro variable that holds the path of folder cs63_export_pkg 
		under which all folders will be made ******/	
	%let path_to_migration=;
	%dabt_make_work_area(dir=&m_staging_area, create_dir=cs63_export_pkg, path=path_to_migration);
	%let path_to_migration=&path_to_migration;
	
	/****** path_to_log is a macro variable that holds the path of log folder under cs63_export_pkg log ******/	
	%let path_to_log=;
	%dabt_make_work_area(dir=&path_to_migration, create_dir=log, path=path_to_log);
	%let path_to_log=&path_to_log;

	/****** Divert log to log folder ******/
	proc printto log="&path_to_log/rmcr_63_source_view_export_&timestamp..log";     /* i18nOK:LINE */
	run;
	
	/****** Create directory for source data xpt -starts ******/		
	%let path_to_source_data_xpt=;
	%dabt_make_work_area(dir=&path_to_migration, create_dir=source_data, path=path_to_source_data_xpt);
	%let path_to_source_data_xpt=&path_to_source_data_xpt;
	/****** Create directory for source data xpt -ends ******/
	
	/****** Create directory for source view -starts ******/		
	%let path_to_source_view_xpt=;
	%dabt_make_work_area(dir=&path_to_source_data_xpt, create_dir=source_view, path=path_to_source_view_xpt);
	%let path_to_source_view_xpt=&path_to_source_view_xpt;
	/****** Create directory for source view  -ends ******/
	
	/********************************************************************
	MACRO DEFINITON STARTS FOR INPBASE VIEWS
	*********************************************************************/
	
	%macro rmcr_63_view_export();
	
	/*start to check if banking content applied*/
	%let bnk_cnt_appl=;
		
	proc sql;
	select count(*) into :bnk_cnt_appl from &lib_apdm..CS_CR_INFO    /* i18nOK:LINE */
	where cr_unique_cd='BANKING_BASEL' and status_sk=1 ;     /* i18nOK:LINE */
	quit;
	
	%let bnk_cnt_appl=&bnk_cnt_appl;

	%if &bnk_cnt_appl ge 1 %then %do;

		/* Run proc content to find the name of all the views in inpbase library */

		proc contents data=inpbase._all_ out=a1 memtype=view noprint;
		run;

		/* Run sql query to find unique names of all the views */

		proc sql;
			create table temp as
			select distinct memname
			from a1
			where memname in ('CORP_ACCT_COUNTRY_SNAPSHOT','CORP_APPL_COUNTRY_SNAPSHOT','CORP_CUST_COUNTRY_SNAPSHOT','CREDIT_BUREAU_DTL_BASE','CREDIT_FACILITY_CNTRY_SNAPSHOT','IND_ACCT_COUNTRY_SNAPSHOT','IND_APPL_COUNTRY_SNAPSHOT','IND_CUST_COUNTRY_SNAPSHOT');  /* i18nOK:LINE */
		 quit;

		/* Create a macro with all the name of views seperated by # */

		data _null_;
			set temp end=last;
			length cm_table_nm $32767.;
			retain cm_table_nm;
			cm_table_nm=catx('#',cm_table_nm,memname);   /* i18nOK:LINE */
			if last then do;
				call symputx('cm_table_nm',cm_table_nm);   /* i18nOK:LINE */
			end;
		run;

		%let temp1 = %eval(%sysfunc(countc(%quote(&cm_table_nm),"#"))+1);    /* i18nOK:LINE */

		/* Below is a macro which creates a work table of all the inpbase views */

		%macro comp(nm);
			%let nm= &nm.;
			data &nm.;
			set inpbase.&nm.;
			run;
		%mend ;

		/* A do loop which calls the above macro to make work table of all the inpbase tables */
		%do i = 1 %to &temp1;

		%let table_nm = %scan(%quote(&cm_table_nm),&i,%str(#));     /* i18nOK:LINE */
		%put &table_nm;

		%comp(&table_nm.);

		%end;



		/* Proc cport to create xpt of all the work table */

		proc cport lib=work file="&path_to_source_view_xpt/inpbase_rmcr.xpt";    /* i18nOK:LINE */
			SELECT CORP_ACCT_COUNTRY_SNAPSHOT CORP_APPL_COUNTRY_SNAPSHOT CORP_CUST_COUNTRY_SNAPSHOT CREDIT_BUREAU_DTL_BASE CREDIT_FACILITY_CNTRY_SNAPSHOT IND_ACCT_COUNTRY_SNAPSHOT IND_APPL_COUNTRY_SNAPSHOT IND_CUST_COUNTRY_SNAPSHOT;
		run; 
		
		%let job_rc=&syscc; 
		
	%end;/*end to check if banking content applied*/
	
	%mend rmcr_63_view_export;
	/********************************************************************
	MACRO DEFINITON ENDS FOR INPBASE VIEWS
	*********************************************************************/
	
	/******************************************************
	Call macros for create export packages at staging area
	*******************************************************/
	%rmcr_63_view_export();

	%if &job_rc > 4 %then %do;
		%put ERROR: Some error occurred in exporting views;
		%return;
	%end;

	
%mend rmcr_63_source_view_export; 

