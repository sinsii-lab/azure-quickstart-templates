/********************************************************************************************************
   	Module		:  rmcr_63_source_data_export

   	Function	:  This macro will be used to create export package for source data from 63 to 81.

	Called-by	:  NONE
	Calls		:  	NONE
					
   	Parameters	:  None
	Author		:   RM Team
	
	Sample call :
					%let m_staging_area_path=/tmp;
					%let m_source_code_path=/tmp/source_code;
					%include "&m_source_code_path/rmcr_63_source_data_export.sas";
					%CSBINIT;
					%rmcr_63_source_data_export();
				   
*********************************************************************************************************/


%macro rmcr_63_source_data_export()/minoperator;

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
	   call symputx('timestamp', put(datetime(),datetime20.));    /* i18nOK:Line */
	run;
	%let timestamp=%sysfunc(ktranslate(&timestamp,'.',':'));   /* i18nOK:Line */
	
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
	proc printto log="&path_to_log/rmcr_63_source_data_export_&timestamp..log";    /* i18nOK:Line */
	run;
	
	/****** Create directory for source data xpt -starts ******/		
	%let path_to_source_data_xpt=;
	%dabt_make_work_area(dir=&path_to_migration, create_dir=source_data, path=path_to_source_data_xpt);
	%let path_to_source_data_xpt=&path_to_source_data_xpt;
	/****** Create directory for source data xpt -ends ******/

	/* Create library_mapping.csv file -starts */
	proc sql noprint;
		create table lib_list as 
			select 
			distinct 
			library_reference as lib
		from &lib_apdm..library_master
		where library_reference is not null;
	quit;
	%dabt_err_chk(type=SQL);

	data _null_;
		file "&path_to_source_data_xpt/library_mapping.csv" dlm=',';      /* i18nOK:Line */
		set lib_list;
		if _n_ =1 then
		put 'FROM_LIB,'     /* i18nOK:Line */
		'TO_LIB'     /* i18nOK:Line */
		;
		put 
		lib
		lib
		;
	run;			
	%dabt_err_chk(type=DATA);

	/****** Create library_mapping.csv file -ends ******/
	
	/****** Create directory for source table -starts ******/		
	%let path_to_source_table_xpt=;
	%dabt_make_work_area(dir=&path_to_source_data_xpt, create_dir=source_table, path=path_to_source_table_xpt);
	%let path_to_source_table_xpt=&path_to_source_table_xpt;
	/****** Create directory for source table  -ends ******/

	/****** Create xpts of libraries of type SRC_TBL(non-info map) library type code- starts ******/
	proc sql;
		create table lib_xpt as
			select library_sk,library_reference from 
			&lib_apdm..library_master 
			where library_type_cd='SRC_TBL';     /* i18nOK:Line */
	quit;
	%dabt_err_chk(type=SQL);

	proc sql;
		select * from lib_xpt;
	quit;

	%let lib_no=&sqlobs;
	%if &sqlobs ge 1 %then %do;
		%do lib_cnt=1 %to &sqlobs;
		
			data _null_;
			obs=&lib_cnt;
			set lib_xpt point=obs;
			call symputx('m_library_sk',library_sk);     /* i18nOK:Line */
			call symputx('m_library_reference',library_reference);    /* i18nOK:Line */
			stop;
			run;				
			%dabt_err_chk(type=DATA);
			
			%let m_src_tbl_nm=;
			proc sql;
			select source_table_nm into :m_src_tbl_nm separated by ' '     /* i18nOK:Line */
			from &lib_apdm..source_table_master where library_sk=&m_library_sk;
			quit;				
			%dabt_err_chk(type=SQL);
			
			%if "&m_src_tbl_nm" ne "" %then %do;
				%if %kupcase(&m_library_reference) eq INPBASE and not(COUNTRY_SNAPSHOT_BASE in &m_src_tbl_nm) %then %do;
					proc cport lib=&m_library_reference file="&path_to_source_table_xpt./&m_library_reference..xpt";     /* i18nOK:Line */
					select &m_src_tbl_nm COUNTRY_SNAPSHOT_BASE;
					run; 
					%let job_rc =&syscc;
				%end;
				
				%else %if %kupcase(&m_library_reference) eq DIM %then %do;
				
					%if not(APPLICANT_DIM in &m_src_tbl_nm) %then %do;
						%let tbl_missing=APPLICANT_DIM;
						%let m_src_tbl_nm=%sysfunc(catx(%str( ),&m_src_tbl_nm,&tbl_missing));   /* i18nOK:Line */
					%end;
					
					%if not(DEFAULT_EVENT_DIM in &m_src_tbl_nm) %then %do;
						%let tbl_missing=DEFAULT_EVENT_DIM;
						%let m_src_tbl_nm=%sysfunc(catx(%str( ),&m_src_tbl_nm,&tbl_missing));   /* i18nOK:Line */
					%end;
					
					%if not(TIME_DIM in &m_src_tbl_nm) %then %do;
						%let tbl_missing=TIME_DIM;
						%let m_src_tbl_nm=%sysfunc(catx(%str( ),&m_src_tbl_nm,&tbl_missing));   /* i18nOK:Line */
					%end;
					
					%if not(FINANCIAL_PRODUCT_DIM in &m_src_tbl_nm) %then %do;
						%let tbl_missing=FINANCIAL_PRODUCT_DIM;
						%let m_src_tbl_nm=%sysfunc(catx(%str( ),&m_src_tbl_nm,&tbl_missing));   /* i18nOK:Line */
					%end;
					
					proc cport lib=&m_library_reference file="&path_to_source_table_xpt./&m_library_reference..xpt";   /* i18nOK:Line */
					select &m_src_tbl_nm;
					run; 
					%let job_rc =&syscc;
					
				%end;
				%else %if %kupcase(&m_library_reference) eq DETAIL %then %do;
				
					%if not(CORPORATE_OWNER_DETAIL in &m_src_tbl_nm) %then %do;
						%let tbl_missing=CORPORATE_OWNER_DETAIL;
						%let m_src_tbl_nm=%sysfunc(catx(%str( ),&m_src_tbl_nm,&tbl_missing));   /* i18nOK:Line */
					%end;
					%if not(BANK_CARD_DETAIL in &m_src_tbl_nm) %then %do;
						%let tbl_missing=BANK_CARD_DETAIL;
						%let m_src_tbl_nm=%sysfunc(catx(%str( ),&m_src_tbl_nm,&tbl_missing));   /* i18nOK:Line */
					%end;
					%if not(COUNTRY_DETAIL in &m_src_tbl_nm) %then %do;
						%let tbl_missing=COUNTRY_DETAIL;
						%let m_src_tbl_nm=%sysfunc(catx(%str( ),&m_src_tbl_nm,&tbl_missing));   /* i18nOK:Line */
					%end;
					proc cport lib=&m_library_reference file="&path_to_source_table_xpt./&m_library_reference..xpt";   /* i18nOK:Line */
					select &m_src_tbl_nm;
					run; 
					%let job_rc =&syscc;
					
				%end;
				%else %do;
					proc cport lib=&m_library_reference file="&path_to_source_table_xpt./&m_library_reference..xpt";    /* i18nOK:Line */
					select &m_src_tbl_nm;
					run; 
					%let job_rc =&syscc;
				%end;
			%end;
		%end;
	%end;

%mend rmcr_63_source_data_export;
	
