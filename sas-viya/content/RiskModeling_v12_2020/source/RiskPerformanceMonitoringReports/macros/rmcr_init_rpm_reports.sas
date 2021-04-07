/*****************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: rmcr_init_rpm_reports  					                       
 *                                                                 
 * LOGIC		: It does following actions specific to RM content:							   
 *					1. Compiles the promotion model macros located on content server.
 *					   It refers to path indicated by &m_cr_promotion_macro_path.
 *
 * USAGE		: %rmcr_init_rpm_reports
 *
 * Called By	: %rmcr_init_wrapper
 *                                                                 
 * PARAMETERS	:  
 *														   
 * Authors		: BIS Team
 *****************************************************************************************/

%macro rmcr_init_rpm_reports;

	/******   smd message extarction ******/
	proc sql;
	create table work.rmcr_message_dtl_rpm_reports  (
	
	locale char(5) ,
	   key char(60) ,
	lineno num 3,
	text char(1200) 
	  );

	insert into work.rmcr_message_dtl_rpm_reports 
	select locale, kstrip(key), lineno, text from &lib_apdm..RMCR_MESSAGE_DETAIL where kupcase(kstrip(cr_type_cd))='RPM_REPORTS';   /* i18nOK:LINE */
	quit;
	proc sort data=work.rmcr_message_dtl_rpm_reports;
	by locale key descending lineno;
	run;
	
	proc datasets lib=work
	memtype=data nolist;
	modify rmcr_message_dtl_rpm_reports;
	index create indx=(LOCALE KEY);
	run;
	quit;

	/******   smd message extarction ******/
	proc sql;
	create table work.rmcr_message_dtl_open_source  (
	
	locale char(5) ,
	   key char(60) ,
	lineno num 3,
	text char(1200) 
	  );

	insert into work.rmcr_message_dtl_open_source 
	select locale, kstrip(key), lineno, text from &lib_apdm..RMCR_MESSAGE_DETAIL where kupcase(kstrip(cr_type_cd))='OPEN_SOURCE_MODEL'; /* I18NOK:LINE */
	quit;
	proc sort data=work.rmcr_message_dtl_open_source nodupkey;
	by locale key descending lineno;
	run;
	
	proc datasets lib=work
	memtype=data nolist;
	modify rmcr_message_dtl_open_source;
	index create indx=(LOCALE KEY);
	run;
	quit;  
	/********************************************************/
	/* 1. Compile CR specific macros 						*/
	/********************************************************/	
	%macro rmcr_compile_macro(path=,m_macro_cd_nm=);

		%let m_macro_cd_nm = &m_macro_cd_nm.;
	
		filename macr_cd filesrvc folderpath="&path./" filename= "&m_macro_cd_nm" debug=http; /* i18nOK:Line */
		
		%if "&_FILESRVC_macr_cd_URI" eq "" %then %do;
			/* %let job_rc = 1012; */
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.INIT_CODE1.1, noquote,&m_macro_cd_nm.,&path.));;
		%end;
		
		%else %do;		
			%include macr_cd / lrecl=64000; /* i18nOK:Line */
		%end;
		
		filename macr_cd clear;
		
	%mend rmcr_compile_macro;
	
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_ui_mm_msr_report.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_ui_mip_msr_report.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_sync_detail_data_in_cas.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_valscr.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_sync_dtl_data_cas_wrppr.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_accura.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_er.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_prec.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_sens.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_spec.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=csbmva_defdata.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=rmcr_enable_rerun_sync_dtl_data.sas);
	%rmcr_compile_macro(path=&m_cr_rpm_reports_macro_path,m_macro_cd_nm=rmcr_apply_server_localisation.sas);
	
%mend rmcr_init_rpm_reports;

