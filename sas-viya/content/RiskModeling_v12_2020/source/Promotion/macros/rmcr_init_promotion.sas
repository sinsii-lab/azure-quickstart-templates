/*****************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: rmcr_init_promotion  					                       
 *                                                                 
 * LOGIC		: It does following actions specific to RM content:							   
 *					1. Compiles the promotion model macros located on content server.
 *					   It refers to path indicated by &m_cr_promotion_macro_path.
 *
 * USAGE		: %rmcr_init_promotion
 *
 * Called By	: %rmcr_init_wrapper
 *                                                                 
 * PARAMETERS	:  
 *														   
 * Authors		: BIS Team
 *****************************************************************************************/

%macro rmcr_init_promotion;

	/******   smd message extarction ******/
	proc sql;
	create table work.rmcr_message_dtl_promotion  (
	
	locale char(5) ,
	   key char(60) ,
	lineno num 3,
	text char(1200) 
	  );

	insert into work.rmcr_message_dtl_promotion 
	select locale, kstrip(key), lineno, text from &lib_apdm..RMCR_MESSAGE_DETAIL where kupcase(kstrip(cr_type_cd))='PROMOTION';   /* i18nOK:LINE */
	quit;
	proc sort data=work.rmcr_message_dtl_promotion;
	by locale key descending lineno;
	run;
	
	proc datasets lib=work
	memtype=data nolist;
	modify rmcr_message_dtl_promotion;
	index create indx=(LOCALE KEY);
	run;
	quit;  

	/********************************************************/
	/* 1. Compile CR specific macros 						*/
	/********************************************************/	
	%macro rmcr_compile_macro(path=,m_macro_cd_nm=);

		%let m_macro_cd_nm = &m_macro_cd_nm.;
	
		filename macr_cd filesrvc folderpath="&path./" filename= "&m_macro_cd_nm" debug=http; /* i18nOK:Line */
		
		%if "&_FILESRVC_init_cd_URI" eq "" %then %do;
			/* %let job_rc = 1012; */
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.INIT_CODE1.1, noquote,&m_macro_cd_nm.,&m_init_file_path.));;
		%end;
		
		%else %do;		
			%include macr_cd / lrecl=64000; /* i18nOK:Line */
		%end;
		
		filename macr_cd clear;
		
	%mend rmcr_compile_macro;
	
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_81_cmmnts_import.sas);
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_81_event_data_import.sas);	
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_81_external_code_import.sas);
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_81_level_data_import.sas);
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_81_score_code_import.sas);	
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_81_update_lib_master.sas);	
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_solution_config_prmtn.sas);
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_solution_data_prmtn.sas);	
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_source_data_prmtn.sas);
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_update_drvd_and_fltr_exp.sas);
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_update_arm_xpt.sas);
	%rmcr_compile_macro(path=&m_cr_promotion_macro_path,m_macro_cd_nm=rmcr_change_owner_authorization.sas);
	%rmcr_compile_macro(path=&m_cr_banking_solution_macro_path,m_macro_cd_nm=rmcr_create_single_table_view.sas);
	%rmcr_compile_macro(path=&m_cr_banking_solution_macro_path,m_macro_cd_nm=rmcr_writeback_views.sas);		
	
%mend rmcr_init_promotion;

