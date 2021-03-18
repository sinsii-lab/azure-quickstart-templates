/*****************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: rmcr_init_banking_solution  					                       
 *                                                                 
 * LOGIC		: It does following actions specific to RM content:							   
 *					1. Compiles the banking content model macros located on content server.
 *					   It refers to path indicated by &m_cr_banking_solution_macro_path.
 *
 * USAGE		: %rmcr_init_banking_solution
 *
 * Called By	: %rmcr_init_wrapper
 *                                                                 
 * PARAMETERS	:  
 *														   
 * Authors		: BIS Team
 *****************************************************************************************/

%macro rmcr_init_banking_solution;

	/******   smd message extarction ******/
	proc sql;
	create table work.rmcr_message_dtl_banking  (
	
	locale char(5) ,
	   key char(60) ,
	lineno num 3,
	text char(1200) 
	  );

	insert into work.rmcr_message_dtl_banking 
	select locale, kstrip(key), lineno, text from &lib_apdm..RMCR_MESSAGE_DETAIL where kupcase(kstrip(cr_type_cd))='BANKING_SOLUTION' and kupcase(kstrip(cr_sub_type_cd))='N' ;   /* I18NOK:LINE */
	quit;
	
	proc sql;
	create table work.rmcr_message_bnkfdn_banking  (
	
	locale char(5) ,
	   key char(60) ,
	lineno num 3,
	text char(1200) 
	  );

	insert into work.rmcr_message_bnkfdn_banking 
	select locale, kstrip(key), lineno, text from &lib_apdm..RMCR_MESSAGE_DETAIL where kupcase(kstrip(cr_type_cd))='BANKING_SOLUTION' and kupcase(kstrip(cr_sub_type_cd))='Y' ;   /* I18NOK:LINE */
	quit;
	
	proc sort data=work.rmcr_message_dtl_banking;
	by locale key descending lineno;
	run;
	
	proc sort data=work.rmcr_message_bnkfdn_banking;
	by locale key descending lineno;
	run;
	
	proc datasets lib=work
	memtype=data nolist;
	modify rmcr_message_dtl_banking;
	index create indx=(LOCALE KEY);
	run;
	quit; 

	proc datasets lib=work
	memtype=data nolist;
	modify rmcr_message_bnkfdn_banking;
	index create indx=(LOCALE KEY);
	run;
	quit;	
	
	/********************************************************/
	/* 1. Compile CR specific macros 						*/
	/********************************************************/	
	%macro rmcr_compile_macro(m_macro_cd_nm=);

		%let m_macro_cd_nm = &m_macro_cd_nm.;
	
		filename macr_cd filesrvc folderpath="&m_cr_banking_solution_macro_path./" filename= "&m_macro_cd_nm" debug=http; /* i18nOK:Line */
		
		%if "&_FILESRVC_init_cd_URI" eq "" %then %do;
			/* %let job_rc = 1012; */
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_banking, RMCR_BANKING_SOLUTION_MSG.INIT_CODE1.1, noquote,&m_macro_cd_nm.,&m_init_file_path.));;
		%end;
		
		%else %do;		
			%include macr_cd / lrecl=64000; /* i18nOK:Line */
		%end;
		
		filename macr_cd clear;
		
	%mend rmcr_compile_macro;

		%global m_cr_banking_solution_ddl_path;
	%let m_cr_banking_solution_ddl_path = &m_cr_version_folder_path./&m_cr_banking_solution_folder_nm./ddl;

	%rmcr_compile_macro(m_macro_cd_nm=rmcr_create_single_table_view.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_bank_parameter.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_cr_check_parameter.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_insert_event_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_insert_level_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_modeling_abt_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_modeling_abt_x_variable.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_purpose_master.sas);
	
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_source_column_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_source_dim_attrib.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_apply_banking_solution.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_bank_insert_apdm_config.sas);
	
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_populate_dabt_apdm_config.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_cr_alt_seq_apdm.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_source_table_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_subject_group_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_sbst_from_path_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_insert_level_key_column_dtl.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_insert_script_level_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_insert_library_master.sas);
	
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_insrt_src_column_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_insrt_src_dim_attrib.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_insrt_src_table_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_insrt_sbst_from_path_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_bank_insert_script_adm_ext.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_level_key_column_dtl.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_csb_insert_library_master.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_ddlgen.sas); 
	
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_fm_ext_staging.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_fm_ext_load_cas.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_fm_ext_scd_type2.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_writeback.sas);
	%rmcr_compile_macro(m_macro_cd_nm=rmcr_writeback_views.sas);
	
%mend rmcr_init_banking_solution;

