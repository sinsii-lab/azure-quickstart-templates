/*************************************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: 	rmcr_init_wrapper  					                       
 *             
 * Assumption	: 	m_cr_unique_cd will be available while calling this macro
 *
 * Logic		: 	Macro invokation performs following tasks:
 *					1. Fetches the root folder and version details from apdm.cs_cr_info. 
 *					2. Declares following global macro variables for path and file names of CR init files:
 *						m_cr_version_folder_path, 
 *						m_cr_open_source_macro_path, m_cr_open_source_folder_nm,
 *						m_cr_promotion_macro_path, m_cr_promotion_folder_nm, 
 *						m_cr_vdmml_macro_path, m_cr_vdmml_folder_nm, 
 *						m_cr_banking_solution_macro_path, m_cr_banking_solution_folder_nm
 *					3. Compiles following CR specific init macros:
 *						rmcr_init_open_source
 *						rmcr_init_promotion
 *						rmcr_init_vdmml
 *						rmcr_init_banking_content
 *
 * USAGE		: %rmcr_init_wrapper
 *
 * Called By	: %csbmva_initialize_cr (which is called inside %csbinit)
 *                                                                 
 * PARAMETERS	:  
 *														   
 * Authors		: BIS Team
 *************************************************************************************************************/

%macro rmcr_init_wrapper;

	%global m_cr_version_folder_path m_cr_open_source_macro_path m_cr_promotion_macro_path m_cr_vdmml_macro_path m_cr_banking_solution_macro_path ;
	%global m_cr_open_source_folder_nm m_cr_promotion_folder_nm m_cr_vdmml_folder_nm m_cr_banking_solution_folder_nm;

	/*********************************************************/
	/* 1. Fetch folder name and version from apdm.cs_cr_info */
	/*********************************************************/
	%let m_cr_folder_nm = ;
	%let m_cr_version_number = ;
	
	proc sql noprint;
		select 	cr_folder_nm,
				cr_version_number
		into	:m_cr_folder_nm,
				:m_cr_version_number				
		from &lib_apdm..cs_cr_info
		where kupcase(cr_unique_cd) = kupcase("&m_cr_unique_cd.");
	quit;
	%let m_cr_folder_nm = &m_cr_folder_nm.;
	%let m_cr_version_number = &m_cr_version_number.;
	
	
	/*********************************************************/
	/* 2. Create path variables */	
	/*********************************************************/
	%let m_cr_open_source_folder_nm = OpenSource;
	%let m_cr_promotion_folder_nm = Promotion;
	/*
	%let m_cr_vdmml_folder_nm = VDMML;*/
	%let m_cr_banking_solution_folder_nm = Banking Solution;
	
	%let m_cr_version_folder_path = /Products/SAS Risk Modeling/&m_cr_folder_nm./&m_cr_version_number.;
	
	%let m_cr_open_source_macro_path = &m_cr_version_folder_path./&m_cr_open_source_folder_nm./Macros;
	
	%let m_cr_promotion_macro_path = &m_cr_version_folder_path./&m_cr_promotion_folder_nm./Macros;
	/*%let m_cr_vdmml_macro_path = &m_cr_version_folder_path./&m_cr_vdmml_folder_nm./Macros;*/
	%let m_cr_banking_solution_macro_path = &m_cr_version_folder_path./&m_cr_banking_solution_folder_nm./Macros;
	
	
	
	/*********************************************************/
	/* 3. Compile CR specific init macros */
	/*********************************************************/	
	%macro rmcr_init_compile(m_init_file_path=, m_init_file_nm=);
	
		%let m_init_file_path = &m_init_file_path.;
		%let m_init_file_nm = &m_init_file_nm.;

		%LET m_init_macro_nm= %sysfunc(kscan("&m_init_file_nm.",1,"."));   /* I18NOK:LINE */
	
		filename init_cd filesrvc folderpath="&m_init_file_path./" filename= "&m_init_file_nm" debug=http; /* i18nOK:Line */
		
		%if "&_FILESRVC_init_cd_URI" eq "" %then %do;
			%put %sysfunc(sasmsg(WORK.RMCR_COMMON, RMCR_COMN_INIT.FILE_NOT_FOUND_ERROR, noquote, &m_cr_config_file_nm., &m_cr_config_file_path.) );
		%end;
		
		%else %do;		
			%include init_cd / lrecl=64000; /* i18nOK:Line */
			/* %&m_init_macro_nm.; */
		%end;
		
		filename init_cd clear;
		
	%mend rmcr_init_compile;
	
	%rmcr_init_compile(m_init_file_path=&m_cr_open_source_macro_path., m_init_file_nm=rmcr_init_open_source.sas);
	
	%rmcr_init_compile(m_init_file_path=&m_cr_promotion_macro_path., m_init_file_nm=rmcr_init_promotion.sas);
	/*%rmcr_init_compile(m_init_file_path=&m_cr_vdmml_macro_path., m_init_file_nm=rmcr_init_vdmml.sas);*/
	%rmcr_init_compile(m_init_file_path=&m_cr_banking_solution_macro_path., m_init_file_nm=rmcr_init_banking_solution.sas);
	
	

%mend rmcr_init_wrapper;