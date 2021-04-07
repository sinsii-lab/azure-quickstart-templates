/**************************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: rmcr_config					                       
 *                                                                 
 * LOGIC		: This macro will get called through CLI after the CR is successfully executed.
 *	     		  It does following actions:
 *					1. Create below folder and path variables:
 *						m_cr_version_folder_path, 
 *						m_cr_open_source_macro_path, m_cr_promotion_macro_path, 
 *						m_cr_vdmml_macro_path, m_cr_banking_solution_macro_path,
 *						m_cr_open_source_folder_nm, m_cr_promotion_folder_nm, 
 *						m_cr_vdmml_folder_nm, m_cr_banking_solution_folder_nm
 * 					2. Check apdm.cs_cr_info for entry of valid CR configuration. 
 *						If there is a valid entry of successful CR execution, then code simply comes out.
 *						If no such record exists, then it performs below configuration tasks.
 *					3. Create a table APDM.RMCR_message_detail for locale specific messages from smd files.
 *						Copies all locale smd files from content server to work path. Then it inserts smd into APDM table
 * 						For banking soltion, there are 24 files having different basename hence this step will run multiple times					
 *					4. Fetch the max seq_no as m_max_seq_no from apdm.cs_cr_info
 *					5.  Insert a record into apdm.cs_cr_info with cr_seq_no = &m_max_seq_no. +1
 *					6. Compile and execute cr config file for each type of content 
 *					6. Create a table APDM.RMCR if it doesn't exist. 
 *						This is for storing locale specific messages from smd files.
 *						Loop through all CR types to populate the table for all text messages. 
 *					7. Update the status of CR configuration in apdm.cs_cr_info
 *
 * USAGE		: %rmcr_config
 *
 * Called By	: rmcr_config_wrapper
 *                                                                 
 * PARAMETERS	:  
 *														   
 * Authors		: BIS Team
 **************************************************************************************************/
 %macro rmcr_config;
 
	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;
 
	%global m_cr_version_folder_path m_cr_open_source_macro_path m_cr_promotion_macro_path m_cr_vdmml_macro_path m_cr_banking_solution_macro_path  ;
	%global m_cr_open_source_folder_nm  m_cr_promotion_folder_nm m_cr_vdmml_folder_nm m_cr_banking_solution_folder_nm ;
	%global m_cr_common_smd_path m_cr_open_source_smd_path  m_cr_promotion_smd_path m_cr_vdmml_smd_path m_cr_banking_solution_smd_path ;
	
	%let m_cr_unique_cd = RM_CONTENT;
	%let m_cr_folder_nm = Risk Modeling Content;
	%let m_cr_version_number = v03.2020;
	%let m_cr_init_file_name = rmcr_init_wrapper.sas;
	%let m_cr_file_path = /Products/SAS Risk Modeling/Risk Modeling Content/v03.2020/Common/Macros;
	%let m_cr_init_stmt = '%rmcr_init_wrapper;';       /* I18NOK:LINE */
	%let yes_flg = Y;
	
	%let m_succ_status_sk = ;
	%let m_fail_status_sk = ;
	%let m_runn_status_sk = ;
	proc sql noprint;			
		select status_sk into:m_fail_status_sk 
			from &lib_apdm..status_master 
			where  status_cd = "FAIL";    /* I18NOK:LINE */

		select status_sk into:m_succ_status_sk 
			from &lib_apdm..status_master 
			where  status_cd = "SUCCESS";   /* I18NOK:LINE */

		select status_sk into:m_runn_status_sk 
			from &lib_apdm..status_master 
			where  status_cd = "RUNNING";   /* I18NOK:LINE */
	quit;
	%dabt_err_chk(type=SQL);
	%let m_fail_status_sk = &m_fail_status_sk.;
	%let m_succ_status_sk = &m_succ_status_sk.;
	%let m_runn_status_sk = &m_runn_status_sk.;

	/************************************************************/
	/* 1. Create path variables 							 	*/	
	/************************************************************/
	%let m_cr_common_folder_nm = Common;
	%let m_cr_open_source_folder_nm = OpenSource;
	
	%let m_cr_promotion_folder_nm = Promotion;
	%let m_cr_vdmml_folder_nm = VDMML;
	%let m_cr_banking_solution_folder_nm = Banking Solution;
	
	%let m_cr_version_folder_path = /Products/SAS Risk Modeling/&m_cr_folder_nm./&m_cr_version_number.;
	
	%let m_cr_open_source_macro_path = &m_cr_version_folder_path./&m_cr_open_source_folder_nm./Macros;
	
	%let m_cr_promotion_macro_path = &m_cr_version_folder_path./&m_cr_promotion_folder_nm./Macros;
	%let m_cr_vdmml_macro_path = &m_cr_version_folder_path./&m_cr_vdmml_folder_nm./Macros;
	%let m_cr_banking_solution_macro_path = &m_cr_version_folder_path./&m_cr_banking_solution_folder_nm./Macros;
	

	%let m_cr_common_smd_path = &m_cr_version_folder_path./&m_cr_common_folder_nm./smd;
	%let m_cr_open_source_smd_path = &m_cr_version_folder_path./&m_cr_open_source_folder_nm./smd;
	
	%let m_cr_promotion_smd_path = &m_cr_version_folder_path./&m_cr_promotion_folder_nm./smd;
	%let m_cr_vdmml_smd_path = &m_cr_version_folder_path./&m_cr_vdmml_folder_nm./smd;
	%let m_cr_banking_solution_smd_path = &m_cr_version_folder_path./&m_cr_banking_solution_folder_nm./smd;
	
	/**********************************************************************************/
	/* 3. Create a table APDM.RMCR for locale specific messages from smd files.		  */
	/**********************************************************************************/
	/* I18NOK:BEGIN*/
	%if %sysfunc(exist(&lib_apdm..rmcr_message_detail)) = 0 %then %do;
		proc sql noprint;
			create table &lib_apdm..rmcr_message_detail (
	   			key char(60) format=$60. informat=$60. label='key',      
				lineno num format=3. informat=3. label='lineno',
				locale char(5) format=$5. informat=$5. label='locale',
				text char(1200) format=$1200. informat=$1200. label='text',
				cr_type_cd char(20) format $20. informat $20. label="cr_type_cd",
				cr_sub_type_cd char(20) format $20. informat $20. label="cr_sub_type_cd"
	  		);
		quit; 
	%end;
	/* I18NOK:END*/
	%let work_lib_path = %sysfunc(pathname(work)); 
	%let m_rel_path =;
	%dabt_make_work_area(dir=&work_lib_path., create_dir=smd, path=m_rel_path);
	
	/* Macro to copy content files in to work relative path */
	%macro all_locale_loop(m_cr_smd_file_nm=);
			filename src filesrvc folderpath="&m_cr_smd_file_path." filename="&m_cr_smd_file_nm." debug=http;			
			filename dst "&m_rel_path./&m_cr_smd_file_nm." ;
			
			%if %sysfunc(fexist(src)) %then %do;			
				data _NULL_;
					infile src end=eof lrecl=32767;
					file dst lrecl=64000 mod;
					length get_statement $32767.;
				
					do while (^eof);
						input;
						put _infile_;
					end;
				run;
			%end;
	%mend all_locale_loop;
	
	/* Macro to create localization text messages data and populate in apdm */
	%macro rmcr_create_message_data(m_cr_smd_file_path= , m_cr_smd_file_nm= , m_cr_type=, cr_sub_type_cd=, m_populate_apdm=N)/ minoperator;;	
	
		%let m_cr_smd_file_path = &m_cr_smd_file_path.;
		%let m_cr_smd_file_nm = &m_cr_smd_file_nm.;
		%let m_cr_type = &m_cr_type.;
		%let m_populate_apdm = &m_populate_apdm.;
	
		%let m_message_ds_nm = %sysfunc(kscan("&m_cr_smd_file_nm.",1 , "."));    /* I18NOK:LINE */
		filename macr_cd filesrvc folderpath="&m_cr_smd_file_path." debug=http;  /* I18NOK:LINE */
		
		/* data step to fetch all files in smd folder */
		data test1;
		   did = dopen('macr_cd'); /* I18NOK:LINE */
		   mcount = dnum(did);
		   do i=1 to mcount;
		      memname = dread(did, i);
		      put i @5 memname;
				output;
		   end;  
		   rc = dclose(did);
		run;  
		
		/* Call all_locale_loop macro for all locales for particular basename */
		data _null_;
		set test1;
		where kstrip(memname) like "&m_message_ds_nm.%";    
		call execute('%all_locale_loop(m_cr_smd_file_nm='||strip(memname)||');');    /* I18NOK:LINE */
		run;
		
		%if &m_cr_type. ne COMMONDUMMY %then %do;
			/* Create a message dataset in WORK library */
			%smd2ds ( DIR = &m_rel_path.,
			BASENAME = &m_message_ds_nm.,
			LIB = work,
			locale= pt_BR zh_CN zh_HK zh_TW ja ko ru es
			);
		%end;
	
		%if "&m_populate_apdm." = "Y" %then %do;  /* I18NOK:LINE */
			/* Append the data from WORK library to apdm.rmcr_message_detail */
			proc sql ;
		
			insert into &lib_apdm..rmcr_message_detail (key, lineno, locale, text, cr_type_cd,cr_sub_type_cd)
				select key, lineno, locale, text, "&m_cr_type." as cr_type_cd, "&cr_sub_type_cd." as cr_sub_type_cd
					from work.&m_message_ds_nm.;
			quit;
			
		%end; /* end - %do block for condition "&m_populate_apdm." = "Y" */
	
	%mend rmcr_create_message_data;
	
	%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_common_smd_path., m_cr_smd_file_nm= rmcr_common.smd , m_cr_type=COMMONDUMMY,cr_sub_type_cd=N, m_populate_apdm=N); /*** dummy call to bypass smderror***/
	%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_common_smd_path., m_cr_smd_file_nm= rmcr_common.smd , m_cr_type=COMMON, cr_sub_type_cd=N, m_populate_apdm=N);
	
	/****************************************************************/
	/* 2. Check apdm.cs_cr_info for entry of valid CR configuration */
	/****************************************************************/	
	%let m_ext_cr_status_sk = ;
	proc sql noprint;
	select coalesce(status_sk,0) into :m_ext_cr_status_sk
		from &lib_apdm..cs_cr_info
		where cr_unique_cd = "&m_cr_unique_cd." and cr_version_number = "&m_cr_version_number." ;
	quit;
	%dabt_err_chk(type=SQL);
	%let m_ext_cr_status_sk = &m_ext_cr_status_sk.;
	
	%if "&m_ext_cr_status_sk." ne "1" %then %do;    /* I18NOK:LINE */
	/****************************************************************/
	/* insert smd messages for CR into rmcr_message_detail 		    */
	/****************************************************************/
		%let access_apdm_smd=riskmodelingcore;
                
        PROC SQL;
        &apdm_connect_string.;
        execute(truncate &access_apdm_smd..rmcr_message_detail;
		) by postgres;
        &apdm_disconnect_string.;
        quit;
	

		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_common_smd_path., m_cr_smd_file_nm= rmcr_common.smd , m_cr_type=COMMONDUMMY,cr_sub_type_cd=N, m_populate_apdm=N); /*** dummy call to bypass smderror***/
	
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=bankfdn_level_master.smd , m_cr_type=BANKING_SOLUTION, cr_sub_type_cd=Y, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=bankfdn_library_master.smd , m_cr_type=BANKING_SOLUTION, cr_sub_type_cd=Y, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=bankfdn_source_column_master.smd , m_cr_type=BANKING_SOLUTION, cr_sub_type_cd=Y, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=bankfdn_source_dim_attrib_value.smd , m_cr_type=BANKING_SOLUTION, cr_sub_type_cd=Y, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=bankfdn_source_table_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=Y,m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=bankfdn_subject_group_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=Y, m_populate_apdm=Y);
		
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=bankfdn_subset_from_path_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=Y, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csb_pool_scheme_type_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csb_pool_schm_type_x_method.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_event_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_external_code_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_level_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_library_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_modeling_abt_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_modeling_abt_x_variable.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_parameter_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_pool_chrstc_cal_type.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_pool_impct_chrstc_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_purpose_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_source_column_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_source_table_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_src_dim_attrib_val_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
	    %rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_subject_group_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_banking_solution_smd_path., m_cr_smd_file_nm=csbmva_subset_from_path_master.smd , m_cr_type=BANKING_SOLUTION,cr_sub_type_cd=N, m_populate_apdm=Y);
		
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_open_source_smd_path., m_cr_smd_file_nm= rmcr_open_source.smd , m_cr_type=OPEN_SOURCE_MODEL, cr_sub_type_cd=N,m_populate_apdm=Y);
		%rmcr_create_message_data(m_cr_smd_file_path= &m_cr_promotion_smd_path., m_cr_smd_file_nm=rmcr_promotion.smd , m_cr_type=PROMOTION, cr_sub_type_cd=N, m_populate_apdm=Y);
	
		/* block #3. Create a table APDM.RMCR for locale specific messages from smd files - ends here */


	/****************************************************************/
	/* 4. Fetch the max seq_no as m_max_seq_no from apdm.cs_cr_info */
	/****************************************************************/
		proc sql;
			delete from &lib_apdm..cs_cr_info
				where cr_unique_cd = "&m_cr_unique_cd." and cr_version_number = "&m_cr_version_number." ;
		quit;
		%let m_max_seq_no =0 ;
		proc sql noprint;
		select coalesce(max(cr_seq_no),0) into :m_max_seq_no
			from &lib_apdm..cs_cr_info;
		quit;
		%dabt_err_chk(type=SQL);
		%let m_max_seq_no = &m_max_seq_no.;
		%let m_cr_seq_no = %sysevalf(&m_max_seq_no. + 1);	
		
	/****************************************************************/
	/* 5. Insert a record into apdm.cs_cr_info 						*/
	/****************************************************************/
		proc sql noprint;
			insert into &lib_apdm..cs_cr_info( cr_seq_no,cr_unique_cd, cr_version_number, cr_folder_nm, cr_init_file_name, 
												cr_init_file_path, cr_init_stmt, status_sk, initiate_by_default_flg, cr_data_path)
				values(&m_cr_seq_no,"&m_cr_unique_cd.", "&m_cr_version_number.", "&m_cr_folder_nm.", "&m_cr_init_file_name", 
						"&m_cr_file_path.", &m_cr_init_stmt., &m_runn_status_sk., "&yes_flg.", "NA");	      /* I18NOK:LINE */	
		quit;
		%dabt_err_chk(type=SQL);
		

	/************************************************************/
	/* 6. Compile and execute CR specific config macros 		*/
	/************************************************************/
		%macro rmcr_exec_cr_config(m_cr_config_file_path=, m_cr_config_file_nm=);
		
			%let m_cr_config_file_path = &m_cr_config_file_path.;
			%let m_cr_config_file_nm = &m_cr_config_file_nm.;
		
			%let m_cr_config_macr_nm = %sysfunc(kscan(&m_cr_config_file_nm., 1,"."));     /* I18NOK:LINE */
		
			filename cr_cd filesrvc folderpath="&m_cr_config_file_path./" filename= "&m_cr_config_file_nm" debug=http; /* i18nOK:Line */
			
			%if "&_FILESRVC_cr_cd_URI" eq "" %then %do;
				%let job_rc = 1012; 
				%put %sysfunc(sasmsg(WORK.RMCR_COMMON, RMCR_COMN_CONFIG.FILE_NOT_FOUND_ERROR, noquote, &m_cr_config_file_nm., &m_cr_config_file_path.) );
			%end;
			
			%else %do;		
				%include cr_cd / lrecl=64000; /* i18nOK:Line */
				%&m_cr_config_macr_nm.;
			%end;
			
			filename cr_cd clear;
			
		%mend rmcr_exec_cr_config;
		
		%rmcr_exec_cr_config(m_cr_config_file_path=&m_cr_open_source_macro_path., m_cr_config_file_nm=rmcr_config_open_source.sas);
		%dabt_err_chk(type=SQL);
		%dabt_err_chk(type=DATA);
		/*
		%rmcr_exec_cr_config(m_cr_config_file_path=&m_cr_promotion_macro_path., m_cr_config_file_nm=rmcr_config_promotion.sas);
		%rmcr_exec_cr_config(m_cr_config_file_path=&m_cr_vdmml_macro_path., m_cr_config_file_nm=rmcr_config_vdmml.sas);
		%rmcr_exec_cr_config(m_cr_config_file_path=&m_cr_banking_solution_macro_path., m_cr_config_file_nm=rmcr_config_banking_solution.sas);
		*/

	
		/**************************************************************/
		/* 7. Update the status of CR configuration in apdm.cs_cr_info*/
		/**************************************************************/
		%if &job_rc. = 0 %then %do;
			%let m_cr_status_sk = &m_succ_status_sk.;
		%end;
		%else %do;
			%let m_cr_status_sk = &m_fail_status_sk.;
		%end;
		
		proc sql ;
			update &lib_apdm..cs_cr_info
				set status_sk = &m_cr_status_sk.
				where cr_unique_cd = "&m_cr_unique_cd." and cr_version_number = "&m_cr_version_number.";
		quit;		

	%end; /* block for condition %if "&m_ext_cr_status_sk." ne "1" ends here */
	%else %do;
		%put %sysfunc(sasmsg(WORK.RMCR_COMMON, RMCR_COMN_CONFIG.CR_DONE_NOTE, noquote) );	
	%end; /* else block for condition %if "&m_ext_cr_status_sk." ne "1" ends here */

%csbmva_connect(m_enable=N); 
	
 %mend rmcr_config;