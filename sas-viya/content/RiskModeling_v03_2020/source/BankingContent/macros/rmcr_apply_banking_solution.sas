/********************************************************************************************************
	Module:  Apply Banking Content CR over generic Credit Scoring Solution
	
	Macro Name : rmcr_apply_banking_solution
	
	Function:  This will do below listed actions.
	
				1. Populated application mart tables with banking related configuration data			
				
	Author:    RM team
	
	Sample invocation:	
	%csbmva_initialize_cr(m_cr_unique_cd=RM_CONTENT);
	%rmcr_init_banking_solution;
		
	%rmcr_apply_banking_solution;
	
*********************************************************************************************************/

%macro rmcr_apply_banking_solution;
	
	/* %global CSB_CR_APPLICATION_FOLDER_NM;	
	%let CSB_CR_APPLICATION_FOLDER_NM = &csb_cr_folder_nm.; */
					
	/*--------- Check if purpose is already populated. If so, walk out -------------*/
	%local m_cnt;	
	%let m_cnt = 0;
	
	proc sql noprint;
		select count(*) into: m_cnt from &lib_apdm..purpose_master;    /* I18NOK:LINE */
	quit;
	
	%let m_cnt = &m_cnt.;
	
	%if &m_cnt gt 0 %then %do;
		%return;
	%end;
			
	/*---------------------Insertion Scripts for Bank Foundation---------------------*/
	
	%rmcr_populate_dabt_apdm_config;
	
	%if &syscc le 4 %then %do;	
		%*--------------------------------------;
		%* Creating Control tables;
		%*--------------------------------------;

		%*bankfdn_fm_check_control_table;
	%end;
	
	%if &syscc le 4 %then %do;	
		%*--------------------------------------;
		%* Creating Parameter Table;
		%*--------------------------------------;
		
		%rmcr_cr_check_parameter;
		
		
	%end;
	
	%if &syscc le 4 %then %do;
		%*--------------------------------------;
		%* Foundation Mart job dependency;
		%*--------------------------------------;
		
		%*bankfdn_fm_check_job_depend;
	%end;
	
	%if &syscc le 4 %then %do;
		%*--------------------------------------;
		%* Control tables for Banking;
		%*--------------------------------------;
		
		%*csbmva_bank_control_tables;
	%end;
	
	%if &syscc le 4 %then %do;
		%*------------------------------------------------;
		%* Parameters required for Credit Scoring Solution;
		%*-----------------------------------------------;
		
		%rmcr_bank_parameter;
		 
		
	%end;
	
	%rmcr_bank_insert_apdm_config;
	
	
	
	proc sql noprint;
	update &lib_apdm..library_master set library_reference = "bankcrfm" where library_sk = 1;     /* I18NOK:LINE */
	update &lib_apdm..source_table_master set library_sk = 1;
	delete from &lib_apdm..library_master where library_sk <> 1;
	update &lib_apdm..source_table_x_level set  has_retained_key_flg = "Y";    /* I18NOK:LINE */
	UPDATE &lib_apdm..Subset_from_path_x_level 	SET select_source_column_sk = 1157 	WHERe select_source_column_sk = 1160; 
	delete from &lib_apdm..source_column_master 	WHERe source_column_sk = 1160;
	UPDATE &lib_apdm..Subset_from_path_x_level 	SET select_source_column_sk = 3320 	WHERe select_source_column_sk = 528;
	delete from &lib_apdm..source_column_master 	WHERe source_column_sk = 528;
	update &lib_apdm..source_column_master 	set im_source_table_nm = "", im_source_column_nm="", calc_expression="" ,is_calc_flg="";
	quit;
	
	%rmcr_csb_cr_alt_seq_apdm(m_lib_apdm=apdm);
	
		
	%if &syscc eq 0 %then %do;		
		proc sql noprint;
			update &lib_apdm..cs_cr_info 
				set status_sk=(select status_sk from &lib_apdm..status_master
								where kupcase(status_cd) eq "SUCCESS" )  /* i18NOK:LINE*/
				where kupcase(cr_unique_cd) = "BANKING_BASEL" ; /* i18NOK:LINE*/
		quit;
	%end;	
	%else %if &syscc gt 0 %then %do;
		proc sql noprint;
			update &lib_apdm..cs_cr_info 
				set status_sk=(select status_sk from &lib_apdm..status_master
								where kupcase(status_cd) eq "FAIL" )  /* i18NOK:LINE*/
				where kupcase(cr_unique_cd) = "BANKING_BASEL" ; /* i18NOK:LINE*/
		quit;
	%end;			
	
%mend rmcr_apply_banking_solution;
