/********************************************************************************************************
   	Module		:  rmcr_81_level_data_import

   	Function	:  This macro will be used to create import level data.

	Called-by	:  rmcr_81_import_wrppr
	Calls		:  None
					
   	Parameters	:  None
	
	Author		:  CSB Team 
				   
*********************************************************************************************************/
%macro rmcr_81_level_data_import();

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;

%let path_of_level_data = &m_staging_area_path./cs63_export_pkg/level_data;
%let lvl_exist=%sysfunc(fileexist(&path_of_level_data/applied_level_info.csv));
%let lvl_code_exist=%sysfunc(fileexist(&path_of_level_data/csbmva_get_subset_appl_list.sas));

%if &lvl_exist eq 1 %then %do;

	data applied_level_info;
	length FILE_NAME $100.;
	infile "&path_of_level_data/applied_level_info.csv" dlm=',' dsd firstobs=2;     /* i18nOK:LINE */
	input FILE_NAME $ CUSTOMIZATIONS_DONE $;
	run;

	data _null_;
	set applied_level_info;
	call symputx('custmztn_done',CUSTOMIZATIONS_DONE);    /* i18nOK:LINE */
	call symputx('file_name',FILE_NAME);   /* i18nOK:LINE */
	run;

	/*************Customization done to file*******************/
	%if &custmztn_done eq Y %then %do;
		%if &lvl_code_exist eq 1 %then %do;
			filename ext_in "&path_of_level_data/csbmva_get_subset_appl_list.sas";/*i18NOK:LINE*/
			filename ext_out filesrvc folderpath="/Products/SAS Risk Modeling/External Code" filename= "csbmva_get_subset_appl_list.sas" debug=http CD="attachment; filename=csbmva_get_subset_appl_list.sas";/* i18nOK:Line */

			data _null_;
			rc=fcopy('ext_in','ext_out');     /* i18nOK:LINE */
			msg=sysmsg();
			put rc = msg=;
			if rc = 0 then do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.LEVEL_CD_SM1.1, noquote)); 
			end;
			else do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.LEVEL_CD_SM2.1, noquote));  
				call symputx('job_rc',1012);    /* i18nOK:LINE */
			end;
			run;

				

				proc sql;
				select level_sk,count(*) into :m_level_sk separated by '~' , :cnt from &lib_apdm..level_master where level_type_sk= (select level_type_sk from &lib_apdm..level_type_master where level_type_cd="APPLCTN");   /* i18nOK:LINE */
				quit;
	
				%if &cnt ge 1 %then %do;
					%do ins=1 %to &cnt;
						%let level_key=%scan(&m_level_sk,&ins.,~);   /* i18nOK:LINE */
						
						 proc sql;
							insert into &lib_apdm..external_code_master(external_code_short_nm,level_sk ,external_code_file_loc,external_code_file_nm,simultns_mltpl_var_calc_flg, parallel_exec_flg,created_dttm,created_by_user,last_processed_dttm,last_processed_by_user)
							values('csbmva_get_subset_appl_list.sas',&level_key.,'/Products/SAS Risk Modeling/External Code','csbmva_get_subset_appl_list.sas','N','N',"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid");   /* i18nOK:LINE */
						 quit;
						 
						proc sql;
								update &lib_apdm..EXTERNAL_CODE_MASTER set external_code_id=put(external_code_sk,8.) where external_code_id is null;
								update &lib_apdm..level_master set accptd_applctn_idntfctn_macro_nm='csbmva_get_subset_appl_list' where level_sk=&level_key;    /* i18nOK:LINE */
						quit;
					%end;
				%end;
	
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.LEVEL_CD_SM3.1, noquote)); 
			
		%end;
	%end;

	/*************Customization not done to file*******************/
	%else %do;
	
		%let bnk_cnt_appl=;
		
		proc sql;
		select count(*) into :bnk_cnt_appl from &lib_apdm..CS_CR_INFO   /* i18nOK:LINE */
		where cr_unique_cd='BANKING_BASEL' and status_sk=1 ;		/* i18nOK:LINE */
		quit;
		
		%let bnk_cnt_appl=&bnk_cnt_appl;

		%if &bnk_cnt_appl ge 1 %then %do;
				
				proc sql;
				select level_sk,count(*) into :m_level_sk separated by '~' , :cnt from &lib_apdm..level_master where level_type_sk= (select level_type_sk from &lib_apdm..level_type_master where level_type_cd="APPLCTN");   /* i18nOK:LINE */
				quit;
	
				%if &cnt ge 1 %then %do;
					%do ins=1 %to &cnt;
						%let level_key=%scan(&m_level_sk,&ins.,~);   /* i18nOK:LINE */
						
						 proc sql;
							insert into &lib_apdm..external_code_master(external_code_short_nm,level_sk ,external_code_file_loc,external_code_file_nm,simultns_mltpl_var_calc_flg, parallel_exec_flg,created_dttm,created_by_user,last_processed_dttm,last_processed_by_user)
							values('csbmva_get_subset_appl_list.sas',&level_key.,'/Products/SAS Risk Modeling/Risk Modeling Content/v03.2020/Banking Solution/macros','csbmva_get_subset_appl_list.sas','N','N',"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid");    /* i18nOK:LINE */
						 quit;
						 
						 proc sql;
							update &lib_apdm..EXTERNAL_CODE_MASTER set external_code_id=put(external_code_sk,8.) where external_code_id is null;
							update &lib_apdm..level_master set accptd_applctn_idntfctn_macro_nm='csbmva_get_subset_appl_list' where level_sk=&level_key;  /* i18nOK:LINE */
						 quit;
					%end;
				%end;
		%end;

		%else %do;
			/****************If SOA configured and banking not applied and customization not done to file************/
			proc sql;
			select * from 
			&lib_apdm..level_master where level_type_sk= (select level_type_sk from &lib_apdm..level_type_master where level_type_cd="APPLCTN");
			quit;

			%if &sqlobs ge 1 %then %do;
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.LEVEL_CD_SM4.1, noquote)); 
			%end;
		%end;
	%end;
%end;
%mend rmcr_81_level_data_import;