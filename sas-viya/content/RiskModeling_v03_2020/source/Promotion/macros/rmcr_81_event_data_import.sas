/********************************************************************************************************
   	Module		:  rmcr_81_event_data_import

   	Function	:  This macro will be used to create import event data.

	Called-by	:  rmcr_81_import_wrppr
	Calls		:  None
					
   	Parameters	:  None
	
	Author		:  CSB Team 
				   
*********************************************************************************************************/
%macro rmcr_81_event_data_import();

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;
%let path_of_event_data = &m_staging_area_path./cs63_export_pkg/event_data;
%let evnt_data=&MCR_DABT_CREATE_EVENT_DATA;
%let evnt_exist=%sysfunc(fileexist(&path_of_event_data/applied_event_info.csv));
%let evnt_code_exist=%sysfunc(fileexist(&path_of_event_data/&evnt_data..sas));

%if &evnt_exist eq 1 %then %do;
	data applied_event_info;
	length FILE_NAME $100.;
	infile "&path_of_event_data/applied_event_info.csv" dlm=',' dsd firstobs=2;    /* i18nOK:LINE */
	input FILE_NAME $ CUSTOMIZATIONS_DONE $;
	run;

	data _null_;
	set applied_event_info;
	call symputx('custmztn_done',CUSTOMIZATIONS_DONE);   /* i18nOK:LINE */
	call symputx('file_name',FILE_NAME);   /* i18nOK:LINE */
	run;

	/*************Check if customization done to file*******************/
	%if &custmztn_done eq Y %then %do;
		%if &evnt_code_exist eq 1 %then %do;
			filename evnt_in "&path_of_event_data/&evnt_data..sas";/*i18NOK:LINE*/
			filename evnt_out filesrvc folderpath="/Products/SAS Risk Modeling/External Code" filename= "&evnt_data..sas" debug=http CD="attachment; filename=&evnt_data..sas";/* i18nOK:Line */
			
			data _null_;
			rc=fcopy('evnt_in','evnt_out');    /* i18nOK:LINE */
			msg=sysmsg();
			put rc = msg=;
			if rc = 0 then do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.EVENT_CD_SM1.1, noquote)); 
			end;
			else do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.EVENT_CD_SM2.1, noquote));  
				call symputx('job_rc',1012);    /* i18nOK:LINE */
			end;
			run;
			
			/*****Setting level_sk =8 by default if it does not contain an entry in event_mapping_dtl************/
			%let m_level_sk=8;
			
			proc sql noprint;
				select level_sk into :m_level_sk from &lib_apdm..event_master as em  inner join &lib_apdm..event_mapping_dtl as emd
				on em.event_sk=emd.event_sk;
			quit;	
			
			proc sql;
				insert into &lib_apdm..external_code_master(external_code_short_nm,level_sk ,external_code_file_loc,external_code_file_nm,simultns_mltpl_var_calc_flg, parallel_exec_flg,created_dttm,created_by_user,last_processed_dttm,last_processed_by_user)
				values("&evnt_data..sas",&m_level_sk,'/Products/SAS Risk Modeling/External Code',"&evnt_data..sas",'N','N',"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid");      /* i18nOK:LINE */
			quit;
			
			proc sql;
				update &lib_apdm..EXTERNAL_CODE_MASTER set external_code_id=put(external_code_sk,8.) where external_code_id is null;
			quit;
			
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.EVENT_CD_SM3.1, noquote));
			
		%end;
	%end;

	/*************Customization not done to file*******************/
	%else %do;
	
		%let bnk_cnt_appl=;
		
		proc sql;
		select count(*) into :bnk_cnt_appl from &lib_apdm..CS_CR_INFO   /* i18nOK:LINE */
		where cr_unique_cd='BANKING_BASEL' and status_sk=1 ;    /* i18nOK:LINE */
		quit;
		
		%let bnk_cnt_appl=&bnk_cnt_appl;
		
		%if &bnk_cnt_appl ge 1 %then %do;
				
				/*****Setting level_sk =8 by default if it does not contain an entry in event_mapping_dtl************/
				%let m_level_sk=8;
				
				proc sql noprint;
					select level_sk into :m_level_sk from &lib_apdm..event_master as em  inner join &lib_apdm..event_mapping_dtl as emd
					on em.event_sk=emd.event_sk;
				quit;
				
				 proc sql;
					insert into &lib_apdm..external_code_master(external_code_short_nm,level_sk ,external_code_file_loc,external_code_file_nm,simultns_mltpl_var_calc_flg, parallel_exec_flg,created_dttm,created_by_user,last_processed_dttm,last_processed_by_user)
					values("&evnt_data..sas",&m_level_sk,'/Products/SAS Risk Modeling/Risk Modeling Content/v03.2020/Banking Solution/macros',"&evnt_data..sas",'N','N',"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid");    /* i18nOK:LINE */
				 quit;
				 
				proc sql;
					update &lib_apdm..EXTERNAL_CODE_MASTER set external_code_id=put(external_code_sk,8.) where external_code_id is null;
				quit;
			
		
		%end;
		
		%else %do;
		/****************If event configured and banking not applied and customization not done to file************/
			proc sql;
			select count(*) from &lib_apdm..event_master;   /* i18nOK:LINE */
			quit;
	
			%if &sqlobs ge 1 %then %do;
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.EVENT_CD_SM4.1, noquote)); 
			%end;
		%end;
	%end;
%end;
%mend rmcr_81_event_data_import;