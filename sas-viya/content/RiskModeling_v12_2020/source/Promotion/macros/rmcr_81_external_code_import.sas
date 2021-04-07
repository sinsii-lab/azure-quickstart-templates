/********************************************************************************************************
   	Module		:  rmcr_81_external_code_import

   	Function	:  This macro will be used to create upload external codes to content server.

	Called-by	:  rmcr_81_import_wrppr
	Calls		:  None
					
   	Parameters	:  None
	
	Author		:  CSB Team 
				   
*********************************************************************************************************/
%macro rmcr_81_external_code_import();

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;

proc sql;
create table external_codes_import as 
select distinct external_code_file_nm from 
&lib_apdm..external_code_master
;
quit;

proc sql;
select * from external_codes_import;
quit;

%let m_ext_code_cnt=&sqlobs;

%if &m_ext_code_cnt > 0 %then %do;
	%do ext_code=1 %to &m_ext_code_cnt;
		data _null_;
		obs=&ext_code;
		set external_codes_import point=obs;
		call symputx('external_code_file',external_code_file_nm);   /* i18nOK:LINE */
		stop;
		run;

		
		%let ext_code_exist=%sysfunc(fileexist(&m_staging_area_path/cs63_export_pkg/external_code/&external_code_file));

		%if &ext_code_exist eq 1 %then %do;	
			filename ext_in "&m_staging_area_path/cs63_export_pkg/external_code/&external_code_file";    /*i18NOK:LINE*/
			filename ext_out filesrvc folderpath="/Products/SAS Risk Modeling/External Code" filename= "&external_code_file" debug=http CD="attachment; filename=&external_code_file";  /* i18nOK:Line */
			
			data _null_;
			rc=fcopy('ext_in','ext_out');   /* i18nOK:LINE */
			msg=sysmsg();
			put rc = msg=;
			if rc = 0 then do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.EXTRNL_CD_SM1.1, noquote)); 
			end;
			else do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.EXTRNL_CD_SM2.1, noquote)); 
				call symputx('job_rc',1012);     /* i18nOK:LINE */
			end;
			run;
		%end;

		%else %do;
			/*data _null_;
				file "&path_to_log/exception_external_code_&timestamp..txt" MOD ;
				put */
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion, RMCR_PROMOTION_MSG.EXTRNL_CD_SM3.1, noquote));
		%end;
	%end;
%end;
	
	%let ext_code_file=%sysfunc(fileexist(&m_staging_area_path/cs63_export_pkg/external_code/external_code.csv));
	%if &ext_code_file eq 1 %then %do;
		data external_code;
		length FILE_NAME $100.;
		infile "&m_staging_area_path/cs63_export_pkg/external_code/external_code.csv" dlm=',' dsd firstobs=2;      /* i18nOK:LINE */
		input FILE_NAME $ CUSTOMIZATIONS_DONE $;
		run;

		data _null_;
		set external_code;
		call symputx('custmztn_done',CUSTOMIZATIONS_DONE);    /* i18nOK:LINE */
		call symputx('file_name',FILE_NAME);    /* i18nOK:LINE */
		run;

		/*************Customization done to file*******************/
		%if &custmztn_done eq Y %then %do;
			proc sql;
				update &lib_apdm..EXTERNAL_CODE_MASTER set external_code_file_loc="/Products/SAS Risk Modeling/External Code"   /* i18nOK:LINE */
				where external_code_file_nm="&file_name";
			quit;
		%end;
		%else %do;
			proc sql;
				update &lib_apdm..EXTERNAL_CODE_MASTER set external_code_file_loc="/Products/SAS Risk Modeling/Risk Modeling Content/v03.2020/Banking Solution/macros"     /* i18nOK:LINE */
				where external_code_file_nm="&file_name";
			quit;
		%end;
	%end;
	
%mend rmcr_81_external_code_import;