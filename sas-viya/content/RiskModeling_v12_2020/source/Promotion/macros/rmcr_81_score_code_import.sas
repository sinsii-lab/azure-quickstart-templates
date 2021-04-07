/********************************************************************************************************
   	Module		:  rmcr_81_score_code_import

   	Function	:  This macro will be used to upload model scoring codes from 63 to content server.

	Called-by	:  rmcr_81_import_wrppr
	Calls		:  None
					
   	Parameters	:  None
	
	Author		:  CSB Team 
				   
*********************************************************************************************************/
%macro rmcr_81_score_code_import();

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;
/*********************************************
Copy score code to staging location -starts
**********************************************/
proc sql;
create table score_codes_upload as 
select * from &lib_apdm..model_master where model_source_type_sk
 ne (select model_source_type_sk from &lib_apdm..model_source_type_master where upcase(model_source_type_cd)='VIYA')
;
quit;

proc sql;
select * from score_codes_upload;
quit;

%let m_scr_code_cnt=&sqlobs;

%if &m_scr_code_cnt > 0 %then %do;
	%do scr_code=1 %to &m_scr_code_cnt;
		data _null_;
		obs=&scr_code;
		set score_codes_upload point=obs;
		call symputx('model_sk',model_id);
		stop;
		run;
		
		
		/********************************************************************************************
		To check if score.sas and scorcard_grouping_code_<model_sk>.sas exist for the model_sk on 63
		*********************************************************************************************/
		%let score_code_exist=%sysfunc(fileexist(&m_staging_area_path/cs63_export_pkg/scoring_code/model/&model_sk/score.sas));
		%let scorecard_group_exist=%sysfunc(fileexist(&m_staging_area_path/cs63_export_pkg/scoring_code/model/&model_sk/scorecard_grouping_code_&model_sk..sas));
		
		%if &score_code_exist eq 1 %then %do;
			filename scr_in "&m_staging_area_path/cs63_export_pkg/scoring_code/model/&model_sk/score.sas";/*i18NOK:LINE*/
			filename scr_out filesrvc folderpath="/Products/SAS Risk Modeling/Model/&model_sk" filename= "score.sas" debug=http CD="attachment; filename=score.sas";/* i18nOK:Line */
			
			data _null_;
			infile scr_in end=eof_flg;
			file scr_out;
			input;
			if _n_ = 1 then do;
				put 'data &m_output_table_nm;';
				put 'set &m_scoring_abt_table_nm;';
			end;
			put _infile_;
			if eof_flg then do;
				put 'run;';
			end;
            run;
			
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.SCORE_CD_SM1.1, noquote, &model_sk.));
			
		%end;
		
		%else %do;
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.SCORE_CD_SM3.1, noquote, &model_sk., &m_staging_area_path., &model_sk.)); 	
		%end;
		
		%if &scorecard_group_exist eq 1 %then %do;
			filename scr_in "&m_staging_area_path/cs63_export_pkg/scoring_code/model/&model_sk/scorecard_grouping_code_&model_sk..sas";/*i18NOK:LINE*/
			filename scr_out filesrvc folderpath="/Products/SAS Risk Modeling/Model" filename= "scorecard_grouping_code_&model_sk..sas" debug=http CD="attachment; filename=scorecard_grouping_code_&model_sk..sas";/*i18nOK:Line */
			
			data _null_;
			rc=fcopy('scr_in','scr_out');
			msg=sysmsg();
			put rc = msg=;
			if rc = 0 then do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.SCORE_CD_SM4.1, noquote, &model_sk.)); 
			end;
			else do;
				put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.SCORE_CD_SM5.1, noquote, &model_sk.)); 
				call symputx('job_rc',1012);
			end;
			run;
		%end;
	%end;
%end;

%mend rmcr_81_score_code_import;	