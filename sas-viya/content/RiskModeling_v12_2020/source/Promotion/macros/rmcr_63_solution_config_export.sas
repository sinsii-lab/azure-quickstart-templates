/********************************************************************************************************
   	Module		:  rmcr_63_solution_config_export

   	Function	:  This macro will be used to create export package from 63 to 81.

	Called-by	:  NONE
	Calls		:  	rmcr_63_internal_data_export
					rmcr_63_external_code_export
					rmcr_63_score_code_export
					rmcr_63_event_data_export
					rmcr_63_level_data_export
					
   	Parameters	:  None
	Author		:   RM Team
	
	Sample call :
					%let m_staging_area_path=/tmp;
					%let m_source_code_path=/tmp/source_code;
					%include "&m_source_code_path/rmcr_63_solution_config_export.sas";
					%CSBINIT;
					%rmcr_63_solution_config_export();
				   
*********************************************************************************************************/

%macro rmcr_63_solution_config_export();

	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;
	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

	/****** Creating global variable m_staging_area which holds the staging path ******/
	%global m_staging_area path_to_migration path_to_log;
	%let m_staging_area=&m_staging_area_path;
	%let m_sourcecode_area=&m_source_code_path;

	/****** Creating a timestamp macro variable that will be used for logs and txt files ******/	
	data _null_;   
	   call symputx('timestamp', put(datetime(),datetime20.));    /* I18NOK:LINE */
	run;
	%let timestamp=%sysfunc(ktranslate(&timestamp,'.',':'));   /* I18NOK:LINE */

	/****** path_to_migration is a macro variable that holds the path of folder cs63_export_pkg 
			under which all folders will be made ******/	
	%let path_to_migration=;
	%dabt_make_work_area(dir=&m_staging_area, create_dir=cs63_export_pkg, path=path_to_migration);
	%let path_to_migration=&path_to_migration;

	/****** path_to_log is a macro variable that holds the path of log folder under cs63_export_pkg log ******/	
	%let path_to_log=;
	%dabt_make_work_area(dir=&path_to_migration, create_dir=log, path=path_to_log);
	%let path_to_log=&path_to_log;

	/****** Divert log to log folder ******/
	proc printto log="&path_to_log/rmcr_63_solution_config_export_&timestamp..log";     /* I18NOK:LINE */
	run;

	/********************************************************************
			MACRO DEFINITON STARTS FOR INTERNAL DATA
	*********************************************************************/
	/* This macro will be used to create export package of internal data 
		libraries, user mapping csv file and derived variable expression 
		file from 63 to staging area.
	*********************************************************************/
	%macro rmcr_63_internal_data_export();

		/****** Create directory for internal data xpt- starts ******/	
		%let path_to_internal_data_xpt=;
		%dabt_make_work_area(dir=&path_to_migration, create_dir=internal_data, path=path_to_internal_data_xpt);
		%let path_to_internal_data_xpt=&path_to_internal_data_xpt;	
		/****** Create directory for internal data xpt- ends ******/
	
		/****** Create xpts at the internal data path- starts ******/	
		proc cport lib=apdm file="&path_to_internal_data_xpt/apdm.xpt"; run;    /* I18NOK:LINE */
		%let job_rc=&syscc;
		proc cport lib=control file="&path_to_internal_data_xpt/control.xpt"; run;   /* I18NOK:LINE */
		%let job_rc=&syscc;
		proc cport lib=csbctrl file="&path_to_internal_data_xpt/csbctrl.xpt"; run;   /* I18NOK:LINE */
		%let job_rc=&syscc;
		proc cport lib=custpara file="&path_to_internal_data_xpt/custpara.xpt"; run;    /* I18NOK:LINE */
		%let job_rc=&syscc;
		/****** Create xpts at the internal data path -ends ******/

		/****** Create user_mapping.csv file- starts *******/
		proc sql noprint;
			create table user_list as 
				select distinct 
					created_by_user as user
				from &lib_apdm..project_master
				where created_by_user is not null
				union
				select distinct 
					created_by_user as user
				from &lib_apdm..model_master
				where created_by_user is not null
			;
		quit;
		%dabt_err_chk(type=SQL);
			
		data _null_;
		file "&path_to_internal_data_xpt/user_mapping.csv" dlm=',';    /* I18NOK:LINE */
		set user_list;
		if _n_ =1 then
		put 'FROM_USER,'    /* I18NOK:LINE */
		'TO_USER'     /* I18NOK:LINE */
		;
		put 
		user
		user
		;
		run;
		%dabt_err_chk(type=DATA);
		/****** Create user_mapping.csv file- ends ******/

		/****** Create derived_variable_expression.csv file- starts ******/
		proc sql;
			create table variable_map as
				select 	'N' as overwrite_flg,       /* I18NOK:LINE */
						drvd.variable_sk,
						drvd.derived_var_calc_expression,
						prj_mstr.project_id,
						prj_mstr.project_short_nm,
						prj_mstr.created_by_user
				from 
					&lib_apdm..derived_variable drvd
				inner join 
					&lib_apdm..modeling_abt_x_variable mdl_x_vrbl
				on drvd.variable_sk=mdl_x_vrbl.variable_sk
				inner join 
					&lib_apdm..modeling_abt_master mdl_abt_mstr
				on mdl_x_vrbl.abt_sk=mdl_abt_mstr.abt_sk
				left join 
					&lib_apdm..project_master prj_mstr
				on mdl_abt_mstr.project_sk=prj_mstr.project_sk
			;
		quit;
		%dabt_err_chk(type=SQL);
		
		proc sql;
		update variable_map
		/* I18NOK:BEGIN */
		set DERIVED_VAR_CALC_EXPRESSION='(((<<SUM_TOT_BAL_L1M>>*<<SUM_CUR_LIMIT_AMT_12MB>>)/NULLIF(<<SUM_CUR_LIMIT_AMT_L1M>>,0))-<<AVG_TOT_BAL_13_24M>>)/NULLIF((<<SUM_CUR_LIMIT_AMT_L1M>>-<<AVG_TOT_BAL_13_24M>>),0)'     
		, OVERWRITE_FLG='Y'								
		where DERIVED_VAR_CALC_EXPRESSION='(((<<SUM_TOT_BAL_L1M>>*<<SUM_CUR_LIMIT_AMT_12MB>>)/<<SUM_CUR_LIMIT_AMT_L1M>>)-<<AVG_TOT_BAL_13_24M>>)/(<<SUM_CUR_LIMIT_AMT_L1M>>-<<AVG_TOT_BAL_13_24M>>)'
		;
		quit;
		%dabt_err_chk(type=SQL);

		data _null_;
			file "&path_to_internal_data_xpt/derived_variable_expression.csv" dlm='~';
				set variable_map;
			if _n_ =1 then
			put 'OVERWRITE_FLG~' 
			'VARIABLE_SK~'
			'DERIVED_VAR_CALC_EXPRESSION~'
			'PROJECT_ID~'
			'PROJECT_NAME~'
			'PROJECT_OWNER'
			/* I18NOK:END */
			;
			put
			overwrite_flg
			variable_sk
			derived_var_calc_expression
			project_id
			project_short_nm
			created_by_user
			;
		run;
		
		/****** Create filter_expression.csv file- starts ******/
		proc sql;
			create table filter_expression as
				select 'N' as overwrite_flg,    /* I18NOK:LINE */
				tne.target_node_sk,
				tne.user_entered_value_txt
				from 
				&lib_apdm..TARGET_NODE_EXPRESSION AS TNE
				where TNE.user_entered_value_flg='Y'     /* I18NOK:LINE */
			;
		quit;
		%dabt_err_chk(type=SQL);

		data _null_;
			file "&path_to_internal_data_xpt/filter_expression.csv" dlm='~';  /* I18NOK:LINE */
				set filter_expression;
			if _n_ =1 then
			put 'OVERWRITE_FLG~'   /* I18NOK:LINE */
			'TARGET_NODE_SK~'    /* I18NOK:LINE */
			'FILTER_EXPRESSION'   /* I18NOK:LINE */
			;
			put
			overwrite_flg
			target_node_sk
			user_entered_value_txt
			;
		run;
		%dabt_err_chk(type=DATA);
		/****** Create filter_expression.csv file -ends ******/

	%mend rmcr_63_internal_data_export;
	
	/********************************************************************
	MACRO DEFINITON ENDS FOR INTERNAL DATA
	*********************************************************************/

	/********************************************************************
	MACRO DEFINITON STARTS FOR EXTERNAL CODES
	*********************************************************************/
	/********************************************************************************************************
	/* This macro will be used to create export package of external codes from 63 to staging area. */
	
	%macro rmcr_63_external_code_export();

		/******	Create directory for external code- starts ******/	
		%let path_to_external_code=;
		%dabt_make_work_area(dir=&path_to_migration, create_dir=external_code, path=path_to_external_code);
		%let path_to_external_code=&path_to_external_code;
		/****** Create directory for external code- ends ******/
		
		proc sql;
			create table external_codes as 
				select external_code_file_loc,external_code_file_nm from 
				&lib_apdm..external_code_master
				;
			select * from external_codes;
		quit;

		%let m_ext_code_cnt=&sqlobs;
			
		
		%if &m_ext_code_cnt > 0 %then %do;
			%do ext_code=1 %to &m_ext_code_cnt;
				data _null_;
				obs=&ext_code;
				set external_codes point=obs;
				call symputx('external_code_loc',external_code_file_loc);     /* I18NOK:LINE */
				call symputx('external_code_file',external_code_file_nm);    /* I18NOK:LINE */
				stop;
				run;
				
				/****** Check if external code is present at external location on 63.If yes copied to staging area. ******/				
				%let ext_code_exist=%sysfunc(fileexist(&external_code_loc/&external_code_file));
				%if &ext_code_exist eq 1 %then %do;
					filename in_code "&external_code_loc/&external_code_file";/*i18NOK:LINE*/
					filename out_code "&path_to_external_code/&external_code_file";/*i18NOK:LINE*/
					
					data _null_;
					rc=fcopy('in_code','out_code');     /* I18NOK:LINE */
					msg=sysmsg();
					put rc = msg=;
					if rc = 0 then do;
						put "External code &external_code_file copied to staging location";/*i18NOK:LINE*/
					end;
					else do;
						put "ERROR: Failed to copy &external_code_file to staging location"; /* i18nOK:Line */
						call symputx('job_rc',1012);   /* I18NOK:LINE */
					end;
					run;
				%end;
				
				%else %do;
					data _null_;
						file "&path_to_log/exception_external_code_&timestamp..txt" MOD ;    /* I18NOK:LINE */
						put 
						"&external_code_file not found at &external_code_loc., so it is not copied to &path_to_external_code location."    /* I18NOK:LINE */
						;
						put;
					run;
				%end;
			%end;
		%end;
		
		%let bnk_cnt_appl=;
		proc sql;
		select count(*) into :bnk_cnt_appl from &lib_apdm..CS_CR_INFO     /* I18NOK:LINE */
		where cr_unique_cd='BANKING_BASEL' and status_sk=1 ;    /* I18NOK:LINE */
		quit;
		%let bnk_cnt_appl=&bnk_cnt_appl;
		
		%if &bnk_cnt_appl ge 1 %then %do;
		
			data _null_;
				file "&path_to_external_code/external_code.csv" dlm=',';    /* I18NOK:LINE */
					put 'FILE_NAME,'				/* I18NOK:LINE */
					'CUSTOMIZATIONS_DONE'           /* I18NOK:LINE */
					;
					put 
					'csbmva_ext_calc_lgd_var.sas,'   /* I18NOK:LINE */
					'N'								/* I18NOK:LINE */
					;
			run;
		%end;
			
	%mend rmcr_63_external_code_export;
	/********************************************************************
	MACRO DEFINITON ENDS FOR EXTERNAL CODES
	*********************************************************************/

	/********************************************************************
	MACRO DEFINITON STARTS FOR SCORE CODES
	*********************************************************************/
	/* This macro will be used to create export package of model scoring 
	codes from 63 to staging area. */

	%macro rmcr_63_score_code_export();

		/****** Project path is a global macro variable which has path till project folder in 63 ******/	
		%let m_project_path=&project_path;

		/****** Create directory for scoring code -starts ******/
		%let path_to_scoring_code=;
		%dabt_make_work_area(dir=&path_to_migration, create_dir=scoring_code/model, path=path_to_scoring_code);
		%let path_to_scoring_code=&path_to_scoring_code;
		/******	Create directory for scoring code -ends ******/		

		
		/****** Copy score code to staging location -starts ******/
		proc sql;
			create table score_codes as 
			select * from &lib_apdm..model_master where model_source_type_sk
			ne (select model_source_type_sk from &lib_apdm..model_source_type_master where upcase(model_source_type_cd)='VIYA')
			;
			select * from score_codes;
		quit;

		%let m_scr_code_cnt=&sqlobs;

		%if &m_scr_code_cnt > 0 %then %do;
			%do scr_code=1 %to &m_scr_code_cnt;
				data _null_;
					obs=&scr_code;
					set score_codes point=obs;
					call symputx('model_sk',model_id);     /* i18nOK:Line */
					call symputx('project_sk',project_sk);   /* i18nOK:Line */
					stop;
				run;
				
				/****** Create folder by model_sk and put scoring code in it ******/				
				%let path_to_model_scoring_code=;
				%dabt_make_work_area(dir=&path_to_scoring_code, create_dir=&model_sk, path=path_to_model_scoring_code);
				%let path_to_model_scoring_code=&path_to_model_scoring_code;
				
				/****** To check if score.sas and scorcard_grouping_code_<model_sk>.sas exist for the model_sk on 63 ******/				
				%let score_code_exist=%sysfunc(fileexist(&m_project_path/&project_sk/model/&model_sk/scoring_code/score.sas));
				%let scorecard_group_exist=%sysfunc(fileexist(&m_project_path/&project_sk/model/&model_sk/scoring_code/scorecard_grouping_code_&model_sk..sas));
				
				%if &score_code_exist eq 1 %then %do;
					filename in_code "&m_project_path/&project_sk/model/&model_sk/scoring_code/score.sas";/*i18NOK:LINE*/
					filename out_code "&path_to_model_scoring_code/score.sas";/*i18NOK:LINE*/
					
					data _null_;
					rc=fcopy('in_code','out_code');    /* i18nOK:Line */
					msg=sysmsg();
					put rc = msg=;
					if rc = 0 then do;
						put "Score code for model &model_sk copied to staging location";/*i18NOK:LINE*/
					end;
					else do;
						put "ERROR: Failed to copy score code for model &model_sk"; /* i18nOK:Line */
						call symputx('job_rc',1012);   /* i18nOK:Line */
					end;
					run;
				%end;
				
				%else %do;
					data _null_;
						file "&path_to_log/exception_score_code_&timestamp..txt" MOD ;  
						put 
						"Score.sas not found for model &model_sk. at &m_project_path/&project_sk/model/&model_sk path, so it is not copied to &path_to_model_scoring_code location."
						;
						put;
					run;
				%end;
				
				%if &scorecard_group_exist eq 1 %then %do;
					filename in_code "&m_project_path/&project_sk/model/&model_sk/scoring_code/scorecard_grouping_code_&model_sk..sas";/*i18NOK:LINE*/
					filename out_code "&path_to_model_scoring_code/scorecard_grouping_code_&model_sk..sas";/*i18NOK:LINE*/
					
					data _null_;
					rc=fcopy('in_code','out_code');       
					msg=sysmsg();
					put rc = msg=;
					if rc=0 then do;
						put "Scorecard grouping code  for model &model_sk copied to staging location";/*i18NOK:LINE*/
					end;
					else do;
						put "ERROR: Failed to copy scorecard grouping code for model &model_sk"; /* i18nOK:Line */
						call symputx('job_rc',1012);
					end;
					run;
				%end;
			%end;
		%end;
		/****** Copy score code to staging location -ends ******/		
			
	%mend rmcr_63_score_code_export;
	/********************************************************************
	MACRO DEFINITON ENDS FOR SCORE CODES
	*********************************************************************/
	
	/********************************************************************
	MACRO DEFINITON STARTS FOR LEVEL DATA
	*********************************************************************/
	
	/* This macro will be used to create export package of 
		level(SOA) data */
	%macro rmcr_63_level_data_export();
	
		proc sql;
		select * from 
		&lib_apdm..level_master where level_type_sk= (select level_type_sk from &lib_apdm..level_type_master where level_type_cd="APPLCTN");
		quit;
		
		%dabt_err_chk(type=SQL);
		
		/* If level is configured level_data folder is created and applied_level_info.csv and level macro is copied to folder */
		%if &sqlobs ge 1 %then %do;
			%let m_appln_level_ext_flg=Y;
			%let path_to_level_data=;
			%dabt_make_work_area(dir=&path_to_migration, create_dir=level_data, path=path_to_level_data);
			%let path_to_level_data=&path_to_level_data;
			
			data _null_;
			file "&path_to_level_data/applied_level_info.csv" dlm=',';
				put 'FILE_NAME,'
				'CUSTOMIZATIONS_DONE'
				;
				put 
				'csbmva_get_subset_appl_list.sas,'
				'N'
				;
			run;
			
			%dabt_err_chk(type=DATA);

			%macro get_ucmacro_path;
				%global host_ucmacro_path;
				/* Setting path of CSB ucmacros */
				%if ((&SYSSCP eq WIN) or (&SYSSCP eq DNTHOST)) %then %do;
					%let host_ucmacro_path = %sysget(SASROOT)/&CSB_PROGRAMS_FOLDER_NM./ucmacros;
				%end;
				%else %do;
					  %let host_ucmacro_path = %sysget(SASROOT)/ucmacros/&CSB_PROGRAMS_FOLDER_NM.;
				%end;
			%mend;
			
			%get_ucmacro_path;
			
			
			filename in_code "&host_ucmacro_path/csbmva_get_subset_appl_list.sas";/*i18NOK:LINE*/
			filename out_code "&path_to_level_data/csbmva_get_subset_appl_list.sas";/*i18NOK:LINE*/
			
			data _null_;
			rc=fcopy('in_code','out_code');
			msg=sysmsg();
			put rc = msg=;
			if rc = 0 then do;
				put "csbmva_get_subset_appl_list.sas copied to level_data folder";/*i18NOK:LINE*/
			end;
			else do;
				put "ERROR: Failed to copy csbmva_get_subset_appl_list.sas to level_data folder"; /* i18nOK:Line */
				call symputx('job_rc',1012);
			end;
			run;
			
			
		%end;
		
		/* If level is not configured level_data a note is displayed */
		%else %do;
			%let m_appln_level_ext_flg=N;
			%put NOTE:There are no SoA of type application are configured;
		%end;

	
	%mend rmcr_63_level_data_export;
	/********************************************************************
	MACRO DEFINITON ENDS FOR LEVEL DATA
	*********************************************************************/
	
	/********************************************************************
	MACRO DEFINITON STARTS FOR EVENT DATA
	*********************************************************************/
	/* This macro will be used to create export package of 
		event data */
	%macro rmcr_63_event_data_export();
	
		proc sql;
		select count(*) from &lib_apdm..event_master;
		quit;
		
		%dabt_err_chk(type=SQL);

		%let evnt= &sqlobs;
		%let evnt_data=&MCR_DABT_CREATE_EVENT_DATA;
		
		/* If event is configured event_data folder is created and applied_event_info.csv and event macro is copied to folder */
		%if &evnt ge 1 %then %do;
			%let path_to_event_data=;
			%dabt_make_work_area(dir=&path_to_migration, create_dir=event_data, path=path_to_event_data);
			%let path_to_event_data=&path_to_event_data;
			
			data _null_;
			file "&path_to_event_data/applied_event_info.csv" dlm=',';
				put 'FILE_NAME,'
				'CUSTOMIZATIONS_DONE'
				;
				put 
				"&evnt_data..sas,"
				'N'
				;
			run;
			
			%dabt_err_chk(type=DATA);
			
			%macro get_ucmacro_path;
				%global host_ucmacro_path;
				/* Setting path of CSB ucmacros */
				%if ((&SYSSCP eq WIN) or (&SYSSCP eq DNTHOST)) %then %do;
					%let host_ucmacro_path = %sysget(SASROOT)/bankfdnsrv/ucmacros;
				%end;
				%else %do;
					  %let host_ucmacro_path = %sysget(SASROOT)/ucmacros/bankfdnsrv;
				%end;
			%mend;
			
			%get_ucmacro_path;
			
			filename in_code "&host_ucmacro_path/&evnt_data..sas";/*i18NOK:LINE*/
			filename out_code "&path_to_event_data/&evnt_data..sas";/*i18NOK:LINE*/
			
			data _null_;
			rc=fcopy('in_code','out_code');
			msg=sysmsg();
			put rc = msg=;
			if rc = 0 then do;
				put "&evnt_data..sas copied to level_data folder";/*i18NOK:LINE*/
			end;
			else do;
				put "ERROR: Failed to copy &evnt_data..sas to level_data folder"; /* i18nOK:Line */
				call symputx('job_rc',1012);
			end;
			run;
			
		%end;
		
		/* If event is not configured level_data a note is displayed */
		%else %do;
			%put NOTE:There are no events configured.;
		%end;
	
	%mend rmcr_63_event_data_export;
	/********************************************************************
	MACRO DEFINITON ENDS FOR EVENT DATA
	*********************************************************************/
	

	/******************************************************
	Call macros for create export packages at staging area
	*******************************************************/

	%rmcr_63_internal_data_export();

	%if &job_rc > 4 %then %do;
		%put ERROR: Some error occurred in exporting internal data;
		%return;
	%end;

	%rmcr_63_external_code_export();

	%if &job_rc > 4 %then %do;
		%put ERROR: Some error occurred in exporting external codes;
		%return;
	%end;


	%rmcr_63_score_code_export();

	%if &job_rc > 4 %then %do;
		%put ERROR: Some error occurred in exporting score codes;
		%return;
	%end;
	
	%rmcr_63_level_data_export();
	
	%if &job_rc > 4 %then %do;
		%put ERROR: Some error occurred in exporting level data;
		%return;
	%end;
	
	%rmcr_63_event_data_export();
	
	%if &job_rc > 4 %then %do;
		%put ERROR: Some error occurred in exporting event data;
		%return;
	%end;
	
	

%mend rmcr_63_solution_config_export;