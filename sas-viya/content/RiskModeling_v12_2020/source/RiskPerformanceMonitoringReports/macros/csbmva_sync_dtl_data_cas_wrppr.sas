/********************************************************************************************************
    Module      :  csbmva_sync_dtl_data_cas_wrppr

    Function    :  This macro will be used to generate detail and other reports data for 
					all pending runs of backtesting / ongoing MM
    
	Called-by   :  None
	
    Calls       :  None

    Parameters  :  m_report_category_cd        -> This is the report category code,that is, ONG for on-going run or BCK for back testing.
                       
                    
    Author      :   CSB Team 
    			

Sample call:
For all pending runs:
%csbmva_sync_dtl_data_cas_wrppr(m_report_category_cd=ONG); For all pending ongoing runs
%csbmva_sync_dtl_data_cas_wrppr(m_report_category_cd=BCK); For all pending backtesting runs
		

 ******************************************************************************************************************************************/  

%macro csbmva_sync_dtl_data_cas_wrppr(m_report_category_cd=);


	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;
	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;
	
	/*********************************************************
	Re-create views to reflect correct data in report-Start
	**********************************************************/
	
	%macro rmcr_recreate_views(m_report_type_sk=);
	
	%dabt_initiate_cas_session(cas_session_ref=recreate_views);
	
	/***********************************
	Initializing macros for MM report
	************************************/
	%if &m_report_type_sk eq 1 %then %do;
		%let m_cas_table=MM_MEASURE_STATS;
		%let m_cas_vw=VW_&m_cas_table.;	
	%end;
	/***********************************
	Initializing macros for MIP report
	************************************/
	%else %if &m_report_type_sk eq 2 %then %do;
		%let m_cas_table=MIP_MEASURE_STATS;
		%let m_cas_vw=VW_&m_cas_table.;
	%end;
	
	proc casutil;
        droptable casdata="&m_cas_vw." incaslib="&rm_reporting_mart_libref." quiet;    
	quit;

	/*****************************************************
	Check for the existence of view.If not present,create.
	*****************************************************/
	%let etls_tableExist = %eval(%sysfunc(exist(&rm_reporting_mart_libref..&m_cas_table., DATA)));
	
	%let etls_viewExist = %eval(%sysfunc(exist(&rm_reporting_mart_libref..&m_cas_vw., DATA)));
	
	%if &etls_tableExist. eq 1 and &etls_viewExist. eq 0 %then %do;
		proc cas;
		table.view /
		caslib="&rm_reporting_mart_libref"
		name="&m_cas_vw"
		replace=TRUE
		tables={{
		caslib="&rm_reporting_mart_libref",
		name="&m_cas_table",
		computedVars={{name="MEASURE_HEALTH"},    /*i18NOK:LINE*/
		
		%if &m_report_type_sk eq 1 %then %do; /*for MM only we need measure category health info*/
			{name="MEASURE_CATEGORY_HEALTH"},     /*i18NOK:LINE*/
		%end;
						
		{name="MODEL_HEALTH"}},     /*i18NOK:LINE*/
		%if &m_report_type_sk eq 1 %then %do; /*computing vars for MM*/		
/* I18NOK:BEGIN */		
			computedVarsprogram="MEASURE_HEALTH=if MEASURE_HEALTH_CD='GOOD' then 'GREEN'   
								else if MEASURE_HEALTH_CD ='BAD' then 'RED'
								else if MEASURE_HEALTH_CD ='AVG' then 'AMBER'
								else if MEASURE_HEALTH_CD ='NONE' then 'GRAY'
								else '';
								
								
								MEASURE_CATEGORY_HEALTH=if MEASURE_CATEGORY_HEALTH_CD='GOOD' then 'GREEN' 
									else if MEASURE_CATEGORY_HEALTH_CD ='BAD' then 'RED'
									else if MEASURE_CATEGORY_HEALTH_CD ='AVG' then 'AMBER'
									else if MEASURE_CATEGORY_HEALTH_CD ='NONE' then 'GRAY'
									else '';
								
								
								MODEL_HEALTH=if MODEL_HEALTH_CD='GOOD' then 'GREEN' 
								else if MODEL_HEALTH_CD ='BAD' then 'RED'
								else if MODEL_HEALTH_CD ='AVG' then 'AMBER'
								else if MODEL_HEALTH_CD ='NONE' then 'GRAY'
								else '';",
		%end;
		%else %do; /*computing vars for MIP*/	
			computedVarsprogram="MEASURE_HEALTH=if MEASURE_HEALTH_CD='GOOD' then 'GREEN' 
								else if MEASURE_HEALTH_CD ='BAD' then 'RED'
								else if MEASURE_HEALTH_CD ='AVG' then 'AMBER'
								else if MEASURE_HEALTH_CD ='NONE' then 'GRAY'
								else '';
																
								MODEL_HEALTH=if MODEL_HEALTH_CD='GOOD' then 'GREEN' 
								else if MODEL_HEALTH_CD ='BAD' then 'RED'
								else if MODEL_HEALTH_CD ='AVG' then 'AMBER'
								else if MODEL_HEALTH_CD ='NONE' then 'GRAY'
								else '';",
		%end;
		%if &m_report_type_sk eq 1 %then %do; /*varlist for MM*/
			varlist={'MODEL_ID','MODEL_NM','VERSION_NO','VERSION_NM','REPORT_CATEGORY_CD','REPORT_CATEGORY_NM',
			'SCORING_AS_OF_YEAR','SCORING_AS_OF_MONTH_NM','SCORING_AS_OF_DTTM','BIN_TYPE_CD','BIN_TYPE_NM',
			'MEASURE_CD','MEASURE_NM','MEASURE_VALUE','MEASURE_DISPLAY_SEQUENCE_NO',
			'MEASURE_THRESHLD_EXCEEDED_FLG','RED_LOWER','RED_UPPER','YELLOW_LOWER',
			'YELLOW_UPPER','GREEN_LOWER','GREEN_UPPER','MEASURE_CATEGORY_CD','MEASURE_CATEGORY_NM',
			'MSR_CTGRY_THRESHLD_EXCEEDED_FLG','MEASURE_CATEGORY_SEQUENCE_NO','MODEL_THRESHLD_EXCEEDED_FLG',
			'PURPOSE_CD','PURPOSE_NM','SUBJECT_OF_ANALYSIS_CD','SUBJECT_OF_ANALYSIS_NM','MODEL_TYPE_CD',
			'MODEL_TYPE_NM','SOURCE_SYSTEM_CD','BOUND_GREEN','BOUND_RED'}
		%end;
		%else %do;/*varlist for MIP*/
			varlist={'MODEL_ID','MODEL_NM','VERSION_NO','VERSION_NM','REPORT_CATEGORY_CD','REPORT_CATEGORY_NM',
			'SCORING_AS_OF_YEAR','SCORING_AS_OF_MONTH_NM','SCORING_AS_OF_DTTM','VARIABLE_COLUMN_NM','VARIABLE_SHORT_NM',
			'MEASURE_CD','MEASURE_NM','MEASURE_VALUE','MEASURE_DISPLAY_SEQUENCE_NO',
			'MEASURE_THRESHLD_EXCEEDED_FLG','RED_LOWER','RED_UPPER','YELLOW_LOWER',
			'YELLOW_UPPER','GREEN_LOWER','GREEN_UPPER','MEASURE_CATEGORY_CD','MEASURE_CATEGORY_NM',
			'MEASURE_CATEGORY_SEQUENCE_NO','MODEL_THRESHLD_EXCEEDED_FLG',
			'PURPOSE_CD','PURPOSE_NM','SUBJECT_OF_ANALYSIS_CD','SUBJECT_OF_ANALYSIS_NM','MODEL_TYPE_CD',
			'MODEL_TYPE_NM','SOURCE_SYSTEM_CD','BOUND_GREEN','BOUND_RED'}
		%end;  /* I18NOK:END */
		}};
		quit;
	
		%dabt_promote_table_to_cas(input_caslib_nm =&rm_reporting_mart_libref.,input_table_nm =&m_cas_vw.);
		
	%end;
	
	%dabt_terminate_cas_session(cas_session_ref=recreate_views);
	
	%mend rmcr_recreate_views;
	
	%rmcr_recreate_views(m_report_type_sk=1); /*Call for MM*/
	%rmcr_recreate_views(m_report_type_sk=2); /*Call for MIP*/
	/*********************************************************
	Re-create views to reflect correct data in report-End
	**********************************************************/
	
	%let job_sk=;
	%let job_sk=&m_job_sk;
	
	%if &m_report_category_cd=ONG %then %do;
		%let m_report_category_sk=1;
	%end;
	%else %if &m_report_category_cd=BCK %then %do;
		%let m_report_category_sk=2;
	%end;
	%else %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM6.1, noquote));
		%goto exit;
	%end;
	
	/****** path to work is a macro which stores physical path of work library *****/
	%let path_to_work=%sysfunc(pathname(work));
	%put &path_to_work;
	
	/****** path_to_log is a macro variable that holds the path of log folder ******/	
	%let path_to_log=;
	%dabt_make_work_area(dir=&path_to_work, create_dir=log, path=path_to_log);
	%let path_to_log=&path_to_log/;
	
/*----------------------------------------------------------------------------------------*/
/* For report category:Backtesting */
/*----------------------------------------------------------------------------------------*/

%if &m_report_category_sk eq 2 %then %do;  
		/*----------------------------------------------------------------------------------------*/
		/*----------------------------------------------------------------------------------------*/
		/* Check if DETAIL_REPORT_BACKTEST exists or not. If not create it. */
		/*----------------------------------------------------------------------------------------*/
		%if %sysfunc(exist(&lib_apdm..DETAIL_REPORT_BACKTEST)) = 0 %then %do;
				proc sql noprint;
					create table &lib_apdm..DETAIL_REPORT_BACKTEST (
						MODEL_SK num label='Model SK',      /* i18NOK:LINE */
						REPORT_SPECIFICATION_SK num label='Report Specification SK',	/* i18NOK:LINE */
						SCORING_AS_OF_TIME_SK num label='Scoring SK',	/* i18NOK:LINE */
						REPORT_CATEGORY_SK num label='Report Category SK',	/* i18NOK:LINE */
						EXECUTION_START_DTTM num format=datetime25.6 informat=datetime25.6 label="Execution Start Time",	/* i18NOK:LINE */
						EXECUTION_END_DTTM num format=datetime25.6 informat=datetime25.6 label="Execution End Time",	/* i18NOK:LINE */
						STATUS_SK num label='Status SK'	/* i18NOK:LINE */
					);
				quit;  
		%end;
		
	/*Delete all entries from DETAIL_REPORT_BACKTEST whose status is Failed, Inprogress*/
	proc sql noprint;
	delete * from &lib_apdm..DETAIL_REPORT_BACKTEST where STATUS_SK in (2,3);
	quit;

	/*Fetch all entries for which backtesting was successful*/
	proc sql noprint;
	create table BACK_RUN_SUCCESS as
	select distinct report_specification_sk,
	scoring_as_of_time_sk
	from &lib_apdm..mm_run
	where status_sk=1
	;
	quit;
	
	/*Fetch only entries for which detail data for backtesting is to be synced*/
	proc sql;
	create table BACK_RUN_TO_SYNC_DETAIL_DATA
	as select a.* from BACK_RUN_SUCCESS a
	left join &lib_apdm..DETAIL_REPORT_BACKTEST b
	on a.report_specification_sk=b.report_specification_sk
	and a.scoring_as_of_time_sk=b.scoring_as_of_time_sk
	where b.status_sk is null;
	quit;

	proc sql noprint;
	select count(*) into :m_count from BACK_RUN_TO_SYNC_DETAIL_DATA;  /* i18NOK:LINE */
	quit;
	
	%put ********Sync detail data for backtesting for records: &m_count********;

	%if &m_count eq 0 %then %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM7.1, noquote));
		%goto exit;
	%end;
	%else %do;
		%do bck=1 %to &m_count;
		
			data _null_;
			obs=&bck;
			set BACK_RUN_TO_SYNC_DETAIL_DATA point=obs;
			call symputx('m_report_specification_sk_run',report_specification_sk);   /* i18NOK:LINE */
			call symputx('m_scoring_as_of_time_sk_run',scoring_as_of_time_sk);   /* i18NOK:LINE */
			stop;
			run;
			
			/*Extract model_sk from MM_REPORT_SPECIFICATION*/
			%let m_model_sk_run=;
			proc sql noprint;
			select model_sk into: m_model_sk_run
			from &lib_apdm..MM_REPORT_SPECIFICATION
			where report_specification_sk=&m_report_specification_sk_run;
			quit;
			
			%let m_model_sk_run=&m_model_sk_run;
			
						
			%put *******Starting block for model=&m_model_sk_run version=&m_report_specification_sk_run and scoring=&m_scoring_as_of_time_sk_run*********;
			
			data _null_;
			var_jb = "&m_model_sk_run._&m_report_specification_sk_run._&m_scoring_as_of_time_sk_run"; /* i18nOK:Line */
			logfile="&path_to_log."||var_jb;/* i18nOK:Line */
			call symput('job_logname',trim(logfile)); /* i18nOK:Line */ 
			call symput('log_nm',trim(var_jb)); /* i18nOK:Line */
			run;
			
			%let log_on_content=&log_nm..log;
			
			filename joblog "&job_logname..log"; /* i18nOK:Line */
           
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM11.1, noquote,&log_on_content,&m_job_sk));
					   
			proc printto log=joblog &dabt_log_clear;
			run;
			
			proc sql noprint;
			insert into &lib_apdm..DETAIL_REPORT_BACKTEST values(&m_model_sk_run,&m_report_specification_sk_run,&m_scoring_as_of_time_sk_run,
					2,"%SYSFUNC(DATETIME(),DATETIME.)"DT,"%SYSFUNC(DATETIME(),DATETIME.)"DT,3);/*2-report category,3 in progress*/
			quit;
			
			
			%let m_sync_status_cd=0;
			
			%csbmva_sync_detail_data_in_cas(m_model_sk=&m_model_sk_run,
							m_report_specification_sk=&m_report_specification_sk_run,
							m_scoring_as_of_time_sk=&m_scoring_as_of_time_sk_run,
							m_report_category_sk=2,
							m_full_refresh_flag=N,
							m_report_type_sk=1,
							m_status_cd=m_sync_status_cd);
													
			%let m_sync_status_cd = &m_sync_status_cd.;
			
			%if &m_sync_status_cd. < 4 %then %do;
				proc sql noprint;
					update &lib_apdm..DETAIL_REPORT_BACKTEST
					set EXECUTION_END_DTTM="%SYSFUNC(DATETIME(),DATETIME.)"DT,
					STATUS_SK=1
					where MODEL_SK=&m_model_sk_run and
					REPORT_SPECIFICATION_SK=&m_report_specification_sk_run and
					SCORING_AS_OF_TIME_SK=&m_scoring_as_of_time_sk_run;
				quit;
			%end;
			%else %do;
				proc sql noprint;
					update &lib_apdm..DETAIL_REPORT_BACKTEST
					set EXECUTION_END_DTTM="%SYSFUNC(DATETIME(),DATETIME.)"DT,
					STATUS_SK=2
					where MODEL_SK=&m_model_sk_run and
					REPORT_SPECIFICATION_SK=&m_report_specification_sk_run and
					SCORING_AS_OF_TIME_SK=&m_scoring_as_of_time_sk_run;
				quit;
			%end;
			
			%put *******Ending block for model=&m_model_sk_run version=&m_report_specification_sk_run and scoring=&m_scoring_as_of_time_sk_run*********;
			
			%if &DABT_DEBUG_FLG=Y %then %do;
				%if &job_sk ne %then %do;
					filename log_in "&job_logname..log"; /* i18nOK:Line */
					filename log_out filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk/" filename= "&log_on_content" debug=http CD="attachment; filename=&log_on_content";  /* i18nOK:Line */
					data _null_;
					rc=fcopy('log_in','log_out');   /* i18nOK:LINE */
					msg=sysmsg();
					put rc = msg=;
					if rc = 0 then do;
						msg=sasmsg("work.rmcr_message_dtl_rpm_reports", "RMCR_RPM_REPORTS.MM_SM9.1", "noquote");	/* i18NOK:LINE */
						put msg;
					end;
					else do;
						msg=sasmsg("work.rmcr_message_dtl_rpm_reports", "RMCR_RPM_REPORTS.MM_SM10.1", "noquote"); 	/* i18NOK:LINE */
						put msg; 
						call symputx('job_rc',1012);     /* i18nOK:LINE */
					end;
					run;
				%end;
			%end;
			
			proc printto;
			run;
			
		%end;/*End for do loop*/

	%end;/*End for else do part for count from BACK_RUN_TO_SYNC_DETAIL_DATA*/
	
	%if &job_sk ne %then %do;
	
		filename scrxpt filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk/" filename= "BCK_&m_job_sk._scratch_tables.xpt" debug=http recfm=n; /*i18NOK:LINE*/
		
		proc cport library=work file=scrxpt memtype=data;
		select BACK_RUN_TO_SYNC_DETAIL_DATA BACK_RUN_SUCCESS;
		run;
		
		filename scrxpt clear;
		
		%dabt_update_job_status(job_sk = &m_job_sk.,return_cd = &job_rc. );
	%end;

	
%end;/*End for report category:Backtesting*/

/*-----------------------------------------------------------------------------------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------------*/
/* For report category:Ongoing */
/*----------------------------------------------------------------------------------------*/

%if &m_report_category_sk eq 1 %then %do; 
	/*----------------------------------------------------------------------------------------*/
	/*----------------------------------------------------------------------------------------*/
	/* Check if DETAIL_REPORT_ONG exists or not. If not create it. */
	/*----------------------------------------------------------------------------------------*/
	%if %sysfunc(exist(&lib_apdm..DETAIL_REPORT_ONG)) = 0 %then %do;
			proc sql noprint;
				create table &lib_apdm..DETAIL_REPORT_ONG (
					MODEL_SK num label='Model SK',      /* i18NOK:LINE */
					REPORT_SPECIFICATION_SK num label='Report Specification SK',	/* i18NOK:LINE */
					SCORING_AS_OF_TIME_SK num label='Scoring SK',	/* i18NOK:LINE */
					REPORT_CATEGORY_SK num label='Report Category SK',	/* i18NOK:LINE */
					EXECUTION_START_DTTM num format=datetime25.6 informat=datetime25.6 label="Execution Start Time",	/* i18NOK:LINE */
					EXECUTION_END_DTTM num format=datetime25.6 informat=datetime25.6 label="Execution End Time",	/* i18NOK:LINE */
					STATUS_SK num label='Status sk'	/* i18NOK:LINE */
				);
			quit;  
	%end;
	
	/*Delete all entries from DETAIL_REPORT_ONG whose status is Failed or Inprogress*/
	proc sql noprint;
	delete * from &lib_apdm..DETAIL_REPORT_ONG where STATUS_SK in (2,3);
	quit;
	
	/*Fetch all entries for which ongoing was successful*/
	proc sql noprint;
	create table ONG_RUN_SUCCESS as
	select 
	distinct
	scr_rslt.scoring_control_detail_sk,
	scr_rslt.scoring_as_of_dttm,
	scr_rslt.report_specification_sk,
	time_dim.time_sk
	from &lib_apdm..actual_result_control_detail act_rslt
	inner join &lib_apdm..scoring_control_detail scr_rslt
	on act_rslt.scoring_control_detail_sk=scr_rslt.scoring_control_detail_sk
	and act_rslt.load_csmart_status_sk=1
	and scr_rslt.load_csmart_status_sk=1
	inner join &lib_apdm..time_dim time_dim
	on scr_rslt.scoring_as_of_dttm=time_dim.period_last_dttm
	
	;
	quit;
	
	/*Fetch only the entries for which detail data for ongoing is to be synced*/
	proc sql;
	create table ONG_RUN_TO_SYNC_DETAIL_DATA
	as select a.* from ONG_RUN_SUCCESS a
	left join &lib_apdm..DETAIL_REPORT_ONG b
	on a.report_specification_sk=b.report_specification_sk
	and a.time_sk=b.scoring_as_of_time_sk
	where b.status_sk is null;
	quit;

	proc sql noprint;
	select count(*) into :m_count from ONG_RUN_TO_SYNC_DETAIL_DATA;		/* i18NOK:LINE */
	quit;
	
	%put ********Sync detail data for ongoing for records: &m_count********;

	%if &m_count eq 0 %then %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM8.1, noquote));
		%goto exit;
	%end;
	%else %do;
		%do ong=1 %to &m_count;
		
			data _null_;
			obs=&ong;
			set ONG_RUN_TO_SYNC_DETAIL_DATA point=obs;
			call symputx('m_report_specification_sk_run',report_specification_sk);	/* i18NOK:LINE */
			call symputx('m_scoring_as_of_time_sk_run',time_sk);	/* i18NOK:LINE */
			stop;
			run;
			
			/*Extract model_sk from MM_REPORT_SPECIFICATION*/
			%let m_model_sk_run=;
			proc sql noprint;
			select model_sk into: m_model_sk_run
			from &lib_apdm..MM_REPORT_SPECIFICATION
			where report_specification_sk=&m_report_specification_sk_run;
			quit;
			
			%let m_model_sk_run=&m_model_sk_run;
			
			/*Fetch m_mining_algorithm from MODEL_MASTER_EXTENSION. If value is JDG_SCR,then run for MIP too.*/
			%let m_mining_algorithm = ;     
   
			proc sql ;
					select model_mining_algorithm into :m_mining_algorithm from &lib_apdm..model_master
					where model_sk = &m_model_sk_run
					and kupcase(model_mining_algorithm) in (&DABT_VALID_SCRCRD_ALGRTHM_VAL.) ; /*i18NOK:LINE*/
			quit;
			%let m_mining_algorithm = &m_mining_algorithm.;
			
			%put *******Starting block for model=&m_model_sk_run version=&m_report_specification_sk_run and scoring=&m_scoring_as_of_time_sk_run*********;
			
			data _null_;
			var_jb = "&m_model_sk_run._&m_report_specification_sk_run._&m_scoring_as_of_time_sk_run"; /* i18nOK:Line */
			logfile="&path_to_log."||var_jb;/* i18nOK:Line */
			call symput('job_logname',trim(logfile)); /* i18nOK:Line */ 
			call symput('log_nm',trim(var_jb)); /* i18nOK:Line */
			run;
			
			%let log_on_content=&log_nm..log;
			
			filename joblog "&job_logname..log"; /* i18nOK:Line */
           
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM11.1, noquote,&log_on_content,&m_job_sk));
					   
			proc printto log=joblog &dabt_log_clear;
			run;
		
			proc sql noprint;
			insert into &lib_apdm..DETAIL_REPORT_ONG values(&m_model_sk_run,&m_report_specification_sk_run,&m_scoring_as_of_time_sk_run,
					1,"%SYSFUNC(DATETIME(),DATETIME.)"DT,"%SYSFUNC(DATETIME(),DATETIME.)"DT,3);/*1-report category(ONG),3 in progress*/
			quit;
			
			%let m_sync_status_cd=0;
			
			%csbmva_sync_detail_data_in_cas(m_model_sk=&m_model_sk_run,
							m_report_specification_sk=&m_report_specification_sk_run,
							m_scoring_as_of_time_sk=&m_scoring_as_of_time_sk_run,
							m_report_category_sk=1,
							m_full_refresh_flag=N,
							m_report_type_sk=1,
							m_status_cd=m_sync_status_cd);
													
			%let m_sync_status_cd = &m_sync_status_cd.;

			/*Call macro for MIP if model is scorecard model*/
			%if &m_mining_algorithm ne %then %do;
				%let mip_sync_status_cd=0;
				
				%csbmva_sync_detail_data_in_cas(m_model_sk=&m_model_sk_run,
							m_report_specification_sk=&m_report_specification_sk_run,
							m_scoring_as_of_time_sk=&m_scoring_as_of_time_sk_run,
							m_report_category_sk=1,
							m_full_refresh_flag=N,
							m_report_type_sk=2,
							m_status_cd=mip_sync_status_cd);
				
				%let mip_sync_status_cd=&mip_sync_status_cd;
				
			%end;
			
			%if &m_mining_algorithm ne %then %do;
				%if	&m_sync_status_cd. < 4 and &mip_sync_status_cd. < 4 %then %do;
					proc sql noprint;
						update &lib_apdm..DETAIL_REPORT_ONG
						set EXECUTION_END_DTTM="%SYSFUNC(DATETIME(),DATETIME.)"DT,
						STATUS_SK=1
						where MODEL_SK=&m_model_sk_run and
						REPORT_SPECIFICATION_SK=&m_report_specification_sk_run and
						SCORING_AS_OF_TIME_SK=&m_scoring_as_of_time_sk_run;
					quit;
				%end;
			%end;
			%else %if &m_sync_status_cd. < 4 %then %do;
				proc sql noprint;
						update &lib_apdm..DETAIL_REPORT_ONG
						set EXECUTION_END_DTTM="%SYSFUNC(DATETIME(),DATETIME.)"DT,
						STATUS_SK=1
						where MODEL_SK=&m_model_sk_run and
						REPORT_SPECIFICATION_SK=&m_report_specification_sk_run and
						SCORING_AS_OF_TIME_SK=&m_scoring_as_of_time_sk_run;
					quit;
			%end;
			%else %do;
				proc sql noprint;
					update &lib_apdm..DETAIL_REPORT_ONG
					set EXECUTION_END_DTTM="%SYSFUNC(DATETIME(),DATETIME.)"DT,
					STATUS_SK=2
					where MODEL_SK=&m_model_sk_run and
					REPORT_SPECIFICATION_SK=&m_report_specification_sk_run and
					SCORING_AS_OF_TIME_SK=&m_scoring_as_of_time_sk_run;
				quit;
			%end;
			
			%put *******Ending block for model=&m_model_sk_run version=&m_report_specification_sk_run and scoring=&m_scoring_as_of_time_sk_run*********;
		
			%if &DABT_DEBUG_FLG=Y %then %do;
				%if &job_sk ne %then %do;
					filename log_in "&job_logname..log"; /* i18nOK:Line */
					filename log_out filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk/" filename= "&log_on_content" debug=http CD="attachment; filename=&log_on_content";  /* i18nOK:Line */
					data _null_;
					rc=fcopy('log_in','log_out');   /* i18nOK:LINE */
					msg=sysmsg();
					put rc = msg=;
					if rc = 0 then do;
						msg=sasmsg("work.rmcr_message_dtl_rpm_reports", "RMCR_RPM_REPORTS.MM_SM9.1", "noquote");	/* i18NOK:LINE */
						put msg; 
					end;
					else do;
						msg=sasmsg("work.rmcr_message_dtl_rpm_reports", "RMCR_RPM_REPORTS.MM_SM10.1", "noquote");	/* i18NOK:LINE */
						put msg; 
						call symputx('job_rc',1012);     /* i18nOK:LINE */
					end;
					run;
				%end;
			%end;
			
			proc printto;
			run;
		
		%end;/*End for do loop*/
	%end;/*End for else do part for count from ONG_RUN_TO_SYNC_DETAIL_DATA*/
	
	%if &job_sk ne %then %do;
	
		filename scrxpt filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk/" filename= "ONG_&m_job_sk._scratch_tables.xpt" debug=http recfm=n; /*i18NOK:LINE*/
		
		proc cport library=work file=scrxpt memtype=data;
		select ONG_RUN_SUCCESS ONG_RUN_TO_SYNC_DETAIL_DATA;
		run;
		
		filename scrxpt clear;
		
		%dabt_update_job_status(job_sk = &m_job_sk.,return_cd = &job_rc. );
	%end;
	
%end; /*End for Report Categor:Ongoing*/

%exit:
%mend csbmva_sync_dtl_data_cas_wrppr;