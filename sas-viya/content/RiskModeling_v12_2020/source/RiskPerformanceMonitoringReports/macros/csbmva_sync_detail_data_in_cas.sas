/********************************************************************************************************
    Module      :  csbmva_sync_detail_data_in_cas


    Function    :  This macro will be used to synchronize MM/MIP measure detail data in cas so that 
                    Model Monitoring (MM)/Model Input Monitoirng (MIP) dashboards and Detail reorts can be generated in Visual Analytics (VA).


    Called-by   :  SAS Macro
    Calls       :  None


    Parameters  :  Mandatory Parameters:
                    m_model_sk                   -> This is the model number.Foreign key to apdm.model_master
                    m_report_specification_sk   -> This is the report specification or version number.Foreign key to apdm.mm_report_specification
                    m_scoring_as_of_time_sk     -> This is time for which the mm measure data is to be taken.Foreign key to apdm.time_dim
                    m_report_category_sk           -> This is the report category,that is, on-going run or back testing.Foreign key to             apdm.Mm_report_category_master.
                    m_full_refresh_flag            -> This is the flag value to identify if we want full refresh of data in cas or just append data in cas.
                                                    Values will be Y/N
                    m_report_type_sk            ->This is the report type,that is, MM or MIP.
                    m_status_cd                    -> This is the flag that holds the value of return code.
                    
    Author        :   CSB Team 
    
    Processing:
	
		* Exit if the macro is called for development data
			Assumption: Detail reports are not seen on development data.
			
		* Extract model,version,scoring date and purpose to populate in feed and measure tables .
			Usage: To populate these columns in FEED and Measure details table.
			
		* Extract model target type, report category code, report type from the macro variables passed-m_model_sk,m_report_category_sk,report_type_cd.
			Usage: From Model target type and report type identify the name of feed table and the applicable measures.
			
		* Check if input feed table for backtest/ongoing exist in memory in RM_BKT/RM_PERF caslibs. 
			If not see if it is physically available as sashdat. If not give a NOTE and come out of macro.		
		
		* Extract report_category_cd and report_category_desc from APDM.MM_REPORT_CATEGORY_MASTER	
		
		* Extract report_type_cd from APDM.MM_REPORT_TYPE_MASTER
			Usage: To pass them in csbmva_ui_mm_msr_report / csbmva_ui_mip_msr_report macro
			
		* Ensure output feed table is available in memory (caslib: RM_RPTMR)
			** Check if output feed table exists in memory in RM_RPTMR.
				If the table exists, 
					*** Check if m_full_refresh_flag='N'.
						If yes,  
							**** check if data already exists in feed table for m_model_sk,
									m_report_specification_sk,m_report_category_sk and m_scoring_as_of_time_sk
									If yes,  
										delet the data.
				If the table doesn't exist, 
					*** Check if it is physically available as sashdat file.
						If yes, 
							load it into memory.
						If sashdat file isn't present,
							execute DDL to create the table and promote it with global scope.
		
		* Generate list of all the measure that belong to report type and target type.
		
		* Check if measure detail table exists in memory.
			If no,
				Check if it is available in form of sashdat file.
				If yes,
					Load in-memory table from sashdat file. 
				If no,
					Execute the DDL to create the table and promote it with global scope.
		
		*Loop over measures(Table existence check):
			*Check if measure table exists in memory in RM_RPTMR ,If not see if it is physically available as sashdat and load.
			If not use DDL to create and promote.
		
		*Loop over measures(Data Population in feed and measure deatil table):	
			*If m_full_refresh_flag='N' check if data already exists in measure table for m_model_sk,m_report_specification_sk,m_report_category_sk
			and m_scoring_as_of_time_sk and range_scheme_type_sk.
			
			Check report type:
			If MM:
				* Extract report subtype code from the report_specification_sk  and active flag='Y'
				Usage: For every measure and every bin type the measure detail table is to be populated
			
				*For every bin type loop over measures and call csbmva_ui_mm_msr_report macro.
				*Append the created work table to existing Cas table with Append=Yes option.
					Also add additional columns like model_sk,report_specification_sk,scoring_as_of_time_sk,report_category_sk

				*Append data to Feed table only for last measure, as feed is common for all measures 
					Also add additional columns like model_sk,report_specification_sk,scoring_as_of_time_sk,report_category_sk
			
			If MIP:
				*Extract variable list from scorecard_bin_group.
				*Call call csbmva_ui_mip_msr_report macro.
				*Append the created work table to existing Cas table with Append=Yes option.
					Also add additional columns like model_sk,report_specification_sk,scoring_as_of_time_sk,report_category_sk

				*Append data to Feed table only for last measure, as feed is common for all measures 
					Also add additional columns like model_sk,report_specification_sk,scoring_as_of_time_sk,report_category_sk
 ******************************************************************************************************************************************/       

%macro csbmva_sync_detail_data_in_cas(m_model_sk=,
								m_report_specification_sk=,
								m_scoring_as_of_time_sk=,
								m_report_category_sk=,
								m_full_refresh_flag=,
								m_report_type_sk=,
								m_status_cd=status_cd);
								

	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;
	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;
	
	%if %symexist(ERROR_FLG)=0 %then %do;
		%global ERROR_FLG;
	%end;
	
	/*----------------------------------------------------------------------------------------*/
	/* Exit if the macro is called for development data */
	/*----------------------------------------------------------------------------------------*/
	%if &m_report_category_sk eq 3 %then %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM2.1, noquote));
		%goto exit;
	%end;
	/*----------------------------------------------------------------------------------------*/
	
	/*----------------------------------------------------------------------------------------*/
	/* Exit if the macro is called for Backtesting and MIP */
	/*----------------------------------------------------------------------------------------*/
	%if &m_report_category_sk eq 2 and &m_report_type_sk eq 2 %then %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM4.1, noquote));
		%goto exit;
	%end;
	/*----------------------------------------------------------------------------------------*/
	
	/* Initiate CAS session named sync_detail_data_cas */
	%dabt_initiate_cas_session(cas_session_ref=sync_detail_data_cas);
	
	/*----------------------------------------------------------------------------------------*/
	/* Extract model,version,scoring date and purpose to populate in feed and measure tables  */
	/*----------------------------------------------------------------------------------------*/
	%let m_time=;
	%let m_model_nm=;
	%let m_report_specification_nm=;
	%let m_purpose=;
	%let m_soa=;
	
	proc sql noprint;
		select period_last_dttm into :m_time 
		from &lib_apdm..time_dim where time_sk=&m_scoring_as_of_time_sk;
		
		select model_short_nm into :m_model_nm 
		from &lib_apdm..model_master where model_sk=&m_model_sk;
		
		select report_specification_nm into :m_report_specification_nm
		from &lib_apdm..mm_report_specification where report_specification_sk=&m_report_specification_sk;

		select purpose_short_nm into :m_purpose from
		&lib_apdm..model_master mm
		inner join &lib_apdm..project_master pm
		on mm.project_sk=pm.project_sk
		and mm.model_sk=&m_model_sk
		inner join &lib_apdm..purpose_master prps_mstr
		on pm.purpose_sk=prps_mstr.purpose_sk;
		
		select lvl.level_short_nm into :m_soa
		from &lib_apdm..model_master mm
		inner join &lib_apdm..level_master lvl
		on mm.level_sk=lvl.level_sk
		and model_sk=&m_model_sk;

	quit;
	
	%dabt_err_chk(type=SQL);
	
	%let m_time=%sysfunc(inputn(&m_time,datetime25.6));	/* i18NOK:LINE */
	%let m_model_nm=&m_model_nm;
	%let m_report_specification_nm=&m_report_specification_nm;
	%let m_purpose=&m_purpose;
	%let m_soa=&m_soa;
	/*----------------------------------------------------------------------------------------*/
	
	/*----------------------------------------------------------------------------------------*/	
	/* Extract model target type, report category code, report type from the macro variables 
		passed - m_model_sk, m_report_category_sk, report_type_cd */
	/*----------------------------------------------------------------------------------------*/
	%let m_model_target_type_cd=;
	proc sql noprint;
		select model_target_type_cd into :m_model_target_type_cd from &lib_apdm..model_target_type_master
		where model_target_type_sk= (select model_target_type_sk from &lib_apdm..MODEL_MASTER_EXTENSION
							where model_key=&m_model_sk.);
	quit;
	
	%dabt_err_chk(type=SQL);
	
	%let m_model_target_type_cd=&m_model_target_type_cd;
	/*----------------------------------------------------------------------------------------*/
	/*----------------------------------------------------------------------------------------*/
	/* Exit if the macro is called for Continuous Models and MIP report type */
	/*----------------------------------------------------------------------------------------*/
	
	%if "&m_model_target_type_cd"="CONTINUOUS" and &m_report_type_sk eq 2 %then %do;    /* i18NOK:LINE */
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM5.1, noquote));
		%goto exit;
	%end;
	
	/*----------------------------------------------------------------------------------------*/
	/* Extract model_category-Application/Behavioural										  */
	/*----------------------------------------------------------------------------------------*/
	%let m_model_category_cd=;
	proc sql noprint;
		select model_category_cd into :m_model_category_cd from &lib_apdm..mm_model_category_master
		where model_category_sk= (select model_category_sk from &lib_apdm..MODEL_MASTER_EXTENSION
							where model_key=&m_model_sk.);
	quit;
	
	%dabt_err_chk(type=SQL);
	
	%let m_model_category_cd=&m_model_category_cd;
	
	
	/*----------------------------------------------------------------------------------------*/
	/* Check if input feed/fact table for backtest/ongoing exist in memory. If not see if it is 
		physically available as sashdat. If not give a NOTE and come out of macro. */
	/*----------------------------------------------------------------------------------------*/
	%if &m_report_category_sk = 1  %then %do;/*Ongoing*/
		%let caslibrary=&RM_MODELPERF_DATA_LIBREF.;
	%end;
	%else %if &m_report_category_sk = 2 %then %do;/*Backtesting*/
		%let caslibrary=&DABT_BACKTESTING_ABT_LIBREF;
	%end;
	
	%if &m_model_target_type_cd.= BINARY %then %do;
		%if &m_report_type_sk=1 %then %do;/*feed name for MM*/
			%let feed_ds_suffix=BINARY_TGT_FEED;
			%let fact_ds_suffix=BINARY_TGT_FACT;
		%end;
		%else %if &m_report_type_sk=2 %then %do;/*feed name for MIP*/
			%let feed_ds_suffix=ATTR_FEED;/*Current measures of MIP are calculated from this feed*/
		%end;
	%end;
	%else %do;
		%let feed_ds_suffix=CONT_TGT_FEED;
		%let fact_ds_suffix=CONT_TGT_FACT;
	%end;
	
	/*Check fact only for Binary target type and MM*/
	%if &m_model_target_type_cd.= BINARY and &m_report_type_sk=1 %then %do;
		%let FACT_TBL_NM=_&m_model_sk._&m_report_specification_sk._&m_scoring_as_of_time_sk._&fact_ds_suffix.;
		%let feed_tableExist = %eval(%sysfunc(exist(&caslibrary..&FACT_TBL_NM., DATA)));
		%if &feed_tableExist. eq 0 %then %do;
			%dabt_load_table_to_cas(m_in_cas_lib_ref=&caslibrary, m_in_table_nm=&FACT_TBL_NM., m_out_cas_lib_ref=&caslibrary, m_out_table_nm=&FACT_TBL_NM., m_replace_if_exists=N, m_promote_flg=Y);
			/*Check if input fact table got loaded*/
			%let feed_tableExist_now = %eval(%sysfunc(exist(&caslibrary..&FACT_TBL_NM., DATA)));
			%if &feed_tableExist_now. eq 0 %then %do;
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM3.1, noquote,&FACT_TBL_NM.));
				%goto exit;
			%end;
		%end;
	%end;
	
	%let FEED_TBL_NM=_&m_model_sk._&m_report_specification_sk._&m_scoring_as_of_time_sk._&feed_ds_suffix.;
	%let feed_tableExist = %eval(%sysfunc(exist(&caslibrary..&FEED_TBL_NM., DATA)));
	%if &feed_tableExist. eq 0 %then %do;
		%dabt_load_table_to_cas(m_in_cas_lib_ref=&caslibrary, m_in_table_nm=&FEED_TBL_NM., m_out_cas_lib_ref=&caslibrary, m_out_table_nm=&FEED_TBL_NM., m_replace_if_exists=N, m_promote_flg=Y);
		/*Check if output feed table got loaded*/
		%let feed_tableExist_now = %eval(%sysfunc(exist(&caslibrary..&FEED_TBL_NM., DATA)));
		%if &feed_tableExist_now. eq 0 %then %do;
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM1.1, noquote,&FEED_TBL_NM.));
			%goto exit;
		%end;
	%end;
	/*----------------------------------------------------------------------------------------*/
	
	/*----------------------------------------------------------------------------------------*/
	/*Initializing output feed table name for MM report 										*/
	/*----------------------------------------------------------------------------------------*/
	%if &m_report_type_sk eq 1 %then %do;
		%if "&m_model_target_type_cd"="BINARY" %then %do;   /* i18NOK:LINE */
			%let m_feed_table=MM_BINARY_TGT_FEED;
		%end;
		%else %if "&m_model_target_type_cd"="CONTINUOUS" %then %do;   /* i18NOK:LINE */
			%let m_feed_table=MM_CONT_TGT_FEED;
		%end;
		
	%end;
	
	/*----------------------------------------------------------------------------------------*/
	/*Initializing output feed table for MIP report												*/
	/*----------------------------------------------------------------------------------------*/
	%else %if &m_report_type_sk eq 2 %then %do;
		%let m_feed_table=MIP_ATTR_FEED;			
	%end;	
	/*----------------------------------------------------------------------------------------*/
	
	/*----------------------------------------------------------------------------------------*/	
	/* Extract report_category_cd and report_category_desc from APDM.MM_REPORT_CATEGORY_MASTER */
	/*----------------------------------------------------------------------------------------*/
	%let m_report_category_cd=;
	%let m_report_category_nm=;
	
	proc sql noprint;
		select report_category_cd, report_category_desc into 
			:m_report_category_cd,
			:m_report_category_nm from &lib_apdm..MM_REPORT_CATEGORY_MASTER
		where report_category_sk=&m_report_category_sk;
	quit;
	
	%dabt_err_chk(type=SQL);
	
	%let m_report_category_cd=&m_report_category_cd;
	%let m_report_category_nm=%sysfunc(kstrip(&m_report_category_nm));
	/*----------------------------------------------------------------------------------------*/
	
	/*----------------------------------------------------------------------------------------*/	
	/* Extract report_type_cd from APDM.MM_REPORT_TYPE_MASTER*/
	/*----------------------------------------------------------------------------------------*/
	
	%let m_report_type_cd=;
	
	proc sql noprint;
			select report_type_cd into :m_report_type_cd from &lib_apdm..MM_REPORT_TYPE_MASTER
			where report_type_sk = &m_report_type_sk;
	quit;
	
	%dabt_err_chk(type=SQL);
	
	%let m_report_type_cd=&m_report_type_cd;
	/*----------------------------------------------------------------------------------------*/
	
	/*----------------------------------------------------------------------------------------*/
	/* Ensure output feed table is available in memory (caslib: RM_RPTMR) */
	/*----------------------------------------------------------------------------------------*/
	%let etls_tableExist = %eval(%sysfunc(exist(&rm_reporting_mart_libref..&m_feed_table., DATA)));
	%if &etls_tableExist. eq 0 %then %do;
		%dabt_load_table_to_cas(m_in_cas_lib_ref=&rm_reporting_mart_libref, m_in_table_nm=&m_feed_table., m_out_cas_lib_ref=&rm_reporting_mart_libref, m_out_table_nm=&m_feed_table., m_replace_if_exists=N, m_promote_flg=Y);
		/*Check if output feed table got loaded*/
		%let out_feed_exist_now = %eval(%sysfunc(exist(&rm_reporting_mart_libref..&m_feed_table., DATA)));
		%if &out_feed_exist_now. eq 0 %then %do;
			filename feed filesrvc folderpath="&m_cr_rpm_reports_ddl_path" filename= "&m_feed_table..sas" debug=http;   /* i18NOK:LINE */
			%include feed;
			%dabt_promote_table_to_cas(input_caslib_nm =&rm_reporting_mart_libref.,input_table_nm =&m_feed_table.);
		%end;
	%end;
	
	/*Delete data from feed table if it exists for same model,version,report_category and scoring time*/
	%if %kupcase("&m_full_refresh_flag") eq "N" %then %do;    /* i18NOK:LINE */

		proc cas;
			simple.numRows result=r /
			table={
			caslib="&rm_reporting_mart_libref",
			name="&m_feed_table",
			where="model_rk=&m_model_sk and report_specification_sk=&m_report_specification_sk and score_time_sk= &m_scoring_as_of_time_sk and report_category_sk= &m_report_category_sk"   /* i18NOK:LINE */
			};
			run;
			print "rows :" r["numrows"];   /* i18NOK:LINE */
			run;
			if (r["numrows"]) then do;   /* i18NOK:LINE */
			table.deleteRows /
			table={
			caslib="&rm_reporting_mart_libref",
			name="&m_feed_table",
			where="model_rk=&m_model_sk and report_specification_sk=&m_report_specification_sk and score_time_sk= &m_scoring_as_of_time_sk and report_category_sk= &m_report_category_sk"  /* i18NOK:LINE */
			}
			;
			end;
			run;
		quit;
		
		
	%end;
		/*%else %do;/*when full refresh=Y drop and create table*/

			/*%dabt_drop_table(m_table_nm=&rm_reporting_mart_libref..&m_cas_table.,m_cas_flg=Y);
			
		%end;	*/
	/*----------------------------------------------------------------------------------------*/
	
	/*----------------------------------------------------------------------------------------*/
	/* Generate list of all the measure that belong to report type and target type */
	/*----------------------------------------------------------------------------------------*/
	%if "&m_model_target_type_cd"="BINARY" and "&m_report_type_cd"="MIP" %then %do;			/* i18NOK:LINE */
		%let m_measure_cd_list=evntstbind#varstbindx#scrshftind#evntshftin#wtgscr#infvalsts;
		/*attribevnt#attribprps#evntshftin#evntstbind#ginindx#infvalsts#ks#perchsq#scrshftind#varstbindx#wtgscr;*/
	%end;
	
	%else %if "&m_model_target_type_cd"="BINARY" and "&m_report_type_cd"="MM" %then %do;		/* i18NOK:LINE */
		%let m_measure_cd_list=sysstbindx#ks#ar#auc#smdcrp#is#kentabp#accura#er#ds#bntest#gnstbllftc#mse#prec#sens#spec#valscr#bsnerrt#nmltst#
		grfrnfrevn#confint#trflgttst#scrdst#scrods#kl#pietra#obsvsest;
		/*accura#ar#auc#bntest#brier#bsnerrt#cier#confint#ds#er#gnstbllftc#grfrnfrevn#hlp#is#kentabp#kl#ks#mse#nmltst#obsvsest#ph#pietra#prec#
		scrdst#scrods#sens#smdcrp#spec#spiegel#sysstbindx#trflgttst#valscr;*/
	%end;
	
	%else %if "&m_model_target_type_cd"="CONTINUOUS" and "&m_report_type_cd"="MM" %then %do;		/* i18NOK:LINE */
		%let m_measure_cd_list=sysstbindx#mape#chsqp#corr#mse#confint;
		/*chsqp#confint#corr#mad#mape#mse#sysstbindx;*/
	%end;
	/*----------------------------------------------------------------------------------------*/	
	
	/* Count the number of measures for given report type and target type */
	%let m_measure_cd_cnt = %eval(%sysfunc(countc(%quote(&m_measure_cd_list),"#"))+1);   /* i18NOK:LINE */
	
	/**************************************************************************************************************
	Check if measure detail table exists in memory if not load from sashdat. If not create with the DDL and promote it
	***************************************************************************************************************/
	
	%do msr_tbl_chk = 1 %to &m_measure_cd_cnt;/*Start loop for every measure_cd.*/
		
			/***********************************************
			Extract individual measure_sk 
			***********************************************/
			%let m_measure_cd = %kupcase(%scan(%quote(&m_measure_cd_list),&msr_tbl_chk,%str(#)));   /* i18NOK:LINE */
			
			/***********************************************
			Name of measure table from &m_measure_cd
			************************************************/
			%if &m_measure_cd=MSE and "&m_model_target_type_cd"="BINARY" %then %do;    /* i18NOK:LINE */
			%let m_msr_table=&m_report_type_cd._&m_measure_cd._BINARY_DETAILS;
			%end;
			%else %if &m_measure_cd=MSE and "&m_model_target_type_cd"="CONTINUOUS" %then %do;   /* i18NOK:LINE */
				%let m_msr_table=&m_report_type_cd._&m_measure_cd._CONT_DETAILS;
			%end;
			%else %if &m_measure_cd=CONFINT and "&m_model_target_type_cd"="CONTINUOUS" %then %do;    /* i18NOK:LINE */
				%let m_msr_table=&m_report_type_cd._&m_measure_cd._CONT_DETAILS;
			%end;
			%else %if &m_measure_cd=CONFINT and "&m_model_target_type_cd"="BINARY" %then %do;   /* i18NOK:LINE */
				%let m_msr_table=&m_report_type_cd._&m_measure_cd._BINARY_DETAILS;
			%end;
			%else %do;
				%let m_msr_table=&m_report_type_cd._&m_measure_cd._DETAILS;
			%end;
			
			/**********************************************************************************************************
			Check if measure table exist in memory, if not load from sashdat. If not create with the DDL and promote it
			***********************************************************************************************************/
			
			%let etls_tableExist = %eval(%sysfunc(exist(&rm_reporting_mart_libref..&m_msr_table., DATA)));
			%if &etls_tableExist. eq 0 %then %do;
				%dabt_load_table_to_cas(m_in_cas_lib_ref=&rm_reporting_mart_libref, m_in_table_nm=&m_msr_table., m_out_cas_lib_ref=&rm_reporting_mart_libref, m_out_table_nm=&m_msr_table., m_replace_if_exists=N, m_promote_flg=Y);
				/*Check if measure detail table got loaded*/
				%let msr_dtl_exist_now = %eval(%sysfunc(exist(&rm_reporting_mart_libref..&m_msr_table., DATA)));
				%if &msr_dtl_exist_now. eq 0 %then %do;
					filename msr filesrvc folderpath="&m_cr_rpm_reports_ddl_path" filename= "&m_msr_table..sas" debug=http; /* i18NOK:LINE */
					%include msr;
					%dabt_promote_table_to_cas(input_caslib_nm =&rm_reporting_mart_libref.,input_table_nm =&m_msr_table.);
				%end;
				/*%let m_full_refresh_flag=Y;*/
			%end;
	%end;
	/*----------------------------------------------------------------------------------------------------------*/
		
	%do msr = 1 %to &m_measure_cd_cnt;/*Start loop for every measure_cd.*/
	
		/*----------------------------------------------------------------------------------------*/
		/* Extract individual measure_sk */
		/*----------------------------------------------------------------------------------------*/
		%let m_measure_cd = %kupcase(%scan(%quote(&m_measure_cd_list),&msr,%str(#)));	/* i18NOK:LINE */
		
		/*----------------------------------------------------------------------------------------*/
		/* Name of measure table from &m_measure_cd */
		/*----------------------------------------------------------------------------------------*/
		%if &m_measure_cd=MSE and "&m_model_target_type_cd"="BINARY" %then %do;	/* i18NOK:LINE */
			%let m_msr_table=&m_report_type_cd._&m_measure_cd._BINARY_DETAILS;
		%end;
		%else %if &m_measure_cd=MSE and "&m_model_target_type_cd"="CONTINUOUS" %then %do;	/* i18NOK:LINE */
			%let m_msr_table=&m_report_type_cd._&m_measure_cd._CONT_DETAILS;
		%end;
		%else %if &m_measure_cd=CONFINT and "&m_model_target_type_cd"="CONTINUOUS" %then %do;	/* i18NOK:LINE */
			%let m_msr_table=&m_report_type_cd._&m_measure_cd._CONT_DETAILS;
		%end;
		%else %if &m_measure_cd=CONFINT and "&m_model_target_type_cd"="BINARY" %then %do;	/* i18NOK:LINE */
			%let m_msr_table=&m_report_type_cd._&m_measure_cd._BINARY_DETAILS;
		%end;
		%else %do;
			%let m_msr_table=&m_report_type_cd._&m_measure_cd._DETAILS;
		%end;
			
		
		%if &m_report_type_sk eq 1 %then %do;/* start call csbmva_ui_mm_msr_report macro for MM */
			
			%let m_range_scheme_type_sk_lst=;
			%let m_range_scheme_type_cd_lst=;
			%let m_bin_type_list=;
			%let m_range_scheme_cnt=;
			
			proc sql noprint;
			select range_scheme_type_sk,range_scheme_type_cd,bin_analysis_scheme_short_nm, count(*)                                        /* i18nOK:Line */
					into :m_range_scheme_type_sk_lst separated by ',' ,:m_range_scheme_type_cd_lst separated by ',' 			/* i18NOK:LINE */
			,:m_bin_type_list separated by ',' , :m_range_scheme_cnt  /* i18nOK:Line */
			from &lib_apdm..REPORT_SPEC_X_BIN_SCHEME vrsn_x_bin
			inner join &lib_apdm..bin_analysis_scheme_defn bin_schm
			on vrsn_x_bin.bin_analysis_scheme_sk=bin_schm.bin_analysis_scheme_sk
			inner join &lib_apdm..range_scheme_type_master rng_schm_typ_mstr
			on bin_schm.bin_analysis_scheme_cd=rng_schm_typ_mstr.source_bin_scheme_cd
			where report_specification_sk =&m_report_specification_sk and active_flg = 'Y';   /* i18NOK:LINE */
			quit;
			
			%dabt_err_chk(type=SQL);

			%let m_range_scheme_cnt=&m_range_scheme_cnt;

			
			%do rpt_subtype = 1 %to &m_range_scheme_cnt;/*Start loop for every report_subtype_cd. if report type is MM*/
		
				%let m_range_subtype_sk = %scan(%quote(&m_range_scheme_type_sk_lst),&rpt_subtype,%str(,)); /* i18NOK:LINE */
				%let m_range_subtype_cd = %scan(%quote(&m_range_scheme_type_cd_lst),&rpt_subtype,%str(,)); /* i18NOK:LINE */
				%let m_bin_type=%scan(%quote(&m_bin_type_list),&rpt_subtype,%str(,)); /* i18NOK:LINE */
				
				
				%if %kupcase("&m_full_refresh_flag") eq "N" %then %do;	/* i18NOK:LINE */
					/*Delete data from feed table if it exists for same model,version,report_category,range scheme type sk and scoring time*/
					/* I18NOK:BEGIN */
					proc cas;
					simple.numRows result=r /
					table={
					caslib="&rm_reporting_mart_libref",
					name="&m_msr_table",
					where="model_sk=&m_model_sk and report_specification_sk=&m_report_specification_sk and scoring_as_of_time_sk= &m_scoring_as_of_time_sk 
							and report_category_sk= &m_report_category_sk and range_scheme_type_sk= &m_range_subtype_sk"	
					/* I18NOK:END */
					};
					run;
					print "rows :" r["numrows"];	/* i18NOK:LINE */
					run;
					if (r["numrows"]) then do;	/* i18NOK:LINE */
					table.deleteRows /
					/* I18NOK:BEGIN */
					table={
					caslib="&rm_reporting_mart_libref",
					name="&m_msr_table",
					where="model_sk=&m_model_sk and report_specification_sk=&m_report_specification_sk and scoring_as_of_time_sk= &m_scoring_as_of_time_sk 
							and report_category_sk= &m_report_category_sk and range_scheme_type_sk= &m_range_subtype_sk"	
					}/* I18NOK:END */
					;
					end;
					run;
					quit;
				 
					
				%end;
				
				%let ERROR_FLG=N;
			
				%csbmva_ui_mm_msr_report ( 	MODEL_KEY = &m_model_sk, 
											REPORT_CATEGORY_CD = &m_report_category_cd,  
											REPORT_TYPE_CD = &m_report_type_cd, 
											REPORT_DATA_GROUP_CD = BIN , 
											REPORT_SUBTYPE_CD = &m_range_subtype_cd, 
											TIME_PERIOD_GROUP_SK = &m_scoring_as_of_time_sk.,
											MEASURE_CD = &m_measure_cd,  
											FILTER_KEY  = ,
											SOURCE_LIB = ,
											IS_REPOOL = false,
											REPORT_SPECIFICATION_SK= &m_report_specification_sk.
										);
				/*----------------------------------------------------------------------------*/
				/*Drop column NO_OF_RECORDS_ACTUAL_ALL if model category is application		   */
				/*-----------------------------------------------------------------------------*/
				
				%if &ERROR_FLG=N %then %do;
					%if &m_model_category_cd=APP_MODEL %then %do;
							%let dsid =%sysfunc(open(REPORT_DATA_DETAIL));
							%let chk_column=%sysfunc(varnum(&dsid,NO_OF_RECORDS_ACTUAL_ALL));
							%let rc=%sysfunc(close(&dsid));
							
							%if &chk_column > 0 %then %do;
						
								data REPORT_DATA_DETAIL(drop=NO_OF_RECORDS_ACTUAL_ALL);
								set REPORT_DATA_DETAIL;
								run;
								
							%end;
					%end;
				
					data &rm_reporting_mart_libref..&m_msr_table.(append=yes);
					set work.REPORT_DATA_DETAIL;
					length MODEL $480 VERSION $500 REPORT_CATEGORY_CD $2400 PURPOSE $500 SUBJECT_OF_ANALYSIS $500 BIN_TYPE $480;
					format SCORING_DATE DATETIME25.6;	/* i18NOK:LINE */
					MODEL="&m_model_nm";
					VERSION="&m_report_specification_nm";
					SCORING_DATE= &m_time;
					REPORT_CATEGORY_CD= "&m_report_category_nm" ;
					PURPOSE="&m_purpose";
					SUBJECT_OF_ANALYSIS="&m_soa";
					BIN_TYPE="&m_bin_type";
					RANGE_SCHEME_TYPE_SK=&m_range_subtype_sk;
					MODEL_SK=&m_model_sk;
					REPORT_SPECIFICATION_SK=&m_report_specification_sk;
					SCORING_AS_OF_TIME_SK= &m_scoring_as_of_time_sk;
					REPORT_CATEGORY_SK= &m_report_category_sk;
					run;
					
					%dabt_err_chk(type=DATA);

					/* Save the CAS table */
					%dabt_save_cas_table(m_in_cas_lib_ref=&rm_reporting_mart_libref, m_in_cas_table_nm=&m_msr_table);
				%end;/*End for ERROR_CODE check*/
				
				/*Append data to Feed table only for the last measure as it is same for all*/
				%if &msr eq &m_measure_cd_cnt %then %do;
				
					data &rm_reporting_mart_libref..&m_feed_table.(append=yes);
					set work.FEED_DATA_DETAIL;
					length MODEL $480 VERSION $500 REPORT_CATEGORY_CD $2400 BIN_TYPE $480;
					format SCORING_DATE DATETIME25.6;	/* i18NOK:LINE */
					MODEL="&m_model_nm";
					VERSION="&m_report_specification_nm";
					SCORING_DATE= &m_time;
					REPORT_CATEGORY_CD= "&m_report_category_nm" ;
					BIN_TYPE="&m_bin_type";
					REPORT_SPECIFICATION_SK=&m_report_specification_sk;
					REPORT_CATEGORY_SK= &m_report_category_sk;
					run;
					
					%dabt_err_chk(type=DATA);

					/* Save the CAS table */
					%dabt_save_cas_table(m_in_cas_lib_ref=&rm_reporting_mart_libref, m_in_cas_table_nm=&m_feed_table);
				
				%end;
									
			%end;/* end for report subtype loop */
		
		%end;/*End for MM report*/
		
		%else %if &m_report_type_sk eq 2 %then %do;/* start loop for MIP */
			
			%if %kupcase("&m_full_refresh_flag") eq "N" %then %do;	/* i18NOK:LINE */
					/*Delete data from feed table if it exists for same model,version,report_category and scoring time*/
					/* I18NOK:BEGIN */
					proc cas;
					simple.numRows result=r /
					table={
					caslib="&rm_reporting_mart_libref",
					name="&m_msr_table",
					where="model_sk=&m_model_sk and report_specification_sk=&m_report_specification_sk and scoring_as_of_time_sk= &m_scoring_as_of_time_sk 
						and report_category_sk= &m_report_category_sk"	
					
					}; /* I18NOK:END */
					run;
					print "rows :" r["numrows"];	/* i18NOK:LINE */
					run;
					if (r["numrows"]) then do;	/* i18NOK:LINE */
					table.deleteRows /
					/* I18NOK:BEGIN */
					table={
					caslib="&rm_reporting_mart_libref",
					name="&m_msr_table",
					where="model_sk=&m_model_sk and report_specification_sk=&m_report_specification_sk and scoring_as_of_time_sk= &m_scoring_as_of_time_sk   
							and report_category_sk= &m_report_category_sk"    
					} /* I18NOK:END */
					;
					end;
					run;
					quit;
				 
					
			%end;
				
			%let m_variable_key_list=;
			
			proc sql noprint;
			select distinct scrcrd_bin_grp_variable_sk into :m_variable_key_list separated by '#'  /* i18NOK:LINE */
			from &lib_apdm..SCORECARD_BIN_GROUP where model_sk=&m_model_sk;
			quit;
			
			%dabt_err_chk(type=SQL);
			
			%let ERROR_FLG=N;
			%csbmva_ui_mip_msr_report(MODEL_KEY=&m_model_sk,
									REPORT_CATEGORY_CD=&m_report_category_cd,
									REPORT_TYPE_CD=&m_report_type_cd,
									REPORT_DATA_GROUP_CD=BIN ,
									TIME_PERIOD_GROUP_SK=&m_scoring_as_of_time_sk.,
									MEASURE_CD=&m_measure_cd,
									VARIABLE_KEY_LST=&m_variable_key_list, 
									REPORT_SPECIFICATION_SK=&m_report_specification_sk.);
											
				
											
			/***************************************************************************
			Loading mm detail measure data for specific measure from work to CAS table
			****************************************************************************/
			/*-------------------------------------------*/
			/*Update the attribute seq no column 		 */
			/*-------------------------------------------*/
			%if &ERROR_FLG=N %then %do;
			
				proc sort data=REPORT_DATA_DETAIL;
				by variable_sk;
				run;
				
				data REPORT_DATA_DETAIL_UPD(drop=N);
				set REPORT_DATA_DETAIL;
				by variable_sk;
				if first.variable_sk then N = 1;
				else N+1;
				attribute_seq_no=N;
				run;
				
				%dabt_err_chk(type=DATA);
				
				/* Append the created work table to existing Cas table with Append=Yes option , also add additional columns like model_sk,report_specification_sk,scoring_as_of_time_sk,report_category_sk */

				data &rm_reporting_mart_libref..&m_msr_table.(append=yes);
				set REPORT_DATA_DETAIL_UPD;
				length MODEL $480 VERSION $500 REPORT_CATEGORY_CD $2400 PURPOSE $500 SUBJECT_OF_ANALYSIS $500 ;
				format SCORING_DATE DATETIME25.6;	/* i18NOK:LINE */
				MODEL="&m_model_nm";
				VERSION="&m_report_specification_nm";
				SCORING_DATE= &m_time;
				REPORT_CATEGORY_CD= "&m_report_category_nm" ;
				PURPOSE="&m_purpose";
				SUBJECT_OF_ANALYSIS="&m_soa";
				MODEL_SK=&m_model_sk;
				REPORT_SPECIFICATION_SK=&m_report_specification_sk;
				SCORING_AS_OF_TIME_SK= &m_scoring_as_of_time_sk;
				REPORT_CATEGORY_SK= &m_report_category_sk;
				run;
				
				%dabt_err_chk(type=DATA);

				/* Save the CAS table */
				%dabt_save_cas_table(m_in_cas_lib_ref=&rm_reporting_mart_libref, m_in_cas_table_nm=&m_msr_table);
			%end; /*End for ERROR CODE*/
			
				/*Append data to Feed table only for the last measure as it is same for all*/
			%if &msr eq &m_measure_cd_cnt %then %do;
			
				/*-------------------------------------------*/
				/*Update the attribute seq no column 		 */
				/*-------------------------------------------*/
				proc sort data=FEED_DATA_DETAIL;
				by variable_sk;
				run;
				
				data FEED_DATA_DETAIL_UPD(drop=N);
				set FEED_DATA_DETAIL;
				by variable_sk;
				if first.variable_sk then N = 1;
				else N+1;
				attribute_seq_no=N;
				run;
				
				%dabt_err_chk(type=DATA);
			
				data &rm_reporting_mart_libref..&m_feed_table.(append=yes);
				set FEED_DATA_DETAIL_UPD;
				length MODEL $480 VERSION $500 REPORT_CATEGORY_CD $2400 ;
				format SCORING_DATE DATETIME25.6;	/* i18NOK:LINE */
				MODEL="&m_model_nm";
				VERSION="&m_report_specification_nm";
				SCORING_DATE= &m_time;
				REPORT_CATEGORY_CD= "&m_report_category_nm" ;
				REPORT_SPECIFICATION_SK=&m_report_specification_sk;
				REPORT_CATEGORY_SK= &m_report_category_sk;
				run;
				
				%dabt_err_chk(type=DATA);
				
				/* Save the CAS table */
				%dabt_save_cas_table(m_in_cas_lib_ref=&rm_reporting_mart_libref, m_in_cas_table_nm=&m_feed_table);
			%end;
		%end; /*End loop for MIP*/ 
	
	%end;/*End loop for every measure*/
	
	/* Terminate the CAS session named sync_detail_data_cas */
	
	%dabt_terminate_cas_session(cas_session_ref=sync_detail_data_cas);
	
	%let &m_status_cd = &job_rc.;
	
	%exit:
	
%mend csbmva_sync_detail_data_in_cas;	