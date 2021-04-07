/************************************************************************************************
       Module:  create_event_data

     Function:  This macro is a placeholder for creating a dataset which contains the event
				date information for each key column. This will be used for calculating the
				abt varaibles as of event date for each level (that is for each customer
				or account). This is applicable for those levels for which event is made applicable.

         Date: 
          SAS:  9.2

    Called-by:	1. bankfdn_call_files_creation macro

        Calls:  None

        Logic:  1. Create a dataset, &out_ds_nm, containing the key columns and event date 
					column, by applying the required logic. abt_bld_dt is provided for
					picking up the data valid as of this date.
					Create libname statements for the input dataset used.
				
   Parameters: INPUT:
				1. abt_bld_dttm	: Date at which abt is set to build
				2. abt_bld_dttm_cas : CAS compliant timestamp format
				3. event_dt_column	: Event Date Column Name.
				4. event_key_column	: 
						Event Dataset Key Column Name. In case of multiple keys,it will be space separated
						For example Scenario: For Account Level ABT, threre are variables at both Account and Customer level variables, Then
											  for this ABT when this event macro is called for processing of variables at Account level, event_key_col will be ACCOUNT_RK
											  for this ABT when this event macro is called for processing of variables at Customer level, event_key_col will be CUSTOMER_RK

				5. abt_lvl_key_column	: 
						ABT Level Key Column Name. In case of multiple keys,it will be space separated.
						This column is passed for information purpose, in case there is separate event dt identification processing required for consolidated variables.
						For example Scenario: For Account Level ABT, threre are variables at both Account and Customer level variables, Then
											  for this ABT when this event macro is called for processing of variables at Account level, abt_lvl_key_column will be ACCOUNT_RK
											  for this ABT when this event macro is called for processing of variables at Customer level, abt_lvl_key_column will be ACCOUNT_RK

				6. event_key_col_cons_lvl_flg: 
						In case of event data set getting called for cons level, It will be Y. Else it will be null.
						This column is passed for information purpose, in case there is separate event dt identification processing required for consolidated variables.
						For example Scenario: For Account Level ABT, threre are variables at both Account and Customer level variables, Then
											  for this ABT when this event macro is called for processing of variables at Account level, event_key_col_cons_lvl_flg will be N
											  for this ABT when this event macro is called for processing of variables at ACustomer level, event_key_col_cons_lvl_flg will be Y

				7. abt_and_cons_lvl_mapping_ds_nm: 
						Applicable only in case of event data set getting called for cons level. 
						It is that name of the dataset which contains mapping of abt level subject keys with cons var level subject keys.
						Incase either imlicit or explict subset is applied, this mapping table contains only those abt level subjects which meets these subset criterion.
						This column is passed for information purpose, in case there is separate event dt identification processing required for consolidated variables.

				8. out_ds_nm	: 
					Two level dataset name of out put expected from thsi macro. This dataset should contain the event_key_column and the event_dt_column. With only one record for each value in event_key_column.


***********Code of this macro is not supposed to access APDM for any Information.*******************

************************************************************************************************/
%macro bankfdn_create_event_data(	abt_sk=,
							abt_purpose_cd=,
							abt_bld_dttm=,
							abt_bld_dttm_cas=,
							event_dt_column=,	

							event_key_column=,	
							event_level_cd=,	

							abt_lvl_key_column=,
							abt_level_cd=,
							abt_type_cd=,

							event_key_col_cons_lvl_flg=,
							abt_and_cons_lvl_mapping_ds_nm=,
							subset_list_ds_nm=,
							out_ds_nm=);

%let abt_sk = &abt_sk;
%let abt_purpose_cd = &abt_purpose_cd;
%let abt_bld_dttm = &abt_bld_dttm;
%let abt_bld_dttm_cas = &abt_bld_dttm_cas;
%let event_dt_column = &event_dt_column;
%let key_col_comma = &event_key_column;
%let event_level_cd = &event_level_cd;
%let abt_lvl_key_column = &abt_lvl_key_column;
%let abt_level_cd = &abt_level_cd;
%let event_key_col_cons_lvl_flg = &event_key_col_cons_lvl_flg;
%let abt_and_cons_lvl_mapping_ds_nm = &abt_and_cons_lvl_mapping_ds_nm;
%let out_ds_nm = &out_ds_nm;

%let m_dabt_output_table_libref = %sysfunc(kscan("&out_ds_nm", 1, "."));
%let m_dabt_output_table_nm = %sysfunc(kscan("&out_ds_nm", 2, "."));

/****/

/* Migrated deployed code from last release might not have abt_type_cd column in stg.abt_info*/
%let cnt_abt_type=0;
proc sql noprint;
select count(*) into:cnt_abt_type from dictionary.columns where kupcase(libname)=%upcase("&lib_stg") and kupcase(memname)="ABT_INFO" and kupcase(name)='ABT_TYPE_CD'; /* i18NOK:LINE */
quit;
%let cnt_abt_type=&cnt_abt_type;

%let m_dabt_abt_type_cd = ;   

%if &cnt_abt_type gt 0 %then %do;   
     proc sql noprint;
	  select abt_type_cd into :m_dabt_abt_type_cd
	  from &lib_stg..abt_info;
     quit;          
     %let m_dabt_abt_type_cd = &m_dabt_abt_type_cd;
%end;
  

/****/

%if &fm_grain = 1 %then %do;
  %let m_src_data_time_grain_align = END;
    %let m_time_freq_cd = MONTH;
%end;
%else %if &fm_grain = 3 %then %do;
   	%let m_src_data_time_grain_align = SAME;
   	%let m_time_freq_cd = DAY;
%end;

%let m_build_dt = %sysfunc(datepart(&abt_bld_dttm.));
%let shift_period = %eval(-&DEFAULT_CAPTURE_PERIOD+1);
%let shifted_default_dt = %sysfunc(intnx(&m_time_freq_cd,&m_build_dt,&shift_period,Begin));

%let m_dabt_build_dt = %sysfunc(datepart(&abt_bld_dttm.));

	%let default_capture_period_start_dt = .;
	%let default_capture_period_end_dt = .;
 	
	%if "&m_dabt_abt_type_cd" = "MDL" %then %do;  /* i18NOK:LINE */ /* m_dabt_abt_type_cd = "MDL" means Modeling ABT*/
		/* if abt time frequency code is monthly and if abt build date is 31-Dec-2012 then default_capture_period starts after is 30-Jun-2012 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 31-Dec-2012 then default_capture_period starts after is 30-Jun-2012 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 30-Dec-2012 then default_capture_period starts after is 30-Jun-2012 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 28-Dec-2012 then default_capture_period starts after is 28-Jun-2012 if default capture period is in 6 month */
		%let default_capture_period_start_dt = %sysfunc(intnx(&m_time_freq_cd.,&m_dabt_build_dt,-&DEFAULT_CAPTURE_PERIOD,&m_src_data_time_grain_align.));
	%end;
	%else %do;
		/* if abt time frequency code is monthly and if abt build date is 31-Dec-2012 then default_capture_period_end_dt is 30-Jun-2013 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 31-Dec-2012 then default_capture_period_end_dt is 30-Jun-2013 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 30-Dec-2012 then default_capture_period_end_dt is 30-Jun-2013 if default capture period is in 6 month */				
		/* if abt time frequency code is daily and if abt build date is 28-Dec-2012 then default_capture_period_end_dt is 28-Jun-2013 if default capture period is in 6 month */
		%let default_capture_period_end_dt = %sysfunc(intnx(&m_time_freq_cd.,&m_dabt_build_dt,&CSB_DEF_CPTR_PERIOD_POST_LGD_SCR,&m_src_data_time_grain_align.));
	%end;	

	
	%put default_capture_period_start_dt = %SYSFUNC(PUTN(&default_capture_period_start_dt., NLDATE.));
	%put default_capture_period_end_dt = %SYSFUNC(PUTN(&default_capture_period_end_dt., NLDATE.));


/*As of now, assumption above is that we don't score or consider defaulted accounts in scoring, back testing ot pooling abt. 
So reference date for all accounts in scoring, back testing ot pooling abt will be abt build date itself.
*/

%if %kupcase(&abt_purpose_cd) eq %kupcase(&pd_purpose_cd) and 
    %kupcase(&abt_level_cd) eq %kupcase(&ind_appl_level_cd) and 
    &csb_bank_config_flg = Y and
    %kupcase(&event_level_cd) eq %kupcase(&ind_appl_level_cd) %then %do;

	proc fedsql &m_fedsql_option.;
		create table &out_ds_nm. as
		select a.application_rk, a.APPLICATION_DT as &event_dt_column 
	    from &M_FM_LIB..APPLICATION_DIM as a, &subset_list_ds_nm as b
	    where a.application_rk = b.application_rk
        and (&abt_bld_dttm_cas. >= VALID_START_DTTM AND &abt_bld_dttm_cas. <=VALID_END_DTTM ) ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&pd_purpose_cd) and 
    %kupcase(&abt_level_cd) eq %kupcase(&corp_appl_level_cd) and 
    &csb_bank_config_flg = Y and
    %kupcase(&event_level_cd) eq %kupcase(&corp_appl_level_cd) %then %do;

	proc fedsql &m_fedsql_option.;
		create table &out_ds_nm. as
		select a.application_rk, a.APPLICATION_DT as &event_dt_column 
	    from &M_FM_LIB..APPLICATION_DIM as a, &subset_list_ds_nm as b
	    where a.application_rk = b.application_rk
        and (&abt_bld_dttm_cas. >= VALID_START_DTTM AND &abt_bld_dttm_cas. <=VALID_END_DTTM ) ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&pd_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&ind_appl_level_cd) and 
		&csb_bank_config_flg = Y and
          %kupcase(&event_level_cd) eq %kupcase(&ind_cust_level_cd)  %then  %do;

    proc fedsql &m_fedsql_option.;
		create table &out_ds_nm. as
		select a.customer_rk, max(a.APPLICATION_DT) as &event_dt_column 
	   	from &M_FM_LIB..APPLICATION_DIM as a, &abt_and_cons_lvl_mapping_ds_nm as ind_appl_x_ind_cust
	    where ind_appl_x_ind_cust.application_rk = a.application_rk
		and (&abt_bld_dttm_cas. >= VALID_START_DTTM AND &abt_bld_dttm_cas. <=VALID_END_DTTM )
		group by a.customer_rk;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&pd_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&corp_appl_level_cd) and 
		  &csb_bank_config_flg = Y and
		  %kupcase(&event_level_cd) eq %kupcase(&corp_cust_level_cd)  %then %do;

	proc fedsql &m_fedsql_option.;
		create table &out_ds_nm. as
		select a.customer_rk, max(a.APPLICATION_DT)  as &event_dt_column 
	   	from &M_FM_LIB..APPLICATION_DIM as a, &abt_and_cons_lvl_mapping_ds_nm as corp_appl_x_corp_cust
	    where corp_appl_x_corp_cust.application_rk = a.application_rk
        and (&abt_bld_dttm_cas. >= VALID_START_DTTM AND &abt_bld_dttm_cas. <=VALID_END_DTTM )
		group by a.customer_rk;
	quit;

%end;

/* Account level LGD ABT */

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&lgd_purpose_cd) and 
    %kupcase(&abt_level_cd) eq %kupcase(&ind_acct_level_cd) and 
    &csb_bank_config_flg = Y and
    %kupcase(&event_level_cd) eq %kupcase(&ind_acct_level_cd) %then %do;

	/*
	Significance of default date: 
		? If a given account has default date which meets conditions specified below then that date becomes the reference date for calculation of LGD ABT variables for that account. Else ABT build date will be the reference date for variable calculations.
			* Input variables will be calculated going back from this date.
			* Outcome variables (LGD, Recovery) will be calculated in recovery period starting from this date

		It is the default event meeting following criterion for a given account or credit facility. 

		? Default event associated with account or credit facility

		? Default event having default date in default capture period 
			* For Modeling ABT: Default capture period is before abt build date. It is defined application level parameter called as DEFAULT_CAPTURE_PERIOD.

			* For Back Testing, Pooling, Actual Calculation ABT: Default capture period is after abt scoring build date. It is defined by application level parameter called CSB_DEF_CPTR_PERIOD_POST_LGD_SCR
				* Note: During scoring, back testing and pooling of LGD model, we have implicit filter which ensure that we score only non-default accounts. We assume that even though user can change implicit filter, modified filter should still ensure that we don't score already defaulted accounts for LGD prediction purpose. Because then it is more of Recovery prediction model. It is not supported feature in CSB by default. 
				
		? Default event which has default status indicated by &DEFAULT_STATUS_CD application level parameter at some point of time
		  Reason/significance of putting this check:			
			* We want to consider only these default status as default for LGD calculation process. Default event with other default status won't be considered as default for LGD process.
			* DEFAULT_STATUS_CD parameter should have one single value.
 	*/

	 /*
	Incase account has multiple default events meeting above criterion then latest default event (then one with maximum default date) is to be considered for LGD calculation. Hence EAD and recovery corresponding to only that default event will be considered.
	It might be the case that account got cured in default capture period itself and hence not default as of abt build date. In that case too, it is treated as default because there was some period in which it was under recovery and got fully recovered and cured as of abt build date with loss as zero
	*/
	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		default_event_dim.account_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &subset_list_ds_nm as subset_rk_list
 	where 
		default_event_dim.account_rk = subset_rk_list.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/*	HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt.			
		%end;
		%else %do;
		/*	HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/			
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 			
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
		
	group by 
		default_event_dim.account_rk
	   ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&lgd_purpose_cd) and 
    %kupcase(&abt_level_cd) eq %kupcase(&corp_acct_level_cd) and 
    &csb_bank_config_flg = Y and
    %kupcase(&event_level_cd) eq %kupcase(&corp_acct_level_cd) %then %do;

	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		default_event_dim.account_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &subset_list_ds_nm as subset_rk_list
 	where 
		default_event_dim.account_rk = subset_rk_list.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/* 	HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/			
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 			
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/* 	HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/			
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 			
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
		
	group by 
		default_event_dim.account_rk
	   ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&lgd_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&corp_acct_level_cd) and 
		  &csb_bank_config_flg = Y and
		  %kupcase(&event_level_cd) eq %kupcase(&facility_level_cd)  %then %do;

	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		acct_x_facility.credit_facility_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &abt_and_cons_lvl_mapping_ds_nm as acct_x_facility
 	where 
		default_event_dim.account_rk = acct_x_facility.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/* 	HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/					
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 			
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/*  	HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
	group by 
		acct_x_facility.credit_facility_rk
	   ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&lgd_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&ind_acct_level_cd) and 
		  &csb_bank_config_flg = Y and
          %kupcase(&event_level_cd) eq %kupcase(&ind_cust_level_cd)  %then  %do;

	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		acct_x_cust.customer_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &abt_and_cons_lvl_mapping_ds_nm as acct_x_cust
 	where 
		default_event_dim.account_rk = acct_x_cust.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
			/* 	HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
			*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
			/* 	HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
			*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
	group by 
		acct_x_cust.customer_rk
	   ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&lgd_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&corp_acct_level_cd) and 
		  &csb_bank_config_flg = Y and
		  %kupcase(&event_level_cd) eq %kupcase(&corp_cust_level_cd)  %then %do;

	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		acct_x_cust.customer_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &abt_and_cons_lvl_mapping_ds_nm as acct_x_cust
 	where 
		default_event_dim.account_rk = acct_x_cust.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/* 	HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/* HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
	group by 
		acct_x_cust.customer_rk
	   ;
	quit;

%end;

/* Credit Facility level LGD */
 	 /* Start of identify default date */
 	/*
		* Incase credit facility has multiple default events meeting above criterion then latest default event (then one with maximum default date) is to be considered for LGD calculation. Hence EAD and recovery corresponding to only those default event will be considered.
				* This is applicable for modeling ABT whose default capture period is before modeling abt build date.
				* This is applicable for scoring ABT whose default capture period is after scoring abt build date.
				* It means all accounts associated with credit facility might have defaulted multiple times in same rhythm/wave.
				
		* Assumption: 
			* As we are supporting LGD processing for credit facility having agreements such that if any one account defaults then all other accounts linked that credit facility is considered as default and EAD of credit facility will be sum of EAD of all those accounts. 
			* All accounts associated with credit facility are expected to have same default date (and hence same recovery period end date) and status indicated by &DEFAULT_STATUS_CD .
			* Our current application don't support scenario that one account of credit facility has defaulted but other is active and does transactions. One need customizations to support that.
			* There will be different default event with each of these accounts

 		? It might be the case that credit facility (hence all its account) got cured in default capture period itself and hence not default as of abt build date. In that case too, it is treated as default because there was some period in which it was under recovery and got fully recovered and cured as of abt build date with loss as zero.

	*/

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&lgd_purpose_cd) and 
    	  %kupcase(&abt_level_cd) eq %kupcase(&facility_level_cd) and 
    	  &csb_bank_config_flg = Y and
    	  %kupcase(&event_level_cd) eq %kupcase(&facility_level_cd) %then %do;

	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		subset_rk_list.credit_facility_rk, max(default_event_dim.default_dt) as &event_dt_column
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &subset_list_ds_nm as subset_rk_list, &M_FM_LIB..account_dim as account_dim
 	where 
		/* two conditions below to get list accounts of associated a given credit facility as of abt build date*/
		subset_rk_list.credit_facility_rk = account_dim.credit_facility_rk and 
		&abt_bld_dttm_cas. >= account_dim.valid_start_dttm and 
		&abt_bld_dttm_cas. <= account_dim.valid_end_dttm and
		
		/* Condition below to get list default event of those accounts*/
		account_dim.account_rk = default_event_dim.account_rk and 
		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/*
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/*
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;

	group by 
		subset_rk_list.credit_facility_rk
	   ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&lgd_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&facility_level_cd) and 
		  &csb_bank_config_flg = Y and
		  %kupcase(&event_level_cd) eq %kupcase(&corp_cust_level_cd)  %then %do;


	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		facility_x_cust.customer_rk, max(default_event_dim.default_dt) as &event_dt_column
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &abt_and_cons_lvl_mapping_ds_nm as facility_x_cust, &M_FM_LIB..account_dim as account_dim 
 	where 
		/* two conditions below to get list accounts of associated a given customer as of abt build date*/
		facility_x_cust.customer_rk = account_dim.primary_customer_rk and 
		&abt_bld_dttm_cas. >= account_dim.valid_start_dttm and 
		&abt_bld_dttm_cas. <= account_dim.valid_end_dttm and
		
		/* Condition below to get list default event of those accounts*/
		account_dim.account_rk = default_event_dim.account_rk and 
		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/* HF FEB 2019
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/* HF FEB 2019
			datepart(default_event_dim.default_dt) > &m_dabt_build_dt. and 
			datepart(default_event_dim.default_dt) <= &default_capture_period_end_dt. 
		*/
		%end;

	group by 
		facility_x_cust.customer_rk
	   ;
	quit;



%end;

/* Account Level CCF */

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&ccf_purpose_cd) and 
          %kupcase(&abt_level_cd) eq %kupcase(&ind_acct_level_cd) and 
          &csb_bank_config_flg = Y and
          %kupcase(&event_level_cd) eq %kupcase(&ind_acct_level_cd) %then %do;
/*
	proc sql noprint;
		create table &out_ds_nm. as
		select a.account_rk, a.DEFAULT_DT as &event_dt_column 
	    from &M_FM_LIB..Account_default_dim as a, &subset_list_ds_nm as b
	    where a.account_rk = b.account_rk
	    and default_status_cd IN (&DEFAULT_STATUS_CD.)
		and (DATEPART(DEFAULT_DT) between &shifted_default_dt and &m_dabt_build_dt)
		and (&abt_bld_dttm. >= VALID_START_DTTM AND &abt_bld_dttm. <=VALID_END_DTTM )
	   ;
	quit;
*/	
	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		default_event_dim.account_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &subset_list_ds_nm as subset_rk_list
	where 
		default_event_dim.account_rk = subset_rk_list.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and

		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/* HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt.
		*/	
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt.		
		%end;
		%else %do;
		/* HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;

	group by 
		default_event_dim.account_rk
		   ;
	quit;

%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&ccf_purpose_cd) and 
          %kupcase(&abt_level_cd) eq %kupcase(&corp_acct_level_cd) and 
          &csb_bank_config_flg = Y and
          %kupcase(&event_level_cd) eq %kupcase(&corp_acct_level_cd) %then %do;
/*
	proc sql noprint;
		create table &out_ds_nm. as
		select a.account_rk,DEFAULT_DT as &event_dt_column 
	    from &M_FM_LIB..Account_default_dim as a, &subset_list_ds_nm as b
	    where a.account_rk = b.account_rk
	    and Default_status_cd IN (&DEFAULT_STATUS_CD.)
		and (datepart(DEFAULT_DT) between &shifted_default_dt and &m_dabt_build_dt)
		and (&abt_bld_dttm. >= VALID_START_DTTM AND &abt_bld_dttm. <=VALID_END_DTTM )
	   ;
	quit;
*/
	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		default_event_dim.account_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &subset_list_ds_nm as subset_rk_list
	where 
		default_event_dim.account_rk = subset_rk_list.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and

		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/*	HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/*	HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;

	group by 
		default_event_dim.account_rk
		   ;
	quit;
%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&ccf_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&corp_acct_level_cd) and 
		  &csb_bank_config_flg = Y and
		  %kupcase(&event_level_cd) eq %kupcase(&facility_level_cd)  %then %do;
/*
	proc sql noprint;
		create table &out_ds_nm. as
		select a.credit_facility_rk, max(DEFAULT_DT) as &event_dt_column 
	    from &M_FM_LIB..Account_default_dim as a, &abt_and_cons_lvl_mapping_ds_nm as corp_acct_x_facility
	    where a.account_rk = corp_acct_x_facility.account_rk
        and Default_status_cd IN (&DEFAULT_STATUS_CD.)
		and (datepart(DEFAULT_DT) between &shifted_default_dt and &m_dabt_build_dt)
		and (&abt_bld_dttm. >= VALID_START_DTTM AND &abt_bld_dttm. <=VALID_END_DTTM ) 
		group by a.credit_facility_rk;
	quit;
*/
	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		acct_x_facility.credit_facility_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &abt_and_cons_lvl_mapping_ds_nm as acct_x_facility
 	where 
		default_event_dim.account_rk = acct_x_facility.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/* HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/* HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
	group by 
		acct_x_facility.credit_facility_rk
	   ;
	quit;
%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&ccf_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&ind_acct_level_cd) and 
		  &csb_bank_config_flg = Y and
          %kupcase(&event_level_cd) eq %kupcase(&ind_cust_level_cd)  %then  %do;
/*
	proc sql noprint;
		create table &out_ds_nm. as
		select a.customer_rk, max(a.DEFAULT_DT) as &event_dt_column 
	    from &M_FM_LIB..Account_default_dim as a, &abt_and_cons_lvl_mapping_ds_nm as ind_acct_x_ind_cust
	    where a.account_rk = ind_acct_x_ind_cust.account_rk
        and Default_status_cd IN (&DEFAULT_STATUS_CD.)
		and (DATEPART(DEFAULT_DT) between &shifted_default_dt and &m_dabt_build_dt)
		and (&abt_bld_dttm. >= VALID_START_DTTM AND &abt_bld_dttm. <=VALID_END_DTTM ) 
		group by a.customer_rk;
	quit;
*/
	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		acct_x_cust.customer_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &abt_and_cons_lvl_mapping_ds_nm as acct_x_cust
 	where 
		default_event_dim.account_rk = acct_x_cust.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/* HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/* HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
	group by 
		acct_x_cust.customer_rk
	   ;
	quit;
%end;

%else %if %kupcase(&abt_purpose_cd) eq %kupcase(&ccf_purpose_cd) and 
		  %kupcase(&abt_level_cd) eq %kupcase(&corp_acct_level_cd) and 
		  &csb_bank_config_flg = Y and
          %kupcase(&event_level_cd) eq %kupcase(&corp_cust_level_cd)  %then  %do;
/*
	proc sql noprint;
		create table &out_ds_nm. as
		select a.customer_rk, max(a.DEFAULT_DT) as &event_dt_column 
	    from &M_FM_LIB..Account_default_dim as a, &abt_and_cons_lvl_mapping_ds_nm as corp_acct_x_corp_cust
	    where a.account_rk = corp_acct_x_corp_cust.account_rk
        and Default_status_cd IN (&DEFAULT_STATUS_CD.)
		and (datepart(DEFAULT_DT) between &shifted_default_dt and &m_dabt_build_dt)
		and (&abt_bld_dttm. >= VALID_START_DTTM AND &abt_bld_dttm. <=VALID_END_DTTM ) 
		group by a.customer_rk;
	quit;
*/
	proc fedsql &m_fedsql_option.;
	create table 
		&out_ds_nm. as
	select 
		acct_x_cust.customer_rk, max(default_event_dim.default_dt) as &event_dt_column 
	from 
		&M_FM_LIB..default_event_dim as default_event_dim, &abt_and_cons_lvl_mapping_ds_nm as acct_x_cust
 	where 
		default_event_dim.account_rk = acct_x_cust.account_rk and 		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do; /* i18NOK:LINE */
		/*	HF FEB2019 Defect S1486520
			default_event_dim.default_dt <= &m_dabt_build_dt. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &default_capture_period_start_dt. 
		%end;
		%else %do;
		/*	HF FEB2019 Defect S1486520
			default_event_dim.default_dt > &m_dabt_build_dt. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		*/
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) > &m_dabt_build_dt. and 
			cast(datepart(cast(default_event_dim.default_dt as timestamp)) as double) <= &default_capture_period_end_dt. 
		%end;
	group by 
		acct_x_cust.customer_rk
	   ;
	quit;
%end;

%mend bankfdn_create_event_data;

