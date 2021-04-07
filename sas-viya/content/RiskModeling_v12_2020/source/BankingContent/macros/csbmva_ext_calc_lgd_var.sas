 %macro csbmva_ext_calc_lgd_var ();
 
 	%if "&m_dabt_outcome_time_frequency_cd" eq "MONTH" %then %do;   /* i18nOK:Line */  /*Outcome/Target period is defined in terms of Months*/
		%let m_outcome_freq_align = END;
		%let m_outcome_freq_cd = DTMONTH;		
	%end;
	%else %if "&m_dabt_outcome_time_frequency_cd" eq "DAY" %then %do;   /* i18nOK:Line */  /*Outcome/Target period is defined in terms of Days*/
		%let m_outcome_freq_align = SAME;
		%let m_outcome_freq_cd = DTDAY;
	%end;
	

	
	%if &FM_GRAIN. eq 1 %then %do; /* For Foundation Mart data grain = Monthly */
		%let m_default_capture_period_aling = END;
		%let m_default_capture_period_freq_cd = DTMONTH;
	%end;
	%else %if &FM_GRAIN. eq 3 %then %do; /* For Foundation Mart data grain = Daily */
		%let m_default_capture_period_aling = SAME;
		%let m_default_capture_period_freq_cd = DTDAY;
	%end;
 	
	%let default_capture_period_start_dt = .;
	%let default_capture_period_end_dt = .;
 	
	%if "&m_dabt_abt_type_cd" = "MDL" %then %do;  /* i18nOK:Line */ /* m_dabt_abt_type_cd = "MDL" means Modeling ABT*/
		/* if abt time frequency code is monthly and if abt build date is 31-Dec-2012 then default_capture_period starts after is 30-Jun-2012 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 31-Dec-2012 then default_capture_period starts after is 30-Jun-2012 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 30-Dec-2012 then default_capture_period starts after is 30-Jun-2012 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 28-Dec-2012 then default_capture_period starts after is 28-Jun-2012 if default capture period is in 6 month */
		%let default_capture_period_start_dt = %sysfunc(intnx(&m_default_capture_period_freq_cd.,&m_dabt_build_dttm,-&DEFAULT_CAPTURE_PERIOD,&m_default_capture_period_aling.));
	%end;
	%else %do;
		/* if abt time frequency code is monthly and if abt build date is 31-Dec-2012 then default_capture_period_end_dt is 30-Jun-2013 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 31-Dec-2012 then default_capture_period_end_dt is 30-Jun-2013 if default capture period is in 6 month */
		/* if abt time frequency code is daily and if abt build date is 30-Dec-2012 then default_capture_period_end_dt is 30-Jun-2013 if default capture period is in 6 month */				
		/* if abt time frequency code is daily and if abt build date is 28-Dec-2012 then default_capture_period_end_dt is 28-Jun-2013 if default capture period is in 6 month */
		
		/* In case of temporary actual calculation being done for date which is less than default capture period end date then instead actual default capture period end date that date for which actuals are being calculated is treated as default capture period end date. Hence min function is added*/
		%let default_capture_period_end_dt = %sysfunc(min(%sysfunc(intnx(&m_default_capture_period_freq_cd.,&m_dabt_build_dttm,&CSB_DEF_CPTR_PERIOD_POST_LGD_SCR,&m_default_capture_period_aling.)),&m_dabt_outcome_period_end_dttm.));
		
	%end;	
	
	%put default_capture_period_start_dt = %SYSFUNC(PUTN(&default_capture_period_start_dt., NLDATM.));
	%put default_capture_period_end_dt = %SYSFUNC(PUTN(&default_capture_period_end_dt., NLDATM.));
	
	%let m_dabt_build_dttm_num = %sysevalf(&m_dabt_build_dttm.);
	%put *************&=m_dabt_build_dttm_num.;
	
	%let m_dabt_out_period_end_dttm_num = %sysevalf(&m_dabt_outcome_period_end_dttm.);
	%put *************&=m_dabt_out_period_end_dttm_num.;

/*
	Significance of default date: 
		? If a given account has default date which meets conditions specified below then that date becomes the reference date for calculation of LGD ABT variables for that account. Else ABT build date will be the reference date for variable calculations.
			? Input variables will be calculated going back from this date.
			? Outcome variables (LGD, Recovery) will be calculated in recovery period starting from this date

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

 /*B represents Corporate Financial Account, A represents Retail Financial Account*/
 %if ("&m_dabt_subject_of_analysis_cd." = "B" or "&m_dabt_subject_of_analysis_cd." =  "A") %then %do;  /* i18nOK:Line */ /* Account as Subject of analysis*/
 
 
 /* Start of identify default date */
 /*
	Incase account has multiple default events meeting above criterion then latest default event (then one with maximum default date) is to be considered for LGD calculation. Hence EAD and recovery corresponding to only that default event will be considered.
	It might be the case that account got cured in default capture period itself and hence not default as of abt build date. In that case too, it is treated as default because there was some period in which it was under recovery and got fully recovered and cured as of abt build date with loss as zero
*/
	
	%dabt_drop_table(m_table_nm=&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_dt,m_cas_flg=Y);
	proc fedsql &m_fedsql_option.;
	create table 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_dt as
	select 
		default_event_dim.account_rk, max(default_event_dim.default_dt) as ref_default_dt

	from 
		bankcrfm.default_event_dim as default_event_dim, &m_dabt_subset_retain_key_libref..&m_dabt_subset_retain_key_tbl_nm. as subset_rk_list
 	where 
		default_event_dim.account_rk = subset_rk_list.account_rk and 
		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do;     /* i18nOK:Line */
			default_event_dim.default_dt <= &m_dabt_build_dttm_num. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		%end;
		%else %do;
			default_event_dim.default_dt > &m_dabt_build_dttm_num. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		%end;
		
		%if "&m_dabt_abt_type_cd" = "ACT" %then %do;      /* i18nOK:Line */
			/* In case of temporary actual calculation being done for as of date which is less than default capture period end date for corresponding scoring run then we will like to consider defaults up to that actual as of date. Hence extra where clause is added to consider defaults only up to that date. So that even if foundation mart is loaded for period after date as of which actual calculations are being done, defaults post actual as of date won't be considered for which actual calculations*/
			/* Note: In case of actual calculations, m_dabt_outcome_period_end_dt will have value Min of (actual as of date, final outcome period end date) */
			 and default_event_dim.default_dt <= &m_dabt_out_period_end_dttm_num.
		%end;
	group by 
		default_event_dim.account_rk
	   ;
	quit;
    /*End of identify default date*/
    
    
    /* Start of identify EAD */
     
      /*
	      In step above we have already identified reference date i.e. default date of default event associated with account for LGD calculation
	      Now get EAD as of identified default event/date for that account
	      Note: we assume that EAD don't change over period of time for given default event. Even if it changes, we take EAD as of default date.

	      Details expected data in dim.default_event_dim table
	      	? At given point in time, one given account cannot have multiple valid default event with same default date
		? For a given default event, default date remains same over period of time.
      */
	%dabt_drop_table(m_table_nm=&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_evnt_dtl,m_cas_flg=Y);
	
	proc fedsql &m_fedsql_option.;   
	create table 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_evnt_dtl as
	select 
		default_event_dim.account_rk, default_event_dim.exposure_at_default_amt, default_event_rk, default_event_dim.default_dt, 
		/* if abt (source data load) time frequency code is monthly  i.e. "&m_dabt_src_data_time_grain_cd" eq "MONTH"*/
			/* Recovery period starts when account defaults and ends when 6 calendar months end after the month in which it defaults */
			/*  If default date is 31-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: Jan 13, Feb 13, ..,May 13 i.e. considering recoveries in period Jan-13 to Jun-2013 (Both Inclusive)  */
			/*  If default date is 28-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: Dec12 (starting after default date), Jan 13, Feb 13, ..,May 13, Jun 13. i.e. considering recoveries in period Dec-12 to Jun-13 (Both Inclusive) */
			/*  If default date is 5-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: Jan 13, Feb 13, ..,May 13, Jun 13 i.e. considering recoveries in period Jan-13 to Jun-2013 (Both Inclusive)  */
			
		/* if abt (source data load) time frequency code is Daily */
			/*  If default date is 31-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: i.e. considering recoveries in period 01-Jan-13:00:00:00 to 30-Jun-2013:23:59:59 (Both Inclusive)  */
			/*  If default date is 30-Nov-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: i.e. considering recoveries in period 01-Dec-13:00:00:00 to 30-May-2013:23:59:59 (Both Inclusive)  */
			/*  If default date is 28-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: i.e. considering recoveries in period 29-Dec-12:00:00:00 to 28-Jun-13:23:59:59  (Both Inclusive) */

			df_dt.ref_default_dt as recovery_period_start_dttm ,	
			
			%dabt_intnx("&m_outcome_freq_cd",df_dt.ref_default_dt,&m_dabt_outcome_period_cnt.,"&m_outcome_freq_align.") as recovery_period_end_dttm	

	from 
		bankcrfm.default_event_dim as default_event_dim, &m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_dt as df_dt
	where 
		df_dt.account_rk = default_event_dim.account_rk and
		df_dt.ref_default_dt = default_event_dim.default_dt and
		default_event_dim.default_dt >= valid_start_dttm and 
		default_event_dim.default_dt <= valid_end_dttm  
	;
	quit;
    
   /*End of identify EAD*/
 
   /* Start of identify recovery cost,amount  */
     /* Same logic is applicable to Modeling, Back Testing, Pooling, Actual Calc ABT */
     /* It is the sum of recovered amount and recovery cost in recovery period corresponding to already identified (in above step) default event for each account. Note: recovery period will be starting from default date of identified default event.
     */
	%dabt_drop_table(m_table_nm=&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_rec_amt,m_cas_flg=Y);
	proc fedsql &m_fedsql_option.;    
	create table 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_rec_amt as
	select 
		recovery_fact_base.account_rk,
		sum(value_at_recovery_amt) as value_at_recovery_amt , 
		sum(recovery_cost_amt) as recovery_cost_amt
	from 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_evnt_dtl as df_evnt_dtl,
		bankcrfm.recovery_fact_base as recovery_fact_base 
	where
		/* two conditions below are to get list of default_event_Sk for given account's given default_event_RK*/
		df_evnt_dtl.account_rk = recovery_fact_base.account_rk and
		/*df_evnt_dtl.default_event_rk  = recovery_fact_base.default_event_rk  and*/

		/* condition below is to extract data from recovery_fact corresponding to identified default event rk 
		recovery_fact.default_event_sk =  default_event_dim.default_event_sk and*/

		/* condition below is to extract recovery data that lie in recovery period starting with to each account's identified default date 
		recovery_fact.time_sk = time_dim.time_sk and */

		recovery_fact_base.period_last_dttm > recovery_period_start_dttm AND 
		recovery_fact_base.period_last_dttm <= recovery_period_end_dttm 

		%if "&m_dabt_abt_type_cd" = "ACT" %then %do;      /* i18nOK:Line */
			/* In case of temporary actual calculation being done for as of date which is less than recovery period end date then we will like to consider recoveries up to that actual as of date. Hence extra where clause is added to consider recoveries only up to that date. So that even if foundation mart is loaded for period after date as of which actual calculations are being done, it won't consider data after date for which actual calculations are being done*/
			/* Note: In case of actual calculations, m_dabt_out_period_end_dttm_num will have value Min of (actual as of date, final outcome period end date) */
			AND recovery_fact_base.period_last_dttm <= &m_dabt_out_period_end_dttm_num.
		%end;
	group by  
		recovery_fact_base.account_rk
	;
	quit;
   
   /* End of identify recovery cost,amount  */
   
    /* Start of LGD calculations and final output table with all required details  */
    
    %dabt_drop_table(m_table_nm=&m_dabt_output_table_libref..&m_dabt_output_table_nm,m_cas_flg=Y);
    proc fedsql &m_fedsql_option.;     
    	create table 
    		&m_dabt_output_table_libref..&m_dabt_output_table_nm as
    	select 
    		df_evnt_dtl.account_rk,
    		df_evnt_dtl.exposure_at_default_amt as EAD, 
    		df_evnt_dtl.default_dt as DEFAULT_DT, 
    		%dabt_zero_if_null(df_rec_amt.value_at_recovery_amt) as VALUE_AT_RECOVERY  , 
    		%dabt_zero_if_null(df_rec_amt.recovery_cost_amt) as RECOVERY_COST, 
  		((%dabt_zero_if_null(df_evnt_dtl.exposure_at_default_amt) + %dabt_zero_if_null(df_rec_amt.recovery_cost_amt) - %dabt_zero_if_null(df_rec_amt.value_at_recovery_amt))/ %dabt_null_if_zero(df_evnt_dtl.exposure_at_default_amt)) as LGD
    	from 
    		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_evnt_dtl as df_evnt_dtl left join &m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_rec_amt as df_rec_amt
    		on (df_evnt_dtl.account_rk = df_rec_amt.account_rk)
    	;
    	quit;   
    
    
     /* End of LGD calculations */
 
 %end; /* Account as Subject of analysis*/
 
 
 /*D represents Credit Facility*/
 %else  %if "&m_dabt_subject_of_analysis_cd." = "D" %then %do;   /* i18nOK:Line */  /* Credit Facility as Subject of analysis*/
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
	%dabt_drop_table(m_table_nm=&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_dt,m_cas_flg=Y);
	proc fedsql &m_fedsql_option.; 
	create table 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_dt as
	select 
		subset_rk_list.credit_facility_rk, max(default_event_dim.default_dt) as ref_default_dt
	from 
		bankcrfm.default_event_dim as default_event_dim, &m_dabt_subset_retain_key_libref..&m_dabt_subset_retain_key_tbl_nm. as subset_rk_list, bankcrfm.account_dim as account_dim
 	where 

		/* two conditions below to get list accounts of associated a given credit facility as of abt build date*/
		subset_rk_list.credit_facility_rk = account_dim.credit_facility_rk and 
		&m_dabt_build_dttm_num. >= account_dim.valid_start_dttm and 
		&m_dabt_build_dttm_num. <= account_dim.valid_end_dttm and
		
		/* Condition below to get list default event of those accounts*/
		account_dim.account_rk = default_event_dim.account_rk and 
		
		default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
		
		%if "&m_dabt_abt_type_cd" = "MDL" %then %do;   /* i18nOK:Line */
			default_event_dim.default_dt <= &m_dabt_build_dttm_num. and 
			default_event_dim.default_dt > &default_capture_period_start_dt. 
		%end;
		%else %do;
			default_event_dim.default_dt > &m_dabt_build_dttm_num. and 
			default_event_dim.default_dt <= &default_capture_period_end_dt. 
		%end;
		%if "&m_dabt_abt_type_cd" = "ACT" %then %do;    /* i18nOK:Line */
			/* In case of temporary actual calculation being done for as of date which is less than default capture period end date for corresponding scoring run then we will like to consider defaults up to that actual as of date. Hence extra where clause is added to consider defaults only up to that date. So that even if foundation mart is loaded for period after date as of which actual calculations are being done, defaults post actual as of date won't be considered for which actual calculations*/
			/* Note: In case of actual calculations, m_dabt_outcome_period_end_dt will have value Min of (actual as of date, final outcome period end date) */
			 and default_event_dim.default_dt <= &m_dabt_out_period_end_dttm_num.
		%end;
	group by 
		subset_rk_list.credit_facility_rk
	   ;
	quit;
    /*End of identify default date*/

	/* Start of identify EAD */
     
	    /* Start of EAD Step 1: */
		/*
			First Identify all accounts associated with credit facility as of abt build date. And get reference default date associated with all those accounts.			
	    */
		%dabt_drop_table(m_table_nm=&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.cr_ac_df_dt,m_cas_flg=Y);
		
		proc fedsql &m_fedsql_option.;
		create table 
			&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.cr_ac_df_dt as
		select 
			subset_rk_list.credit_facility_rk, account_dim.account_rk, max(default_event_dim.default_dt) as ref_default_dt
		from 
			bankcrfm.default_event_dim as default_event_dim, &m_dabt_subset_retain_key_libref..&m_dabt_subset_retain_key_tbl_nm. as subset_rk_list, bankcrfm.account_dim as account_dim
	 	where 
			/* two conditions below to get list accounts of associated a given credit facility as of abt build date*/
			subset_rk_list.credit_facility_rk = account_dim.credit_facility_rk and 
			(&m_dabt_build_dttm_num. >= account_dim.valid_start_dttm and &m_dabt_build_dttm_num. <=account_dim.valid_end_dttm ) and
			
			/* Condition below to get list default event of those accounts*/
			account_dim.account_rk = default_event_dim.account_rk and 
			
			default_event_dim.default_status_cd =  &DEFAULT_STATUS_CD. and
			
			%if "&m_dabt_abt_type_cd" = "MDL" %then %do;   /* i18nOK:Line */
				default_event_dim.default_dt <= &m_dabt_build_dttm_num. and 
				default_event_dim.default_dt > &default_capture_period_start_dt. 
			%end;
			%else %do;
				default_event_dim.default_dt > &m_dabt_build_dttm_num. and 
				default_event_dim.default_dt <= &default_capture_period_end_dt. 
			%end;
		group by 
			subset_rk_list.credit_facility_rk, account_dim.account_rk
		   ;
		quit;
		/* End of of EAD Step 1: 

		/* Start of EAD Step 2: */
		/*
			Get EAD and default event rk (corresponding to default event having identified default date) for all accounts of credit facility 
			Also to get start and end date of recovery period date for each of those.
	    */
	    	%dabt_drop_table(m_table_nm=&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.cr_ac_df_evnt,m_cas_flg=Y);
		proc fedsql &m_fedsql_option.;   
		create table 
			&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.cr_ac_df_evnt as
		select 
			cr_ac_df_dt.credit_facility_rk,default_event_dim.account_rk, 
			default_event_dim.default_event_rk, 			
			default_event_dim.exposure_at_default_amt, default_event_dim.default_dt, 	

			/* if abt (source data load) time frequency code is monthly  i.e. "&m_dabt_src_data_time_grain_cd" eq "MONTH"*/
				/* Recovery period starts when account defaults and ends when 6 calendar months end after the month in which it defaults */
				/*  If default date is 31-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: Jan 13, Feb 13, ..,May 13 i.e. considering recoveries in period Jan-13 to Jun-2013 (Both Inclusive)  */
				/*  If default date is 28-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: Dec12 (starting after default date), Jan 13, Feb 13, ..,May 13, Jun 13. i.e. considering recoveries in period Dec-12 to Jun-13 (Both Inclusive) */
				/*  If default date is 5-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: Jan 13, Feb 13, ..,May 13, Jun 13 i.e. considering recoveries in period Jan-13 to Jun-2013 (Both Inclusive)  */
				
			/* if abt (source data load) time frequency code is Daily */
				/*  If default date is 31-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: i.e. considering recoveries in period 01-Jan-13:00:00:00 to 30-Jun-2013:23:59:59 (Both Inclusive)  */
				/*  If default date is 30-Nov-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: i.e. considering recoveries in period 01-Dec-13:00:00:00 to 30-May-2013:23:59:59 (Both Inclusive)  */
				/*  If default date is 28-Dec-2012 and outcome(recovery) period is 6 Months then recovery_period will be spanning across 6 Months: i.e. considering recoveries in period 29-Dec-12:00:00:00 to 28-Jun-13:23:59:59  (Both Inclusive) */

				default_event_dim.default_dt as recovery_period_start_dttm ,	
				
				%dabt_intnx("&m_outcome_freq_cd",default_event_dim.default_dt,&m_dabt_outcome_period_cnt.,"&m_outcome_freq_align.") as recovery_period_end_dttm	

		from 
			bankcrfm.default_event_dim as default_event_dim, &m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.cr_ac_df_dt as cr_ac_df_dt
		where 
			cr_ac_df_dt.account_rk = default_event_dim.account_rk and
			cr_ac_df_dt.ref_default_dt = default_event_dim.default_dt and

			/* Condition below is to ensure that we get EAD as of default date. 
			It is also to ensure that we don't get duplicate records for a given credit facility and given account*/
			(default_event_dim.default_dt >= valid_start_dttm and 
			 default_event_dim.default_dt <= valid_end_dttm ) 
		;
		quit;

		/* End of of EAD Step 2: 

		/* Start of EAD Step 3: */
		/*
		  Calculate credit facility level EAD as sum of EADs of all accounts associated with credit facility 
	        */
	   	 %dabt_drop_table(m_table_nm=&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.ead,m_cas_flg=Y);
		proc fedsql &m_fedsql_option.;   
			create table &m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.ead as
			select 
				credit_facility_rk, 
				sum(exposure_at_default_amt) as exposure_at_default_amt		
			from 
				&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.cr_ac_df_evnt
			group by 
				credit_facility_rk
			   ;
		quit;
		/* End of of EAD Step 3: */ 
   /*End of identify EAD*/

   /* Start of identify recovery cost,amount  */
     /* Same logic is applicable to Modeling, Back Testing, Pooling, Actual Calc ABT */
     /* It is the sum of recovered amount and recovery cost in recovery period corresponding to already identified (in above step) default event for each account.
		Note: recovery period will be starting from default date of identified default event.
		Its is sum of all recoveries and cost for that identified default events that lie between default date and (default date + Recovery period).
     */

	%dabt_drop_table(m_table_nm=&m_dabt_output_table_libref..&m_dabt_scratch_table_cd.df_rec_amt,m_cas_flg=Y);
	proc fedsql &m_fedsql_option.;   
	create table 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_rec_amt as
	select 
		cr_ac_df_evnt.credit_facility_rk,
		sum(value_at_recovery_amt) as value_at_recovery_amt , 
		sum(recovery_cost_amt) as recovery_cost_amt
	from 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.cr_ac_df_evnt as cr_ac_df_evnt,
		bankcrfm.recovery_fact_base as recovery_fact_base

	where
		/* two conditions below are to get list of default_event_sk for given account's given default_event_rk*/
		cr_ac_df_evnt.account_rk = recovery_fact_base.account_rk and
		/*cr_ac_df_evnt.default_event_rk  = recovery_fact_base.default_event_rk  and*/

		/* condition below is to extract data from recovery_fact corresponding to identified default event sk */
		/*recovery_fact.default_event_sk = recovery_fact.default_event_sk and*/

		/* condition below is to extract recovery data that lie in recovery period starting with to each account's identified default date */
		/*recovery_fact.time_sk = time_dim.time_sk and */

		recovery_fact_base.period_last_dttm > recovery_period_start_dttm and 
		recovery_fact_base.period_last_dttm <= recovery_period_end_dttm 
		%if "&m_dabt_abt_type_cd" = "ACT" %then %do;     /* i18nOK:Line */
			/* In case of temporary actual calculation being done for as of date which is less than recovery period end date then we will like to consider recoveries up to that actual as of date. Hence extra where clause is added to consider recoveries only up to that date. So that even if foundation mart is loaded for period after date as of which actual calculations are being done, it won't consider data after date for which actual calculations are being done*/
			/* Note: In case of actual calculations, m_dabt_out_period_end_dttm_num will have value Min of (actual as of date, final outcome period end date) */
			and recovery_fact_base.period_last_dttm <= &m_dabt_out_period_end_dttm_num.
		%end;

	group by  
		cr_ac_df_evnt.credit_facility_rk
	;
	quit;
   
   /* End of identify recovery cost,amount  */

	    /* Start of LGD calculations and final output table with all required details  */
    
    %dabt_drop_table(m_table_nm=&m_dabt_output_table_libref..&m_dabt_output_table_nm,m_cas_flg=Y);
		proc fedsql &m_fedsql_option.;     
    create table 
    	&m_dabt_output_table_libref..&m_dabt_output_table_nm as
    select 
    	df_dt.credit_facility_rk,
    	ead.exposure_at_default_amt as EAD, 
    	df_dt.ref_default_dt as DEFAULT_DT, 
    	%dabt_zero_if_null(df_rec_amt.value_at_recovery_amt) as VALUE_AT_RECOVERY  , 
    	%dabt_zero_if_null(df_rec_amt.recovery_cost_amt) as RECOVERY_COST, 
  	((%dabt_zero_if_null(ead.exposure_at_default_amt) + %dabt_zero_if_null(df_rec_amt.recovery_cost_amt) - %dabt_zero_if_null(df_rec_amt.value_at_recovery_amt))/%dabt_null_if_zero(ead.exposure_at_default_amt)) as LGD
    from 
		&m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_dt as df_dt left join &m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.ead as ead 
			on (df_dt.credit_facility_rk = ead.credit_facility_rk)
		left join &m_dabt_scratch_table_libref..&m_dabt_scratch_table_cd.df_rec_amt as df_rec_amt
    		on (ead.credit_facility_rk = df_rec_amt.credit_facility_rk)		
    ;
    quit;   

    
     /* End of LGD calculations */
    
 %end;/* Credit Facility as Subject of analysis*/
 
 /*Error tracking is pending.*/
 %let m_dabt_extrnl_calc_success_flg = Y;


%mend csbmva_ext_calc_lgd_var;
%csbmva_ext_calc_lgd_var();
