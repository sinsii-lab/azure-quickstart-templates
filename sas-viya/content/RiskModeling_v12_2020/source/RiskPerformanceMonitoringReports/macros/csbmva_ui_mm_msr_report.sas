/**********************************************************************************************
Copyright (c) 2010 by SAS Institute Inc., Cary, NC, USA.

Module         : csbmva_ui_mm_msr_report.sas
Function       : This macro calls the measure specific macro to generate
				 model monitoring reports

Authors        :  CSB Team
SAS            :  9.4

Called-by      :  Mid Tier
Calls          :  Measure specific macro

Input Datasets :  DIM.ANALYTICAL_MODEL_DIM
				  APDM.MODEL_MASTER_EXTENSION
				  APDM.MODEL_MASTER_EXTENSION
				  APDM.MODEL_MASTER_EXTENSION
				  APDM.MODEL_MASTER_EXTENSION

				  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_PD_FACT
				  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_PD_POOL_FD
								OR
                  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_LGD_FACT
                  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_LGD_POOL_FD
                                OR
				  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_CCF_FACT
                  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_CCF_POOL_FD
                  
Output Datasets:  Dataset for Reports

Logic          :  Call the measure specific macro to generate
				  model monitoring reports

Parameters     :  model_key  	  	 	 -> Key of the model
                  REPORT_CATEGORY_CD     -> Specify whether backtesting reports or ongoing.
                  REPORT_TYPE_CD    	 -> specify whether Model Monitoring or model input monitoring
                  REPORT_DATA_GROUP_CD	 -> Specify whether pool based or bin based.
                  REPORT_SUBTYPE_CD		 -> Specify report subtype e.g. POOL based , score based , Pd based , LGD based, CCF based.
				  TIME_PERIOD_GROUP_SK   -> Score time point Key
                  MEASURE_CD	     	 -> Code for the measure
                  FILTER_KEY			-> Key for the selected filter (optional param) 
				  FILTER_LIB			-> Libref for filter data (optional param) 
				  REPORT_SPECIFICATION_SK --> Report Specification SK
**********************************************************************************************/


%macro csbmva_ui_mm_msr_report ( 	MODEL_KEY 			= , REPORT_CATEGORY_CD 		= ,  
									REPORT_TYPE_CD 		= , REPORT_DATA_GROUP_CD 	= , 
									REPORT_SUBTYPE_CD 	= , TIME_PERIOD_GROUP_SK 	= ,
									MEASURE_CD 			= , FILTER_KEY 				= ,
									SOURCE_LIB			= , IS_REPOOL				= false,
									REPORT_SPECIFICATION_SK=
									);



%let MODEL_KEY = &MODEL_KEY.;
%let REPORT_CATEGORY_CD=&REPORT_CATEGORY_CD.;
%let REPORT_TYPE_CD= &REPORT_TYPE_CD;    
%let REPORT_DATA_GROUP_CD=&REPORT_DATA_GROUP_CD.;
%let REPORT_SUBTYPE_CD=&REPORT_SUBTYPE_CD;
%let TIME_PERIOD_GROUP_SK=&TIME_PERIOD_GROUP_SK.;
%let score_time_sk = &TIME_PERIOD_GROUP_SK.;
%let REPORT_SPECIFICATION_SK = &REPORT_SPECIFICATION_SK.;
%let measure_cd = %lowcase(&measure_cd.);
%let out_lib=work;
%let out_data=REPORT_DATA_DETAIL;
%let output_ds=&out_lib..&out_data.;
%let output_ds_feed=WORK.FEED_DATA_DETAIL;
%let output_ds1=WORK.MEASURE_ADDITIONAL_STATS;

*==================================================;
* Defining local macro variables ;
*==================================================;

%local   feed_ds_suffix i model_type_sk report_type_sk report_subtype_sk range_scheme_type_sk range_scheme_type_sk model_type_cd in_ds_lib 
 m_inp_ds_nm feed_ds_nm model_target_type_sk model_target_type_cd ;

*==================================================;
* Delete output ds if already exist ;
*==================================================;

proc datasets library = work noprint ;
	delete REPORT_DATA_DETAIL FEED_DATA_DETAIL MEASURE_ADDITIONAL_STATS;
quit;

*==================================================;
* Deriving Library based on report category code ;
*==================================================;

%if &FILTER_KEY ge 0 or &IS_REPOOL. = true %then %do;
	/* Source Library */
	%let in_ds_lib = &SOURCE_LIB.;
	%let feed_lib = &SOURCE_LIB.;
%end;
%else %if %upcase(&REPORT_CATEGORY_CD.) = BCK  %then %do;
	/* Source Library */
	%let in_ds_lib = &DABT_BACKTESTING_ABT_LIBREF;
	%let feed_lib = &DABT_BACKTESTING_ABT_LIBREF;
%end;
%else %do;
	/* Source Library */
	%let in_ds_lib = &RM_MODELPERF_DATA_LIBREF.;
	%let feed_lib = &RM_MODELPERF_DATA_LIBREF.;
%end;

*==================================================;
* Generating Input dataset name;
*==================================================;


%let feed_ds_suffix=;
%let model_type_sk=;
%let report_type_sk=;
%let report_subtype_sk=;
%let range_scheme_type_sk=;
%let model_type_cd=;
%let model_target_type_sk=;
%let model_target_type_cd=;

/*proc sql noprint;*/
/*	select model_type_sk into:model_type_sk from &csb_apdm_libref..model_master_extension*/
/*	where model_key=&model_key.;*/
/*quit;*/

proc sql noprint;
	select model_target_type_sk into:model_target_type_sk from &csb_apdm_libref..model_master_extension
	where model_key=&model_key.;
quit;

proc sql noprint;
	select model_target_type_cd into:model_target_type_cd from &csb_apdm_libref..model_target_type_master
	where model_target_type_sk=&model_target_type_sk.;
quit;

proc sql noprint;
	select report_type_sk into:report_type_sk from &csb_apdm_libref..Mm_report_type_master
	where report_type_cd="&report_type_cd.";
quit;

proc sql noprint;
	select report_subtype_sk into:report_subtype_sk from &csb_apdm_libref..mm_report_subtype_master
	where report_subtype_cd="&report_subtype_cd.";
quit;

proc sql noprint;
	select range_scheme_type_sk into:range_scheme_type_sk from &csb_apdm_libref..Range_scheme_type_master
	where report_subtype_sk=&report_subtype_sk.;
quit;

/*proc sql noprint;*/
/*	select feed_ds_suffix into:feed_ds_suffix from &csb_apdm_libref..Model_bin_feed_table_spec*/
/*	where model_type_sk=&model_type_sk. and range_scheme_type_sk=&range_scheme_type_sk.;*/
/*quit;*/

/*proc sql noprint;*/
/*	select model_type_cd into:model_type_cd from &csb_apdm_libref..model_type_master*/
/*	where model_type_sk=&model_type_sk.;*/
/*quit;*/

/*%let model_type_cd = &model_type_cd;*/
/*%let model_type_cd=%str(%')&model_type_cd.%str(%');*/

%if &measure_cd= accura or &measure_cd=er or &measure_cd=sens or &measure_cd=spec or &measure_cd=prec %then %do;
  %let feed_ds_suffix=BINARY_TGT_FACT;
  %let m_inp_ds_nm=_&model_key._&REPORT_SPECIFICATION_SK._&score_time_sk._&feed_ds_suffix.;
  %let feed_ds_nm=_&model_key._&REPORT_SPECIFICATION_SK._&score_time_sk._&feed_ds_suffix.;

%end;
%else %do;
		%if %upcase(&REPORT_DATA_GROUP_CD )= BIN %then %do;
			%if &model_target_type_cd.= BINARY %then %do;
				%let feed_ds_suffix=BINARY_TGT_FEED;
			%end;
			%else %do;
				%let feed_ds_suffix=CONT_TGT_FEED;
			%end;
		%end;
		%else %if  %upcase(&REPORT_DATA_GROUP_CD)= PLG %then %do;
			%if &model_target_type_cd.= BINARY %then %do;
				%let feed_ds_suffix=BINARY_TGT_POOL_FEED;
			%end;
			%else %do;
				%let feed_ds_suffix=CONT_TGT_POOL_FEED;
			%end;
		%end;

	%let m_inp_ds_nm=_&model_key._&REPORT_SPECIFICATION_SK._&score_time_sk._&feed_ds_suffix.;
	%let feed_ds_nm=_&model_key._&REPORT_SPECIFICATION_SK._&score_time_sk._&feed_ds_suffix.;

		%let pool_seq_exist=0;
		%let range_seq_exist=0;
		%let dsid=%sysfunc(open(&feed_lib..&m_inp_ds_nm));
		%let pool_seq_exist = %sysfunc(varnum(&dsid,POOL_SEQ_NO));
		%let range_seq_exist = %sysfunc(varnum(&dsid,RANGE_SEQ_NO));
		%let rc=%sysfunc(close(&dsid));
	
		%let order_seq_col = ;
		%if &range_seq_exist gt 0 %then %do;
			%let order_seq_col = RANGE_SEQ_NO;
		%end;
		%else %if &pool_seq_exist gt 0 %then %do;
			%let order_seq_col = POOL_SEQ_NO;
		%end;
	
		proc sql;

			create table input_ds as
				select * from  &feed_lib..&m_inp_ds_nm.
				%if &IS_REPOOL. = false and  %upcase(&REPORT_DATA_GROUP_CD) ne PLG %then %do;
				where range_scheme_type_sk=&range_scheme_type_sk.
				%end;
				%if &order_seq_col ne %then %do;
					order by &order_seq_col
				%end;
				;
          quit;

%end;




*==================================================;
* Create FEED dataset ;
*==================================================;

%local input_col_act_tmp input_col_dev_tmp cnt_stab_col dsid rc;

%let cnt_stab_col=0;
%let dsid=%sysfunc(open(&feed_lib..&feed_ds_nm));
%let cnt_stab_col=%sysfunc(varnum(&dsid,NO_OF_RECORDS_ACTUAL_ALL));
%let rc=%sysfunc(close(&dsid));

%if &measure_cd ne  accura and &measure_cd ne er and &measure_cd ne sens and  &measure_cd ne spec and &measure_cd ne prec %then %do;
 
	proc sql;
	  	create table &output_ds_feed
					%if &cnt_stab_col gt 0 and "&measure_cd" eq "sysstbindx" %then %do;  /* i18NOK:LINE*/
						(rename = ( NO_OF_RECORDS_ACTUAL_ALL = NO_OF_RECORDS_ACTUAL ) )
					%end;
		as
	    	select 
				* 
			from 
				input_ds
					%if "&measure_cd" eq "sysstbindx" %then %do; 
						%if &cnt_stab_col gt 0 %then %do;
							(drop= NO_OF_RECORDS_ACTUAL )
						%end;
					%end;
					%else %do;
						%if &cnt_stab_col gt 0 %then %do;
							(drop= NO_OF_RECORDS_ACTUAL_ALL )
						%end;
					%end;
					
			;
	quit;


%end;

*==================================================;
* Renaming few of the columns in output dataset;
*==================================================;

/*%if %upcase(&REPORT_DATA_GROUP_CD )= BIN %then %do;*/
/**/
/*	%let dsid=%sysfunc(open(&output_ds_feed,i));*/
/*	%let pool_seq_exist = %sysfunc(varnum(&dsid,POOL_SEQ_NO));*/
/*	%let pool_name_exist = %sysfunc(varnum(&dsid,POOL_NAME));*/
/*	%let rc=%sysfunc(close(&dsid));*/
/**/
/*	data &output_ds_feed;*/
/*		set &output_ds_feed;*/
/*		%if &pool_seq_exist ne 0 %then %do;*/
/*			rename pool_seq_no = range_seq_no;*/
/*		%end;*/
/*		%if &pool_name_exist ne 0 %then %do;*/
/*			rename POOL_NAME = range_name;*/
/*		%end;*/
/*	run;*/
/**/
/*%end;*/
/**/

*==================================================;
* Calling corresponding measure macro;
*==================================================;
%if %upcase(&REPORT_DATA_GROUP_CD )= BIN %then %do;
%let pool_based_ind = 0;

%let predicted_var_sk=.;
%let m_risk_incr_with_value_incr=.;
%let seq_no=range_seq_no;
%let seq_name=range_name;

proc sql noprint;
select predicted_var_sk into: predicted_var_sk from &csb_apdm_libref..Range_scheme_type_master
where range_scheme_type_sk= &range_scheme_type_sk.;
quit;

proc sql noprint;
select risk_incr_with_value_incr_flg into: m_risk_incr_with_value_incr from &csb_apdm_libref..Model_predicted_var_master
where predicted_var_sk= &predicted_var_sk.;
quit;
%end;

%else %if  %upcase(&REPORT_DATA_GROUP_CD)= PLG %then %do;

%let pool_based_ind = 1;
%let m_risk_incr_with_value_incr=.;
%let seq_no=pool_seq_no;
%let seq_name=pool_name;

data _null_;
  a = put(&score_time_sk.,cs_period_last_dttm_fmt.);
  call symput('period_end_dttm',a);
run;

proc sql noprint;
select put(pool_scheme_sk,12.) into: m_pool_sch_sk 
			from   csbridge.analytical_model_x_pool_scheme
					where valid_start_dttm <= &period_end_dttm <= valid_end_dttm
					and model_rk = &model_key;					
					quit;

proc sql noprint;
select pool_scheme_type_cd into: pool_scheme_type_cd from &csb_apdm_libref..pool_scheme
where pool_scheme_sk=&m_pool_sch_sk.;
quit;
%if &pool_scheme_type_cd=SCR %then %do;
 %let m_risk_incr_with_value_incr=N;
%end;
%else %do;
 %let m_risk_incr_with_value_incr=Y;
%end;
%end;



		%if &measure_cd=sysstbindx %then %do;
		%csbmva_sysstbindx (input_ds=input_ds,output_ds=&output_ds.,flag=1,seq_no_colname=&seq_no.,seq_name_colname=&seq_name.,max_range_value_colname=,
		est_result_col_name=,risk_incr_with_value_incr_flg=);
		%end;

		%else %if &measure_cd=mse %then %do;
		%csbmva_mse(input_ds=input_ds , output_ds=&output_ds. , flag=1, model_target_type=&model_target_type_cd.);
		%end;


		%else %if &measure_cd= accura or &measure_cd=er or &measure_cd=sens or &measure_cd=spec or &measure_cd=prec %then %do;
		%csbmva_&measure_cd(input_ds=&feed_lib..&feed_ds_nm,output_ds=&output_ds.,flag=1);
		%end;

        %else %if &measure_cd=nmltst %then %do;
  		%csbmva_nmltst (report_specification_sk = &report_specification_sk.,report_category_cd = &report_category_cd,
						input_ds =input_ds,output_ds=&output_ds.,est_result_col_name=expected_probability_of_event, measure_fact_lib =&in_ds_lib. ,report_type_cd=&REPORT_TYPE_CD., flag=1,pool_based_ind = &pool_based_ind.,seq_no_colname=&seq_no. ,max_range_value_colname=max_range_value,range_scheme_type_sk=&range_scheme_type_sk.,IS_REPOOL=&IS_REPOOL.);
		%end;


		%else %if &measure_cd= bntest or &measure_cd=gnstbllftc or &measure_cd=grfrnfrevn or &measure_cd=scrdst or &measure_cd=scrods or &measure_cd=trflgttst or &measure_cd=nmltst %then %do;
		%csbmva_&measure_cd(input_ds=input_ds,output_ds=&output_ds., flag =1,model_rk=&model_key.,report_type_cd=&REPORT_TYPE_CD.,seq_no_colname=&seq_no.,max_range_value_colname=max_range_value,est_result_col_name=expected_probability_of_event);
		%end;
	 
	
		%else %do;

			%if %kupcase(&model_target_type_cd.)= BINARY
			%then %do;

				%if &measure_cd=confint %then %do;
				%csbmva_confint(input_ds=input_ds ,output_ds=&output_ds.,flag=1,seq_name_colname=&seq_name.,seq_no_colname=&seq_no.,max_range_value_colname=max_range_value,est_result_col_name=expected_probability_of_event,model_target_type=&model_target_type_cd.,act_result_col_name=actual_probability_of_event);
				%end;
				%else %do;
				%csbmva_&measure_cd. (input_ds=input_ds,output_ds=&output_ds.,flag=1,seq_no_colname=&seq_no.,seq_name_colname=&seq_name.,max_range_value_colname=max_range_value,
				est_result_col_name=expected_probability_of_event,risk_incr_with_value_incr_flg=&m_risk_incr_with_value_incr.);
				%end;
			%end;

			%else  %do;
				%if &measure_cd=confint %then %do;
				%csbmva_confint(input_ds=input_ds ,output_ds=&output_ds.,flag=1,seq_name_colname=&seq_name.,seq_no_colname=&seq_no.,max_range_value_colname=max_range_value,est_result_col_name=expected_outcome_value,model_target_type=&model_target_type_cd.,act_result_col_name=actual_outcome_value);
				%end;
				%else %do;
				%csbmva_&measure_cd.(input_ds =input_ds,output_ds = &output_ds.,flag=1, est_result_col_name= expected_outcome_value,act_result_col_name= actual_outcome_value,seq_no_colname=&seq_no.);
				%end;
			%end;
			

		%end;



	%let dsid=%sysfunc(open(&output_ds.,i));

		%let pool_seq_exist = %sysfunc(varnum(&dsid,POOL_SEQ));
		%let pool_seq_no_exist = %sysfunc(varnum(&dsid,POOL_SEQ_NO));
		%let pool_name_exist = %sysfunc(varnum(&dsid,POOL_NAME));
		%let range_seq_no_exist = %sysfunc(varnum(&dsid,range_seq_no));
		%let range_cutoff_exist = %sysfunc(varnum(&dsid,range_cutoff));


		%let rc=%sysfunc(close(&dsid));


		proc datasets lib=&out_lib. NODETAILS noprint;
			modify &out_data.;
		%if %upcase(&REPORT_DATA_GROUP_CD )= BIN %then %do;
			%if &pool_seq_exist ne 0 %then %do;
				rename pool_seq = range_seq;
			%end;
			%if &pool_seq_no_exist ne 0 %then %do;
				rename pool_seq_no = range_seq_no;
			%end;
			
			%if &pool_name_exist ne 0 %then %do;
				rename POOL_NAME = range_name;
			%end;
		%end;
			%if &range_cutoff_exist ne 0 %then %do;
			rename range_cutoff = MAX_RANGE_VALUE;
			%end;
		run;
		quit;

/*		data &output_ds;*/
/*			set &output_ds;*/
/*			%if &pool_seq_exist ne 0 %then %do;*/
/*				rename pool_seq = range_seq;*/
/*			%end;*/
/*			%if &pool_seq_no_exist ne 0 %then %do;*/
/*				rename pool_seq_no = range_seq_no;*/
/*			%end;*/
/*			%if &pool_name_exist ne 0 %then %do;*/
/*				rename POOL_NAME = range_name;*/
/*			%end;*/
/**/
/*			%if &range_cutoff_exist ne 0 %then %do;*/
/*			rename range_cutoff = MAX_RANGE_VALUE;*/
/*			%end;*/
/**/
/*		run;*/

		%let order_seq_col = ;
		%if &range_seq_no_exist gt 0 %then %do;
			%let order_seq_col = RANGE_SEQ_NO;
		%end;
		%else %if &pool_seq_no_exist gt 0 %then %do;
			%let order_seq_col = POOL_SEQ_NO;
		%end;
		
		%if &order_seq_col. ne %then %do;
			proc sort data=&output_ds.;
				by &order_seq_col.;
			quit;
		%end;
	

	
proc sql;
create table &output_ds1.
(additional_info_cd char(1000),
additional_info_seq_no num,
value num format=18.5);
quit;

%let measure_lst='CI','ALPHA','STAT';
%let report_nm = ;

proc sql noprint;
select name into :report_nm separated by ','
from dictionary.columns where libname='WORK' and memname='REPORT_DATA_DETAIL' and  kupcase(name) IN (&measure_lst) ;
quit;
%let report_nm = &report_nm;
%if "&report_nm" ne "" %then %do;
	proc sql noprint;
		create table test_in as 
		select distinct &report_nm
		from work.REPORT_DATA_DETAIL;
	quit;

	proc transpose data=test_in out=test_out;
	run;
	proc sql noprint;
		insert into &output_ds1.(additional_info_cd,additional_info_seq_no, value)
		select kupcase(_NAME_), case kupcase(_NAME_) when 'CI' then 1 when 'ALPHA' then 2 when 'STAT' then 3  else . end as additional_info_seq_no, COL1
		from test_out;
	quit;
%end;

%mend csbmva_ui_mm_msr_report;
