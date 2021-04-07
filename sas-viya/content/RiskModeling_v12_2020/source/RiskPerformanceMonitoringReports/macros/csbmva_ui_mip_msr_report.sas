/**********************************************************************************************
Copyright (c) 2010 by SAS Institute Inc., Cary, NC, USA.

Module         : csbmva_ui_mip_msr_report.sas
Function       : This macro calls the measure specific macro to generate
				 model input monitoring reports

Authors        :  CSB Team
SAS            :  9.4

Called-by      :  Mid Tier
Calls          :  Measure specific macro

Input Datasets :  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_PD_SRANGE_FD
				  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_PD_pRANGE_FD
				  _<MODEL_RK>_<REPORT_SPECIFICATION_SK>_<SCORE_TIME_SK>_PD_ATTR_FD
                  
Output Datasets:  Dataset for MIP Reports

Logic          :  Call the measure specific macro to generate
				  model input monitoring reports

Parameters     :      model_key  	 -> Key of the model
                  score_time_sk      -> Score time point Key
                     measure_cd    	 -> Code for the measure
                    variable_sk   	 -> Key for the significant variable of the model
                      output_ds		 -> Output dataset
			 REPORT_CATEGORY_CD      -> Report category code for example (ONG: ongoing/BCK-backtesting)
			     REPORT_TYPE_CD      -> Report type code for example MM: Model monitoring /MIP : Model input monitoring
		   REPORT_DATA_GROUP_CD      -> Indicates that the reports are pool based/bin based
			  REPORT_SUBTYPE_CD		 -> Indicates the report subtype for example (PD/SCR/LGD/CCF/POOL)
			  REPORT_SPECIFICATION_SK --> Report Specification SK

**********************************************************************************************/

%macro csbmva_ui_mip_msr_report(MODEL_KEY=, REPORT_CATEGORY_CD=,REPORT_TYPE_CD=,REPORT_DATA_GROUP_CD=,
TIME_PERIOD_GROUP_SK=,MEASURE_CD=,VARIABLE_KEY_LST=, REPORT_SPECIFICATION_SK=);

%let MODEL_KEY = &MODEL_KEY.;
%let REPORT_CATEGORY_CD=&REPORT_CATEGORY_CD.;
%let REPORT_TYPE_CD= &REPORT_TYPE_CD;    
%let REPORT_DATA_GROUP_CD=&REPORT_DATA_GROUP_CD.;
%let REPORT_SUBTYPE_CD=SCR;
%let TIME_PERIOD_GROUP_SK=&TIME_PERIOD_GROUP_SK.;
%let score_time_sk = &TIME_PERIOD_GROUP_SK.;
%let REPORT_SPECIFICATION_SK = &REPORT_SPECIFICATION_SK.;
%let measure_cd = %lowcase(&measure_cd.);
%let output_ds=WORK.REPORT_DATA_DETAIL;
%let output_ds_col_list=WORK.ATTRIBUTE_COLUMNS;
%let output_ds_grf_data=WORK.ATTRIBUTE_GRAPH_DATA_DETAIL;


*==================================================;
* Defining local macro variables ;
*==================================================;

%local feed_ds_suffix input_ds model_type_sk report_type_sk report_subtype_sk range_scheme_type_sk
output_ds output_ds_col_list output_ds_grf_data ;

*==================================================;
* Delete output ds if already exist ;
*==================================================;

proc datasets library = work ;
	delete REPORT_DATA_DETAIL FEED_DATA_DETAIL FEED_DATA_DETAIL_TEMP ATTRIBUTE_COLUMNS ATTRIBUTE_GRAPH_DATA_DETAIL;
quit;

*==================================================;
* Getting the count of variables ;
*==================================================;

%let var_cnt = %eval(%sysfunc(countc(&VARIABLE_KEY_LST., '#'))+1); /* i18nOK:Line */

*==================================================;
* Generating input dataset name ;
*==================================================;

%let feed_ds_suffix=;
%let model_type_sk=;
%let report_type_sk=;
%let report_subtype_sk=;
%let range_scheme_type_sk=;

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

%if &measure_cd ne attribevnt and &measure_cd ne attribprps %then %do;
 %let feed_ds_suffix=ATTR_FEED;
%end;
%else %do;
	%let feed_ds_suffix=RANGE_FEED;
%end;


%let input_ds = &RM_MODELPERF_DATA_LIBREF.._&model_key._&REPORT_SPECIFICATION_SK._&score_time_sk._&feed_ds_suffix.;


*==================================================;
* Create feed tables ;
*==================================================;

%local input_col_act_tmp input_col_dev_tmp cnt_stab_col dsid rc;

%let cnt_stab_col=0;
%let dsid=%sysfunc(open(&input_ds.));
%let cnt_stab_col=%sysfunc(varnum(&dsid,NO_OF_RECORDS_ACTUAL_ALL));
%let rc=%sysfunc(close(&dsid));

%if &cnt_stab_col gt 0 %then %do; /*Additional condition for hot_fix related to score shift index calculation*/
	data work.FEED_DATA_DETAIL_TEMP;
	set &input_ds. %if "&measure_cd" eq "wtgscr"  or "&measure_cd" eq "scrshftind" or "&measure_cd" eq "attribprps" or "&measure_cd" eq"varstbindx" %then %do;	/* i18NOK:LINE */
		(drop= NO_OF_RECORDS_ACTUAL )
	%end;
	%else %do;
		(drop= NO_OF_RECORDS_ACTUAL_ALL )
	%end;
	;
	
	%if "&measure_cd" eq "wtgscr"  or "&measure_cd" eq "scrshftind" or "&measure_cd" eq "attribprps" or "&measure_cd" eq"varstbindx" %then %do;	/* i18NOK:LINE */
		rename NO_OF_RECORDS_ACTUAL_ALL=NO_OF_RECORDS_ACTUAL;
	%end;
	where variable_sk in ( %sysfunc(tranwrd(&variable_key_lst.,%str(#),%str( ))) )   /* i18NOK:LINE */
	%if &measure_cd. eq attribevnt or &measure_cd. eq attribprps %then %do;
	and  range_scheme_type_sk=&range_scheme_type_sk.;
	%end;
	;
	run;
   
%end;
%else %do;
proc sql;
  create table work.FEED_DATA_DETAIL_TEMP as 
	select 
		* 
	from &input_ds. 
	where variable_sk in ( %sysfunc(tranwrd(&variable_key_lst.,%str(#),%str(,))) )	/* i18NOK:LINE */
	%if &measure_cd. eq attribevnt or &measure_cd. eq attribprps %then %do;
	and  range_scheme_type_sk=&range_scheme_type_sk.;
	%end;
	;
quit;
	
%end;

*==================================================;
* Add variable name to feed table created above
*==================================================;

proc sql;
create table work.FEED_DATA_DETAIL as 
select 
var.variable_short_nm as variable,
feed.* 
from work.FEED_DATA_DETAIL_TEMP feed
left join &csb_apdm_libref..modeling_abt_x_variable var
on feed.variable_sk=var.variable_sk
order by ATTRIBUTE_SEQ_NO;
quit;

*==================================================;
* Generating report data for all variables ;
*==================================================;

%do var_loop = 1 %to &var_cnt;

	%let variable_sk = %kscan(&variable_key_lst,&var_loop,%str(#)); 
	
	*================================================================;
	* Extract variable name from variable_sk to add to measure table
	*================================================================;
	%let m_variable_nm=;
	proc sql noprint;
		select variable_short_nm into :m_variable_nm
	from &csb_apdm_libref..modeling_abt_x_variable
	where variable_sk=&variable_sk
	;
	quit;

	%let m_variable_nm=%nrbquote(&m_variable_nm);

	*==================================================;
	* Subsetting data for selected variable ;
	*==================================================;

	proc datasets library = work nodetails noprint nowarn;
		delete indata;
	quit;
	proc sql;
	  create table indata as
		select * from &input_ds. where variable_sk =&variable_sk. 
		%if &measure_cd. eq attribevnt or &measure_cd. eq attribprps %then %do;
			and  range_scheme_type_sk=&range_scheme_type_sk.;
		%end;
	;
	quit;

	*==================================================;
	* Calling respective macro ;
	*==================================================;

	%if &measure_cd = scrshftind %then %do;

		%csbmva_scrshftind(input_ds=indata,output_ds=&output_ds._&variable_sk);

		proc sort data = &output_ds._&variable_sk;
		by attribute_seq_no;
		run;


	%end;
	%else %if &measure_cd = evntshftin %then %do;

		%csbmva_evntshftin(input_ds=indata,output_ds=&output_ds._&variable_sk);
	  
		proc sort data = &output_ds._&variable_sk;
		by attribute_seq_no;
		run;


	%end;
	%else %if &measure_cd = varstbindx %then %do;

		%csbmva_varstbindx(input_ds=indata,output_ds=&output_ds._&variable_sk);
	  
		proc sort data = &output_ds._&variable_sk;
		by attribute_seq_no;
		run;


	%end;
	%else %if &measure_cd = evntstbind %then %do;

		%csbmva_evntstbind(input_ds=indata,output_ds=&output_ds._&variable_sk);
	  
		proc sort data = &output_ds._&variable_sk;
		by attribute_seq_no;
		run;



	%end;
	%else %if &measure_cd = infvalsts %then %do;

		%csbmva_infvalsts(input_ds=indata,output_ds=&output_ds._&variable_sk);
	  
		proc sort data = &output_ds._&variable_sk;
		by attribute_seq_no;
		run;



	%end;
	%else %if &measure_cd = ginindx %then %do;

	   %csbmva_ginindx(input_ds=indata,Output_ds=&output_ds._&variable_sk,outgraph_ds=graph_gini);


	%end;
	%else %if &measure_cd = ksmip %then %do;

		%csbmva_ksmip(input_ds=indata, out_graph_ds=&output_ds._&variable_sk);



	%end;
	%else %if &measure_cd = perchsq %then %do;

		%csbmva_perchsq(input_ds=indata,output_ds=&output_ds._&variable_sk);
	  


	%end;
	%else %if &measure_cd = wtgscr %then %do;

		%csbmva_wtgscr(input_ds=indata,output_ds=&output_ds._&variable_sk);

		proc sort data = &output_ds._&variable_sk;
		by attribute_seq_no;
		run;


	%end;
	%else %if &measure_cd = attribevnt %then %do;

		%let out_lib = work;

		%csbmva_attribevnt (input_ds =indata,output_ds =&output_ds._&variable_sk. );
		%csbmva_ui_get_attrib_col_list(varsk =&variable_sk.,output_ds= &output_ds_col_list.,measure_cd=&measure_cd.,modelrk=&model_key.);
		%csbmva_ui_get_attrib_graph_data(varsk=&variable_sk.,modelrk=&model_key.,input_ds=&output_ds._&variable_sk.,output_ds=&output_ds_grf_data.,measure_cd=&measure_cd.);



	%end;

	%else %if &measure_cd = attribprps %then %do;

		%csbmva_attribprps (input_ds =indata,output_ds =&output_ds._&variable_sk );
		%csbmva_ui_get_attrib_col_list(varsk =&variable_sk.,output_ds= &output_ds_col_list.,measure_cd=&measure_cd,modelrk=&model_key);
		%csbmva_ui_get_attrib_graph_data(varsk=&variable_sk.,modelrk=&model_key.,input_ds=&output_ds._&variable_sk.,output_ds=&output_ds_grf_data.,measure_cd=&measure_cd.);

	%end;

	*==================================================;
	* Append to final dataset ;
	*==================================================;
	
	data &output_ds._&variable_sk;
	length variable $3000;
		variable_sk = &variable_sk.;
		variable="&m_variable_nm";
		set &output_ds._&variable_sk;
	run;
	
	proc append base = &output_ds data = &output_ds._&variable_sk force;
	run;

%end;

%mend csbmva_ui_mip_msr_report;
