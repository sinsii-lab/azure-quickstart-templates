


%macro csbmva_defdata(input_ds=,stat=,out_graph_ds = ,output_ds = ,out_stat_ds=,seq_no_colname=,seq_name_colname=,est_result_col_name=,max_range_value_colname=,act_result_col_name=);
%let seq_no_colname=&seq_no_colname.;
%let seq_name_colname=&seq_name_colname.;
%let est_result_col_name=&est_result_col_name.;
%let max_range_value_colname=&max_range_value_colname.;
%let act_result_col_name=&act_result_col_name.;
%let ERROR_FLG=Y;


%if &stat = 'sysstbindx' %then %do;/*i18NOK:LINE */
data &output_ds;
set &input_ds;
   Per_Of_records_Actual=.;
   Per_of_records_Dev=.;
   Cum_Per_Of_records_Actual=.;
   Cum_Per_Of_records_dev =.;
   Diff_AD =.;
   Ratio_AD =.;
   ln_Ratio_AD =.;
   sysstbindx =.;
keep &seq_name_colname. &seq_no_colname. Per_Of_records_Actual Per_of_records_Dev
     Cum_Per_Of_records_Actual Cum_Per_Of_records_dev Diff_AD Ratio_AD ln_Ratio_AD sysstbindx;
run;

%end;

%else %if &stat = 'AR' %then/*i18NOK:LINE */
%do;

  %if &out_graph_ds ne %then %do;
	data &out_graph_ds;
	set &input_ds;
		&seq_no_colname. = .;
		cum_totacc= .;
		caprespcum= .;
		cum_propRan_events= .;
		cum_propIdealEvents= .;
		cum_prop_nevents =.;
		prop_events= .;
		cilow=.;
		cihigh=.;
		ar=.;
		ci=.;
		alpha=.;
		seq_no=.;
		balance=.;
		PROP_IDEALEVENTS=.;
		PROP_TOTACC=.;
		PROP_NON_EVENTS=.;
		PROP_CUM_TOTACC=.;
		PROP_RAND_EVENTS=.;



		run;
  %end;
    %if &out_stat_ds ne %then %do;
	  data &out_stat_ds;
	  AR = .;
	  run; 
    %end;
%end;




%else %if &stat = 'ROC' %then /*i18NOK:LINE */
	%do;
/*I18N-scnhuh-20080420-1051*/
%if &out_graph_ds ne %then %do;
		data &out_graph_ds;
/*I18N-scnhuh-20080420-1051*/	
			set &input_ds;
			range_cutoff= .;
			Sensitivity= .;
			Specificity1= .;
			Accurancy=.;
			precision=.;
            pietra=.;
			bsnerrt=.;
cilow =.;
cihigh=.;
auc=.;
ci=.;
alpha=.;
TP=.;
FN=.;
TN=.;
FP=.;
KS=.;
Dist_events =.;
depth=.;
dist_all=.;
specificity=.;
er=.;
xdecile=.;
Dist_non_events=.;




            keep &seq_no_colname.
			range_cutoff
			Sensitivity
			Specificity1
			Accurancy
			precision
			pietra
			bsnerrt
			cilow
			cihigh
			auc
			ci
			alpha
			TP
			FN
			TN
			FP
			KS
			Dist_events
			depth
			dist_all
			specificity
			er
			xdecile
			Dist_non_events


;
             run;
%end;

%if &out_stat_ds ne %then %do;
	data &out_stat_ds;
	length _NAME_ $10.;
	length Col1 8.;
	Col1=.;
	_NAME_="auc"; /*i18NOK:LINE */
	output;
	_NAME_="pietra"; /*i18NOK:LINE */
	output;
	_NAME_="bsnerrt"; /*i18NOK:LINE */
	output;
	run;
	%end;

 %end;



%else %if &stat = 'KS_ACT' %then /*i18NOK:LINE */
%do;
data &out_graph_ds;
		set &input_ds;
range_cutoff = .;
dist_non_event_act=.;
dist_event_act=.;
ks_act=.;
keep range_cutoff  dist_non_event_act dist_event_act 
ks_act
;
run;
%end;
%else %if &stat = 'KS_EST' %then /*i18NOK:LINE */
%do;
data &out_graph_ds;
		set &input_ds;
range_cutoff = .;
Dist_non_event_est=.;
Dist_event_est=.;
ks_est=.;
keep range_cutoff  Dist_non_event_est Dist_event_est 
ks_est
;
run;
%end;

%else %if &stat = 'KS' %then /*i18NOK:LINE */
%do;
data &out_graph_ds;
set &input_ds;
KS=.;
cum_prop_events=.;
cum_prop_non_events=.;
scorecard_points=.;
keep KS cum_prop_non_events cum_prop_events scorecard_points;
run;
%end;



%else %if &stat = 'Brier' %then /*i18NOK:LINE */
%do;
data &output_ds;
brier=.;
run;
%end;
%else %if &stat = 'Distance' %then /*i18NOK:LINE */
%do;
data &out_stat_ds;
	length _NAME_ $6.;
	length Col1 8.;
	Col1=.;
	_NAME_="ph";	/* i18NOK:LINE */
	output;
	_NAME_="ds";	/* i18NOK:LINE */
	output;
	_NAME_="is";	/* i18NOK:LINE */
	output;
	_NAME_="kl";	/* i18NOK:LINE */
	output;
	
run;
%end;
%else %if &stat = 'HL' %then /*i18NOK:LINE */
%do;

%if &out_graph_ds ne %then %do;
	data &out_graph_ds;
	set &input_ds;
&seq_no_colname. = .;
Max_Score_Points = .;
prop_records_actual=.;
prop_actual_bad  = .;
prop_est_bad = .;
hlp =.;
stat=.;
keep 
&seq_no_colname. 
MAX_SCORE_POINTS
prop_records_actual
prop_actual_bad
prop_est_bad
hlp
stat;
run;

%end;
 %if &output_ds ne %then %do;

data &output_ds;
hlp=.;
hlstat =.;
run;
%end;
%end;



%else %if &stat = 'EventNonEv' %then /*i18NOK:LINE */
%do;
data &output_ds;
		set &input_ds;
		
/*MAX_SCORE_POINTS= .;*/
Prop_events= .;
Prop_non_events= .;
cum_prop_nevents= .;
cum_prop_events= .; 
prop_non_events_frac=.;
/*&seq_no_colname.=.;*/
keep &seq_no_colname. &max_range_value_colname. Prop_events Prop_non_events cum_prop_nevents cum_prop_events prop_non_events_frac;
run;
%end;



%else %if &stat = 'KendSom' %then /*i18NOK:LINE */
%do;
%if &out_graph_ds ne %then %do;
	data &out_graph_ds;
	set &input_ds;
	&seq_no_colname.= &seq_no_colname.;
	&max_range_value_colname.=&max_range_value_colname.;
	&est_result_col_name.= &est_result_col_name.;
	actual_probability_of_event =.;
kentabp = .;
smdcrp = .;
stat =.;


run;
%end;

 %if &output_ds ne %then %do;
	 
   
data &output_ds;
kentabp=.;
smdcrp=.;  
kentabpval=.; 
smdcrpval=.; 
run;
%end;
%end;

%else %if &stat = 'PD_Def' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
/*&seq_name_colname. = .;*/
/*&seq_no_colname.= .;*/
/*NO_OF_RECORDS_ACTUAL= .;*/
/*NO_OF_RECORDS_DEV= .;*/
/*ESTIMATED_PD= .;*/
/*MAX_SCORE_POINTS= .;*/
/*No_Of_Actual_Bads= .;*/
Obs_probability_of_event= .;
obsvsest= .; 
/*level_sk =.;*/
/**/
/*keep &seq_name_colname. &seq_no_colname. NO_OF_RECORDS_ACTUAL NO_OF_RECORDS_DEV ESTIMATED_PD */
/*MAX_SCORE_POINTS  No_Of_Actual_Bads ObsPD obsvsest level_sk;*/
run;
%end;


%else %if &stat = 'Lift' %then /*i18NOK:LINE */
%do;
data &out_graph_ds;
set &input_ds;
/*&seq_no_colname.=.;*/
cum_totacc=.;
Lift=.;
base=.;
ideal=.;
PROP_CUM_TOTACC=.;
/*keep &seq_no_colname. cum_totacc Lift base ideal ;*/
run;
%end;


%else %if &stat = 'Bin' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds(rename=(&seq_no_colname.= pool_seq));
no_of_events_actual_binomial=.;
no_of_events_act_binomial_corr = .;
BClow=.;
BChigh=.;
Traffic=.;
BClow_corr=.;
BChigh_corr=.;
Traffic_corr=.;
prop_records_actual=.;
keep pool_seq &max_range_value_colname. no_of_events_actual_binomial NO_OF_RECORDS_ACTUAL no_of_events_actual_binomial no_of_events_act_binomial_corr BClow BChigh  Traffic BClow_corr BChigh_corr Traffic_corr  prop_records_actual &est_result_col_name.;
run;
%end;


%else %if &stat = 'Norm' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
/*&seq_no_colname.=.;*/
NClow=.;
NChigh=.;
Zcalc=.;
Traffic=.;
SUMDIFF=.;
STDEV=.;
DIFF=.;
pool_seq=.;


run;
%end;


%else %if &stat = 'Traffic' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
&seq_no_colname.=.;
&max_range_value_colname. =.;
TClow=.;
TChigh=.;
Traffic=.;
NO_OF_RECORDS_ACTUAL=.;
No_Of_Actual_Bads_traffic=.;
TRAFFIC=.;


keep &seq_no_colname. &max_range_value_colname. NO_OF_RECORDS_ACTUAL No_Of_Actual_Bads_traffic TClow TChigh Traffic ;
run;
%end;





%else %if &stat = 'Inter' %then /*i18NOK:LINE */
%do;
data &output_ds;
	length Measure_nm $6.;
	length Col1 8.;
	Col1=.;
	Grade = .;
	Measure_nm="ph"; /* I18NOK:LINE */
	output;
	Measure_nm="ds"; /* I18NOK:LINE */
	output;
	Measure_nm="is"; /* I18NOK:LINE */
	output;
	Measure_nm="kl"; /* I18NOK:LINE */
	output;
	Measure_nm="roc"; /* I18NOK:LINE */
	output;
	Measure_nm="ar"; /* I18NOK:LINE */
	output;
	Measure_nm="ks"; /* I18NOK:LINE */
	output;
run;
%end;
%else %if &stat = 'MAPE' %then /*i18NOK:LINE */
%do;
data &output_ds;
MAPE=.;
run;
%end;
%else %if &stat = 'CHISQ' %then /*i18NOK:LINE */
%do;
data &output_ds;
chsqp=.;
stat =.;
run;
%end;
%else %if &stat = 'CORR' %then /*i18NOK:LINE */
%do;
data &output_ds;
    length _TYPE_ $6.;
	length measure_value 8.;
	measure_value=.;
	_TYPE_="corr"; /* I18NOK:LINE */
	output;
run;
%end;
%else %if &stat = 'ERRMEA' %then /*i18NOK:LINE */
%do;
data &output_ds;
    MSE = .;
	MAD =.;
run;
%end;




%else %if &stat = 'PDCI' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
/*&seq_name_colname. = "";*/
/*estimated_pd = .;*/
Actual_probability_of_event=.;
/*no_of_records_actual =.;*/
/*&seq_no_colname. = .;*/
LowerConf=.;
UpperConf=.;

/*keep &seq_name_colname. &seq_no_colname.  &max_range_value_colname. no_of_records_actual &est_result_col_name. Actual_PD LowerConf UpperConf;*/

/*I18N-scnjih-20080418-0907*/

/*	PD  		label = "Esimated PD"*/
/*	ActualPD	label = "Default Rate"*/
/*	No_of_Acc   label = "No. of Accounts"*/
/*	&seq_name_colname.	label = "Pool Name"*/
/*	&seq_no_colname.	label = "Pool Sequence"*/
/*	;*/

	
/*I18N-scnjih-20080418-0907*/
run;
%end;


%else %if &stat = 'LGDCI' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
/*&seq_name_colname. = "";*/
/*estimated_lgd = .;*/
/*Actual_lgd=.;*/
/*no_of_records_actual =.;*/
/*&seq_no_colname. = .;*/
LowerConf=.;
UpperConf=.;

/*keep &seq_name_colname. &seq_no_colname.  NO_OF_RECORDS_ACTUAL Estimated_LGD Actual_LGD LowerConf UpperConf;*/
run;
%end;


%else %if &stat = 'CCFCI' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
/*&seq_name_colname. = "";*/
/*estimated_ccf = .;*/
/*Actual_ccf=.;*/
/*no_of_records_actual =.;*/
/*&seq_no_colname. = .;*/
LowerConf=.;
UpperConf=.;
/**/
/*keep &seq_name_colname. &seq_no_colname.  NO_OF_RECORDS_ACTUAL Estimated_CCF Actual_CCF LowerConf UpperConf;*/
run;

%end;


%else %if &stat = 'GINI' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
Gini = .;
keep  ATTRIBUTE_NAME gini;
run;
%end;
%else %if &stat = 'Getchi' %then /*i18NOK:LINE */
%do;
/*proc sql;*/
/*create table &output_ds as select distinct(put(VARIABLE_NAME,$20.)) as VARIABLE_NAME from &input_ds;*/
/*quit;*/
data &output_ds;
set &output_ds;
p_value =.;
run;
%end;
%else %if &stat = 'infowoe' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
WOE =.;
infoval =.;
keep  ATTRIBUTE_NAME WOE infoval;
run;
%end;
%else %if &stat = 'report_Info_woe' %then /*i18NOK:LINE */
%do;
data &output_ds;
set &input_ds;
WOE =0;
infoval =.;
keep  ATTRIBUTE_NAME ATTRIBUTE_SEQ_NO WOE infoval ;
run;
%end;

%else %if &stat = 'POOLSTAB' %then /*i18NOK:LINE */
%do;
data &output_ds;
   set &input_ds;
   LowerConf=.;
   UpperConf=.;
run;
%end; 
%else %if &stat = 'ConfAR' %then /*i18NOK:LINE */
%do;
data &output_ds;
    length NAME $6.;
	Mid_Pt=.;
	CI_low=.;
	CI_high =.;
	NAME="%sysfunc(sasmsg(smd_ds.bismsg,crs.measure_nm.measure_nm_ar.1, noquote))";
	output;
run;
%end;
%else %if &stat = 'ConfAUC' %then /*i18NOK:LINE */
%do;
data &output_ds;
    length NAME $6.;
	Mid_Pt=.;
	CI_low=.;
	CI_high =.;
	NAME="%sysfunc(sasmsg(smd_ds.bismsg,crs.measure_nm.measure_nm_roc.1, noquote))";
	output;
run;
%end;
%else %if &stat = 'CIER' %then /*i18NOK:LINE */
%do;
data &output_ds;
cier =.;
run;
%end;

%else %if &stat = 'spiegel' %then /*i18NOK:LINE */
%do;
data &output_ds;
spiegel =.;
spip=.;
run;
%end;


%mend csbmva_defdata;
