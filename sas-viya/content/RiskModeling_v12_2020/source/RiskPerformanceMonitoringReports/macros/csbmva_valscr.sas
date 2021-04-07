




%macro csbmva_valscr (input_ds= , output_ds= ,flag=,seq_no_colname=,seq_name_colname=,max_range_value_colname=,est_result_col_name=,risk_incr_with_value_incr_flg=);


%if &flag = 1 %then %do;
data _NULL_;

name=put(time(),mmss8.1);

name1=kscan(name,1,":.");

name2=kscan(name,2,":.");

name3=kscan(name,3,":.");

name4=ktrim(kleft(name1))||ktrim(kleft(name2))||ktrim(kleft(name3));/*i18NOK:LINE */

call symput('log_name',"&LOG_PATH_CS"||"&sysmacroname"||ktrim(kleft(name4))||".log");/*i18NOK:LINE */

run;

%end;



%csbmva_ds (input_ds=&input_ds, output_ds=disstat, flag=2,max_range_value_colname=&max_range_value_colname.,est_result_col_name=&est_result_col_name.,seq_no_colname=&seq_no_colname.,seq_name_colname=&seq_name_colname.,risk_incr_with_value_incr_flg=&risk_incr_with_value_incr_flg.);
%csbmva_ks(input_ds=&input_ds,output_ds=ks,flag=2,max_range_value_colname=&max_range_value_colname.,est_result_col_name=&est_result_col_name.,seq_no_colname=&seq_no_colname.,seq_name_colname=&seq_name_colname.,risk_incr_with_value_incr_flg=&risk_incr_with_value_incr_flg.);
%csbmva_auc(input_ds=&input_ds,output_ds=auc1,flag=2,max_range_value_colname=&max_range_value_colname.,est_result_col_name=&est_result_col_name.,seq_no_colname=&seq_no_colname.,seq_name_colname=&seq_name_colname.,risk_incr_with_value_incr_flg=&risk_incr_with_value_incr_flg.);
%csbmva_ar(input_ds=&input_ds,output_ds=ar,flag=2,max_range_value_colname=&max_range_value_colname.,est_result_col_name=&est_result_col_name.,seq_no_colname=&seq_no_colname.,seq_name_colname=&seq_name_colname.,risk_incr_with_value_incr_flg=&risk_incr_with_value_incr_flg.);


proc sql noprint;

select max(ks_act)
	into: ks_val
	from work.ks;

select  ar 	
into: ar_val
from work.ar;

select COL1 
       into: auc_val	  
from work.auc1
 where kupcase(_NAME_) eq kupcase("auc") ;/*i18NOK:LINE */

quit;

proc sql noprint;
insert into Disstat
(_NAME_,COL1)
values 
("ks",&ks_val)/*i18NOK:LINE */
values
("ar",&ar_val)/*i18NOK:LINE */
values
("roc",&auc_val)	/*i18NOK:LINE */
;
quit;


%csbmva_valscr_batch(input_ds=apdm.Validation_grade,output_ds=validation_score2);


%csbmva_interpolation1(input_ds =disstat ,validation_score=validation_score2,output_ds=&output_ds);

data &output_ds;
set &output_ds (rename =(COL1 = measure_value) rename =(grade =validation_score) rename =(Measure_nm =MEASURE_CD));
measure_cd = compress(measure_cd);/*i18NOK:LINE */
run;


data measure_dim;
set &csb_apdm_libref..measure_master (keep = measure_cd measure_nm );
measure_cd = compress(measure_cd);/*i18NOK:LINE */
run;


proc sql;
insert into measure_dim
(measure_cd,measure_nm)
values
("roc" ,"ROC Statistic");/*i18NOK:LINE */
quit;



proc sort data = &output_ds;
by measure_cd;
run;

proc sort data = measure_dim;
by measure_cd;
run;

data &output_ds;
merge &output_ds(in =a) measure_dim(in = b);
by measure_cd;
if a =1 and b =1 then output &output_ds;
run;




data &output_ds;
set &output_ds (keep = measure_nm measure_value validation_score );
run;

data &output_ds;
retain measure_nm measure_value validation_score ;
set &output_ds;
run;


%mend csbmva_valscr ;
