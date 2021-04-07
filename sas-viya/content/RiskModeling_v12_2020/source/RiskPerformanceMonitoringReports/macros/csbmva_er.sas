/*
Macro Name	: csbmva_er
Function	: Generates confusion matrix from given input and calculates various measures. 
Created by	: BIS Team
Date		: sep , 2010
SAS Version	: 9.2

Parameters	: 

input_ds			: Name of the input data set 

output_ds		: Nane fo the date set which stores various measures calculated based on confustion matrix.

Flag :            The flag denotes that output_ds is used for storing measure value or used for data view in the screen.

 
Input Dataset(s) Format:
-------------------------------------------------------------------------
Name				Type		Format
------------------------------------------------------------------------
ACCOUNT_RK          Numeric		Default Format
actual_outcome_value	    Numeric		Default Format
PD                  Numeric		Default Format
estimated_outcome_value  Numeric		Default Format
scorecard_points    Numeric		Default Format
------------------------------------------------------------------------



output_ds(s) Format:
-------------------------------------------------------------------------
Name			Type		Format
------------------------------------------------------------------------
Measure_nm		Character	Default Format
Measure_value	Numeric		Default Format
------------------------------------------------------------------------

Logic:

1. Generate 2X2 table based on variables which stores Actual Good&Bad and Predicted Good&Bad.
2. Calculate various measures like Accuracy, Sensetivity, Specificity and Precision based on Confusion Matrix
   
Notes:
1. It is assumed that data set of specified as input_ds is present and has necessarry required rights.
2. It is assumed that data set of specified as input_ds has valid specified fileds.
3. For all output datasets ,it is assumend that
	a. it has valid name.
	b. Macro has necessary required rights to create the datasets with specified name @ specified location.
	c. Name of the data-set is unique OR if dataset with similar name already exists then it can be safely overwritten.


/*Prints Confusion matrix and related measures */
options mlogic mprint;
%macro csbmva_er(input_ds=,output_ds=,flag=);

%Let NEVENT=0;
%Let EVENT=1;
%Let TP=0;
%Let FN=0;
%Let FP=0;
%Let TN=0;

%*dabt_initiate_cas_session(cas_session_ref=cal_er_measure);

proc freqtab data=&input_ds noprint;
tables estimated_outcome_value*actual_outcome_value /out=output_ConfMat_ds(drop=PERCENT) norow nocol nopercent;
run;

data _NULL_;
set output_ConfMat_ds;
if estimated_outcome_value=&NEVENT and actual_outcome_value=&NEVENT then
	call symput('TN',count);/*i18NOK:LINE */
else if estimated_outcome_value=&NEVENT and actual_outcome_value=&EVENT then
	call symput('FN',count);/*i18NOK:LINE */
else if estimated_outcome_value=&EVENT and actual_outcome_value=&NEVENT then
	call symput('FP',count);/*i18NOK:LINE */
else if estimated_outcome_value=&EVENT and actual_outcome_value=&EVENT then
	call symput('TP',count);/*i18NOK:LINE */
run;

%*dabt_terminate_cas_session(cas_session_ref=cal_er_measure);

%if &flag =1 %then %do;
data _NULL_;

name=put(time(),mmss8.1);

name1=kscan(name,1,":.");

name2=kscan(name,2,":.");

name3=kscan(name,3,":.");

name4=ktrim(kleft(name1))||ktrim(kleft(name2))||ktrim(kleft(name3));/*i18NOK:LINE */

call symput('log_name',"&LOG_PATH_CS"||"&sysmacroname"||ktrim(kleft(name4))||".log");/*i18NOK:LINE */

run;



data &output_ds(drop = i);
length Actual $10.;
do i  = 1 to 2;
if i =1 then do;
Predicted_non_event =&TN ;
Predicted_event = &FP;
Actual = "GOOD";/*i18NOK:LINE *//* The value GOOD is read as a code in midtier and a resource bundle key is mainteined to resolve the locale specific value*/
total = Predicted_non_event+Predicted_event;
OUTPUT;
END;
IF i =2 THEN DO;
Predicted_non_event = &FN;
Predicted_event = &TP;
Actual = "BAD";/*i18NOK:LINE *//* The value BAD is read as a code in midtier and a resource bundle key is mainteined to resolve the locale specific value*/
total = Predicted_non_event+Predicted_event;
OUTPUT;
END;
END;
RUN;
proc sql noprint;
select sum(Predicted_non_event ) , sum(Predicted_event) , sum(total) into: a,:b,:c from
&output_ds;
quit;
%put &a;
proc sql noprint;
insert into &output_ds
(Actual,Predicted_non_event,Predicted_event,total)
values ("TOTAL",&a,&b,&c);/*i18NOK:LINE *//* The value TOTAL is read as a code in midtier and a resource bundle key is mainteined to resolve the locale specific value*/
quit;

%end;



%if &flag = 2 %then %do;


/*Prints Accuracy and Sensetivity Measures*/
data work.measure;
%if %eval(&tp + &fp + &tn + &fn) ne 0 %then  %do;
accura=(&TP+&TN)/(&tp + &fp + &tn + &fn);
er=1.0 -accura;
%end;
%else %do;
	accura=.;
	er = .;
%end;
%if  %eval(&FN+&TP) ne 0 %then %do;
	sens= &TP/(&FN+&TP);
%end;
%else %do;
	sens=.;
%end;
%if  %eval(&TN+&FP) ne 0 %then %do;	
	spec= &TN/(&TN+&FP);
%end;
%else %do;
	spec =.;
%end;
%if %eval(&TP+&FP) ne 0 %then %do;
     prec = &TP/ (&TP+&FP);
%end;
%else %do;
	prec =.;
%end;
run;
 
proc transpose data=work.measure out=&output_ds NAME=Measure_nm;
run;

data &output_ds;
set &output_ds(rename=(COL1=Measure_value));
label Measure_nm=;
run;

%end;


%mend csbmva_er;
