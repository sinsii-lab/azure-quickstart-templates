/********************************************************************************************************
   	Module		:  rmcr_config_open_source

   	Function	:  This macro will be used to update the Model_source_type_master table with scoring code location and updates the dflt_column_idntfctn_pattern
				   in scoring_output_type_master
	Called-by	:  Risk modeling Content Post Installation Wrapper macro
	Calls		:  None

						
	Author:   CSB Team
	
	Input :	scoring_code_macro_location
	
	Processing:
		-> This macro updates the model_source_type_master table with the scoring_code_macro_nm and scoring_code_macro_location.
				macro variables used are m_scoring_code_macro_nm, m_scoring_code_macro_location
		-> update dflt_column_idntfctn_pattern in scoring_output_type_master

*********************************************************************************************************/

%macro rmcr_config_open_source;

******************************************************************************;
* Start - Update MODEL_SOURCE_TYPE_MASTER table with scoring code path and location;
******************************************************************************;
%let m_scoring_code_macro_location = &m_cr_open_source_macro_path.;
%let m_scoring_code_macro_nm = rmcr_score_python_model;

proc sql noprint;
	update &lib_apdm..MODEL_SOURCE_TYPE_MASTER 
	 set scoring_code_macro_location ="&m_scoring_code_macro_location." ,
	 scoring_code_macro_nm="&m_scoring_code_macro_nm."
	 where kupcase(kstrip(model_source_type_cd)) = 'PYTHON' ;    /* I18NOK:LINE */
quit;

%if sqlobs lt 0 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_ERRMSG.CONFIG_CODE1.1, noquote));
%end;

******************************************************************************;
* Start - update dflt_column_idntfctn_pattern in scoring_output_type_master for prob_1;
******************************************************************************;

proc sql noprint;
	create table scoring_output_type as select scoring_output_type_sk as 
		scoring_output_type_sk, catx(',', dflt_column_idntfctn_pattern, 'var1') as    /* I18NOK:LINE */
		dflt_column_idntfctn_pattern from &lib_apdm..scoring_output_type_master  where 
		kupcase(kstrip(scoring_output_type_cd)) eq "PROB_1";    /* I18NOK:LINE */
quit;


proc sql noprint;
	update &lib_apdm..scoring_output_type_master sotm set 
		dflt_column_idntfctn_pattern=(select dflt_column_idntfctn_pattern from 
		scoring_output_type sot where 
		sotm.scoring_output_type_sk=sot.scoring_output_type_sk) where 
		kupcase(kstrip(scoring_output_type_cd)) eq "PROB_1" and find(dflt_column_idntfctn_pattern,'var1','i') eq 0; /* I18NOK:LINE */
quit;

%if sqlobs lt 0 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_ERRMSG.CONFIG_CODE2.1, noquote));
%end;
/* End - update dflt_column_idntfctn_pattern in scoring_output_type_master for prob_1*/



******************************************************************************;
* Start - update dflt_column_idntfctn_pattern in scoring_output_type_master for pr_value;
******************************************************************************;

proc sql noprint;
	create table scoring_output_type as select scoring_output_type_sk as 
		scoring_output_type_sk, catx(',', dflt_column_idntfctn_pattern, 'var1') as     /* I18NOK:LINE */
		dflt_column_idntfctn_pattern from &lib_apdm..scoring_output_type_master  where 
		kupcase(kstrip(scoring_output_type_cd)) eq "PR_VALUE";      /* I18NOK:LINE */
quit;


proc sql noprint;
	update &lib_apdm..scoring_output_type_master sotm set 
		dflt_column_idntfctn_pattern=(select dflt_column_idntfctn_pattern from 
		scoring_output_type sot where 
		sotm.scoring_output_type_sk=sot.scoring_output_type_sk) where 
		kupcase(kstrip(scoring_output_type_cd)) eq "PR_VALUE" and find(dflt_column_idntfctn_pattern,'var1','i') eq 0;    /* I18NOK:LINE */
quit;

%if sqlobs lt 0 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_ERRMSG.CONFIG_CODE2.1, noquote));
%end;
/* End - update dflt_column_idntfctn_pattern in scoring_output_type_master for pr_value*/
%mend rmcr_config_open_source;