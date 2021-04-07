%macro rmcr_update_drvd_and_fltr_exp();

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;
	
%let path_of_internal_folder=&m_staging_area_path./cs63_export_pkg/internal_data;


/**************************************************
Update CAS compliant derived variable expression
***************************************************/
data DERIVED_VAR_EXPRESSION;
length DERIVED_VAR_CALC_EXPRESSION $10000.;
infile "&path_of_internal_folder./derived_variable_expression.csv" dlm='~' dsd firstobs=2;    /* i18nOK:LINE */
input OVERWRITE_FLG $ VARIABLE_SK DERIVED_VAR_CALC_EXPRESSION $ PROJECT_ID PROJECT_NAME $ PROJECT_OWNER $;
run;

proc sql;
	update &lib_apdm..DERIVED_VARIABLE drvd_apdm set derived_var_calc_expression = 
	(select DERIVED_VAR_CALC_EXPRESSION from work.DERIVED_VAR_EXPRESSION  drvd_wrk where drvd_apdm.variable_sk=drvd_wrk.variable_sk )
	where drvd_apdm.variable_sk in ( select variable_sk from work.DERIVED_VAR_EXPRESSION) ;
quit;

%dabt_err_chk(type=SQL);
		
%if &job_rc gt 4 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.SOL_CONFIG_SM6.1, noquote)); 
	%goto exit;
%end;
		
/**************************************************
Update CAS compliant filter expression
***************************************************/
data TARGET_NODE_EXPRESSION;
length FILTER_EXPRESSION $10000.;
infile "&path_of_internal_folder./filter_expression.csv" dlm='~' dsd firstobs=2;    /* i18nOK:LINE */
input OVERWRITE_FLG $ TARGET_NODE_SK FILTER_EXPRESSION ~$;
run;

proc sql;
	update &lib_apdm..TARGET_NODE_EXPRESSION trgt_node_apdm set user_entered_value_txt = 
	(select FILTER_EXPRESSION from work.TARGET_NODE_EXPRESSION trgt_node_wrk where trgt_node_apdm.TARGET_NODE_SK=trgt_node_wrk.TARGET_NODE_SK )
	where trgt_node_apdm.TARGET_NODE_SK in (select TARGET_NODE_SK from work.TARGET_NODE_EXPRESSION);
	update &lib_apdm..TARGET_NODE_EXPRESSION set user_entered_value_txt = '.' where user_entered_value_flg='Y' and user_entered_value_txt is missing;    /* i18nOK:LINE */
quit;

%dabt_err_chk(type=SQL);
		
%if &job_rc gt 4 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.SOL_CONFIG_SM6.1, noquote)); 
	%goto exit;
%end;

%exit:

%mend rmcr_update_drvd_and_fltr_exp;
