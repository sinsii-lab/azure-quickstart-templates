%macro rmcr_update_parameter_value();

	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;
	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

/*--------------------------------------------------
Update parameter values in case promotion done in v03
---------------------------------------------------*/
proc sql;
/* I18NOK:BEGIN */
update &lib_apdm..parameter_value_dtl
set parameter_value="( kupcase(TYPE) in ('DOUBLE','INT64', 'INT32') and ( kupcase(SUBSTR(NAME ,length(NAME)-4,5))='_DTTM' OR substr(FORMAT,1,8) = 'DATETIME' OR substr(FORMAT,1,6) = ('NLDATM')))" /* i18nOK:LINE */
where kupcase(parameter_nm)="CASE_CONDN_DTTM_DATA_TYPE";

update &lib_apdm..parameter_value_dtl
set parameter_value="( kupcase(TYPE) in ('DOUBLE','INT64', 'INT32') and ( kupcase(SUBSTR(NAME ,length(NAME)-2,3))='_DT' OR ( substr(FORMAT,1,4) = 'DATE' AND substr(FORMAT,1,8) <> 'DATETIME' ) OR substr(FORMAT,1,6) = ('NLDATE') ))"
where kupcase(parameter_nm)="CASE_CONDN_DATE_DATA_TYPE";

update &lib_apdm..parameter_value_dtl
set parameter_value="( kupcase(TYPE) in ('DOUBLE','INT64', 'INT32'))"
where kupcase(parameter_nm)="CASE_CONDN_NUM_DATA_TYPE";

update &lib_apdm..parameter_value_dtl
set parameter_value="( kupcase(TYPE)='CHAR' and ( kupcase(SUBSTR(NAME,length(NAME)-2,3))='_CD' or ( kupcase(NAME) contains '_FLG' and LENGTH(NAME) = 1)))"
where kupcase(parameter_nm)="CASE_CONDN_DIM_COLUMN_TYPE";

update &lib_apdm..parameter_value_dtl
set parameter_value="( kupcase(TYPE) in ('DOUBLE','INT64', 'INT32')) and ( kupcase(SUBSTR(NAME ,length(NAME)-3,4)) in ('_AMT', '_CNT', '_PCT') or kupcase(SUBSTR(NAME ,length(NAME)-5,6)) in ( '_VALUE') or kupcase(SUBSTR(NAME ,length(NAME)-2,3)) in ('_RT') )"
where kupcase(parameter_nm)="CASE_CONDN_MSR_COLUMN_TYPE";

update &lib_apdm..parameter_value_dtl
set parameter_value="( kupcase(TYPE) in ('DOUBLE','INT64', 'INT32') and kupcase(SUBSTR(NAME ,length(NAME)-2,3)) in ('_RK', '_SK') )"
where kupcase(parameter_nm)="CASE_CONDN_KEY_COLUMN_TYPE";

update &lib_apdm..parameter_value_dtl
set parameter_value="( kupcase(TYPE)='VARCHAR')"
where kupcase(parameter_nm)="CASE_CONDN_CHR_DATA_TYPE";

update &lib_apdm..parameter_value_dtl
set parameter_value="/&M_FILE_SRVR_ROOT_FOLDER_NM./External Code"
where kupcase(parameter_nm)="DABT_EXTERNAL_CODE_PATH_LOCATION";
/* I18NOK:END */
quit;

%dabt_err_chk(type=SQL);

%if &job_rc gt 4 %then %do;
	%goto exit;
%end;

%exit:

%mend rmcr_update_parameter_value;