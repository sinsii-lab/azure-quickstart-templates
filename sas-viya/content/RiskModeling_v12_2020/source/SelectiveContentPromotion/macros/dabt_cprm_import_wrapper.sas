/********************************************************************************************************
   	Module		:  dabt_cprm_import_wrapper

   	Function	:  This macro imports all the specified entities on the target machine.

   	Parameters	:  import_package_path  	  	 	-> Location where the package from the source machine has been kept
                   import_analysis_report_path		-> Location at which the Pre Validation report will be created
                   mode								-> Execution options
															ANALYSE - Validates whether all the entities can be successfully imported on the target machine
															EXECUTE - Imports the entities on the target machine
					
*********************************************************************************************************/
%macro dabt_cprm_import_wrapper (import_package_path = ,import_analysis_report_path = , mode = ) ;

%let syscc = 0;

%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

%if "&import_analysis_report_path" eq "" %then %do;
	%let import_analysis_report_path = %sysfunc(pathname(work));
%end;
*==================================================;
* Defining local macro variables ;
*==================================================;

%local m_cprm_src_apdm ; /*Stores libref of source apdm. dabt_assign_lib macro will assign value to this*/

%local m_cprm_scr_lib ;  /*Stores libref for scratch. dabt_assign_lib macro will assign value to this*/

%local m_cprm_imp_ctl  ; /*Stores libref for control library. This lib will have CPRM_IMPORT_PARAMETER_LIST. */

%local m_cprm_log_path  ; /*Stores the path where the logs will be created */

%let import_spec_file = cprm_import_specification_file.csv;

*==================================================;
* Validate input Parameters ;
*==================================================;

/* 1. Parameter : import_package_path */ 
/* %let rc = %sysfunc(filename(fileref,&import_package_path.)) ;  */
/* %if %sysfunc(fexist(&fileref)) eq 0 %then %do; */
/* 	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR11, noquote, &import_package_path.) ); */
/* 	%abort ; */
/* %end; */
/*  */
/* %let rc = %sysfunc(filename(fileref,&import_analysis_report_path.)) ;  */
/* %if %sysfunc(fexist(&fileref)) eq 0 %then %do; */
/* 	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR2, noquote, &import_analysis_report_path.) ); */
/* 	%abort ; */
/* %end; */

/* 2. Parameter : import_analysis_report_path */ 
libname _tst "&import_analysis_report_path.";

data _tst.x;
run;

%if &syserr > 0 %then %do;
	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR3, noquote, &import_analysis_report_path.) );
	%return ;
%end;

proc datasets lib = _tst noprint nodetails;
	delete x;
quit;

/* 3. Parameter : mode */ 
%if %upcase(&mode) ne ANALYZE AND %upcase(&mode) ne ANALYSE and %upcase(&mode) ne EXECUTE %then %do;
	%let job_rc=1012;
	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR4, noquote, &import_analysis_report_path.) ) ANALYZE/ANALYSE/EXECUTE;
	%return ;
%end;

%if %upcase(&mode) eq ANALYZE %then %do;
	%let mode = ANALYSE;
%end;

%let mode = %upcase(&mode);

*==================================================;
* Create directories for staging tables and logs;
*==================================================;

/* dabt_assign_libs will declare library and send back librefs*/
%dabt_assign_libs(tmp_lib=m_cprm_scr_lib,m_workspace_type=CPRM_IMP,src_lib = m_apdm_lib,
                    import_analysis_report_path = &import_analysis_report_path., m_cprm_src_apdm_lib= m_cprm_src_apdm, 
                    m_cprm_ctl_lib = m_cprm_imp_ctl,log_path = m_cprm_log_path);

*==================================================;
* Cleaning up the older data;
*==================================================;

/* %dabt_dir_delete(dirname = &import_analysis_report_path./logs,deleteMode=FILE);  */
/* %dabt_dir_delete(dirname = &import_analysis_report_path./control,deleteMode=FILE); */
/* %dabt_dir_delete(dirname = &import_analysis_report_path./scratch,deleteMode=FILE); */
/*i18NOK:BEGIN*/
data _null_;
    rc=filename('fname', "&import_analysis_report_path./cprm_pre_import_analysis_report.csv");	
    if rc = 0 and fexist('fname') then	
       rc=fdelete('fname');
run;  
/*i18NOK:END*/
*==================================================;
* Diverting log;
*==================================================;

/* Redirecting the logs. */
/* proc printto log = "&m_cprm_log_path.dabt_cprm_import_wrapper.log" new;	/*i18NOK:LINE */
/* run; */

*==================================================;
* Extract source APDM data ;
*==================================================;

*filename _xpt "&import_package_path./source_apdm.xpt";	/*i18NOK:LINE*/


/*%if %sysfunc(fexist(_xpt)) eq 0 %then %do;
	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR6, noquote, source_apdm.xpt, &import_package_path.) );
	%return ;
%end;*/
filename apdm_xpt filesrvc folderpath="&import_package_path." filename= "source_apdm.xpt" debug=http CD="attachment; filename=source_apdm.xpt";
proc cimport lib = &m_cprm_src_apdm. infile = apdm_xpt;	/*i18NOK:LINE*/
quit;

*filename _xpt clear;

*==================================================;
* Create the Dataset for Pre-Import Analysis report ;
*==================================================;

proc sql;

	CREATE TABLE &m_cprm_scr_lib..CPRM_PRE_IMPORT_ANALYSIS_DTL (
		PROMOTION_ENTITY_NM VARCHAR(5000) NOT NULL, 
		PROMOTION_ENTITY_TYPE_CD VARCHAR(10) NOT NULL,
		ASSOC_ENTITY_NM  VARCHAR(5000) NOT NULL, 
		ASSOC_ENTITY_TYPE_CD  VARCHAR(10) NOT NULL,
		PRESENT_IN_TGT_FLG CHAR(1) ,
		PRESENT_IN_IMPORT_PACKAGE_FLG  CHAR(1) ,
		REFERRED_IN_OTHER_ENTITY_FLG CHAR(1),
		UNIQUE_CONSTRAINT_VIOLATION_FLG  CHAR(1),
		DIFFERENT_DEFN_FLG CHAR(1),
		ASS_ENT_IMPORT_ACTION_CD VARCHAR(60) NOT NULL,
		ASS_ENT_IMPORT_ACTION_DESC VARCHAR(1200) NOT NULL,		
		ADDNL_INFO VARCHAR(6000)
	);

quit;

*==================================================;
* Create control table for MasterLoop ;
*==================================================;

filename imp_file filesrvc folderpath="&import_package_path." filename= "&import_spec_file." debug=http CD="attachment; filename=&import_spec_file.";


%if %sysfunc(fexist(imp_file)) eq 0 %then %do;
	%let job_rc=1012;
	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR7, noquote, &import_package_path.) );
	%return;
%end;

PROC IMPORT OUT= &m_cprm_scr_lib..cprm_export_spec_tmp DATAFILE= imp_file 
            DBMS=csv REPLACE; 
     GETNAMES=NO ; guessingrows=32767; DATAROW = 2;
RUN;

data &m_cprm_scr_lib..cprm_export_specification;
	set &m_cprm_scr_lib..cprm_export_spec_tmp;
	rename var1 = ENTITY_TYPE_CD;
	rename var2 = ENTITY_TYPE_NM;
	rename var3 = ENTITY_KEY;
	rename var4 = ENTITY_NM;
	rename var5 = ENTITY_DESC;
	rename var6 = PROMOTE_FLG;
	rename var7 = USER;
run;


proc sql;

	CREATE TABLE &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST (
		JOB_NO NUM(8) NOT NULL, 
		ENTITY_TYPE_CD VARCHAR(30) NOT NULL,
		ENTITY_TYPE_NM  VARCHAR(360) NOT NULL, 
		ENTITY_KEY  NUM(8) NOT NULL,
		ENTITY_NM VARCHAR(360) NOT NULL ,
		ENTITY_DESC  VARCHAR(1800) ,
		PROMOTE_FLG CHAR(1) NOT NULL,
		USER VARCHAR(100),
		ENTITY_IMPORT_SEQ_NO  NUM(8) NOT NULL,
		IMPORT_MACRO_NM VARCHAR(1800) NOT NULL,
		IMPORT_ANALYSIS_RETURN_CD NUM(8), 
		IMPORT_EXECUTION_RETURN_CD NUM(8),		
		CONSTRAINT PRIM_KEY PRIMARY KEY (ENTITY_TYPE_CD, ENTITY_KEY,ENTITY_IMPORT_SEQ_NO)
	);

quit;

proc sql nowarn; /* S1443811 : nowarn added to suppress warning while inserting into char cols */
	insert into &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST(
		JOB_NO, ENTITY_TYPE_CD, ENTITY_TYPE_NM, ENTITY_KEY, ENTITY_NM, ENTITY_DESC, PROMOTE_FLG,USER, ENTITY_IMPORT_SEQ_NO, IMPORT_MACRO_NM, IMPORT_ANALYSIS_RETURN_CD,IMPORT_EXECUTION_RETURN_CD 
	) 
	select
		monotonic() as JOB_NO,
		a.ENTITY_TYPE_CD as ENTITY_TYPE_CD,
		a.ENTITY_TYPE_NM as ENTITY_TYPE_NM,
		b.ENTITY_KEY as ENTITY_KEY,
		b.ENTITY_NM as ENTITY_NM,
		b.ENTITY_DESC as ENTITY_DESC,
		'Y' as PROMOTE_FLG,			/*i18NOK:LINE*/
		b.USER as USER,
		a.ENTITY_IMPORT_SEQ_NO,
		a.IMPORT_MACRO_NM,
		. as IMPORT_ANALYSIS_RETURN_CD,
		. as IMPORT_EXECUTION_RETURN_CD
	from &lib_apdm..CPRM_ENTITY_MASTER a , &m_cprm_scr_lib..cprm_export_specification b
	where a.entity_type_cd = b.ENTITY_TYPE_CD
	AND PROMOTE_FLG = &CHECK_FLAG_TRUE.;
quit;

data &m_cprm_imp_ctl..cprm_import_param_list_tmp;
set &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST;
run;

%let param_list_row_cnt = 0;
proc sql noprint;
	select count(*) into :param_list_row_cnt		/*i18NOK:LINE*/
	from &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST;
quit;

%if &param_list_row_cnt = 0 %then %do;
	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR8, noquote) );
	%return;
%end;

*============================================================================;
* Extract pool development data ;
*============================================================================;

%local pool_import_exist;
%let pool_import_exist = 0;

proc sql noprint;
	select 1 into :pool_import_exist
	from &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST
	where ENTITY_TYPE_CD = 'POOL';				/*i18NOK:LINE*/
quit;

%if &pool_import_exist > 0 %then %do;
			
	filename pool_xpt "&import_package_path./pool/pool.xpt";	/*i18NOK:LINE*/

	%if %sysfunc(fexist(pool_xpt)) eq 0 %then %do;
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR6, noquote, pool.xpt, %quote(&import_package_path./pool) );
		%return ;
	%end;
	%else %do;
		%local path_of_pool_dev_data ; 
		%dabt_make_work_area(dir=&import_analysis_report_path./scratch, create_dir=source_pool_dev_data, path=path_of_pool_dev_data);
			
		libname cprmpool "&import_analysis_report_path./scratch/source_pool_dev_data";	/*i18NOK:LINE*/
		proc cimport lib = cprmpool infile = "&import_package_path./pool/pool.xpt";		/*i18NOK:LINE*/
		quit;

		filename pool_xpt clear;

	%end;

%end;

*==================================================;
* Include the Master Loop Job for pre-validation ;
*==================================================;

%let user_provided_mode = &mode.;
%let mode = ANALYSE;

%if &syscc > 4 %then %do;
	%goto ERROR_STATE;
%end;

%let syscc_before_job = &syscc.;

/*%let dabtjobs_path = %sysfunc(pathname(dabtjobs,F));

%include "&dabtjobs_path./MasterLoopValidateImportJob.sas";*/	
%let m_parm_cnt=;

proc sql noprint;
select count(*) into :m_parm_cnt from &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST
where PROMOTE_FLG='Y';
quit;

proc sql noprint;
create table &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST_Y
as
select * from &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST
where PROMOTE_FLG='Y';
quit;

%let m_parm_cnt=&m_parm_cnt;

%do imp=1 %to &m_parm_cnt;

	data _null_;
	obs=&imp;
	set &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST_Y point=obs;
	call symputx('entity_key',entity_key);
	call symputx('job_no',job_no);
	call symputx('owned_by',user);
	call symputx('m_entity_cd',ENTITY_TYPE_CD);
	call execute('%nrstr('||import_macro_nm||')');
	stop;
	run;

	/*If entity type is project or model validate user in Analyse Model*/
	%if &m_entity_cd eq MODEL or &m_entity_cd eq PROJECT %then %do;

		proc http url="&BASE_URI./identities/users/&owned_by"/* i18nOK:Line */
						 method='HEAD'/* i18nOK:Line */
						 oauth_bearer=sas_services
						 ct="application/json";/* i18nOK:Line */
						 DEBUG LEVEL=3;
						run; quit;

		%if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %do;
			%let job_rc=1012;
			/*%put ERROR: User &owned_by mentioned in cprm_import_specification_file for &m_entity_cd not found in target ;*/
			%put %sysfunc(sasmsg(work.DABT_CPRM_MISC, RMCR_CPRM_IMPORT_WRAPPER1.1, noquote, &owned_by., &m_entity_cd.) );
			%return;
		%end;

	%end;

	Libname cpctrl "&import_analysis_report_path/control";

	%let param_table = cpctrl.CPRM_IMPORT_PARAMETER_LIST;
	%let job_rc = &syscc;
	%dabt_generic_on_success (job_no = &job_no, param_table= &param_table, return_cd_column_nm = IMPORT_ANALYSIS_RETURN_CD);

%end;

/*VDMML model*/
%dabt_cprm_import_vdmml;

%if &syscc_before_job > &syscc. %then %do;
	%let syscc = &syscc_before_job;
%end;

/* Redirecting the logs. */
/* proc printto log = "&m_cprm_log_path.dabt_cprm_import_wrapper.log";		/*i18NOK:LINE */
/* run; */
*==================================================;
* If application in debug mode upload xpt to job sk ;
*==================================================;
%if &DABT_DEBUG_FLG eq Y %then %do;

	Libname cpctrl "&import_analysis_report_path/control"; 
	filename ctrl_xpt filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk" filename= "debug_control.xpt" debug=http CD="attachment; filename=debug_control.xpt";
	proc cport lib = cpctrl file = ctrl_xpt memtype=data;	/*i18NOK:LINE*/
	quit;
	
	Libname cpscr "&import_analysis_report_path/scratch"; 
	filename scr_xpt filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk" filename= "debug_scratch.xpt" debug=http CD="attachment; filename=debug_scratch.xpt";
	proc cport lib = cpscr file = scr_xpt memtype=data;	/*i18NOK:LINE*/
	quit;
	
%end;

*==================================================;
* Generate PreValidation Report ;
*==================================================;

/****************************************************************
 As part of CSBCR-1411, error message codes and description is changed
	NO_ERROR_IMPORT changed to PRE_IMPORT_ANALYSIS_SUCCESS
	NO_ERROR_DELETE_FROM_TGT chnaged to DELETE_FROM_TGT_ANALYSIS_SUCCESS

*******************************************************************/
data &m_cprm_scr_lib..CPRM_PRE_IMPORT_ANALYSIS_DTL;
	set &m_cprm_scr_lib..CPRM_PRE_IMPORT_ANALYSIS_DTL;
	if ASS_ENT_IMPORT_ACTION_CD = "NO_ERROR_IMPORT" then do ;
		ASS_ENT_IMPORT_ACTION_CD="PRE_IMPORT_ANALYSIS_SUCCESS";
		ASS_ENT_IMPORT_ACTION_DESC="%sysfunc(sasmsg(work.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC1, noquote))";
	end;
	else if ASS_ENT_IMPORT_ACTION_CD = "NO_ERROR_DELETE_FROM_TGT" then do ;
		ASS_ENT_IMPORT_ACTION_CD="DELETE_FROM_TGT_ANALYSIS_SUCCESS";
		ASS_ENT_IMPORT_ACTION_DESC="%sysfunc(sasmsg(work.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC2, noquote))";
	end;
run;

data &m_cprm_scr_lib..CPRM_PRE_IMPORT_ANLYS_OUT;
	set &m_cprm_scr_lib..CPRM_PRE_IMPORT_ANALYSIS_DTL;
	drop ASS_ENT_IMPORT_ACTION_CD ADDNL_INFO;
run;


/*filename exp_file "&import_analysis_report_path./cprm_pre_import_analysis_report.csv" &dabt_csv_export_encoding_option;	/*i18NOK:LINE*/
/*CSV file for Pre import analysis report will be available in job_sk folder*/

filename exp_file filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk" filename= "cprm_pre_import_analysis_report.csv" debug=http CD="attachment; filename=cprm_pre_import_analysis_report.csv";/* i18nOK:Line */

proc export
     data=&m_cprm_scr_lib..CPRM_PRE_IMPORT_ANALYSIS_DTL
     outfile = exp_file  /* i18NOK:LINE */
     DBMS= csv
     replace label;
 run;

*==================================================;
* Check if there are errors in PreValidation ;
*==================================================;

/* Check for errors */
%let m_error = 0;
proc sql noprint;
	select 
		count(*) into :m_error		/*i18NOK:LINE*/
	from 
		&m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST
	where
		import_analysis_return_cd is NULL or import_analysis_return_cd > 4;
quit;

%if &m_error > 0 or &syscc > 4 %then %do;
	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR9, noquote) );
	%return ;
%end;

*==================================================;
* Include the Master Loop Job for import ;
*==================================================;

%if &user_provided_mode = EXECUTE %then %do;

	%let mode = EXECUTE;
	
/* 	%include "&dabtjobs_path./MasterLoopImportEntityJob.sas";		/*i18NOK:LINE */
		
	%let m_parm_cnt=;

	proc sql noprint;
	select count(*) into :m_parm_cnt from &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST
	where PROMOTE_FLG='Y';
	quit;

	proc sql noprint;
	create table &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST_Y
	as
	select * from &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST
	where PROMOTE_FLG='Y';
	quit;

	%let m_parm_cnt=&m_parm_cnt;
	
	/************************************************* CSBCR-14112 START **********************************************************
		As part of CSBCR-14112, added below logic to maintain info on source target IDs mapping for project and model
		for each run it creates a dataset source_target_entity_mapping and a csv. 
		for each run the records gets inserted in to apdm.CPRM_SRC_TGT_ENTITY_MAPPING table with latest_import_flg = Y 
	
	*****************************************************************************************************************/
	%dabt_drop_table(m_table_nm=&m_cprm_imp_ctl..source_target_entity_mapping); 	
	
	%let cprm_entity_type_cd_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_CD_LABEL, noquote)); 
	%let cprm_source_entity_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_SOURCE_ENTITY_NM_LABEL, noquote)); 
	%let cprm_source_entity_id_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_SOURCE_ENTITY_ID_LABEL, noquote)); 
	%let cprm_target_entity_id_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_TARGET_ENTITY_ID_LABEL, noquote)); 
	%let cprm_created_dttm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_CREATED_DTTM_LABEL, noquote)); 
	%let cprm_created_by_user_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_CREATED_BY_USER_LABEL, noquote)); 
	
	proc sql noprint;
		create table &m_cprm_imp_ctl..source_target_entity_mapping ( 
			ENTITY_TYPE_CD  character(10) not null 
					label="&cprm_entity_type_cd_label.", 
			SOURCE_ENTITY_NM  character(360) not null 
					label="&cprm_source_entity_nm_label.", 
			SOURCE_ENTITY_ID  numeric(10) not null 
					label="&cprm_source_entity_id_label.", 
			TARGET_ENTITY_ID  numeric(10)  not null 
					label="&cprm_target_entity_id_label.", 
			CREATED_DTTM  date not null FORMAT =DATETIME25.6 INFORMAT=DATETIME25.6
					label="&cprm_created_dttm_label.", 
			CREATED_BY_USER  character(360)
					label="&cprm_created_by_user_label."
	
		);
	quit;

	
	%do imp=1 %to &m_parm_cnt;

		data _null_;
		obs=&imp;
		set &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST_Y point=obs;
		call symputx('entity_key',entity_key);
		call symputx('src_entity_nm',entity_nm);
		call symputx('src_entity_type_cd',entity_type_cd);
		call symputx('job_no',job_no);
		call symputx('owned_by',user);
		call execute('%nrstr('||import_macro_nm||')');
		stop;
		run;


		Libname cpctrl "&import_analysis_report_path/control";

		%let param_table = cpctrl.CPRM_IMPORT_PARAMETER_LIST;
		%let job_rc = &syscc;
		
		/********  CSBCR-14112 Start - Logic to update apdm and create dataset *****/
		%if &job_rc le 4 %then %do;
			%if &src_entity_type_cd. eq PROJECT or &src_entity_type_cd. eq MODEL %then %do;
				proc sql noprint;
					insert into &m_cprm_imp_ctl..source_target_entity_mapping 
					(ENTITY_TYPE_CD,SOURCE_ENTITY_NM,SOURCE_ENTITY_ID,TARGET_ENTITY_ID,CREATED_DTTM,CREATED_BY_USER)
					VALUES ("&src_entity_type_cd.","&src_entity_nm.",&entity_key.,&m_tgt_mpng_sk.,"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid")
					;
				quit;
				
				proc sql;
					update &lib_apdm..CPRM_SRC_TGT_ENTITY_MAPPING 
						set LATEST_IMPORT_FLG = "N" where ENTITY_TYPE_CD = "&src_entity_type_cd." 
						and TARGET_ENTITY_ID = &m_tgt_mpng_sk.
					;
				quit;
				
				proc sql noprint;
					insert into &lib_apdm..CPRM_SRC_TGT_ENTITY_MAPPING 
					(ENTITY_TYPE_CD,SOURCE_ENTITY_NM,SOURCE_ENTITY_ID,TARGET_ENTITY_ID,LATEST_IMPORT_FLG,CREATED_DTTM,CREATED_BY_USER)
					values ("&src_entity_type_cd.","&src_entity_nm.",&entity_key.,&m_tgt_mpng_sk.,"Y","%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid")
					; 
				QUIT; 
			
			%end;
		%end;
		/******** CSBCR-14112 End ****/
		%dabt_generic_on_success (job_no = &job_no, param_table= &param_table, return_cd_column_nm = IMPORT_EXECUTION_RETURN_CD);

	%end;

/************************ START - Export source target mapping info dataset ********/

	proc sql noprint; 
		select count(*) into: m_mpng_rec_cnt /* i18nOK:Line */
			from &m_cprm_imp_ctl..source_target_entity_mapping
	quit;
	
	%dabt_err_chk(type=SQL);

	%let m_mpng_rec_cnt = &m_mpng_rec_cnt.;
	
	%if &m_mpng_rec_cnt. gt 0 %then %do;
		proc sort data=&m_cprm_imp_ctl..source_target_entity_mapping out=&m_cprm_imp_ctl..source_target_entity_mapping;
			by entity_type_cd source_entity_nm;
		run;
				
	
	filename mpg_file filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk" filename= "source_target_entity_mapping.csv" encoding='utf-8' debug=http; /* i18nOK:Line */
		
		/*Data present in &m_cprm_imp_ctl..source_target_entity_mapping table will be exported to csv format*/
		proc export
			data=&m_cprm_imp_ctl..source_target_entity_mapping
			outfile = mpg_file  /* i18NOK:LINE */
			DBMS= csv
			replace label;
		run;
		
		/*Export check : If export is not successful. Put message in log and return.*/
		%if &syserr. ne 0 %then %do;
			/* Some issue occured while creation of export mapping csv file */
			%put  %sysfunc(sasmsg(work.DABT_CPRM_MISC, ERR_EXPORT_SRC_TGT_MAP_EXCEL_NOT_CREATED noquote));	
		%end;
	%end;
	%else %do;
		/* Noting found to export to entity mapping CSV file.  */
		%put  %sysfunc(sasmsg(work.DABT_CPRM_MISC, ERR_EXPORT_SRC_TGT_MAP_EMPTY_DS, noquote));	
	%end;	

/************************ End - Export source target mapping info dataset ********/

	/*VDMML model*/
%dabt_cprm_import_vdmml;
	/*Redirecting the logs.*/
/* 	proc printto log = "&m_cprm_log_path.dabt_cprm_import_wrapper.log";	/*i18NOK:LINE */
/* 	run; */
	*==================================================;
	* If application in debug mode upload xpt to job sk ;
	*==================================================;
	%if &DABT_DEBUG_FLG eq Y %then %do;

		Libname cpctrl "&import_analysis_report_path/control"; 
		filename ctrl_xpt filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk" filename= "debug_control.xpt" debug=http CD="attachment; filename=debug_control.xpt";
		proc cport lib = cpctrl file = ctrl_xpt memtype=data;	/*i18NOK:LINE*/
		quit;
		
		Libname cpscr "&import_analysis_report_path/scratch"; 
		filename scr_xpt filesrvc folderpath="/&m_file_srvr_job_folder_path/&m_job_sk" filename= "debug_scratch.xpt" debug=http CD="attachment; filename=debug_scratch.xpt";
		proc cport lib = cpscr file = scr_xpt memtype=data;	/*i18NOK:LINE*/
		quit;
		
	%end;

	/************************************************************
	Models and projects appear on SAS Drive
	*************************************************************/
	
	proc http url="&BASE_URI./searchIndex/index/typeJobs/riskDataProject"/* i18nOK:Line */
				 method='POST'/* i18nOK:Line */
				 oauth_bearer=sas_services
				 ct="application/json";/* i18nOK:Line */
				 DEBUG LEVEL=3;
				run; quit;
				
	proc http url="&BASE_URI./searchIndex/index/typeJobs/riskModel"/* i18nOK:Line */
				 method='POST'/* i18nOK:Line */
				 oauth_bearer=sas_services
				 ct="application/json";/* i18nOK:Line */
				 DEBUG LEVEL=3;
				run; quit;

	%let m_error = 0;
	proc sql noprint;
		select 
			count(*) into :m_error		/*i18NOK:LINE*/
		from 
			&m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST
		where
			import_execution_return_cd is NULL or import_execution_return_cd > 4;
	quit;

	%if &m_error > 0 or &syscc > 4 %then %do;
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.ERROR10, noquote) );
		%return ;
	%end;

%end;


*==================================================;
* Check for errors ;
*==================================================;
%ERROR_STATE:

%if &syscc > 4 %then %do;
	%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_IMPORT_WRAPPER.GENERIC_ERROR, noquote) );
	%return;
%end;

%dabt_drop_table(m_table_nm=&m_cprm_imp_ctl..cprm_import_param_list_tmp);

*======================================================;
* Updating created by field as per user provided inputs
*======================================================;

%if &user_provided_mode = EXECUTE %then %do;
	%if (%symexist(find_source_machine_nm) and %symexist(replace_target_machine_nm)) %then %do; 
		%if ("&find_source_machine_nm."  ne "") and ("&replace_target_machine_nm." ne "") %then %do;
			%bankfdn_update_user();
		%end;
	%end;
%end;


%mend dabt_cprm_import_wrapper;
