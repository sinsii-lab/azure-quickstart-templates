/********************************************************************************************************
   	Module		:  rmcr_score_python_model

   	Function	:  This macro will be used to Score the CAS published python model and native python model
	Called-by	:  %dabt_apply_scoring_code
	Calls		:  None

						
	Author:   CSB Team
	Input : m_model_sk
	
	Processing:
		-> Extarct Model_source_type_sk and Model_workspace_name
				query model_master and get Model_source_type_sk into &m_model_source_type_sk and
				model_workspace_name into &m_model_workspace_name
		
		-> Score SWAT/non-SWAT based PYTHON  model 
			If m_model_workspace_name = SWAT then 
				 check rm_mdl caslib to get the astore metadata as .sashdat and load it into memory and use proc astore to score the SWAT model 
			If m_model_workspace_name = SKLEARN then use the score code and use proc scoreaccel to publish and score

*********************************************************************************************************/
%macro rmcr_score_python_model(m_model_sk = );

*******************************************************************;
* Start - Extarct Model_source_type_sk and last_registered_model_nm;
*******************************************************************;
%let m_model_source_type_sk=;
%let m_model_published_nm=;
%let m_model_short_nm=;
%let m_model_workspace_name=;

proc sql noprint;
	select 
		model_source_type_sk,
		last_registered_model_nm ,
		model_short_nm,
		model_workspace_name
			into 
		:m_model_source_type_sk,
		:m_model_published_nm,
		:m_model_short_nm,
		:m_model_workspace_name
	from &lib_apdm..model_master
	where model_sk = &m_model_sk.;
quit;

%let m_model_short_nm=&m_model_short_nm.;
%let m_model_published_nm = &m_model_published_nm.;
%let m_model_source_type_sk = &m_model_source_type_sk.;
%let m_model_workspace_name=&m_model_workspace_name.;


/* End - Extarct Model_source_type_sk and last_registered_model_nm  */

**************************************************************************;
* Start - Score SWAT/non-SWAT based PYTHON model published in RMPublishDestination ;
**************************************************************************;

%let m_model_source_type_cd=;

proc sql noprint;
	select model_source_type_cd into :m_model_source_type_cd
	from &lib_apdm..MODEL_SOURCE_TYPE_MASTER
	where model_source_type_sk = &m_model_source_type_sk.;
quit;



%dabt_initiate_cas_session(cas_session_ref=rmcr_score_python_model);

%if &m_model_source_type_cd. eq PYTHON %then %do;

	%let m_astore_castable=%sysfunc(kstrip(MDL_ASTORE_&m_model_sk.));
	
	/*Name with which publish destination was created in SAS Env Manager. This is the table in which model was published*/
	%let m_model_table_caslib = &RM_PUBLISHED_DEST_CAS_LIB.;
	%let m_model_table_nm = &RM_PUBLISHED_DEST_CAS_TABLE_NM.;
	%let m_scoring_libref = &m_scoring_libref;
	%let m_scoring_table_nm = %lowcase(&m_scoring_table_nm);
	%let m_scored_libref = &m_scored_libref.;
	%let m_scored_table_nm = %lowcase(&m_scored_table_nm);
	%let m_scoring_caslib = &m_scoring_libref.;
	
	%if &m_model_workspace_name. eq SWAT %then %do;
		%local m_in_table_format_var;
		
		proc cas;
			table.fileInfo result=Files / caslib="rm_mdl";     /* I18NOK:LINE */
			exist_Files=findtable(Files);
		
			if exist_Files then
				saveresult Files dataout=work.in_file_info;
		quit;
		
		/* derive the file format type of required source file name */
		proc sql noprint;
			select kscan(name, 2, '.') into :m_in_table_format_var separated by  "."  /* i18nOK:Line */
				from work.in_file_info where kupcase(kscan(name, 1, '.'))=%kupcase("&m_astore_castable."); /* i18nOK:Line */
		quit;
		
		%let m_in_table_format="&m_in_table_format_var";
		
		%if %sysfunc(find(&m_in_table_format., sashdat)) ge 1 %then %do; /* i18nOK:Line */
			%let m_in_table_nm=&m_astore_castable..sashdat;
			
			proc cas; 
				table.loadTable / casout={caslib="rm_mdl", name="&m_astore_castable.",    /* I18NOK:LINE */
					promote="FALSE" replace="TRUE"} /* i18nOK:Line */
					caslib="rm_mdl", path="&m_in_table_nm.";     /* I18NOK:LINE */
				run;
			quit;
		%end;

		%let work_lib_path = %sysfunc(pathname(work));

		filename scr_out "&work_lib_path/score.sas"; 		/*i18NOK:LINE*/
		filename scr_in filesrvc 
			folderpath="/Products/SAS Risk Modeling/Model/&m_model_sk"      /* I18NOK:LINE */
			filename="score.sas" debug=http CD="attachment; filename=score.sas"; /* i18nOK:Line */

		data _null_;
			rc=fcopy('scr_in', 'scr_out');      /* I18NOK:LINE */
			msg=sysmsg();
			put rc=msg=;
		
			if rc=0 then
				do;
					put "Score code for model &m_model_sk copied to work lib path";				/*i18NOK:LINE*/
				end;
			else
				do;
					put 
						"ERROR: Failed to copy score code for model &m_model_sk to to work lib path"; /* i18nOK:Line */
					call symputx('job_rc', 1012);      /* I18NOK:LINE */
				end;
		run;
		
		proc astore;
			score data=&M_SCORING_LIBREF..&M_SCORING_TABLE_NM.
				          rstore=rm_mdl.&m_astore_castable.
				          epcode=scr_out 
				out=&M_SCORED_LIBREF..&M_SCORED_TABLE_NM.;
			quit;
	
	%end;

	%else %if &m_model_workspace_name. eq SKLEARN %then %do;
		
			/***** sklearn scoring**/
				filename scr_cd filesrvc folderpath="/Products/SAS Risk Modeling/Model/&m_model_sk."  filename="score.sas" debug=http;  /* I18NOK:LINE */
			
			proc scoreaccel ;
			   publishmodel 
			      modelname="&m_model_short_nm." 
			      modeltype=DS2
			      modeltable="&m_model_table_caslib..&m_model_table_nm."
			      programfile=scr_cd
			      replacemodel=yes
			      promotetable=no
			      persisttable=no
			      ;
			quit;
			
			proc scoreaccel ;
			   runModel
			      modelname="&m_model_short_nm." 
			      modeltable="&m_model_table_caslib..&m_model_table_nm."
			      intable="&m_scoring_caslib..&m_scoring_table_nm."
			      outtable="&m_scored_libref..&m_scored_table_nm."
			      ;
			quit;
	%end;
     
%end;

/* End - Score SWAT/non-SWAT based PYTHON model published in RMPublishDestination */

%else %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.SCORE_PYTHON_MODEL_SM1.1, noquote,&m_model_source_type_sk));
%end;



%mend ;