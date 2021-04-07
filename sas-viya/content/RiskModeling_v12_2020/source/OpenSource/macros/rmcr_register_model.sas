/********************************************************************************************************
   Module:  rmcr_register_model 

   Function:  This macro imports the Opensource model into Risk Modeling

   Parameters: INPUT: 
			1. m_model_type				            : Type of Model to be imported into RM
													  Allowed values are {PYTHON,R,SAS_CODE,PMML,EMN}
			2. m_model_name 			            : Name with which Model is to be registered in Risk Modeling
													  In case of Python Models name should be same as 
													  the name registered in ModelRepository using SASCTL
			
			3. m_published_model_name 	            : Name with which Model is already Published in to RMPublishDestination
													  Applicable for SWAT based python model.
													 
			4. m_target_variable_name 	            : Modeling Target variable Column name. 
													  It should match with physical varible name specified in RM Analytical Data Builder.
													
			5. m_input_variable_names               : Comma seperated Significant Input Variable Column name.
													  It should match with physical varible name specified in RM Analytical Data Builder.
			
			6. m_abt_table_name                     : Physical Name of Modeling Dataset used to develop Model. 
													  It should macth with Dataset sepecified in RM Analytical Data Builder
 													

			7. m_output_variable_names	            : Output Variables of Model
														
			8. m_model_scoring_code_location		: Scoring code location in the content folder
			
	Mandatory inputs : m_model_type, m_model_name, m_abt_table_name, m_target_variable_name
	
	Processing:
		-> Check for missing input parameters and send error message
			 m_model_type, m_model_name, m_abt_table_name, m_target_variable_name are mandatory.
		
		-> Check for user provided model type input parameter(m_model_type) or send error message
				Find out  m_model_type value exist in  model_source_type_cd column from model_source_type_master table  
			
		-> Check for user provided model name (m_model_name) exists in Risk Modeling or not
				Find out m_model_name value exists in model_short_nm column from model_master table else pass error message
		
		-> Check for user provided physical modeling dataset (m_abt_table_name) and associoted project_id is exists or not
				extract project_sk from project_master by joining with modeling_abt_master on abt_table_name.
					lets save the project_sk into macro variable m_project_id after converting into base64 format
				
		-> Check whether user provided m_target_variable_name is present as outcome variable in respective modeling dataset defined in RM
				Based m_abt_table_name look up in modeling_abt_master to get abt_sk
				Based m_target_variable_name look up in variable_master to get variable_sk
				use abt_sk and variable_sk to look up in modeling_abt_x_variable to check whether that variable is having outcome_variable_flg = 'Y' 					
		
		-> Get m_input_variable_names and m_output_variable_names
				If model_type ='PYTHON' get input,output variables from model repository API
					hit model repository API with filter on user provided model name (m_model_name) and get id from output JSON
						lets call it m_model_module_id
				
					hit variable endpoint of respective model using m_model_module_id
						declare libname for output JSON to process
				
					Get input and output variables from above step
						post process the input variables in required format to pass into RiskModelingJobs JSON
							Validate atleast one significant variable should be available
							Validate whether they are present as input variable in respective modeling dataset defined in RM
							
						Post process the ouput variables in required format to pass into RiskModelingJobs JSON
												
				if model_type is not 'PYTHON' then ask user to provide physical names of input and output vars. 
					Check whether ..exist..
					Check for missing values in input, output variables and send error message.
				
		-> Register model into Risk Modeling
				prepare parsing JSON to hit RiskModelingJobs API
					If m_published_model_name is specified, pass it to RM API
										
				GET method to hit RiskModelingJobs API
				POST method to register model into Risk modeling by hiting RiskModelingJobs API
				
		-> update model source type in model_master after model is registered into RM
				update the model_master table with appropriate model_source_type_sk because current API allows only 'VIYA'
				join with model_source_type_master 

		-> Remove input which were actually Target Var for that model. But repository returned them as Input

		-> Score code preparation 
			If model type is python  
				Hit model repository to get scoring code
				   SKLEARN-move the score content in respective model's content server
				   SWAT-Fetching AStoreMetadata and promoting the astore file content
				if model type is 'R'
				get score code from user and update the score as per our scoring code requirement and save in respective model content path
				
			else send error message with SYS_PROCHTTP_STATUS_PHRASE and SYS_PROCHTTP_STATUS_CODE
		
		-> Run model validatioby executing Sample scoring of model ( for future expansion )
*********************************************************************************************************/

%macro rmcr_register_model(m_model_type=,
						m_model_name=,
						m_target_variable_name=,
						m_input_variable_names=NA,
						m_abt_table_name=,
						m_output_variable_names=NA,
						m_model_scoring_code_location=NA);

%let m_model_type = &m_model_type.;
%let m_model_name=&m_model_name.;
%if %sysfunc(kupcase(%sysfunc(kstrip(&m_model_type.)))) eq PYTHON %then %do;
	%let m_published_model_name=&m_model_name.;
%end;
%else %do;
	%let m_published_model_name=NA;
%end;
%let m_target_variable_name=&m_target_variable_name.;
%let m_input_variable_names=&m_input_variable_names.;
%let m_abt_table_name=&m_abt_table_name.;
%let m_output_variable_names=&m_output_variable_names.;
%let m_model_scoring_code_location=&m_model_scoring_code_location.;
%let BASE_URI=%sysfunc(getoption(servicesbaseurl));
**********************************************************************;
** Start - Check for missing input parameters and send error message *;
**********************************************************************;
%let err_log=;

data _null_;
	if missing("&m_model_type.") or missing("&m_model_name.") or missing("&m_target_variable_name.") or  
		missing("&m_abt_table_name.")  then do;
			err_log= 1;
	end;

	else do;
		err_log=0;
	end;
	call symput('err_log',err_log);   /* I18NOK:LINE */
run;

%if &err_log. eq 1 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM1.1, noquote));  /* i18nOK:Line */
	%return;
%end;


/* End - Check for missing input parameters and send error message */

****************************************************************************************************;
** Start - Check for user provided model type input parameter(m_model_type) or send error message  *;
****************************************************************************************************;

proc sql noprint;
	select *
	from &lib_apdm..MODEL_SOURCE_TYPE_MASTER where kupcase(kstrip(model_source_type_cd))=kupcase(kstrip("&m_model_type."));
quit;

%if &sqlobs. LE 0 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM2.1, noquote,&m_model_type.));
	%return;
%end;

/* End - Check for user provided model type input parameter(m_model_type) or send error message  */

***************************************************************************************************************;
** Start - Check for user provided model name (m_model_name) exists in Risk Modeling else send error message  *;
***************************************************************************************************************;

proc sql noprint;
	select *
	from &lib_apdm..model_master where kupcase(kstrip(model_short_nm))=kupcase(kstrip("&m_model_name."));
quit;

%if &sqlobs. gt 0 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM3.1, noquote,&m_model_name.)); /* i18nOK:Line */
	%return;
%end;

/* End - Check for user provided model name (m_model_name) exists in Risk Modeling else send error message  */

************************************************************************************;
** Start - Check for user provided physical modeling dataset (m_abt_table_name)    *;
**         and associoted project_id exists else send error message                *;
************************************************************************************;
%let m_project_sk=;
%let m_project_short_nm=;
%let m_project_id=;

proc sql noprint;
	select mam.project_sk , pm.project_short_nm into :m_project_sk,:m_project_short_nm
 		from &lib_apdm..MODELING_ABT_MASTER mam
		inner join &lib_apdm..PROJECT_MASTER pm 
		on mam.project_sk=pm.project_sk
		where kupcase(kstrip(mam.abt_table_nm))=kupcase(kstrip("&m_abt_table_name."));
quit;

%if &sqlobs. lt 1 %then %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM4.1, noquote,&m_abt_table_name.)); /* i18nOK:Line */
		%return;
%end;

%let m_project_id = %sysfunc(cats(PRJ_,%sysfunc(left(&m_project_sk.))));
%let m_project_id = %scan(%sysfunc(putc(&m_project_id,$base64x64.)),1,'=');

/* End - Check for user provided physical modeling dataset (m_abt_table_name) 
         and associoted project_id is exists else send error message  */

**********************************************************************************************;
* Start - Check whether user provided m_target_variable_name is present as outcome variable  *;
*          in respective modeling dataset defined in RM                                      *;
**********************************************************************************************;

%let m_outcome_variable_flg = ; 
proc sql noprint;
	select av.outcome_variable_flg into :m_outcome_variable_flg 
	from &lib_apdm..modeling_abt_master
	(where=(kstrip(kupcase(abt_table_nm)) = kstrip(kupcase("&m_abt_table_name.")))) as abt 
	inner join &lib_apdm..modeling_abt_x_variable as av
	on (abt.abt_sk = av.abt_sk)
	inner join &lib_apdm..variable_master 
	(where=(kstrip(kupcase(variable_column_nm)) = kstrip(kupcase("&m_target_variable_name."))))  as vm
	on (av.variable_sk = vm.variable_sk); 
quit;

%if &m_outcome_variable_flg. NE Y %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM5.1, noquote,&m_target_variable_name.)); /* i18nOK:Line */
	%return;
%end;

/* End - Check whether user provided m_target_variable_name is present as outcome variable  
           in respective modeling dataset defined in RM  */

*******************************************************************;
** Start - Get m_input_variable_names and m_output_variable_names *;
*******************************************************************;

%if &m_model_type. eq PYTHON and &m_input_variable_names eq NA  %then %do;
	filename resp temp;
	filename resp_hdr temp;

	%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

	proc http url="&BASE_URI/modelRepository/models/?filter=eq(name,%27&m_model_name.%27)" /* i18nOK:Line */
		method='get'/* i18nOK:Line */
		oauth_bearer=sas_services out=resp headerout=resp_hdr headerout_overwrite 
			ct="application/json";
		DEBUG LEVEL=3;
	run;
	quit;
	
	libname resp json fileref=resp;
	
	%if &SYS_PROCHTTP_STATUS_CODE eq 200 %then %do;
		%let m_model_module_id=;	
		proc sql noprint;
			select id into :m_model_module_id
			from resp.items where name ="&m_model_name.";
		quit;
		
		%if &sqlobs eq 1 %then %do;
			%let m_model_module_id=&m_model_module_id.;
			
			proc http url="&BASE_URI/modelRepository/models/&m_model_module_id" /* i18nOK:Line */
			method='get'/* i18nOK:Line */
			oauth_bearer=sas_services out=resp headerout=resp_hdr headerout_overwrite 
				ct="application/json";
			DEBUG LEVEL=3;
			run;
			quit;
			
			libname resp json fileref=resp; 
			
			%if &SYS_PROCHTTP_STATUS_CODE eq 200 %then %do;
				%let m_py_input_variable_names=;
				%let m_rm_input_var_cnt=;
				%let m_rm_matching_input_var_cnt=;
				%let m_algorithm=;
				
				proc sql noprint;
					select  kstrip(value) into :m_algorithm from 
					resp.alldata where kstrip(p1)='algorithm';
				quit;

				proc sql noprint;
					select "'"||kstrip(name)||"'",count(name) into :m_py_input_variable_names separated by ',' , :m_rm_input_var_cnt
					from RESP.INPUTVARIABLES where role='input';
				quit;

				%let m_py_input_variable_names_unqt=;
				proc sql noprint;
					select kstrip(name) into :m_py_input_variable_names_unqt separated by ',' 
					from RESP.INPUTVARIABLES where role='input';
				quit;
				%let m_py_input_variable_names_unqt = &m_py_input_variable_names_unqt.;

				/*** Validate whether model has minimum one significant variable  **/

				%if &m_rm_input_var_cnt. le 0 %then %do;
						%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM7.1, noquote,&m_model_name.));
						%return;
				%end;
	
				/*** Validate whether variables are present as input variable in respective modeling dataset defined in RM  **/
				 
				proc sql noprint;
					select count(vm.variable_sk) into :m_rm_matching_input_var_cnt
					from &lib_apdm..modeling_abt_master
					(where=(kstrip(kupcase(abt_table_nm)) = kstrip(kupcase("&m_abt_table_name.")))) as abt 
					inner join &lib_apdm..modeling_abt_x_variable as av
					on (abt.abt_sk = av.abt_sk)
					inner join &lib_apdm..variable_master 
					(where=(kstrip(kupcase(variable_column_nm)) in (&m_py_input_variable_names.)))  as vm 
					on (av.variable_sk = vm.variable_sk); 
				quit;


				%if &m_rm_matching_input_var_cnt. NE &m_rm_input_var_cnt. %then %do;
					%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM7.2, noquote));  /* i18nOK:Line */
					%return;
				%end;

				%let m_input_variable_names = &m_py_input_variable_names_unqt.;
				
				%let m_py_output_variable_names=;
				proc sql noprint;
					select strip(name)||'#'||type into :m_py_output_variable_names separated by ','
					from RESP.OUTPUTVARIABLES where role='output' and strip(name) not in ('rc','msg');
				quit;
				
				%let m_output_variable_names = &m_py_output_variable_names.;
				
			%end;
		
			%else %do;
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM8.1, noquote,&SYS_PROCHTTP_STATUS_CODE.));  /* i18nOK:Line */
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM9.1, noquote,&SYS_PROCHTTP_STATUS_PHRASE.)); /* i18nOK:Line */
				%return;
			%end;
		%end;
		%else %do; 
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM10.1, noquote,&m_model_name.)); /* i18nOK:Line */
			%return;
		%end;
	%end;
	
	%else %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM11.1, noquote,&SYS_PROCHTTP_STATUS_CODE.));    /* i18nOK:Line */
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM9.1, noquote,&SYS_PROCHTTP_STATUS_PHRASE.));  /* i18nOK:Line */
		%return;
	%end;	
%end;	

%else %do;  /** non python models get input/output variables from user */

%let outputvar=;
data _null_;
	i=count("&m_output_variable_names", ',')+1;
	put i;
	format outputvar $200.;
	format outputvar1 $200.;

	do j=1 to i;
		outputvar1=cats(scan("&m_output_variable_names", j, ','), '#decimal,');
		outputvar=cats(outputvar, outputvar1);
	end;
	call symput('outputvar', substr(outputvar, 1, length(outputvar)-1));
run;

%let m_output_variable_names = &outputvar.;

%end;

/* End - Get m_input_variable_names and m_output_variable_names */


********************************************************;
** Start - Register model into Risk Modeling           *;
********************************************************;

filename parsejsn temp; 

data _null_;
	file parsejsn OLD lrecl=64000;
	length stmt_str $32767.;
	put '{';
	stmt_str=cat('	"type": "createModels"', ',');
	put stmt_str;
	stmt_str=cat('	"modelDetails": [');
	put stmt_str;
	put '		{';
	stmt_str=cat('			"links": []', ',');
	put stmt_str;
	stmt_str=cat('			"version": 1', ',');
	put stmt_str;
	stmt_str=cat('			"projectId": "', kstrip("&m_project_id"), '",');
	put stmt_str;
	stmt_str=cat('			"name": "', kstrip("&m_model_name"), '",');
	put stmt_str;
	stmt_str=cat('			"description": null', ',');
	put stmt_str;
	stmt_str=cat('			"source": "viya"', ',');
	put stmt_str;
	stmt_str=cat('			"id": "2f7adbe5-d8a6-4c58-9095-79c9d8c6fc7b"', ',');
	put stmt_str;
	stmt_str=cat('			"registeredName": "', kstrip("&m_published_model_name"),'",');
	put stmt_str;
	stmt_str=cat('			"abtTableName": "', kstrip("&m_abt_table_name"), '",');
	put stmt_str;
	stmt_str=cat('			"targetVariableName": "', kstrip("&m_target_variable_name"), '",');
	put stmt_str;
	stmt_str=cat('			"createdBy": "', kstrip("&sysuserid"), '",');
	put stmt_str;
	stmt_str=cat('			"creationTimeStamp": "2019-11-21T04:48:25.081Z"', ',');
	put stmt_str;
	stmt_str=cat('			"isCompatibleToAbt": true', ',');
	put stmt_str;
	stmt_str=cat('			"registeredDate": "2019-11-21T04:48:25.081Z"', ',');
	put stmt_str;
	stmt_str=cat('			"inputVariables": "', kstrip("&m_input_variable_names"), '",'); 
	put stmt_str;
	stmt_str=cat('			"isCaptured": true', ',');
	put stmt_str;
	if kstrip("&m_algorithm.") ne '' then do; 
		stmt_str=cat('			"algorithm": "', kstrip("&m_algorithm"), '",');
	end;
	else do;
		stmt_str=cat('			"algorithm": null', ',');
	end;
	put stmt_str;
	stmt_str=cat('			"function": null', ',');
	put stmt_str;
	stmt_str=cat('			"outputVariables": "', kstrip("&m_output_variable_names"), '",');
	put stmt_str;
	stmt_str=cat('			"projectName": "', kstrip("&m_project_short_nm"), '",');
	put stmt_str;
	stmt_str=cat('			"modelStagingId": "2f7adbe5-d8a6-4c58-9095-79c9d8c6fc7b"', ',');
	put stmt_str;
	put ' 			"error": ""';
	put '		}';
	put '	]';
	put '}';
run;

filename resp temp;
filename resp_hdr temp;


proc http url="&BASE_URI/riskModelingCore/jobs"/* i18nOK:Line */
	method='get'/* i18nOK:Line */
	oauth_bearer=sas_services out=resp headerout=resp_hdr headerout_overwrite 
		ct="application/json";
	DEBUG LEVEL=3;
run;

quit;

/* POST method to RiskModeling Core REST API to register model into RM */

libname resp json fileref=resp;

data _null_;
	infile resp_hdr scanover truncover;
	input @'Date:' lastmod $40.;
	call symputx('last_modified', lastmod, 'g');
run;

%put INFO: Last-Modified timestamp is &last_modified.;

filename respput temp;

proc http url="&BASE_URI/riskModelingCore/jobs"/* i18nOK:Line */
	method='POST'/* i18nOK:Line */
	oauth_bearer=sas_services in=parsejsn out=respput headerout=resp_hdr 
		headerout_overwrite ct="application/vnd.sas.cs.import.model.job+json";
	headers "If-Unmodified-Since"="&last_modified.";
	DEBUG LEVEL=3;
run;

quit;

/* End - Register model into Risk Modeling   */

***********************************************************************************************************************;
** Start - update model source type in model_master after model is registered into RM and send respective message  *;
***********************************************************************************************************************;
%let m_model_sk=;

proc sql noprint;
	select model_id into :m_model_sk from &lib_apdm..model_master where 
		kupcase(kstrip(model_short_nm))=kupcase(kstrip("&m_model_name."));
quit;

proc sql noprint;
	update &lib_apdm..model_master set model_source_type_sk = 
		(select model_source_type_sk from &lib_apdm..model_source_type_master 
			where kupcase(kstrip(model_source_type_cd))=kupcase(kstrip("&m_model_type."))) 
			where model_id = "&m_model_sk.";
quit;

%let m_model_sk = %sysfunc(kstrip(&m_model_sk.));

	/*End - update model source type in model_master after model is registered into RM   */


%if &SYS_PROCHTTP_STATUS_CODE. ne 202 %then %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM12.1, noquote,&m_model_name.));			/* i18nOK:Line */
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM13.1, noquote,&SYS_PROCHTTP_STATUS_CODE.));    /* i18nOK:Line */
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM14.1, noquote,&SYS_PROCHTTP_STATUS_PHRASE.));  /* i18nOK:Line */
	%return;
%end;

***********************************************************************************************************************;
** Start - Remove input which were actually Target Var for that model. But repository returned them as Input;
***********************************************************************************************************************;
%let m_tgt_var_sk= .;
proc sql noprint;
	select variable_sk into :m_tgt_var_sk
	from &lib_apdm..MODEL_X_ACT_OUTCOME_VAR
	where model_sk = &m_model_sk. and target_variable_flg="Y";
quit;
%let m_tgt_var_sk = &m_tgt_var_sk.;

proc sql noprint;
	delete from &lib_apdm..MODEL_X_SCR_INPUT_VARIABLE 
	where model_sk = &m_model_sk. and variable_sk = &m_tgt_var_sk;
quit;

*************************************************************************;
** Start - If Python, extract Scoring code from model repository        *;
*************************************************************************;
%if &SYS_PROCHTTP_STATUS_CODE. eq 202 %then %do;
	%if &m_model_type. eq PYTHON  %then %do;
		/* Get the contents_id from model content URI */
		proc http url="&BASE_URI/modelRepository/models/&m_model_module_id/contents" /* i18nOK:Line */
			method='get'/* i18nOK:Line */
			oauth_bearer=sas_services out=resp headerout=resp_hdr headerout_overwrite 
				ct="application/json";
			DEBUG LEVEL=3;
			run;
		quit;
		
		libname resp json fileref=resp; 
		
		%let m_contents_id=;
		proc sql noprint;
			select id into :m_contents_id from resp.items where name='dmcas_epscorecode.sas';
		quit;
		
		%if &sqlobs ge 1 %then %do;
			filename scrcd temp;
			
			proc http url="&BASE_URI/modelRepository/models/&m_model_module_id/contents/&m_contents_id/content" /* i18nOK:Line */
				method='get'/* i18nOK:Line */
				oauth_bearer=sas_services out=scrcd headerout=resp_hdr headerout_overwrite 
					ct="text/vnd.sas.source.sas";
				DEBUG LEVEL=3;
				run;
			quit;
			
			filename dst filesrvc folderpath="/Products/SAS Risk Modeling/Model/&m_model_sk." 
				filename="score.sas" debug=http;
				
			%if %sysfunc(fexist(scrcd)) %then  %do;
			
				data _NULL_;
					infile scrcd end=eof lrecl=32767;
					file dst lrecl=64000 old;
					length get_statement $32767.;
					do while (^eof);
						input;
						put _infile_;
					end;
				run;
					
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM15.1, noquote,&m_model_name.));	

				/***** Fetching AStoreMetadata and promoting the astore file content in rm_mdl caslib for SWAT based python model***/

				%let m_astore_contents_id=;
				proc sql noprint;
					select id into :m_astore_contents_id from resp.items where name='AstoreMetadata.json';
				quit;
				
				%if &sqlobs eq 1  %then %do;
					proc http url="&BASE_URI/modelRepository/models/&m_model_module_id/contents/&m_astore_contents_id./content" /* i18nOK:Line */
						method='get'/* i18nOK:Line */
						oauth_bearer=sas_services out=resp headerout=resp_hdr headerout_overwrite 
							ct="application/json";
						DEBUG LEVEL=3;
					run;
					quit;
					
					libname resp json fileref=resp;
					
					%let m_astore_caslib=;
					%let m_astore_hdat=;
					proc sql noprint;
						select kstrip(value)||'.sashdat' into :m_astore_hdat from resp.alldata 
						where p1='name';
						select value into :m_astore_caslib from resp.alldata
						where p1='caslib';
					quit;
					
					%dabt_initiate_cas_session(cas_session_ref=rmcr_register_model);
					
					%let m_astore_castable=;
					%let m_astore_castable=%sysfunc(kstrip(MDL_ASTORE_&m_model_sk.));
					%let m_astore_hdat=%sysfunc(kstrip(&m_astore_hdat.));
					
					proc casutil;
						load casdata="&m_astore_hdat." incaslib="&m_astore_caslib."
						casout="&m_astore_castable." outcaslib="&DABT_MODELING_ABT_LIBREF." replace;
					quit;
					%dabt_save_cas_table(m_in_cas_lib_ref=&DABT_MODELING_ABT_LIBREF., m_in_cas_table_nm=&m_astore_castable.);
/* 					%dabt_promote_table_to_cas(input_caslib_nm =&DABT_MODELING_ABT_LIBREF.,input_table_nm =&m_astore_castable.);	 */				
					%dabt_terminate_cas_session(cas_session_ref=rmcr_register_model);

					/*** update apdm.model_master table with model_workspace_name as SWAT  ***/ 
					proc sql noprint;
						update &lib_apdm..model_master set model_workspace_name = 'SWAT'
								where model_id = "&m_model_sk.";
					quit;
				%end;
				%else %do;
					/*** update apdm.model_master table with model_workspace_name as SKLEARN  ***/ 
					proc sql noprint;
						update &lib_apdm..model_master set model_workspace_name = 'SKLEARN'
								where model_id = "&m_model_sk.";
					quit;
				%end;
			%end;
			
			%else %do;
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM16.1, noquote));					/* i18nOK:Line */
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM17.1, noquote,&SYS_PROCHTTP_STATUS_CODE.));	 		/* i18nOK:Line */
				%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM18.1, noquote,&SYS_PROCHTTP_STATUS_PHRASE.));	 	/* i18nOK:Line */
				%return;
			%end;
			
		%end;
		
		%else %do;
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM19.1, noquote,&m_model_name.));	     /* i18nOK:Line */
			%return;
		%end;
		
	%end;	
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM20.1, noquote,&m_model_name.,&m_model_sk.));	 
%end;

/* End - Score code preparation and update model source type in model_master after model is registered into RM succesfuly */



%mend ;
