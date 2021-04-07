
%csbinit;   /**** This macro enables users to use preconfigured macro variables provided by SAS Risk Modeling. ****/


%macro rmcr_sklrn_mdl_chk;

%let m_model_name=SKLRN6;  						/*** The name that you want to enter must match with the model name in the model repository. ***/
%let m_abt_table_name=PDO_RET_FIN_ACC_ABT;  	/*** The name that you want to enter must match with the physical name of the analytical data set used to develop the model. ***/


*******************************************************************;
**** Start - Extract model metadata from the model repository. ****;
*******************************************************************;

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


*******************************************************************;
****   End - Extract model metadata from the model repository.****;
*******************************************************************;

libname resp json fileref=resp;

*******************************************************************;
**** Start - Extract model score code from the model repository.***;
*******************************************************************;
	
%if &SYS_PROCHTTP_STATUS_CODE eq 200 %then %do;
	%let m_model_module_id=;
	
	proc sql noprint;
		select id
			into :m_model_module_id
			from resp.items where name="&m_model_name.";
	quit;
	%let m_model_module_id=&m_model_module_id.;

	%if &sqlobs eq 1 %then %do;
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

*******************************************************************;
**** End - Extract model score code from the model repository.*****;
*******************************************************************;

			cas myses;
			caslib _all_ assign;
		
				/***** 
				For more information on below Proc , refer 
				https://documentation.sas.com/?docsetId=proc&docsetTarget=p05rmiw27grg9qn1fpffnyvfeoza.htm&docsetVersion=9.4&locale=en
				****/	
				
****************************************************************************;
**** Start - Scores the model using score code extracted from above step****;
****************************************************************************;
			proc scoreaccel ;
			   publishmodel 
			      modelname="&m_model_name." 
			      modeltype=DS2
			      modeltable="RM_PBDST.RM_PUBL_DESTN_TABLE_NM"   /*** SAS Risk Modeling specific CAS Destination to publish models ***/
			      programfile=scrcd      						/****** file reference to package score code created by sasctl ****/
			      replacemodel=yes
			      promotetable=no
			      persisttable=no
			      ;
			quit;
			
			proc scoreaccel ;
			   runModel
			      modelname="&m_model_name." 
		      	  modeltable="RM_PBDST.RM_PUBL_DESTN_TABLE_NM"
			      intable="RM_MDL.&m_abt_table_name."	   /*** Modeling dataset input provided by user ***/
			      outtable="RM_MDL.SANITY_OUT"			  /***  Scored dataset gets stored in SANITY_OUT cas table***/
			      ;
			quit;

	/************************************************************************************************************************************
		Go to the "SANITY_OUT" table in the CAS library "RM_MDL" to check whether the data in the 'var1' column is populated. 
		The probable cause of this issue is that the model is not properly scored. 
		The following list explains some possible reasons for the model not being scored properly:
		(1) The prerequisites are not completed. 
		(2) The versions of the Scikit-learn and sasctl packages that are used for the model development are not compatible with each other.
	*****************************************************************************************************************************************/
****************************************************************************;
****   End - Scores the model using score code extracted from above step****;
****************************************************************************;		
		%end;
		%else %do;
			%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM19.1, noquote,&m_model_name.));	     /* i18nOK:Line */
			%return;
		%end;
	%end;
	%else %do; 
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM10.1, noquote,&m_model_name.)); /* i18nOK:Line */
		%return;
	%end;
		
%end;
%else %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source,RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM11.1, noquote,&SYS_PROCHTTP_STATUS_CODE.)); /* i18nOK:Line */
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_open_source, RMCR_OPEN_SOURCE_MSG.REGISTER_MODEL_SM9.1, noquote, &SYS_PROCHTTP_STATUS_PHRASE.)); /* i18nOK:Line */
	%return;
%end;

%mend rmcr_sklrn_mdl_chk;

%rmcr_sklrn_mdl_chk;  /**** Executes the macro ***/
