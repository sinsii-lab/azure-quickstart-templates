/*****************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: rmcr_writeback  					                       
 *                                                                 
 * LOGIC		: This macro is invoked to generate the Views from base tables.
 *
 * USAGE		: %rmcr_writeback
  Parameters: 
		INPUT: 
			1. m_scoring_from	= Start date in date9 format to begin loading scoring data in writeback tables.If from date is blank then a default sas date is taken i.e. 01Jan1960.
			2. m_scoring_to     = TO date in date9 format to begin loading scoring data in writeback tables. If m_scoring_to date is not specified then it will fetch current system date.
			3. m_model_id       = Model ID to load the scoring data of particular specified model.More than 1 Model_Id should be passed as # seperated e.g 10#12#13
			
			
	Purpose:
	
	 Created below a macro rmcr_writeback which enables the user to load the scoring data in writeback tables.
	 
	Logic/Processing:
		
		Define necessary macro parameters 
		Check if m_model_id is not blank then find the model and add ' before and after each model number 
		Check for the specified date if From date is greater than To date, if yes, then walkout
		Initiate cas session.
		Creating cas work tables for the required APDM tables.
		Creating work tables of APDM tables which will be further used to fetch data.
		
		
		Process two seperate macro for Binary and Continuous model to append data of every RM_ARM table in writeback tables.
			-Macro for Binary write back table.
				Checking the library Flag,and make changes accordingly for Library either or CASUSER or RM_ARM 
				Call for writeback_cleanup macro to delete the existing data
				Fedsql to create the extract of data which needs to be loaded for the specified time period
				Appending Extract to final data set
					
		Calculating the total count of tables with predicted result in RM_ARM library
			- Calculate count in a macro variable and create a macro variable with comma seperated name of all the tables.
			- Check if the above model_id is specified, then fetch tables pertaining to the same model only.
			- If no model_id specified then fetch data for all models.
		
		Delete the existing records from the writeback table
			Checking the existence of writeback table(writeback_all_model),Do not delete if the table does not exist
			
		Initiate a DO loop from 1 to the calculated count of all the tables. Beginning of Do-Loop for creation processing each table individually.
			Extract individual RM_ARM table name from the comma seperated macro variable so as to process them further.
			If the scoring_from date is missing then load for entire table.
			Else Create a work table from RM_ARM's extracted table with data which belong to the specified dates by user which lie between FROM and TO date.
			Based on the nomenclature of the tables push the above created work tables to Binary or Continuous model macro to process the data.
		
		Create one data set for both binary and continuous model
		
		End of Do Loop.		
		
		Terminate the cas session initiated above, if the value for m_cardinality_lib_dtl was blank.
		
Sample : %rmcr_writeback(m_scoring_from=19Jan1960,m_scoring_to=03Mar2020,m_model_id=10#12#13);
 *****************************************************************************************/
  
 
%macro rmcr_writeback(m_scoring_from=.,m_scoring_to=,m_model_id=.); 

options mprint mlogic symbolgen;

/* Define necessary macro parameters */

	%let m_scoring_from=&m_scoring_from;
	%let m_scoring_to=&m_scoring_to; 
	%let tbl_nm_dtl=writeback_all_model;
	%let lib_crfm=bankcrfm;
	%let m_model_id=&m_model_id;
	%let model_id=.;

/* Check if m_model_id is not blank then find the model and add ' before and after each model number */	
	%if &m_model_id ne . %then %do;
		
		data _null_;
		model_id=cats("'",tranwrd("&m_model_id","#","','"),"'");   /* I18NOK:LINE */
		call symput('model_id',model_id);    /* I18NOK:LINE */
		run;
		
		%let model_id=&model_id;
	
	%end;
	
	
	/* Check for the specified date if From date is greater than To date, if yes, then walkout */
	
	%if '&m_scoring_from'd > '&m_scoring_to'd %then %do;
		return;
	%end;

	%else %do;
	
		%if &m_scoring_to. eq . or &m_scoring_to. eq  %then %do;
		%let m_scoring_to = %sysfunc(today(), date9.);     /* I18NOK:LINE */
		%end;


	/* Initiate session after retrieving necessary information */	
			%dabt_initiate_cas_session(cas_session_ref = mysession);
			
		%let lib_dt=RM_ARM;

			/* Creating cas work tables for the required APDM tables */

		DATA casuser.MODEL_MASTER;
		 SET APDM.MODEL_MASTER; 
		RUN;

		DATA casuser.LEVEL_MASTER;
		 SET APDM.LEVEL_MASTER; 
		RUN;

		DATA casuser.PURPOSE_MASTER; 
		 SET APDM.PURPOSE_MASTER;
		RUN;

		DATA casuser.Project_MASTER;
		 SET APDM.Project_MASTER;
		RUN;

		DATA casuser.SUBJECT_GROUP_MASTER;
		 SET APDM.SUBJECT_GROUP_MASTER;
		RUN;

		DATA casuser.MODEL_ANALYSIS_TYPE_MASTER;
		SET apdm.MODEL_ANALYSIS_TYPE_MASTER;
		RUN;

		data casuser.MODEL_MASTER_EXTENSION;
		set apdm.MODEL_MASTER_EXTENSION;
		run;
		
		%let etls_tblExist =%eval(%sysfunc(exist(&lib_crfm..writeback_all_model, DATA)));
		%if (&etls_tblExist ne 0) %then %do;
			data casuser.writeback_all_model;
				set bankcrfm.writeback_all_model;
		%end;
		
		
	
	/* Process two seperate macro for Binary and Continuous model to append data of every RM_ARM table in writeback tables */
		
		/* Macro for Binary write back table */

		%Macro rmcr_binary_writeback(tbl_nm=,lib_flg='Y');   /* I18NOK:LINE */

			%let tbl_nm=&tbl_nm;
			%let lib_flg=&lib_flg;
			
			/* Checking the library Flag,and make changes accordingly for Library either or CASUSER or RM_ARM */
			
			%if &lib_flg='N' %then %do;   /* I18NOK:LINE */
				%let lib_dt=CASUSER;
			%end;

			%else %do;
				%let lib_dt=RM_ARM;
			%end;
			
			/* Fedsql to create the extract of data which needs to be loaded for the specified time period */
			PROC FEDSQL sessref=mysession;
				create table casuser.binary_&i as select
					SUBJECT_RK, 
					LEVEL_MASTER.level_type_sk AS SUBJECT_ANALYSIS_TYPE_CD,
					LEVEL_MASTER.level_short_nm AS SUBJECT_ANALYSIS_NAME,
					LEVEL_MASTER.level_key_column_nm AS Level_key_type,
					PURPOSE_MASTER.purpose_cd AS PURPOSE_TYPE_CD,
					PURPOSE_MASTER.purpose_short_nm AS PURPOSES_NAME,
					MODEL_ANALYSIS_TYPE_MASTER.model_analysis_type_cd as model_analysis_type_cd,
					MODEL_ANALYSIS_TYPE_MASTER.model_analysis_type_short_nm AS model_analysis_type_name,
					MODEL_MASTER_EXTENSION.model_type_cd as model_type_cd,
					MODEL_MASTER_EXTENSION.model_product_type_cd as model_product_type_cd,
					SUBJECT_GROUP_MASTER.subject_group_cd as subject_group_cd,
					SUBJECT_GROUP_MASTER.subject_group_short_nm AS subject_group_short_nm,
					MODEL_MASTER.MODEL_ID as MODEL_ID,
					MODEL_MASTER.model_short_nm as MODEL_NAME,
					SCORING_AS_OF_DTTM as LATEST_SCORE_DTTM,
					SCORE as LATEST_SCORE_POINTS_NO, 
					PREDICTED_PROB_OF_EVENT as LATEST_SCORE_NO,
					CURRENT_TIMESTAMP as CREATED_DTTM,
					CURRENT_TIMESTAMP as PROCESSED_DTTM
					FROM &lib_dt..&tbl_nm 
						INNER JOIN casuser.MODEL_MASTER as MODEL_MASTER ON 
							(&tbl_nm..MODEL_SK = MODEL_MASTER.MODEL_SK)  /* Scoring model sk need to ne changed to Model_Sk */
						INNER JOIN casuser.LEVEL_MASTER as LEVEL_MASTER ON 
							(MODEL_MASTER.LEVEL_SK = LEVEL_MASTER.level_sk)
						INNER JOIN casuser.PROJECT_MASTER as PROJECT_MASTER ON 
							(MODEL_MASTER.project_sk = PROJECT_MASTER.project_sk)
						Inner join casuser.PURPOSE_MASTER as PURPOSE_MASTER ON	
							(PROJECT_MASTER.purpose_sk=PURPOSE_MASTER.purpose_sk)
						Inner join casuser.MODEL_ANALYSIS_TYPE_MASTER as MODEL_ANALYSIS_TYPE_MASTER ON
							(PURPOSE_MASTER.model_analysis_type_sk=MODEL_ANALYSIS_TYPE_MASTER.model_analysis_type_sk)
						Inner join casuser.MODEL_MASTER_EXTENSION as MODEL_MASTER_EXTENSION ON
							(MODEL_MASTER.model_id=MODEL_MASTER_EXTENSION.model_id)
						left join casuser.SUBJECT_GROUP_MASTER as SUBJECT_GROUP_MASTER ON
							(PROJECT_MASTER.subject_group_sk=SUBJECT_GROUP_MASTER.subject_group_sk);
			quit;
		
		/* Appending to final data set for binary */
		
			data casuser.binary_writeback_data (append=yes);
				set casuser.binary_&i;
				length CREATED_BY $28;
				CREATED_BY="&sysuserid.";
			run;

		%Mend rmcr_binary_writeback;

		/* Macro for Continuous writeback table */

		%Macro rmcr_continuous_writeback(tbl_nm=,lib_flg='Y');	/* I18NOK:LINE */

			%let tbl_nm=&tbl_nm;
			%let lib_flg=&lib_flg;
			
			/* Checking the library Flag,and make changes accordingly for Library either or CASUSER or RM_ARM */
			
			%if &lib_flg='N' %then %do;	/* I18NOK:LINE */
				%let lib_dt=CASUSER;
			%end;
			%else %do;
				%let lib_dt=RM_ARM;
			%end;
			
			/* Fedsql to create the extract of data which needs to be loaded for the specified time period */
			PROC FEDSQL sessref=mysession;
				create table casuser.continuous_&i as select
					SUBJECT_RK, 
					LEVEL_MASTER.level_type_sk AS SUBJECT_ANALYSIS_TYPE_CD,
					LEVEL_MASTER.level_short_nm AS SUBJECT_ANALYSIS_NAME,
					LEVEL_MASTER.level_key_column_nm AS Level_key_type,
					PURPOSE_MASTER.purpose_cd AS PURPOSE_TYPE_CD,
					PURPOSE_MASTER.purpose_short_nm AS PURPOSES_NAME,
					MODEL_ANALYSIS_TYPE_MASTER.model_analysis_type_cd as model_analysis_type_cd,
					MODEL_ANALYSIS_TYPE_MASTER.model_analysis_type_short_nm AS model_analysis_type_name,
					MODEL_MASTER_EXTENSION.model_type_cd as model_type_cd,
					MODEL_MASTER_EXTENSION.model_product_type_cd as model_product_type_cd,
					SUBJECT_GROUP_MASTER.subject_group_cd as subject_group_cd,
					SUBJECT_GROUP_MASTER.subject_group_short_nm AS subject_group_short_nm,
					MODEL_MASTER.MODEL_ID as MODEL_ID,
					MODEL_MASTER.model_short_nm as MODEL_NAME,
					SCORING_AS_OF_DTTM as LATEST_SCORE_DTTM,
					. as LATEST_SCORE_POINTS_NO, 
					PREDICTED_OUTCOME_VALUE as LATEST_SCORE_NO,
					CURRENT_TIMESTAMP as CREATED_DTTM,
					CURRENT_TIMESTAMP as PROCESSED_DTTM
					FROM &lib_dt..&tbl_nm 
						INNER JOIN casuser.MODEL_MASTER as MODEL_MASTER ON 
							(&tbl_nm..MODEL_SK = MODEL_MASTER.MODEL_SK)
						INNER JOIN casuser.LEVEL_MASTER as LEVEL_MASTER ON 
							(MODEL_MASTER.LEVEL_SK = LEVEL_MASTER.level_sk)
						INNER JOIN casuser.PROJECT_MASTER as PROJECT_MASTER ON 
							(MODEL_MASTER.project_sk = PROJECT_MASTER.project_sk)
						Inner join casuser.PURPOSE_MASTER as PURPOSE_MASTER ON	
							(PROJECT_MASTER.purpose_sk=PURPOSE_MASTER.purpose_sk)
						Inner join casuser.MODEL_ANALYSIS_TYPE_MASTER as MODEL_ANALYSIS_TYPE_MASTER ON
							(PURPOSE_MASTER.model_analysis_type_sk=MODEL_ANALYSIS_TYPE_MASTER.model_analysis_type_sk)
						Inner join casuser.MODEL_MASTER_EXTENSION as MODEL_MASTER_EXTENSION ON
							(MODEL_MASTER.model_id=MODEL_MASTER_EXTENSION.model_id)
						left join casuser.SUBJECT_GROUP_MASTER as SUBJECT_GROUP_MASTER ON
							(PROJECT_MASTER.subject_group_sk=SUBJECT_GROUP_MASTER.subject_group_sk);
			quit;

			/* Appending to final data set for Continuous */
			
			data casuser.continuous_writeback_data (append=yes);
			set casuser.continuous_&i;
			length CREATED_BY $28;
				CREATED_BY="&sysuserid. ";
			run;
			
		%Mend rmcr_continuous_writeback;

		/* Calculating the total count of tables with predicted result in RM_ARM library */
			
			
			proc sql noprint ; 
					select count(*), memname into :m_tbl_cnt,:ins_tbl_list separated by '#'		/* I18NOK:LINE */
					from SASHELP.VTABLE
					where kupcase(kcompress(libname)) = kupcase(kcompress("&lib_dt"))
						and kupcase(kcompress(memname)) contains '_PRDCTD_RSLT'		/* I18NOK:LINE */
						and memtype = 'DATA'	/* I18NOK:LINE */
						%if &model_id ne . %then %do;
							and (scan(memname,1,'_') in (&model_id))	/* I18NOK:LINE */
						%end;
					;
			quit;

			%let m_tbl_cnt=&m_tbl_cnt;
			%let ins_tbl_list=&ins_tbl_list;
				
				/* Delete the existing records from the writeback table */

				/* Checking the existence of table,Do not delete if the table does not exist */
				%let etls_Exist = %eval(%sysfunc(exist(casuser.&tbl_nm_dtl, DATA))); 
			
				%if &etls_Exist. ne 0 %then %do;
				
					/* Checking if the From date is empty then assign a dummy date so that all existing data can be deleted */
					%if &m_scoring_from eq . or &m_scoring_from. eq %then %do;
						%let m_delete_from=01Jan1960;
					%end;
				
					%else %do;
						%let m_delete_from=&m_scoring_from;
					%end;
					
					%if &model_id eq . or &model_id eq  %then %do;
						proc cas; 
							table.deleteRows/
							table={
							caslib="casuser",   /* I18NOK:LINE */
							name="&tbl_nm_dtl",
							where="'&m_delete_from'd  <= datepart(LATEST_SCORE_DTTM) <= '&m_scoring_to'd" /* I18NOK:LINE */
							};
						quit;
					%end;
					
					%else %do;
						proc cas; 
								table.deleteRows/
								table={
								caslib="casuser",   /* I18NOK:LINE */
								name="&tbl_nm_dtl",
								where="'&m_delete_from'd  <= datepart(LATEST_SCORE_DTTM) <= '&m_scoring_to'd and (MODEL_ID in (&model_id))"	/* I18NOK:LINE */
								};
						quit;
					%end;
				%end;
				
				/* Extracter records */
				data casuser.writeback;
				set casuser.writeback_all_model;
				run;

			%Do i=1 %To &m_tbl_cnt; /* Beginning of Do-Loop for creation processing each table individually */

			/*Extract individual RM_ARM table name from the passed string so as to process them further. */
					
				%let selected_tbl_nm = %scan(%quote(&ins_tbl_list.),&i,%str(#));	/* I18NOK:LINE */


				/* If the scoring_from date is missing then load for entire table */
					
				%if (&m_scoring_from. eq . or &m_scoring_from. eq) and (&m_scoring_to. eq . or &m_scoring_to. eq) %then %do;
				
					%if (%sysfunc(index(&selected_tbl_nm.,_BIN_))>0) %then %do;  /* I18NOK:LINE */
						%rmcr_binary_writeback(tbl_nm=&selected_tbl_nm.);
					%end;
					
					%else %if (%sysfunc(index(&selected_tbl_nm.,_CONT_))>0) %then %do;	/* I18NOK:LINE */
						%rmcr_continuous_writeback(tbl_nm=&selected_tbl_nm.);
					%end;
					
				%end;	

				
				%else %do;
					%let lib_dt=RM_ARM;
				  %let m_scoring_from_dt=%sysfunc(inputn(&m_scoring_from.,date9.));     /* I18NOK:LINE */
						%let m_scoring_to_dt=%sysfunc(inputn(&m_scoring_to.,date9.));   /* I18NOK:LINE */

					Proc fedsql sessref=mysession;
						create table casuser.&selected_tbl_nm as
						select * from &lib_dt..&selected_tbl_nm as a
						where cast(datepart(cast(a.SCORING_AS_OF_DTTM as timestamp)) as double) <= &m_scoring_to_dt. and 			
							cast(datepart(cast(a.SCORING_AS_OF_DTTM as timestamp)) as double) >= &m_scoring_from_dt. ;


					Quit;
					
					%if (%sysfunc(index(&selected_tbl_nm.,_BIN_))>0) %then %do;		/* I18NOK:LINE */
						%rmcr_binary_writeback(tbl_nm=&selected_tbl_nm.,lib_flg='N');	/* I18NOK:LINE */
					%end;
					
					%else %if (%sysfunc(index(&selected_tbl_nm.,_CONT_))>0) %then %do;	/* I18NOK:LINE */
						%rmcr_continuous_writeback(tbl_nm=&selected_tbl_nm.,lib_flg='N');	/* I18NOK:LINE */
					%end;
				
				%end;	
			%end;/* End of Do-Loop for i */
						
	
		/* Create one data set for both binary and continuous model */
		%let etls_tblExist =%eval(%sysfunc(exist(casuser.binary_writeback_data, DATA))); 
		%if (&etls_tblExist ne 0) %then %do; 
			
			Data casuser.writeback(append=yes);
			set casuser.binary_writeback_data;
			run;
		%end;
		%let etls_tblExist =%eval(%sysfunc(exist(casuser.continuous_writeback_data, DATA))); 
						
		%if (&etls_tblExist ne 0) %then %do; 		
			Data casuser.writeback(append=yes);
			set casuser.continuous_writeback_data ;
			run;
		%end;
		
	/* Add Dabt promote table and save table to promote and save tables in bankcrfm library */
	
	%let etls_tblExist =%eval(%sysfunc(exist(&lib_crfm..writeback_all_model, DATA)));
		%if (&etls_tblExist ne 0) %then %do;
			%dabt_drop_table(m_table_nm=bankcrfm.writeback_all_model, m_cas_flg=Y);
		%end;
			
			
			data bankcrfm.writeback_all_model;
				set casuser.writeback;
				run;
	
			%dabt_save_cas_table(m_in_cas_lib_ref=&lib_crfm, m_in_cas_table_nm=writeback_all_model);
			%dabt_promote_table_to_cas(input_caslib_nm =&lib_crfm,input_table_nm =writeback_all_model);
		
		/* Terminate session after retrieving necessary information */	
			%dabt_terminate_cas_session(cas_session_ref = mysession);
			
		
	%end;

%mend rmcr_writeback;

