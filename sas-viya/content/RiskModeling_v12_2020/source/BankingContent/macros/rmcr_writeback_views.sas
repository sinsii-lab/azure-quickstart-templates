/*****************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: rmcr_writeback_views  					                       
 *                                                                 
 * LOGIC		: This macro is invoked to generate the Views from base tables.
 *
 * USAGE		: %rmcr_writeback_views
  Parameters	: NA
		 
			
  Purpose:
	
	 Created below a macro rmcr_writeback_views which enables the user to generate writeback views
	 
	Logic/Processing:
	
	Define macro variables for different types(Account,credit,application,customer)
	
	Initiate cas session
	
	Create a combined dataset for binary and continuous models
		fed sql to create casuser table from writeback table
	
	Append data to above created fed sql from continuous models to create casuser table from writeback table
		Append the data to form a single table 
	
	Check if view exist , else create view for 	ACCOUNT_SCORE_DETAIL
	
	Check if view exist , else create view for 	APPLICATION_SCORE_DETAIL
	
	Check if view exist , else create view for CREDIT_FACILITY_SCORE_DETAIL
	
	Check if view exist , else create view for CUSTOMER_SCORE_DETAIL

	Terminate cas session
	
 *****************************************************************************************/
 
%Macro rmcr_writeback_views;

/* Define macro variables for different types(Account,credit,application,customer) */

	%let typ_acc=ACCOUNT_RK;
	%let typ_cus=CUSTOMER_RK;
	%let typ_cre=CREDIT_FACILITY_RK;
	%let typ_app=APPLICATION_RK;
	%let lib_data=bankcrfm;

/* Initiate session after retrieving necessary information */	
	%dabt_initiate_cas_session(cas_session_ref = mysession);

/* Check if view exist , else create view for 	ACCOUNT_SCORE_DETAIL */

	%let etls_viewExist = %eval(%sysfunc(exist(bankcrfm.ACCOUNT_SCORE_DETAIL, DATA)) or 
											%sysfunc(exist(bankcrfm.ACCOUNT_SCORE_DETAIL, VIEW))); 

	%if &etls_viewExist. eq 0 %then %do;
		proc cas;
		table.view/PROMOTE=TRUE
			/* I18NOK:BEGIN */
		caslib="&lib_data"
		name="ACCOUNT_SCORE_DETAIL"
		tables={{
		name="writeback_all_model"
		caslib="&lib_data"
		varlist={'CREATED_BY','LATEST_SCORE_DTTM','LATEST_SCORE_NO','LATEST_SCORE_POINTS_NO','MODEL_PRODUCT_TYPE_CD','MODEL_TYPE_CD','CREATED_DTTM','PROCESSED_DTTM'}
				computedVars={{name="&typ_acc."}{name="MODEL_RK"}}
				computedVarsprogram="&typ_acc.=SUBJECT_RK;MODEL_RK=MODEL_ID"
		where="LEVEL_KEY_TYPE EQ '&typ_acc.'"
			/* I18NOK:END */
		}
		}
		;
		run;
		quit;
	%end;

/* Check if view exist , else create view for 	APPLICATION_SCORE_DETAIL */
	%let etls_viewExist = %eval(%sysfunc(exist(bankcrfm.APPLICATION_SCORE_DETAIL, DATA)) or 
											%sysfunc(exist(bankcrfm.APPLICATION_SCORE_DETAIL, VIEW))); 

	%if &etls_viewExist. eq 0 %then %do;
		proc cas;
		table.view/PROMOTE=TRUE
			/* I18NOK:BEGIN */
		caslib="&lib_data."
		name="APPLICATION_SCORE_DETAIL"
		tables={{
		name="writeback_all_model"
		caslib="&lib_data"
		varlist={'CREATED_BY','LATEST_SCORE_DTTM','LATEST_SCORE_NO','LATEST_SCORE_POINTS_NO','MODEL_PRODUCT_TYPE_CD','MODEL_TYPE_CD','CREATED_DTTM','PROCESSED_DTTM'}
				computedVars={{name="&typ_app."}{name="MODEL_RK"}{name="LATEST_ESTIMATED_RT"}}
				computedVarsprogram="&typ_app.=SUBJECT_RK;MODEL_RK=MODEL_ID;LATEST_ESTIMATED_RT=LATEST_SCORE_NO"
 		where="LEVEL_KEY_TYPE='&typ_app.'" 
			/* I18NOK:END */
		}
		}
		;
		run;
		quit;
	%end;

/* Check if view exist , else create view for CREDIT_FACILITY_SCORE_DETAIL */
	
	%let etls_viewExist = %eval(%sysfunc(exist(bankcrfm.CREDIT_FACILITY_SCORE_DETAIL, DATA)) or 
											%sysfunc(exist(bankcrfm.CREDIT_FACILITY_SCORE_DETAIL, VIEW))); 

	%if &etls_viewExist. eq 0 %then %do;
		proc cas;
		table.view/PROMOTE=TRUE
			/* I18NOK:BEGIN */
		caslib="&lib_data"
		name="CREDIT_FACILITY_SCORE_DETAIL"
		tables={{
		name="writeback_all_model"
		caslib="&lib_data"
		varlist={'CREATED_BY','LATEST_SCORE_DTTM','LATEST_SCORE_NO','LATEST_SCORE_POINTS_NO','MODEL_PRODUCT_TYPE_CD','MODEL_TYPE_CD','CREATED_DTTM','PROCESSED_DTTM'}
				computedVars={{name="&typ_cre."}{name="MODEL_RK"}}
				computedVarsprogram="&typ_cre.=SUBJECT_RK;MODEL_RK=MODEL_ID"
 		where="LEVEL_KEY_TYPE='&typ_cre.'" 
			/* I18NOK:END */
		}
		}
		;
		run;
		quit;
	%end;

/* Check if view exist , else create view for CUSTOMER_SCORE_DETAIL */
	
	%let etls_viewExist = %eval(%sysfunc(exist(bankcrfm.CUSTOMER_SCORE_DETAIL, DATA)) or 
											%sysfunc(exist(bankcrfm.CUSTOMER_SCORE_DETAIL, VIEW))); 

	%if &etls_viewExist. eq 0 %then %do;
		proc cas;
		table.view/PROMOTE=TRUE
			/* I18NOK:BEGIN */
		caslib="&lib_data"
		name="CUSTOMER_SCORE_DETAIL"
		tables={{
		name="writeback_all_model"
		caslib="&lib_data"
		varlist={'CREATED_BY','LATEST_SCORE_DTTM','LATEST_SCORE_NO','LATEST_SCORE_POINTS_NO','MODEL_PRODUCT_TYPE_CD','MODEL_TYPE_CD','CREATED_DTTM','PROCESSED_DTTM'}
				computedVars={{name="&typ_cus."}{name="MODEL_RK"}}
				computedVarsprogram="&typ_cus.=SUBJECT_RK;MODEL_RK=MODEL_ID"
 		where="LEVEL_KEY_TYPE='&typ_cus.'" 
			/* I18NOK:END */
		}
		}
		;
		run;
		quit;
	%end;
/* Terminate session after retrieving necessary information */	
	%dabt_terminate_cas_session(cas_session_ref = mysession);
			
%Mend rmcr_writeback_views;