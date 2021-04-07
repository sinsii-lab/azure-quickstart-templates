/********************************************************************************************************
   Module:  rmcr_fm_ext_load_cas.

   Function:  This macro populates FM Extension tables from lib_ref to CAS.

   Parameters: INPUT: Library path. All Mandatory Input tables. CAS_LOAD_FLAG to be Y in 
					  load_control table.
			
   Mandatory inputs : 	ACCOUNT_DEFAULT_DIM
						ACCOUNT_OTHER_INFO_BASE
						CREDIT_FACILITY_DEFAULT_DIM
						country_snapshot_base;
	
   Processing:	Replaces the four tables to CAS. By checking the CAS load status flag in load_control
				table. If running status is finished and cas load status is N then if load the data 
				to CAS environment.
					
*********************************************************************************************************/

%macro rmcr_fm_ext_load_cas(lib_ref=/* Path to staging(SAS) else leave blank for work */,
										start_period=/* Loading period start date(mm/dd/yyyy)*/ ,end_period=/* Loading period End date(mm/dd/yyyy)*/);

%if %nrbquote(&lib_ref) ne %then
	%do;
/*		libname mybase "%nrbquote(&lib_path)";*/
		%let mybase=&lib_ref.;
	%end;
%else
	%do;
		%let mybase=work;
	%end;

*=========================================================================================================;
*	Function: Loading all the final tables to CAS;
*	Essential Tables:  	ACCOUNT_DEFAULT_DIM;
*						ACCOUNT_OTHER_INFO_BASE;
*						CREDIT_FACILITY_DEFAULT_DIM;
*						country_snapshot_base;
*=========================================================================================================;

/* Initiate CAS session */
 %dabt_initiate_cas_session(cas_session_ref=fm_ext); 

data _null_;
	start_period1 = input("&start_period",mmddyy10.); /* i18nOK:Line */
	call symput('start_period1',start_period1); /* i18nOK:Line */
	end_period1 = input("&end_period",mmddyy10.); /* i18nOK:Line */
	call symput('end_period1',end_period1); /* i18nOK:Line */
run;

%let m1_start=%sysfunc(intnx(month,&start_period1,0,beginning));
%let m1_end=%sysfunc(intnx(month,&end_period1,0,end));

proc sql;
	select count(*) into :count_cl from &mybase..load_control     /* I18NOK:LINE */
		where (datepart(load_end_dttm) between &m1_start. and &m1_end.) and 
			RUN_STATUS = "Finished" and CAS_LOAD_STATUS="N";    /* I18NOK:LINE */
quit;

%if %eval(&count_cl) > 0 %then
	%do;
		%let cur_tbl=&mybase..ACCOUNT_DEFAULT_DIM;

		proc casutil;
			droptable casdata="ACCOUNT_DEFAULT_DIM" incaslib="bankcrfm" quiet;		/* I18NOK:LINE */
			load data=&cur_tbl. casout="ACCOUNT_DEFAULT_DIM" outcaslib="bankcrfm";   /* I18NOK:LINE */
		run;

		quit;

		%dabt_save_cas_table(m_in_cas_lib_ref=bankcrfm, m_in_cas_table_nm=ACCOUNT_DEFAULT_DIM);
		%dabt_promote_table_to_cas(input_caslib_nm =bankcrfm,input_table_nm =ACCOUNT_DEFAULT_DIM);

		/* Terminame the cas session */
		/*%dabt_terminate_cas_session(cas_session_ref=fm_extension);*/
		%let cur_tbl=&mybase..ACCOUNT_OTHER_INFO_BASE;

		proc casutil;
			droptable casdata="ACCOUNT_OTHER_INFO_BASE" incaslib="bankcrfm" quiet;		/* I18NOK:LINE */
			load data=&cur_tbl. casout="ACCOUNT_OTHER_INFO_BASE" outcaslib="bankcrfm";     /* I18NOK:LINE */
		run;

		quit;

		%dabt_save_cas_table(m_in_cas_lib_ref=bankcrfm, m_in_cas_table_nm=ACCOUNT_OTHER_INFO_BASE);
		%dabt_promote_table_to_cas(input_caslib_nm =bankcrfm,input_table_nm =ACCOUNT_OTHER_INFO_BASE);

		/* Terminame the cas session */
		/*%dabt_terminate_cas_session(cas_session_ref=fm_extension);*/
		%let cur_tbl=&mybase..CREDIT_FACILITY_DEFAULT_DIM;

		proc casutil;
			droptable casdata="CREDIT_FACILITY_DEFAULT_DIM" incaslib="bankcrfm" quiet;	/* I18NOK:LINE */
			load data=&cur_tbl. casout="CREDIT_FACILITY_DEFAULT_DIM" outcaslib="bankcrfm";     /* I18NOK:LINE */
		run;

		quit;

		%dabt_save_cas_table(m_in_cas_lib_ref=bankcrfm, m_in_cas_table_nm=CREDIT_FACILITY_DEFAULT_DIM);
		%dabt_promote_table_to_cas(input_caslib_nm =bankcrfm,input_table_nm =CREDIT_FACILITY_DEFAULT_DIM);

		/* Terminame the cas session */
		/*%dabt_terminate_cas_session(cas_session_ref=fm_extension); */
		%let cur_tbl=&mybase..country_snapshot_base;

		proc casutil;
			droptable casdata="country_snapshot_base" incaslib="bankcrfm" quiet;	/* I18NOK:LINE */
			load data=&cur_tbl. casout="country_snapshot_base" outcaslib="bankcrfm";     /* I18NOK:LINE */
		run;

		quit;

		%dabt_save_cas_table(m_in_cas_lib_ref=bankcrfm, m_in_cas_table_nm=country_snapshot_base);
		%dabt_promote_table_to_cas(input_caslib_nm =bankcrfm,input_table_nm =country_snapshot_base);

		/* Terminame the cas session */
		/*%dabt_terminate_cas_session(cas_session_ref=fm_extension);*/

		/* Updating the flag cas_load_status flag to Y */
		proc sql;
			update &mybase..load_control set CAS_LOAD_STATUS="Y"     /* I18NOK:LINE */
				where (datepart(load_end_dttm) between &m1_start. and &m1_end.) and 
					RUN_STATUS = "Finished" and CAS_LOAD_STATUS="N";    /* I18NOK:LINE */
		quit;

	%end;

%dabt_terminate_cas_session(cas_session_ref=fm_ext); 


%mend rmcr_fm_ext_load_cas;