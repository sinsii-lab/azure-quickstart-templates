/********************************************************************************************************
   Module:  rmcr_fm_ext_staging.

   Function:  This macro populates FM Extension tables.

   Parameters: INPUT: Library path, start period and end period.
			
   Mandatory inputs : All the Essential tables:
						Account_Dim
						Default_Event_Dim
						Credit_facility_dim
						Financial_Product_Dim
						Account_detail
						Country_Detail;
	
   Processing:	Performs Slowly Changing Dimension type 2 and incremental load of the four ABT Tables.
					
*********************************************************************************************************/

%macro rmcr_fm_ext_staging(lib_ref=/* libref to staging(SAS) else leave blank for work */,
				start_period=/* Loading period start date(mm/dd/yyyy)*/ ,end_period=/* Loading period End date(mm/dd/yyyy)*/);


/* Initiate CAS session */
 %dabt_initiate_cas_session(cas_session_ref=fm_ext);  

/********************************************************************************************************
	Function: Performing all initial checks;
	Essential Tables:  	Account_Dim
						Default_Event_Dim
						Credit_facility_dim
						Financial_Product_Dim
						Account_detail
						Country_Detail;

*********************************************************************************************************/

%if %nrbquote(&lib_ref) ne %then
	%do;
		%let mybase=&lib_ref.;
	%end;
%else
	%do;
		%let mybase=work;
	%end;

/* Creating or checking load_Control in mybase. */

%if %sysfunc(exist(&mybase..load_control,data)) eq 0 %then
	%do;

		proc sql;
			create table &mybase..LOAD_CONTROL
				(
				LOAD_START_DTTM num format=NLDATM21. informat=NLDATM21. label='Load Start Datetime',   /* I18NOK:LINE */
				LOAD_END_DTTM num format=NLDATM21. informat=NLDATM21. label='Load End Datetime',   /* I18NOK:LINE */
				PREV_LOAD_END_DTTM num format=NLDATM21. informat=NLDATM21. label='Previous month Load End Datetime',   /* I18NOK:LINE */
				LOAD_FLG char(1) format=$1. informat=$1. label='Load Flag',   /* I18NOK:LINE */
				RUN_STATUS char(10) label='Run status',    /* I18NOK:LINE */
				CAS_LOAD_STATUS char(10) label='CAS Load status'    /* I18NOK:LINE */
				);
		quit;

	%end;

/* Pre Code to set error to zero. */
data _null_;
run;

%let syscc = 0;

%macro error(err=/*Passing the syserr*/,stp=/*Passing the step where the code failed*/);
	%if %eval(&err) > 4 or %eval(&syscc) > 4 %then
		%do;
			%let op = %sysfunc(open(&mybase..load_control));
			%let count = %sysfunc(attrn(&op.,nobs));
			%let rc = %sysfunc(close(&op.));

			%if &count. > 0 %then
				%do;
					proc sql;
						update &mybase..load_control set RUN_STATUS = "Failed"     /* I18NOK:LINE */
								where RUN_STATUS = "Running";    /* I18NOK:LINE */
					quit;
				%end;

/*			%put process failed at &stp.;*/

			%abort return;
		%end;
%mend error;

/********************************************************************************************************
   Module:  rmcr_populate_load_control.

   Function:  Populate load_control table and check presence of initial tables.
					  
   Mandatory inputs : All the Input tables are Mandatory.
	
   Processing:	This macro populates load_control table and check the presence of mandatory tables.
*********************************************************************************************************/

/* Initial table check */
%let etls_tabs= 
	Account_Dim
	Load_Control
	Default_Event_Dim
	Credit_facility_dim
	Financial_Product_Dim
	Account_detail
	Country_Detail
	Account_Default_Dim
	ACCOUNT_OTHER_INFO_BASE
	Country_snapshot_base
	Credit_facility_default_dim;

%do i = 1 %to %sysfunc(countw(&etls_tabs));
	%let tab=%qscan(&etls_tabs,&i);     /* I18NOK:LINE */
	%let etls_tab&i = %sysfunc(exist(&mybase..&tab.,data));

	%if &&etls_tab&i eq 0 %then
		%do;
			%let current_tbl=initial step. All essential tables are not present;
			%error(err=111,stp=&current_tbl.);
		%end;
%end;

data _null_;
	start_period1 = input("&start_period",mmddyy10.); /* i18nOK:Line */
	call symput('start_period1',start_period1); /* i18nOK:Line */
	end_period1 = input("&end_period",mmddyy10.); /* i18nOK:Line */
	call symput('end_period1',end_period1); /* i18nOK:Line */
run;

%let m1_start=%sysfunc(intnx(month,&start_period1,0,beginning));
%let m1_end=%sysfunc(intnx(month,&end_period1,0,end));

proc sql;
	select count(*) into :count_lc from &mybase..load_control     /* I18NOK:LINE */
		where (datepart(load_end_dttm) between &m1_start. and &m1_end.) and 
			RUN_STATUS = "Failed";    /* I18NOK:LINE */
quit;

%if %eval(&count_lc) > 0 %then
	%do;
		proc sql;
			update &mybase..load_control set RUN_STATUS="Running"    /* I18NOK:LINE */
				where (datepart(load_end_dttm) between &m1_start. and &m1_end.) and 
					RUN_STATUS = "Failed";    /* I18NOK:LINE */
		quit;

		%goto after_lc;
	%end;

/* Creating unique constraint */
data &mybase..load_control(index= (XPK_FM_LOAD_CONTROL = (LOAD_START_DTTM  LOAD_END_DTTM)/unique));
	set &mybase..load_control;
run;

/*Finding minimum time_sk and  corresponding PERIOD_FIRST_DTTM from time_dim.*/
proc sql noprint;
	select min(time_sk), min(PERIOD_FIRST_DTTM) format = datetime21. into: min_sk, :min_first_dttm from &mybase..TIME_DIM ;/* i18nOK:Line */
quit;

%let min_sk = &min_sk.;
%let min_first_dttm = &min_first_dttm ; 

/*Finding dates for which load control  is to be loaded alongwith previous time_sk*/
proc sql noprint;
	create table &mybase..withsk_load_control  as
		select PERIOD_FIRST_DTTM,
			PERIOD_LAST_DTTM,
		case 
			when (time_sk-1) le 0 then &min_sk. 
			else (time_sk-1) 
		end 
	as time_sk_temp,
		time_sk as time_sk_orig
	from &mybase..TIME_DIM 
		where PERIOD_FIRST_DTTM <= dhms(&end_period1.,23,59,59)
			and PERIOD_LAST_DTTM >= dhms(&start_period1.,0,0,0);
quit;

/*Checking if first time history load cover entire time range. This will update the previous_load_end_dttm to the minimum time_sk in the time_dim.*/
proc sql noprint;

%let hist_load_rows=0;
/* 		select count(*) into: hist_load_rows  */
/* 			from work.foundation_mart_load_dates; */

	select min(time_sk_orig) into: min_load_strt_sk 
		from &mybase..withsk_load_control;

	/**/
	%if %eval(&hist_load_rows.) eq 0 %then %do;
		update &mybase..withsk_load_control 
		set time_sk_temp=&min_sk.
		where time_sk_orig=&min_load_strt_sk.;
	%end;
quit;

proc sql noprint;
	insert into &mybase..load_control
		(
		LOAD_START_DTTM,
		LOAD_END_DTTM,
		PREV_LOAD_END_DTTM,
		LOAD_FLG,
		RUN_STATUS,
		CAS_LOAD_STATUS
		)
	select 
		a.PERIOD_FIRST_DTTM,
		a.PERIOD_LAST_DTTM,
	case 
		when a.PERIOD_FIRST_DTTM = "&min_first_dttm"dt then (a.PERIOD_FIRST_DTTM - 1) 
		else b.PERIOD_LAST_DTTM 
	end 
		as PREV_LOAD_END_DTTM,
	&CHECK_FLAG_TRUE. ,
	&RUNNING_STATUS.,
	"N"     /* I18NOK:LINE */
from &mybase..withsk_load_control a left join &mybase..TIME_DIM  b
	on a.time_sk_temp=b.time_sk
	;
quit;

/* Checking for errors in populating load_control */
%let current_tbl=Loading load_control.;
%error(err=&syserr,stp=&current_tbl.);

/********************************************************************************************************
   Module:  rmcr_fm_ext_account_default_dim.

   Function:  This macro performs Slowly Changing dimension type 2 on account_default_dim.

   Parameters: INPUT: bankcrfm.account_default_dim,bankcrfm.LOAD_CONTROL,bankcrfm.ACCOUNT_DIM,
					  bankcrfm.ACCOUNT_DETAIL,bankcrfm.DEFAULT_EVENT_DIM
			
   Mandatory inputs : All the Input tables are Mandatory.
	
   Processing:	This macro checks the staging observations of account_default_dim table 
				with final observations of specified columns and stores the changes as a 
				new observation. Finally appends the non matching observations to the 
				final table(account_default_dim) and Loads it to CAS.
				Tasks Being Performed:
					->* Start - Loading CAS table to staging(work);
					->* Creation of Intermediate tables with respective filters;
					->* Assigning all the columns and table names to macros;
					->* Source and Target table both are in Teradata ;
					->* Source or Target table not in Teradata ;
					
*********************************************************************************************************/

*=================================================;
* Start - Loading CAS table to staging(work);
*=================================================;

%after_lc:


*==========================================================;
* Creation of Intermediate tables with respective filters;
*==========================================================;

/*LOAD_CONTROL_CURRENT_CYCLE*/
proc datasets lib =&mybase. nolist nowarn memtype = (data view);
   delete LOAD_CONTROL_CURRENT_CYCLE;
quit;

proc sql;
   create table &mybase..LOAD_CONTROL_CURRENT_CYCLE as
      select
         LOAD_START_DTTM   
            label = 'LOAD_START_DTTM',   /* I18NOK:LINE */
         LOAD_END_DTTM   
            label = 'LOAD_END_DTTM',    /* I18NOK:LINE */
         PREV_LOAD_END_DTTM   
            label = 'PREV_LOAD_END_DTTM'    /* I18NOK:LINE */
   from &mybase..LOAD_CONTROL
      where kupcase(LOAD_FLG)  eq  %kupcase(&CHECK_FLAG_TRUE.) 
         & RUN_STATUS IN (&RUNNING_STATUS.)
   ;
quit;

/*ACCOUNT_DIM_EXT*/
proc datasets lib =&mybase. nolist nowarn memtype = (data view);
   delete ACCOUNT_DIM_EXT;
quit;

proc sql;
   create table &mybase..ACCOUNT_DIM_EXT as
   select
      ACCOUNT_DIM.ACCOUNT_SK length = 8,
      ACCOUNT_DIM.ACCOUNT_RK length = 8,
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',   /* I18NOK:LINE */
      ACCOUNT_DIM.OPEN_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT',    /* I18NOK:LINE */
      ACCOUNT_DIM.ACCOUNT_STATUS_CD length = 3,
      ACCOUNT_DIM.ACCOUNT_TYPE_CD length = 3,
      ACCOUNT_DIM.ACCOUNT_SUB_TYPE_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM.PRIMARY_CUSTOMER_RK as CUSTOMER_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',    /* I18NOK:LINE */
      ACCOUNT_DIM.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',    /* I18NOK:LINE */
      ACCOUNT_DIM.PRIMARY_PRODUCT_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Product Key',    /* I18NOK:LINE */
      ACCOUNT_DIM.APPLICATION_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Financial Application Key',    /* I18NOK:LINE */
      ACCOUNT_DIM.OWNED_BY_INTERNAL_ORG_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Internal Organization Retained Key',    /* I18NOK:LINE */
      ACCOUNT_DIM.INDIVIDUAL_ORGANIZATION_CD length = 3   
         label = 'INDIVIDUAL_ORGANIZATION_CD',    /* I18NOK:LINE */
      ACCOUNT_DIM.CURRENT_LIMIT_AMT length = 8,
      ACCOUNT_DIM.OVERDRAFT_LIMIT_AMT length = 8   
         format = NLNUM18.5
         informat = NLNUM18.5
         label = 'Overdraft Limit Amount',     /* I18NOK:LINE */
      ACCOUNT_DIM.COLLATERAL_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM.ALLOW_OVERDRAFT_FLG length = 1   
         label = 'Allow_Overdraft_Flg',    /* I18NOK:LINE */
      ACCOUNT_DIM.CLOSE_REASON_CD length = 3   
         label = '_CD',    /* I18NOK:LINE */
      ACCOUNT_DIM.CLOSE_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT'   /* I18NOK:LINE */
   from
      &mybase..ACCOUNT_DIM, 
      &mybase..LOAD_CONTROL_CURRENT_CYCLE
   where
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM >= ACCOUNT_DIM.VALID_START_DTTM
      and LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM <= ACCOUNT_DIM.VALID_END_DTTM
   ;
quit;


/*ACCOUNT_DIM_DETAIL*/
proc datasets lib =&mybase. nolist nowarn memtype = (data view);
   delete ACCOUNT_DIM_DETAIL;
quit;

proc sql;
   create table &mybase..ACCOUNT_DIM_DETAIL as
   select
      ACCOUNT_DIM_EXT.ACCOUNT_SK length = 8,
      ACCOUNT_DIM_EXT.ACCOUNT_RK length = 8,
      ACCOUNT_DIM_EXT.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',    /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.OPEN_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT',    /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.ACCOUNT_STATUS_CD length = 3,
      ACCOUNT_DIM_EXT.ACCOUNT_TYPE_CD length = 3,
      ACCOUNT_DIM_EXT.ACCOUNT_SUB_TYPE_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.CUSTOMER_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.PRIMARY_PRODUCT_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Product Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.APPLICATION_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Financial Application Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.OWNED_BY_INTERNAL_ORG_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Internal Organization Retained Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.INDIVIDUAL_ORGANIZATION_CD length = 3   
         label = 'INDIVIDUAL_ORGANIZATION_CD',    /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.CURRENT_LIMIT_AMT length = 8,
      ACCOUNT_DIM_EXT.OVERDRAFT_LIMIT_AMT length = 8   
         format = NLNUM18.5
         informat = NLNUM18.5
         label = 'Overdraft Limit Amount',    /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.COLLATERAL_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.ALLOW_OVERDRAFT_FLG length = 1   
         label = 'Allow_Overdraft_Flg',   /* I18NOK:LINE */
      ACCOUNT_DETAIL.MATURITY_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.CLOSE_REASON_CD length = 3   
         label = '_CD',    /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.CLOSE_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT'    /* I18NOK:LINE */
   from
      &mybase..ACCOUNT_DIM_EXT, 
      &mybase..ACCOUNT_DETAIL
   where
      ACCOUNT_DIM_EXT.ACCOUNT_RK = ACCOUNT_DETAIL.ACCOUNT_RK
      and ACCOUNT_DIM_EXT.LOAD_END_DTTM >= ACCOUNT_DETAIL.VALID_START_DTTM
      and ACCOUNT_DIM_EXT.LOAD_END_DTTM <= ACCOUNT_DETAIL.VALID_END_DTTM
   ;
quit;


/*DEFAULT_EVENT_DIM_EXT*/
proc datasets lib =&mybase. nolist nowarn memtype = (data view);
   delete DEFAULT_EVENT_DIM_EXT;
quit;

proc sql;
   create table &mybase..DEFAULT_EVENT_DIM_EXT as
   select
      DEFAULT_EVENT_DIM.ACCOUNT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      DEFAULT_EVENT_DIM.DEFAULT_EVENT_SK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      DEFAULT_EVENT_DIM.DEFAULT_EVENT_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Default Event Retained Key',   /* I18NOK:LINE */
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',   /* I18NOK:LINE */
      DEFAULT_EVENT_DIM.DEFAULT_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = 'Default Date',   /* I18NOK:LINE */
      DEFAULT_EVENT_DIM.DEFAULT_STATUS_CD length = 3   
         label = 'Default Status Code',    /* I18NOK:LINE */
      DEFAULT_EVENT_DIM.EXPOSURE_AT_DEFAULT_AMT length = 8   
         format = NLNUM18.5
         informat = NLNUM18.5
         label = 'Exposure At Default Amount'    /* I18NOK:LINE */
   from
      &mybase..DEFAULT_EVENT_DIM, 
      &mybase..LOAD_CONTROL_CURRENT_CYCLE
   where
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM >= DEFAULT_EVENT_DIM.VALID_START_DTTM
      and LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM <= DEFAULT_EVENT_DIM.VALID_END_DTTM
   ;
quit;

/*ACCOUNT_DEFAULT_DIM*/
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete ACCOUNT_DEFAULT_DIM_TEMP;
quit;

proc sql;
   create table &mybase..ACCOUNT_DEFAULT_DIM_TEMP as
   select
      ACCOUNT_DIM_EXT.ACCOUNT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.ACCOUNT_STATUS_CD length = 3   
         format = $3.
         informat = $3.
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.CUSTOMER_RK length = 8,
      DEFAULT_EVENT_DIM_EXT.DEFAULT_DT length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = '_DT',    /* I18NOK:LINE */
      DEFAULT_EVENT_DIM_EXT.DEFAULT_EVENT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',    /* I18NOK:LINE */
      DEFAULT_EVENT_DIM_EXT.DEFAULT_STATUS_CD length = 3   
         format = $3.
         informat = $3.
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.OPEN_DT length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = '_DTTM',  /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.ACCOUNT_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.INDIVIDUAL_ORGANIZATION_CD length = 3   
         label = 'Individual Organization Code',  /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.LOAD_END_DTTM as PERIOD_LAST_DTTM length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = 'Load End Datetime',  /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.LOAD_END_DTTM length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = 'Load End Datetime'    /* I18NOK:LINE */
   from
      &mybase..ACCOUNT_DIM_EXT left join 
      &mybase..DEFAULT_EVENT_DIM_EXT as DEFAULT_EVENT_DIM_EXT
         on
         (
            ACCOUNT_DIM_EXT.ACCOUNT_RK = DEFAULT_EVENT_DIM_EXT.ACCOUNT_RK
            and ACCOUNT_DIM_EXT.LOAD_END_DTTM = DEFAULT_EVENT_DIM_EXT.LOAD_END_DTTM
            and ACCOUNT_DIM_EXT.ACCOUNT_RK > &DUMMY_RK.
            and DEFAULT_EVENT_DIM_EXT.DEFAULT_STATUS_CD IN (&DEFAULT_STATUS_CD.)
         )
   ;
quit;

*==============================================================;
* Assigning all the SCD columns and table names to macros;
*==============================================================;

%let etls_table = %nrquote(&mybase..ACCOUNT_DEFAULT_DIM);
%let etls_lib   = %nrquote(ABT Base Tables);

%let SYSLAST = %nrquote(&mybase..ACCOUNT_DEFAULT_DIM_TEMP); 

%let _INPUT_count = 1; 
%let _INPUT = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _INPUT_connect = ;
%let _INPUT_engine = ;
%let _INPUT_memtype = DATA;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/ACCOUNT_DEFAULT_DIM_TEMP_A5YP1KJX.BM00000P%(WorkTable%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let _INPUT0 = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _INPUT0_connect = ;
%let _INPUT0_engine = ;
%let _INPUT0_memtype = DATA;
%let _INPUT0_options = %nrquote();
%let _INPUT0_alter = %nrquote();
%let _INPUT0_path = %nrquote(/ACCOUNT_DEFAULT_DIM_TEMP_A5YP1KJX.BM00000P%(WorkTable%));
%let _INPUT0_type = 1;
%let _INPUT0_label = %nrquote();

%let _OUTPUT_count = 1; 
%let _OUTPUT = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_connect = null;
%let _OUTPUT_engine = BASE;
%let _OUTPUT_memtype = DATA;
%let _OUTPUT_options = %nrquote();
%let _OUTPUT_alter = %nrquote();
/*%let _OUTPUT_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Data Sources/ABT Base Tables/ACCOUNT_DEFAULT_DIM%(Table%));*/
%let _OUTPUT_type = 1;
%let _OUTPUT_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_keep = ACCOUNT_RK VALID_START_DTTM ACCOUNT_STATUS_CD CREDIT_FACILITY_RK 
        CUSTOMER_RK VALID_END_DTTM DEFAULT_DT DEFAULT_EVENT_RK 
        DEFAULT_STATUS_CD PROCESSED_DTTM OPEN_DT ACCOUNT_TYPE_CD 
        INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT_col_count = 13;
%let _OUTPUT_col0_name = ACCOUNT_RK;
%let _OUTPUT_col0_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col0_length = 8;
%let _OUTPUT_col0_type = ;
%let _OUTPUT_col0_format = 12.;
%let _OUTPUT_col0_informat = 12.;
%let _OUTPUT_col0_label = %nrquote(_RKorSK);
%let _OUTPUT_col0_input0 = ACCOUNT_RK;
%let _OUTPUT_col0_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col0_exp = ;
%let _OUTPUT_col0_input = ACCOUNT_RK;
%let _OUTPUT_col0_input_count = 1;
%let _OUTPUT_col1_name = VALID_START_DTTM;
%let _OUTPUT_col1_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col1_length = 8;
%let _OUTPUT_col1_type = ;
%let _OUTPUT_col1_format = NLDATM21.;
%let _OUTPUT_col1_informat = NLDATM21.;
%let _OUTPUT_col1_label = %nrquote(_DTTM);
%let _OUTPUT_col1_input0 = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col1_exp = Case When &FM_Grain=1 then INTNX("&Month_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')    /* I18NOK:LINE */     
        When &FM_Grain=2 then INTNX("&Week_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')         /* I18NOK:LINE */
        When &FM_Grain=3 then INTNX("&Day_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')      /* I18NOK:LINE */
	Else .         
End;
%let _OUTPUT_col1_input = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_input_count = 1;
%let _OUTPUT_col2_name = ACCOUNT_STATUS_CD;
%let _OUTPUT_col2_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col2_length = 3;
%let _OUTPUT_col2_type = $;
%let _OUTPUT_col2_format = ;
%let _OUTPUT_col2_informat = ;
%let _OUTPUT_col2_label = %nrquote(_CD);
%let _OUTPUT_col2_input0 = ACCOUNT_STATUS_CD;
%let _OUTPUT_col2_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col2_exp = ;
%let _OUTPUT_col2_input = ACCOUNT_STATUS_CD;
%let _OUTPUT_col2_input_count = 1;
%let _OUTPUT_col3_name = CREDIT_FACILITY_RK;
%let _OUTPUT_col3_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col3_length = 8;
%let _OUTPUT_col3_type = ;
%let _OUTPUT_col3_format = 12.;
%let _OUTPUT_col3_informat = 12.;
%let _OUTPUT_col3_label = %nrquote(_RKorSK);
%let _OUTPUT_col3_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT_col3_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col3_exp = ;
%let _OUTPUT_col3_input = CREDIT_FACILITY_RK;
%let _OUTPUT_col3_input_count = 1;
%let _OUTPUT_col4_name = CUSTOMER_RK;
%let _OUTPUT_col4_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col4_length = 8;
%let _OUTPUT_col4_type = ;
%let _OUTPUT_col4_format = 12.;
%let _OUTPUT_col4_informat = 12.;
%let _OUTPUT_col4_label = %nrquote(_RKorSK);
%let _OUTPUT_col4_input0 = CUSTOMER_RK;
%let _OUTPUT_col4_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col4_exp = ;
%let _OUTPUT_col4_input = CUSTOMER_RK;
%let _OUTPUT_col4_input_count = 1;
%let _OUTPUT_col5_name = VALID_END_DTTM;
%let _OUTPUT_col5_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col5_length = 8;
%let _OUTPUT_col5_type = ;
%let _OUTPUT_col5_format = NLDATM21.;
%let _OUTPUT_col5_informat = NLDATM21.;
%let _OUTPUT_col5_label = %nrquote(_DTTM);
%let _OUTPUT_col5_exp = ;
%let _OUTPUT_col5_input_count = 0;
%let _OUTPUT_col6_name = DEFAULT_DT;
%let _OUTPUT_col6_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col6_length = 8;
%let _OUTPUT_col6_type = ;
%let _OUTPUT_col6_format = NLDATM21.;
%let _OUTPUT_col6_informat = NLDATM21.;
%let _OUTPUT_col6_label = %nrquote(_DT);
%let _OUTPUT_col6_input0 = DEFAULT_DT;
%let _OUTPUT_col6_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col6_exp = ;
%let _OUTPUT_col6_input = DEFAULT_DT;
%let _OUTPUT_col6_input_count = 1;
%let _OUTPUT_col7_name = DEFAULT_EVENT_RK;
%let _OUTPUT_col7_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col7_length = 8;
%let _OUTPUT_col7_type = ;
%let _OUTPUT_col7_format = 12.;
%let _OUTPUT_col7_informat = 12.;
%let _OUTPUT_col7_label = %nrquote(_RKorSK);
%let _OUTPUT_col7_input0 = DEFAULT_EVENT_RK;
%let _OUTPUT_col7_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col7_exp = ;
%let _OUTPUT_col7_input = DEFAULT_EVENT_RK;
%let _OUTPUT_col7_input_count = 1;
%let _OUTPUT_col8_name = DEFAULT_STATUS_CD;
%let _OUTPUT_col8_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col8_length = 3;
%let _OUTPUT_col8_type = $;
%let _OUTPUT_col8_format = ;
%let _OUTPUT_col8_informat = ;
%let _OUTPUT_col8_label = %nrquote(_CD);
%let _OUTPUT_col8_input0 = DEFAULT_STATUS_CD;
%let _OUTPUT_col8_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col8_exp = ;
%let _OUTPUT_col8_input = DEFAULT_STATUS_CD;
%let _OUTPUT_col8_input_count = 1;
%let _OUTPUT_col9_name = PROCESSED_DTTM;
%let _OUTPUT_col9_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col9_length = 8;
%let _OUTPUT_col9_type = ;
%let _OUTPUT_col9_format = NLDATM21.;
%let _OUTPUT_col9_informat = NLDATM21.;
%let _OUTPUT_col9_label = %nrquote(_DTTM);
%let _OUTPUT_col9_exp = datetime();
%let _OUTPUT_col9_input_count = 0;
%let _OUTPUT_col10_name = OPEN_DT;
%let _OUTPUT_col10_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col10_length = 8;
%let _OUTPUT_col10_type = ;
%let _OUTPUT_col10_format = NLDATM21.;
%let _OUTPUT_col10_informat = NLDATM21.;
%let _OUTPUT_col10_label = %nrquote(_DT);
%let _OUTPUT_col10_input0 = OPEN_DT;
%let _OUTPUT_col10_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col10_exp = ;
%let _OUTPUT_col10_input = OPEN_DT;
%let _OUTPUT_col10_input_count = 1;
%let _OUTPUT_col11_name = ACCOUNT_TYPE_CD;
%let _OUTPUT_col11_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col11_length = 3;
%let _OUTPUT_col11_type = $;
%let _OUTPUT_col11_format = ;
%let _OUTPUT_col11_informat = ;
%let _OUTPUT_col11_label = %nrquote(_CD);
%let _OUTPUT_col11_input0 = ACCOUNT_TYPE_CD;
%let _OUTPUT_col11_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col11_exp = ;
%let _OUTPUT_col11_input = ACCOUNT_TYPE_CD;
%let _OUTPUT_col11_input_count = 1;
%let _OUTPUT_col12_name = INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT_col12_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT_col12_length = 3;
%let _OUTPUT_col12_type = $;
%let _OUTPUT_col12_format = ;
%let _OUTPUT_col12_informat = ;
%let _OUTPUT_col12_label = %nrquote(_CD);
%let _OUTPUT_col12_input0 = INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT_col12_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT_col12_exp = ;
%let _OUTPUT_col12_input = INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT_col12_input_count = 1;

%let _OUTPUT0 = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_connect = null;
%let _OUTPUT0_engine = BASE;
%let _OUTPUT0_memtype = DATA;
%let _OUTPUT0_options = %nrquote();
%let _OUTPUT0_alter = %nrquote();
/*%let _OUTPUT0_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Data Sources/ABT Base Tables/ACCOUNT_DEFAULT_DIM%(Table%));*/
%let _OUTPUT0_type = 1;
%let _OUTPUT0_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT0_keep = ACCOUNT_RK VALID_START_DTTM ACCOUNT_STATUS_CD CREDIT_FACILITY_RK 
        CUSTOMER_RK VALID_END_DTTM DEFAULT_DT DEFAULT_EVENT_RK 
        DEFAULT_STATUS_CD PROCESSED_DTTM OPEN_DT ACCOUNT_TYPE_CD 
        INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT0_col_count = 13;
%let _OUTPUT0_col0_name = ACCOUNT_RK;
%let _OUTPUT0_col0_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col0_length = 8;
%let _OUTPUT0_col0_type = ;
%let _OUTPUT0_col0_format = 12.;
%let _OUTPUT0_col0_informat = 12.;
%let _OUTPUT0_col0_label = %nrquote(_RKorSK);
%let _OUTPUT0_col0_input0 = ACCOUNT_RK;
%let _OUTPUT0_col0_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col0_exp = ;
%let _OUTPUT0_col0_input = ACCOUNT_RK;
%let _OUTPUT0_col0_input_count = 1;
%let _OUTPUT0_col1_name = VALID_START_DTTM;
%let _OUTPUT0_col1_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col1_length = 8;
%let _OUTPUT0_col1_type = ;
%let _OUTPUT0_col1_format = NLDATM21.;
%let _OUTPUT0_col1_informat = NLDATM21.;
%let _OUTPUT0_col1_label = %nrquote(_DTTM);
%let _OUTPUT0_col1_input0 = PERIOD_LAST_DTTM;
%let _OUTPUT0_col1_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col1_exp = Case When &FM_Grain=1 then INTNX("&Month_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')   /* I18NOK:LINE */      
        When &FM_Grain=2 then INTNX("&Week_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')         /* I18NOK:LINE */
        When &FM_Grain=3 then INTNX("&Day_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')       /* I18NOK:LINE */
	Else .         
End;
%let _OUTPUT0_col1_input = PERIOD_LAST_DTTM;
%let _OUTPUT0_col1_input_count = 1;
%let _OUTPUT0_col2_name = ACCOUNT_STATUS_CD;
%let _OUTPUT0_col2_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col2_length = 3;
%let _OUTPUT0_col2_type = $;
%let _OUTPUT0_col2_format = ;
%let _OUTPUT0_col2_informat = ;
%let _OUTPUT0_col2_label = %nrquote(_CD);
%let _OUTPUT0_col2_input0 = ACCOUNT_STATUS_CD;
%let _OUTPUT0_col2_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col2_exp = ;
%let _OUTPUT0_col2_input = ACCOUNT_STATUS_CD;
%let _OUTPUT0_col2_input_count = 1;
%let _OUTPUT0_col3_name = CREDIT_FACILITY_RK;
%let _OUTPUT0_col3_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col3_length = 8;
%let _OUTPUT0_col3_type = ;
%let _OUTPUT0_col3_format = 12.;
%let _OUTPUT0_col3_informat = 12.;
%let _OUTPUT0_col3_label = %nrquote(_RKorSK);
%let _OUTPUT0_col3_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT0_col3_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col3_exp = ;
%let _OUTPUT0_col3_input = CREDIT_FACILITY_RK;
%let _OUTPUT0_col3_input_count = 1;
%let _OUTPUT0_col4_name = CUSTOMER_RK;
%let _OUTPUT0_col4_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col4_length = 8;
%let _OUTPUT0_col4_type = ;
%let _OUTPUT0_col4_format = 12.;
%let _OUTPUT0_col4_informat = 12.;
%let _OUTPUT0_col4_label = %nrquote(_RKorSK);
%let _OUTPUT0_col4_input0 = CUSTOMER_RK;
%let _OUTPUT0_col4_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col4_exp = ;
%let _OUTPUT0_col4_input = CUSTOMER_RK;
%let _OUTPUT0_col4_input_count = 1;
%let _OUTPUT0_col5_name = VALID_END_DTTM;
%let _OUTPUT0_col5_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col5_length = 8;
%let _OUTPUT0_col5_type = ;
%let _OUTPUT0_col5_format = NLDATM21.;
%let _OUTPUT0_col5_informat = NLDATM21.;
%let _OUTPUT0_col5_label = %nrquote(_DTTM);
%let _OUTPUT0_col5_exp = ;
%let _OUTPUT0_col5_input_count = 0;
%let _OUTPUT0_col6_name = DEFAULT_DT;
%let _OUTPUT0_col6_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col6_length = 8;
%let _OUTPUT0_col6_type = ;
%let _OUTPUT0_col6_format = NLDATM21.;
%let _OUTPUT0_col6_informat = NLDATM21.;
%let _OUTPUT0_col6_label = %nrquote(_DT);
%let _OUTPUT0_col6_input0 = DEFAULT_DT;
%let _OUTPUT0_col6_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col6_exp = ;
%let _OUTPUT0_col6_input = DEFAULT_DT;
%let _OUTPUT0_col6_input_count = 1;
%let _OUTPUT0_col7_name = DEFAULT_EVENT_RK;
%let _OUTPUT0_col7_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col7_length = 8;
%let _OUTPUT0_col7_type = ;
%let _OUTPUT0_col7_format = 12.;
%let _OUTPUT0_col7_informat = 12.;
%let _OUTPUT0_col7_label = %nrquote(_RKorSK);
%let _OUTPUT0_col7_input0 = DEFAULT_EVENT_RK;
%let _OUTPUT0_col7_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col7_exp = ;
%let _OUTPUT0_col7_input = DEFAULT_EVENT_RK;
%let _OUTPUT0_col7_input_count = 1;
%let _OUTPUT0_col8_name = DEFAULT_STATUS_CD;
%let _OUTPUT0_col8_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col8_length = 3;
%let _OUTPUT0_col8_type = $;
%let _OUTPUT0_col8_format = ;
%let _OUTPUT0_col8_informat = ;
%let _OUTPUT0_col8_label = %nrquote(_CD);
%let _OUTPUT0_col8_input0 = DEFAULT_STATUS_CD;
%let _OUTPUT0_col8_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col8_exp = ;
%let _OUTPUT0_col8_input = DEFAULT_STATUS_CD;
%let _OUTPUT0_col8_input_count = 1;
%let _OUTPUT0_col9_name = PROCESSED_DTTM;
%let _OUTPUT0_col9_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col9_length = 8;
%let _OUTPUT0_col9_type = ;
%let _OUTPUT0_col9_format = NLDATM21.;
%let _OUTPUT0_col9_informat = NLDATM21.;
%let _OUTPUT0_col9_label = %nrquote(_DTTM);
%let _OUTPUT0_col9_exp = datetime();
%let _OUTPUT0_col9_input_count = 0;
%let _OUTPUT0_col10_name = OPEN_DT;
%let _OUTPUT0_col10_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col10_length = 8;
%let _OUTPUT0_col10_type = ;
%let _OUTPUT0_col10_format = NLDATM21.;
%let _OUTPUT0_col10_informat = NLDATM21.;
%let _OUTPUT0_col10_label = %nrquote(_DT);
%let _OUTPUT0_col10_input0 = OPEN_DT;
%let _OUTPUT0_col10_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col10_exp = ;
%let _OUTPUT0_col10_input = OPEN_DT;
%let _OUTPUT0_col10_input_count = 1;
%let _OUTPUT0_col11_name = ACCOUNT_TYPE_CD;
%let _OUTPUT0_col11_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col11_length = 3;
%let _OUTPUT0_col11_type = $;
%let _OUTPUT0_col11_format = ;
%let _OUTPUT0_col11_informat = ;
%let _OUTPUT0_col11_label = %nrquote(_CD);
%let _OUTPUT0_col11_input0 = ACCOUNT_TYPE_CD;
%let _OUTPUT0_col11_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col11_exp = ;
%let _OUTPUT0_col11_input = ACCOUNT_TYPE_CD;
%let _OUTPUT0_col11_input_count = 1;
%let _OUTPUT0_col12_name = INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT0_col12_table = &mybase..ACCOUNT_DEFAULT_DIM;
%let _OUTPUT0_col12_length = 3;
%let _OUTPUT0_col12_type = $;
%let _OUTPUT0_col12_format = ;
%let _OUTPUT0_col12_informat = ;
%let _OUTPUT0_col12_label = %nrquote(_CD);
%let _OUTPUT0_col12_input0 = INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT0_col12_input0_table = &mybase..ACCOUNT_DEFAULT_DIM_TEMP;
%let _OUTPUT0_col12_exp = ;
%let _OUTPUT0_col12_input = INDIVIDUAL_ORGANIZATION_CD;
%let _OUTPUT0_col12_input_count = 1;


%let Surrogate_key = ;
%let Surrogate_key_count = 0;
%let Surrogate_key0 = 0;
%let Business_key = ACCOUNT_RK;
%let Business_key_count = 1;
%let Business_key0 = 1;
%let Business_key1 = ACCOUNT_RK;
%let Tab_scd_def = %nrquote(&mybase..scddef_account_default_dim);
%let SCD2columns = DEFAULT_DT DEFAULT_STATUS_CD ACCOUNT_STATUS_CD CREDIT_FACILITY_RK CUSTOMER_RK 
DEFAULT_EVENT_RK OPEN_DT ACCOUNT_TYPE_CD INDIVIDUAL_ORGANIZATION_CD;
%let SCD2columns_count = 9;
%let SCD2columns0 = 9;
%let SCD2columns1 = DEFAULT_DT;
%let SCD2columns2 = DEFAULT_STATUS_CD;
%let SCD2columns3 = ACCOUNT_STATUS_CD;
%let SCD2columns4 = CREDIT_FACILITY_RK;
%let SCD2columns5 = CUSTOMER_RK;
%let SCD2columns6 = DEFAULT_EVENT_RK;
%let SCD2columns7 = OPEN_DT;
%let SCD2columns8 = ACCOUNT_TYPE_CD;
%let SCD2columns9 = INDIVIDUAL_ORGANIZATION_CD;
%let Etls_Xref = %nrquote(&mybase..xref_account_default_dim);

/* List of target columns to keep  */ 
%let _keep = ACCOUNT_RK VALID_START_DTTM ACCOUNT_STATUS_CD CREDIT_FACILITY_RK 
        CUSTOMER_RK VALID_END_DTTM DEFAULT_DT DEFAULT_EVENT_RK 
        DEFAULT_STATUS_CD PROCESSED_DTTM OPEN_DT ACCOUNT_TYPE_CD 
        INDIVIDUAL_ORGANIZATION_CD;
/* List of target columns to keep  */ 
%let keep = ACCOUNT_RK VALID_START_DTTM ACCOUNT_STATUS_CD CREDIT_FACILITY_RK 
        CUSTOMER_RK VALID_END_DTTM DEFAULT_DT DEFAULT_EVENT_RK 
        DEFAULT_STATUS_CD PROCESSED_DTTM OPEN_DT ACCOUNT_TYPE_CD 
        INDIVIDUAL_ORGANIZATION_CD;

%macro scd_type2_1_1_wrapper;

%let _OUTPUT0_engine=%upcase(&_OUTPUT0_engine);
%let _INPUT0_engine=%upcase(&_INPUT0_engine);

%let target_lib = %kscan(&_OUTPUT0,1,%str(.));
%let target_table = %kscan(&_OUTPUT0,2,%str(.));

*==============================================================;
* Source and Target table both are in Teradata ;
*==============================================================;

%if &_INPUT0_engine eq &_OUTPUT0_engine %then %do;

	%if &_INPUT0_engine eq TERADATA or &_INPUT0_engine eq ORACLE %then %do;

		option DBIDIRECTEXEC;

		proc sql;
	  		select count(*) into :load_cnt    /* I18NOK:LINE */
	  		from &mybase..load_control_current_cycle;
		quit;
		%let load_cnt = &load_cnt;

		proc sql;
	  		select load_end_dttm format = NLDATM20. into :load_dttm1-:load_dttm&load_cnt
	  		from &mybase..load_control_current_cycle
	  		order by load_end_dttm;
		quit;

		%do load_no = 1 %to &load_cnt;
	 
			%let load_dttm&load_no = &&load_dttm&load_no;
		
			%let scd_macro_name =bankfdn_scd_type2_1_1_%lowcase(&_INPUT0_engine.);
		
			%&scd_macro_name.;

		%end;

	%end;
	%else %do;

		%rmcr_fm_ext_scd_type2;

	%end;

%end;

*==============================================================;
* Source or Target table not in Teradata ;
*==============================================================;

%if &_INPUT0_engine ne &_OUTPUT0_engine %then %do;
	%rmcr_fm_ext_scd_type2;
%end;

%mend scd_type2_1_1_wrapper;
%scd_type2_1_1_wrapper;


/* Abort if any error occurs */
%let current_tbl=&mybase..ACCOUNT_DEFAULT_DIM;
%error(err=&syserr,stp=&current_tbl);


data _null_;run;

/********************************************************************************************************
   Module:  rmcr_fm_ext_account_other_info_base.

   Function:  This macro performs Slowly Changing dimension type 2 on account_other_info_base.

   Parameters: INPUT: bankcrfm.ACCOUNT_OTHER_INFO_BASE,bankcrfm.LOAD_CONTROL,bankcrfm.ACCOUNT_DIM,
					  bankcrfm.ACCOUNT_DETAIL
			
   Mandatory inputs : All the Input tables are Mandatory.
	
   Processing:	This macro checks the staging observations of ACCOUNT_OTHER_INFO_BASE table 
				with final observations of specified columns and stores the changes as a 
				new observation. Finally appends the non matching observations to the 
				final table(ACCOUNT_OTHER_INFO_BASE) and Loads it to CAS.
				Tasks Being Performed:
					->* Start - Loading CAS table to staging(work);
					->* Creation of Intermediate tables with respective filters;
					->* Assigning all the SCD columns and table names to macros;
					->* Finding the Schema for Target Table ;
					->* Finding the Schema for Source Table ;
					->* Source and Target table both are in SAS ;
					->* Source is SAS and Target table is in Other Database ;
					->* Deleting Temporary Table ;
					->* Source and Target table both in Other Database ;
					
*********************************************************************************************************/

*==============================================;
* Start - Loading CAS table to staging(work);
*==============================================;

/* Initiate cas session for loading fm extension */

/*%dabt_initiate_cas_session(cas_session_ref=fm_extension); */

*==========================================================;
* Creation of Intermediate tables with respective filters;
*==========================================================;

/* LOAD_CONTROL_CURRENT_CYCLE */
/*Created in the previous step*/

/*ACCOUNT_DIM_EXT*/
/*Created in the previous step*/

/*ACCOUNT_DIM_DETAIL*/
/*Created in the previous step*/


/*CREDIT_FACILITY_DIM_EXT*/
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete CREDIT_FACILITY_DIM_EXT;
quit;

proc sql;
   create table &mybase..CREDIT_FACILITY_DIM_EXT as
   select
      CREDIT_FACILITY_DIM.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',   /* I18NOK:LINE */
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',   /* I18NOK:LINE */
      CREDIT_FACILITY_DIM.CREDIT_FACILITY_SK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',  /* I18NOK:LINE */
      CREDIT_FACILITY_DIM.CREDIT_FACILITY_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = 'Credit Facility Type Code',   /* I18NOK:LINE */
      CREDIT_FACILITY_DIM.CUSTOMER_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Customer Key'   /* I18NOK:LINE */
   from
      &mybase..CREDIT_FACILITY_DIM, 
      &mybase..LOAD_CONTROL_CURRENT_CYCLE
   where
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM >= CREDIT_FACILITY_DIM.VALID_START_DTTM
      and LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM <= CREDIT_FACILITY_DIM.VALID_END_DTTM
   ;
quit;

/*FINANCIAL_PRODUCT_DIM_EXT*/

proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete FINANCIAL_PRODUCT_DIM_EXT;
quit;

proc sql;
   create table &mybase..FINANCIAL_PRODUCT_DIM_EXT as
   select
      FINANCIAL_PRODUCT_DIM.PRODUCT_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Product Key',  /* I18NOK:LINE */
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',  /* I18NOK:LINE */
      FINANCIAL_PRODUCT_DIM.PRODUCT_SK length = 8,
      FINANCIAL_PRODUCT_DIM.FINANCIAL_PRODUCT_TYPE_CD length = 3   
         label = 'Financial Product Type Code',  /* I18NOK:LINE */
      FINANCIAL_PRODUCT_DIM.PRODUCT_SUB_TYPE_CD length = 3
   from
      &mybase..LOAD_CONTROL_CURRENT_CYCLE, 
      &mybase..FINANCIAL_PRODUCT_DIM
   where
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM >= FINANCIAL_PRODUCT_DIM.VALID_START_DTTM
      and LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM <= FINANCIAL_PRODUCT_DIM.VALID_END_DTTM
   ;
quit;


/*ACCOUNT_PRODUCT_CRD*/
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete ACCOUNT_PRODUCT_CRD_TMP1;
quit;

proc sql;
   create table &mybase..ACCOUNT_PRODUCT_CRD_TMP1 as
   select
      ACCOUNT_DIM_DETAIL.ACCOUNT_SK length = 8,
      ACCOUNT_DIM_DETAIL.ACCOUNT_RK length = 8,
      ACCOUNT_DIM_DETAIL.LOAD_END_DTTM length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = 'Load End Datetime',  /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.OPEN_DT length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = '_DT',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.ACCOUNT_STATUS_CD length = 3,
      ACCOUNT_DIM_DETAIL.ACCOUNT_TYPE_CD length = 3,
      ACCOUNT_DIM_DETAIL.ACCOUNT_SUB_TYPE_CD length = 3   
         label = '_CD',    /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.CUSTOMER_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.PRIMARY_PRODUCT_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Product Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.APPLICATION_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Financial Application Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.OWNED_BY_INTERNAL_ORG_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Internal Organization Retained Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.INDIVIDUAL_ORGANIZATION_CD length = 3   
         label = 'INDIVIDUAL_ORGANIZATION_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.CURRENT_LIMIT_AMT length = 8,
      ACCOUNT_DIM_DETAIL.OVERDRAFT_LIMIT_AMT length = 8   
         format = NLNUM18.5
         informat = NLNUM18.5
         label = 'Overdraft Limit Amount',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.COLLATERAL_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.ALLOW_OVERDRAFT_FLG length = 1   
         label = 'Allow_Overdraft_Flg',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.MATURITY_DT length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = '_DT',   /* I18NOK:LINE */
      CREDIT_FACILITY_DIM_EXT.CREDIT_FACILITY_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = 'Credit Facility Type Code',    /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.CLOSE_REASON_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_DIM_DETAIL.CLOSE_DT length = 8   
         format = NLDATM20.
         informat = NLDATM20.   
         label = '_DT'   /* I18NOK:LINE */
   from
      &mybase..ACCOUNT_DIM_DETAIL, 
      &mybase..CREDIT_FACILITY_DIM_EXT
   where
      ACCOUNT_DIM_DETAIL.CREDIT_FACILITY_RK = CREDIT_FACILITY_DIM_EXT.CREDIT_FACILITY_RK
      and ACCOUNT_DIM_DETAIL.LOAD_END_DTTM = CREDIT_FACILITY_DIM_EXT.LOAD_END_DTTM
   ;
quit;


/* ACCOUNT_PRODUCT_CRD */
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete ACCOUNT_PRODUCT_CRD;
quit;

proc sql;
   create table &mybase..ACCOUNT_PRODUCT_CRD as
   select
      ACCOUNT_PRODUCT_CRD_TMP1.ACCOUNT_SK length = 8,
      ACCOUNT_PRODUCT_CRD_TMP1.ACCOUNT_RK length = 8,
      ACCOUNT_PRODUCT_CRD_TMP1.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.OPEN_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.ACCOUNT_STATUS_CD length = 3,
      ACCOUNT_PRODUCT_CRD_TMP1.ACCOUNT_TYPE_CD length = 3,
      ACCOUNT_PRODUCT_CRD_TMP1.ACCOUNT_SUB_TYPE_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.CUSTOMER_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.PRIMARY_PRODUCT_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Product Key',  /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.APPLICATION_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Financial Application Key',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.OWNED_BY_INTERNAL_ORG_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Internal Organization Retained Key',  /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.INDIVIDUAL_ORGANIZATION_CD length = 3   
         label = 'INDIVIDUAL_ORGANIZATION_CD',  /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.CURRENT_LIMIT_AMT length = 8,
      ACCOUNT_PRODUCT_CRD_TMP1.OVERDRAFT_LIMIT_AMT length = 8   
         format = NLNUM18.5
         informat = NLNUM18.5
         label = 'Overdraft Limit Amount',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.COLLATERAL_CD length = 3   
         label = '_CD',    /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.ALLOW_OVERDRAFT_FLG length = 1   
         label = 'Allow_Overdraft_Flg',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.MATURITY_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT',    /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.CREDIT_FACILITY_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = 'Credit Facility Type Code',    /* I18NOK:LINE */
      FINANCIAL_PRODUCT_DIM_EXT.PRODUCT_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Product Key',    /* I18NOK:LINE */
      FINANCIAL_PRODUCT_DIM_EXT.FINANCIAL_PRODUCT_TYPE_CD length = 3   
         label = 'Financial Product Type Code',    /* I18NOK:LINE */
      FINANCIAL_PRODUCT_DIM_EXT.PRODUCT_SUB_TYPE_CD length = 3,
      ACCOUNT_PRODUCT_CRD_TMP1.CLOSE_REASON_CD length = 3   
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD_TMP1.CLOSE_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT'   /* I18NOK:LINE */
   from
      &mybase..ACCOUNT_PRODUCT_CRD_TMP1, 
      &mybase..FINANCIAL_PRODUCT_DIM_EXT
   where
      ACCOUNT_PRODUCT_CRD_TMP1.PRIMARY_PRODUCT_RK = FINANCIAL_PRODUCT_DIM_EXT.PRODUCT_RK
      and ACCOUNT_PRODUCT_CRD_TMP1.LOAD_END_DTTM = FINANCIAL_PRODUCT_DIM_EXT.LOAD_END_DTTM
   ;
quit;


/*ACCOUNT_DIM_PREV_EXT*/
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete ACCOUNT_DIM_PREV_EXT;
quit;

proc sql;
   create table &mybase..ACCOUNT_DIM_PREV_EXT as
   select
      ACCOUNT_DIM.ACCOUNT_SK length = 8   
         label = '_RKorSK',   /* I18NOK:LINE */
      ACCOUNT_DIM.ACCOUNT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',  /* I18NOK:LINE */
      ACCOUNT_DIM.CURRENT_LIMIT_AMT length = 8   
         label = '_AMT',   /* I18NOK:LINE */
      ACCOUNT_DIM.OVERDRAFT_LIMIT_AMT length = 8   
         label = '_AMT',    /* I18NOK:LINE */
      LOAD_CONTROL_CURRENT_CYCLE.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',   /* I18NOK:LINE */
      LOAD_CONTROL_CURRENT_CYCLE.PREV_LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
   from
      &mybase..ACCOUNT_DIM, 
      &mybase..LOAD_CONTROL_CURRENT_CYCLE
   where
      ACCOUNT_DIM.VALID_START_DTTM <= LOAD_CONTROL_CURRENT_CYCLE.PREV_LOAD_END_DTTM
      and ACCOUNT_DIM.VALID_END_DTTM >= LOAD_CONTROL_CURRENT_CYCLE.PREV_LOAD_END_DTTM
      and ACCOUNT_DIM.ACCOUNT_RK > &DUMMY_RK.
   ;
quit;


/*ACCOUNT_OTHER_INFO_BASE*/
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete ACCOUNT_OTH_INF_BASETMP;
quit;

proc sql;
   create table &mybase..ACCOUNT_OTH_INF_BASETMP as
   select
      ACCOUNT_PRODUCT_CRD.ACCOUNT_RK length = 8,
      ACCOUNT_PRODUCT_CRD.LOAD_END_DTTM as PERIOD_LAST_DTTM length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = 'PERIOD_LAST_DTTM',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD length = 3   
         format = $3.
         informat = $3.,
      ACCOUNT_PRODUCT_CRD.ACCOUNT_SUB_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = '_CD',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD.CUSTOMER_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',    /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD.CREDIT_FACILITY_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = 'Credit Facility Type Code',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD.FINANCIAL_PRODUCT_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = 'Financial Product Type Code',   /* I18NOK:LINE */
      ACCOUNT_PRODUCT_CRD.PRODUCT_SUB_TYPE_CD length = 3   
         format = $3.
         informat = $3.,
      CASE WHEN ACCOUNT_PRODUCT_CRD.MATURITY_DT is not null and 
      ACCOUNT_PRODUCT_CRD.CLOSE_REASON_CD IN (&CLOSE_REASON_CD_PPY) 
      THEN  
      ((intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT,ACCOUNT_PRODUCT_CRD.MATURITY_DT) - intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT,ACCOUNT_PRODUCT_CRD.CLOSE_DT ))   /* I18NOK:LINE */
      *100) / 
      intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT,ACCOUNT_PRODUCT_CRD.MATURITY_DT)   /* I18NOK:LINE */
      ELSE  . 
      END as UNUTILIZED_MTH_TO_TENURE_PCT length = 8,
      CASE WHEN ACCOUNT_PRODUCT_CRD.MATURITY_DT is not null 
      and ACCOUNT_PRODUCT_CRD.CLOSE_REASON_CD  IN (&CLOSE_REASON_CD_PPY) THEN  
      (intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT ,ACCOUNT_PRODUCT_CRD.MATURITY_DT ) -    /* I18NOK:LINE */
      intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT ,ACCOUNT_PRODUCT_CRD.CLOSE_DT )) ELSE . END as UNUTILIZED_MTH_CNT length = 8,  /* I18NOK:LINE */
      CASE WHEN  ACCOUNT_PRODUCT_CRD.MATURITY_DT is not null and intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT,ACCOUNT_PRODUCT_CRD.MATURITY_DT)   /* I18NOK:LINE */
      > &LONG_TERM_DEP_MTH_CNT THEN &TERM_TYPE_LNG. 
      WHEN ACCOUNT_PRODUCT_CRD.MATURITY_DT is not null and intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT,ACCOUNT_PRODUCT_CRD.MATURITY_DT)    /* I18NOK:LINE */
      <=  &LONG_TERM_DEP_MTH_CNT AND ACCOUNT_PRODUCT_CRD.MATURITY_DT is not null and intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT,ACCOUNT_PRODUCT_CRD.MATURITY_DT)   /* I18NOK:LINE */
      > &SHORT_TERM_DEP_MTH_CNT THEN &TERM_TYPE_MID. 
      WHEN ACCOUNT_PRODUCT_CRD.MATURITY_DT is not null and intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT,ACCOUNT_PRODUCT_CRD.MATURITY_DT)   /* I18NOK:LINE */
      <= &SHORT_TERM_DEP_MTH_CNT THEN &TERM_TYPE_SHT.  
      Else '' 
      END as TERM_TYPE_CD length = 3   
         format = $3.
         informat = $3.,
      Case When ACCOUNT_PRODUCT_CRD.MATURITY_DT is not null 
      THEN intck('DTMONTH',ACCOUNT_PRODUCT_CRD.OPEN_DT ,ACCOUNT_PRODUCT_CRD.MATURITY_DT )    /* I18NOK:LINE */
      ELSE . END as TENURE_MTH_CNT length = 8,
      CASE WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD IN (&COR_ACCOUNT_TYPE) 
      & ACCOUNT_PRODUCT_CRD.ACCOUNT_SUB_TYPE_CD  IN     
               (&CHK_ACCOUNT_SUB_TYPE) THEN  &ACC_HIER_CHK 
      WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD   IN (&CCD_ACCOUNT_TYPE) 
      THEN &ACC_HIER_CCD 
      WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD IN (&COR_ACCOUNT_TYPE) 
      & ACCOUNT_PRODUCT_CRD.ACCOUNT_SUB_TYPE_CD  IN   
                (&SAV_ACCOUNT_SUB_TYPE,&CHK_ACCOUNT_SUB_TYPE) &  
      ACCOUNT_PRODUCT_CRD.ALLOW_OVERDRAFT_FLG  IN (&CHECK_FLAG_TRUE)  THEN 
      &ACC_HIER_OD 
          WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD  IN (&COR_ACCOUNT_TYPE) 
      AND  ACCOUNT_PRODUCT_CRD.ACCOUNT_SUB_TYPE_CD   IN    
                (&RDP_ACCOUNT_SUB_TYPE) THEN &ACC_HIER_RD 
          WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD  IN (&COR_ACCOUNT_TYPE) 
      AND ACCOUNT_PRODUCT_CRD.ACCOUNT_SUB_TYPE_CD  IN   
                (&SAV_ACCOUNT_SUB_TYPE) THEN &ACC_HIER_SAV 
          WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD   IN 
      (&MTG_ACCOUNT_TYPE, &LON_ACCOUNT_TYPE)   
       & (ACCOUNT_PRODUCT_CRD.COLLATERAL_CD  IS NOT NULL)    
                 THEN &ACC_HIER_SLN 
          WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD  IN (&COR_ACCOUNT_TYPE) 
      AND  ACCOUNT_PRODUCT_CRD.ACCOUNT_SUB_TYPE_CD   IN   
                 (&TDP_ACCOUNT_SUB_TYPE) THEN &ACC_HIER_TD 
          WHEN ACCOUNT_PRODUCT_CRD.ACCOUNT_TYPE_CD  IN (&LON_ACCOUNT_TYPE) 
      AND (ACCOUNT_PRODUCT_CRD.COLLATERAL_CD  IS NULL) THEN &ACC_HIER_ULN 
          ELSE ' '      /* I18NOK:LINE */
          END as ACCOUNT_HIERARCHY_CD length = 3   
         format = $3.
         informat = $3.,
      CASE WHEN ACCOUNT_PRODUCT_CRD.CURRENT_LIMIT_AMT  is not null 
      AND ACCOUNT_DIM_PREV_EXT.CURRENT_LIMIT_AMT  is null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.CURRENT_LIMIT_AMT  is null 
      AND ACCOUNT_DIM_PREV_EXT.CURRENT_LIMIT_AMT  is not null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.CURRENT_LIMIT_AMT  > ACCOUNT_DIM_PREV_EXT.CURRENT_LIMIT_AMT  
      THEN 1 
      ELSE 0 
      END as LIMIT_INCR_CNT length = 8,
      CASE WHEN ACCOUNT_PRODUCT_CRD.CURRENT_LIMIT_AMT  is not null 
      AND ACCOUNT_DIM_PREV_EXT.CURRENT_LIMIT_AMT  is null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.CURRENT_LIMIT_AMT  is null AND 
      ACCOUNT_DIM_PREV_EXT.CURRENT_LIMIT_AMT  is not null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.CURRENT_LIMIT_AMT  < ACCOUNT_DIM_PREV_EXT.CURRENT_LIMIT_AMT  
      THEN 1  
      ELSE 0 
      END as LIMIT_DECR_CNT length = 8,
      CASE WHEN ACCOUNT_PRODUCT_CRD.OVERDRAFT_LIMIT_AMT  is not null 
      AND ACCOUNT_DIM_PREV_EXT.OVERDRAFT_LIMIT_AMT  is null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.OVERDRAFT_LIMIT_AMT  is null 
      AND ACCOUNT_DIM_PREV_EXT.OVERDRAFT_LIMIT_AMT  is not null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.OVERDRAFT_LIMIT_AMT  > ACCOUNT_DIM_PREV_EXT.OVERDRAFT_LIMIT_AMT 
      THEN 1  
      ELSE 0 
      END as OD_LIMIT_INC_CNT length = 8,
      CASE WHEN ACCOUNT_PRODUCT_CRD.OVERDRAFT_LIMIT_AMT  is not null 
      AND ACCOUNT_DIM_PREV_EXT.OVERDRAFT_LIMIT_AMT  is null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.OVERDRAFT_LIMIT_AMT  is null 
      AND ACCOUNT_DIM_PREV_EXT.OVERDRAFT_LIMIT_AMT  is not null 
      THEN 0  
      WHEN ACCOUNT_PRODUCT_CRD.OVERDRAFT_LIMIT_AMT  < ACCOUNT_DIM_PREV_EXT.OVERDRAFT_LIMIT_AMT  
      THEN 1  
      ELSE 0 
      END as OD_LIMIT_DEC_CNT length = 8
   from
      &mybase..ACCOUNT_PRODUCT_CRD left join 
      &mybase..ACCOUNT_DIM_PREV_EXT
         on
         (
            ACCOUNT_PRODUCT_CRD.ACCOUNT_RK = ACCOUNT_DIM_PREV_EXT.ACCOUNT_RK
            and ACCOUNT_PRODUCT_CRD.LOAD_END_DTTM = ACCOUNT_DIM_PREV_EXT.LOAD_END_DTTM
         )
   ;
quit;

*==============================================================;
* Assigning all the SCD columns and table names to macros;
*==============================================================;

%let etls_table = %nrquote(&mybase.ACCOUNT_OTHER_INFO_BASE);
%let etls_lib   = %nrquote(ABT Base Tables);

%let SYSLAST = %nrquote(&mybase..ACCOUNT_OTH_INF_BASETMP); 

%let _INPUT_count = 1; 
%let _INPUT = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _INPUT_connect = null;
%let _INPUT_engine = BASE;
%let _INPUT_memtype = DATA;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/ACCOUNT_OTH_INF_BASETMP_A5YP1KJX.BM00000Q%(WorkTable%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let _INPUT1 = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _INPUT1_connect = null;
%let _INPUT1_engine = BASE;
%let _INPUT1_memtype = DATA;
%let _INPUT1_options = %nrquote();
%let _INPUT1_alter = %nrquote();
%let _INPUT1_path = %nrquote(/ACCOUNT_OTH_INF_BASETMP_A5YP1KJX.BM00000Q%(WorkTable%));
%let _INPUT1_type = 1;
%let _INPUT1_label = %nrquote();

%let _OUTPUT_count = 1; 
%let _OUTPUT = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_connect = null;
%let _OUTPUT_engine = BASE;
%let _OUTPUT_memtype = DATA;
%let _OUTPUT_options = %nrquote();
%let _OUTPUT_alter = %nrquote();
/*%let _OUTPUT_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Data Sources/ABT Base Tables/ACCOUNT_OTHER_INFO_BASE%(Table%));*/
%let _OUTPUT_type = 1;
%let _OUTPUT_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_keep = ACCOUNT_RK PERIOD_LAST_DTTM LIMIT_INCR_CNT LIMIT_DECR_CNT 
        OD_LIMIT_INC_CNT OD_LIMIT_DEC_CNT CUSTOMER_RK CREDIT_FACILITY_RK 
        ACCOUNT_TYPE_CD ACCOUNT_SUB_TYPE_CD ACCOUNT_HIERARCHY_CD 
        CREDIT_FACILITY_TYPE_CD FINANCIAL_PRODUCT_TYPE_CD PRODUCT_SUB_TYPE_CD 
        TENURE_MTH_CNT TERM_TYPE_CD UNUTILIZED_MTH_CNT 
        UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT_col_count = 18;
%let _OUTPUT_col0_name = ACCOUNT_RK;
%let _OUTPUT_col0_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col0_length = 8;
%let _OUTPUT_col0_type = ;
%let _OUTPUT_col0_format = 12.;
%let _OUTPUT_col0_informat = 12.;
%let _OUTPUT_col0_label = %nrquote(_RKorSK);
%let _OUTPUT_col0_input0 = ACCOUNT_RK;
%let _OUTPUT_col0_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col0_exp = ;
%let _OUTPUT_col0_input = ACCOUNT_RK;
%let _OUTPUT_col0_input_count = 1;
%let _OUTPUT_col1_name = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col1_length = 8;
%let _OUTPUT_col1_type = ;
%let _OUTPUT_col1_format = NLDATM21.;
%let _OUTPUT_col1_informat = NLDATM21.;
%let _OUTPUT_col1_label = %nrquote(_DTTM);
%let _OUTPUT_col1_input0 = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col1_exp = ;
%let _OUTPUT_col1_input = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_input_count = 1;
%let _OUTPUT_col2_name = LIMIT_INCR_CNT;
%let _OUTPUT_col2_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col2_length = 8;
%let _OUTPUT_col2_type = ;
%let _OUTPUT_col2_format = ;
%let _OUTPUT_col2_informat = ;
%let _OUTPUT_col2_label = %nrquote(LIMIT_INCR_CNT);
%let _OUTPUT_col2_input0 = LIMIT_INCR_CNT;
%let _OUTPUT_col2_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col2_exp = ;
%let _OUTPUT_col2_input = LIMIT_INCR_CNT;
%let _OUTPUT_col2_input_count = 1;
%let _OUTPUT_col3_name = LIMIT_DECR_CNT;
%let _OUTPUT_col3_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col3_length = 8;
%let _OUTPUT_col3_type = ;
%let _OUTPUT_col3_format = ;
%let _OUTPUT_col3_informat = ;
%let _OUTPUT_col3_label = %nrquote(LIMIT_DECR_CNT);
%let _OUTPUT_col3_input0 = LIMIT_DECR_CNT;
%let _OUTPUT_col3_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col3_exp = ;
%let _OUTPUT_col3_input = LIMIT_DECR_CNT;
%let _OUTPUT_col3_input_count = 1;
%let _OUTPUT_col4_name = OD_LIMIT_INC_CNT;
%let _OUTPUT_col4_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col4_length = 8;
%let _OUTPUT_col4_type = ;
%let _OUTPUT_col4_format = ;
%let _OUTPUT_col4_informat = ;
%let _OUTPUT_col4_label = %nrquote(OD_LIMIT_INC_CNT);
%let _OUTPUT_col4_input0 = OD_LIMIT_INC_CNT;
%let _OUTPUT_col4_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col4_exp = ;
%let _OUTPUT_col4_input = OD_LIMIT_INC_CNT;
%let _OUTPUT_col4_input_count = 1;
%let _OUTPUT_col5_name = OD_LIMIT_DEC_CNT;
%let _OUTPUT_col5_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col5_length = 8;
%let _OUTPUT_col5_type = ;
%let _OUTPUT_col5_format = ;
%let _OUTPUT_col5_informat = ;
%let _OUTPUT_col5_label = %nrquote(OD_LIMIT_DEC_CNT);
%let _OUTPUT_col5_input0 = OD_LIMIT_DEC_CNT;
%let _OUTPUT_col5_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col5_exp = ;
%let _OUTPUT_col5_input = OD_LIMIT_DEC_CNT;
%let _OUTPUT_col5_input_count = 1;
%let _OUTPUT_col6_name = CUSTOMER_RK;
%let _OUTPUT_col6_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col6_length = 8;
%let _OUTPUT_col6_type = ;
%let _OUTPUT_col6_format = 12.;
%let _OUTPUT_col6_informat = 12.;
%let _OUTPUT_col6_label = %nrquote(_RKorSK);
%let _OUTPUT_col6_input0 = CUSTOMER_RK;
%let _OUTPUT_col6_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col6_exp = ;
%let _OUTPUT_col6_input = CUSTOMER_RK;
%let _OUTPUT_col6_input_count = 1;
%let _OUTPUT_col7_name = CREDIT_FACILITY_RK;
%let _OUTPUT_col7_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col7_length = 8;
%let _OUTPUT_col7_type = ;
%let _OUTPUT_col7_format = 12.;
%let _OUTPUT_col7_informat = 12.;
%let _OUTPUT_col7_label = %nrquote(_RKorSK);
%let _OUTPUT_col7_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT_col7_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col7_exp = ;
%let _OUTPUT_col7_input = CREDIT_FACILITY_RK;
%let _OUTPUT_col7_input_count = 1;
%let _OUTPUT_col8_name = ACCOUNT_TYPE_CD;
%let _OUTPUT_col8_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col8_length = 3;
%let _OUTPUT_col8_type = $;
%let _OUTPUT_col8_format = ;
%let _OUTPUT_col8_informat = ;
%let _OUTPUT_col8_label = %nrquote(_CD);
%let _OUTPUT_col8_input0 = ACCOUNT_TYPE_CD;
%let _OUTPUT_col8_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col8_exp = ;
%let _OUTPUT_col8_input = ACCOUNT_TYPE_CD;
%let _OUTPUT_col8_input_count = 1;
%let _OUTPUT_col9_name = ACCOUNT_SUB_TYPE_CD;
%let _OUTPUT_col9_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col9_length = 3;
%let _OUTPUT_col9_type = $;
%let _OUTPUT_col9_format = ;
%let _OUTPUT_col9_informat = ;
%let _OUTPUT_col9_label = %nrquote(_CD);
%let _OUTPUT_col9_input0 = ACCOUNT_SUB_TYPE_CD;
%let _OUTPUT_col9_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col9_exp = ;
%let _OUTPUT_col9_input = ACCOUNT_SUB_TYPE_CD;
%let _OUTPUT_col9_input_count = 1;
%let _OUTPUT_col10_name = ACCOUNT_HIERARCHY_CD;
%let _OUTPUT_col10_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col10_length = 3;
%let _OUTPUT_col10_type = $;
%let _OUTPUT_col10_format = ;
%let _OUTPUT_col10_informat = ;
%let _OUTPUT_col10_label = %nrquote(_CD);
%let _OUTPUT_col10_input0 = ACCOUNT_HIERARCHY_CD;
%let _OUTPUT_col10_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col10_exp = ;
%let _OUTPUT_col10_input = ACCOUNT_HIERARCHY_CD;
%let _OUTPUT_col10_input_count = 1;
%let _OUTPUT_col11_name = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT_col11_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col11_length = 3;
%let _OUTPUT_col11_type = $;
%let _OUTPUT_col11_format = ;
%let _OUTPUT_col11_informat = ;
%let _OUTPUT_col11_label = %nrquote(_CD);
%let _OUTPUT_col11_input0 = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT_col11_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col11_exp = ;
%let _OUTPUT_col11_input = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT_col11_input_count = 1;
%let _OUTPUT_col12_name = FINANCIAL_PRODUCT_TYPE_CD;
%let _OUTPUT_col12_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col12_length = 3;
%let _OUTPUT_col12_type = $;
%let _OUTPUT_col12_format = ;
%let _OUTPUT_col12_informat = ;
%let _OUTPUT_col12_label = %nrquote(_CD);
%let _OUTPUT_col12_input0 = FINANCIAL_PRODUCT_TYPE_CD;
%let _OUTPUT_col12_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col12_exp = ;
%let _OUTPUT_col12_input = FINANCIAL_PRODUCT_TYPE_CD;
%let _OUTPUT_col12_input_count = 1;
%let _OUTPUT_col13_name = PRODUCT_SUB_TYPE_CD;
%let _OUTPUT_col13_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col13_length = 3;
%let _OUTPUT_col13_type = $;
%let _OUTPUT_col13_format = ;
%let _OUTPUT_col13_informat = ;
%let _OUTPUT_col13_label = %nrquote(_CD);
%let _OUTPUT_col13_input0 = PRODUCT_SUB_TYPE_CD;
%let _OUTPUT_col13_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col13_exp = ;
%let _OUTPUT_col13_input = PRODUCT_SUB_TYPE_CD;
%let _OUTPUT_col13_input_count = 1;
%let _OUTPUT_col14_name = TENURE_MTH_CNT;
%let _OUTPUT_col14_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col14_length = 8;
%let _OUTPUT_col14_type = ;
%let _OUTPUT_col14_format = ;
%let _OUTPUT_col14_informat = ;
%let _OUTPUT_col14_label = %nrquote(TENURE_MTH_CNT);
%let _OUTPUT_col14_input0 = TENURE_MTH_CNT;
%let _OUTPUT_col14_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col14_exp = ;
%let _OUTPUT_col14_input = TENURE_MTH_CNT;
%let _OUTPUT_col14_input_count = 1;
%let _OUTPUT_col15_name = TERM_TYPE_CD;
%let _OUTPUT_col15_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col15_length = 3;
%let _OUTPUT_col15_type = $;
%let _OUTPUT_col15_format = ;
%let _OUTPUT_col15_informat = ;
%let _OUTPUT_col15_label = %nrquote(_CD);
%let _OUTPUT_col15_input0 = TERM_TYPE_CD;
%let _OUTPUT_col15_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col15_exp = ;
%let _OUTPUT_col15_input = TERM_TYPE_CD;
%let _OUTPUT_col15_input_count = 1;
%let _OUTPUT_col16_name = UNUTILIZED_MTH_CNT;
%let _OUTPUT_col16_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col16_length = 8;
%let _OUTPUT_col16_type = ;
%let _OUTPUT_col16_format = ;
%let _OUTPUT_col16_informat = ;
%let _OUTPUT_col16_label = %nrquote(UNUTILIZED_MTH_CNT);
%let _OUTPUT_col16_input0 = UNUTILIZED_MTH_CNT;
%let _OUTPUT_col16_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col16_exp = ;
%let _OUTPUT_col16_input = UNUTILIZED_MTH_CNT;
%let _OUTPUT_col16_input_count = 1;
%let _OUTPUT_col17_name = UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT_col17_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT_col17_length = 8;
%let _OUTPUT_col17_type = ;
%let _OUTPUT_col17_format = NLNUM9.4;
%let _OUTPUT_col17_informat = NLNUM9.4;
%let _OUTPUT_col17_label = %nrquote(UNUTILIZED_MTH_TO_TENURE_PCT);
%let _OUTPUT_col17_input0 = UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT_col17_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT_col17_exp = ;
%let _OUTPUT_col17_input = UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT_col17_input_count = 1;

%let _OUTPUT1 = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_connect = null;
%let _OUTPUT1_engine = BASE;
%let _OUTPUT1_memtype = DATA;
%let _OUTPUT1_options = %nrquote();
%let _OUTPUT1_alter = %nrquote();
/*%let _OUTPUT1_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Data Sources/ABT Base Tables/ACCOUNT_OTHER_INFO_BASE%(Table%));*/
%let _OUTPUT1_type = 1;
%let _OUTPUT1_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT1_keep = ACCOUNT_RK PERIOD_LAST_DTTM LIMIT_INCR_CNT LIMIT_DECR_CNT 
        OD_LIMIT_INC_CNT OD_LIMIT_DEC_CNT CUSTOMER_RK CREDIT_FACILITY_RK 
        ACCOUNT_TYPE_CD ACCOUNT_SUB_TYPE_CD ACCOUNT_HIERARCHY_CD 
        CREDIT_FACILITY_TYPE_CD FINANCIAL_PRODUCT_TYPE_CD PRODUCT_SUB_TYPE_CD 
        TENURE_MTH_CNT TERM_TYPE_CD UNUTILIZED_MTH_CNT 
        UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT1_col_count = 18;
%let _OUTPUT1_col0_name = ACCOUNT_RK;
%let _OUTPUT1_col0_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col0_length = 8;
%let _OUTPUT1_col0_type = ;
%let _OUTPUT1_col0_format = 12.;
%let _OUTPUT1_col0_informat = 12.;
%let _OUTPUT1_col0_label = %nrquote(_RKorSK);
%let _OUTPUT1_col0_input0 = ACCOUNT_RK;
%let _OUTPUT1_col0_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col0_exp = ;
%let _OUTPUT1_col0_input = ACCOUNT_RK;
%let _OUTPUT1_col0_input_count = 1;
%let _OUTPUT1_col1_name = PERIOD_LAST_DTTM;
%let _OUTPUT1_col1_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col1_length = 8;
%let _OUTPUT1_col1_type = ;
%let _OUTPUT1_col1_format = NLDATM21.;
%let _OUTPUT1_col1_informat = NLDATM21.;
%let _OUTPUT1_col1_label = %nrquote(_DTTM);
%let _OUTPUT1_col1_input0 = PERIOD_LAST_DTTM;
%let _OUTPUT1_col1_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col1_exp = ;
%let _OUTPUT1_col1_input = PERIOD_LAST_DTTM;
%let _OUTPUT1_col1_input_count = 1;
%let _OUTPUT1_col2_name = LIMIT_INCR_CNT;
%let _OUTPUT1_col2_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col2_length = 8;
%let _OUTPUT1_col2_type = ;
%let _OUTPUT1_col2_format = ;
%let _OUTPUT1_col2_informat = ;
%let _OUTPUT1_col2_label = %nrquote(LIMIT_INCR_CNT);
%let _OUTPUT1_col2_input0 = LIMIT_INCR_CNT;
%let _OUTPUT1_col2_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col2_exp = ;
%let _OUTPUT1_col2_input = LIMIT_INCR_CNT;
%let _OUTPUT1_col2_input_count = 1;
%let _OUTPUT1_col3_name = LIMIT_DECR_CNT;
%let _OUTPUT1_col3_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col3_length = 8;
%let _OUTPUT1_col3_type = ;
%let _OUTPUT1_col3_format = ;
%let _OUTPUT1_col3_informat = ;
%let _OUTPUT1_col3_label = %nrquote(LIMIT_DECR_CNT);
%let _OUTPUT1_col3_input0 = LIMIT_DECR_CNT;
%let _OUTPUT1_col3_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col3_exp = ;
%let _OUTPUT1_col3_input = LIMIT_DECR_CNT;
%let _OUTPUT1_col3_input_count = 1;
%let _OUTPUT1_col4_name = OD_LIMIT_INC_CNT;
%let _OUTPUT1_col4_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col4_length = 8;
%let _OUTPUT1_col4_type = ;
%let _OUTPUT1_col4_format = ;
%let _OUTPUT1_col4_informat = ;
%let _OUTPUT1_col4_label = %nrquote(OD_LIMIT_INC_CNT);
%let _OUTPUT1_col4_input0 = OD_LIMIT_INC_CNT;
%let _OUTPUT1_col4_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col4_exp = ;
%let _OUTPUT1_col4_input = OD_LIMIT_INC_CNT;
%let _OUTPUT1_col4_input_count = 1;
%let _OUTPUT1_col5_name = OD_LIMIT_DEC_CNT;
%let _OUTPUT1_col5_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col5_length = 8;
%let _OUTPUT1_col5_type = ;
%let _OUTPUT1_col5_format = ;
%let _OUTPUT1_col5_informat = ;
%let _OUTPUT1_col5_label = %nrquote(OD_LIMIT_DEC_CNT);
%let _OUTPUT1_col5_input0 = OD_LIMIT_DEC_CNT;
%let _OUTPUT1_col5_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col5_exp = ;
%let _OUTPUT1_col5_input = OD_LIMIT_DEC_CNT;
%let _OUTPUT1_col5_input_count = 1;
%let _OUTPUT1_col6_name = CUSTOMER_RK;
%let _OUTPUT1_col6_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col6_length = 8;
%let _OUTPUT1_col6_type = ;
%let _OUTPUT1_col6_format = 12.;
%let _OUTPUT1_col6_informat = 12.;
%let _OUTPUT1_col6_label = %nrquote(_RKorSK);
%let _OUTPUT1_col6_input0 = CUSTOMER_RK;
%let _OUTPUT1_col6_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col6_exp = ;
%let _OUTPUT1_col6_input = CUSTOMER_RK;
%let _OUTPUT1_col6_input_count = 1;
%let _OUTPUT1_col7_name = CREDIT_FACILITY_RK;
%let _OUTPUT1_col7_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col7_length = 8;
%let _OUTPUT1_col7_type = ;
%let _OUTPUT1_col7_format = 12.;
%let _OUTPUT1_col7_informat = 12.;
%let _OUTPUT1_col7_label = %nrquote(_RKorSK);
%let _OUTPUT1_col7_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT1_col7_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col7_exp = ;
%let _OUTPUT1_col7_input = CREDIT_FACILITY_RK;
%let _OUTPUT1_col7_input_count = 1;
%let _OUTPUT1_col8_name = ACCOUNT_TYPE_CD;
%let _OUTPUT1_col8_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col8_length = 3;
%let _OUTPUT1_col8_type = $;
%let _OUTPUT1_col8_format = ;
%let _OUTPUT1_col8_informat = ;
%let _OUTPUT1_col8_label = %nrquote(_CD);
%let _OUTPUT1_col8_input0 = ACCOUNT_TYPE_CD;
%let _OUTPUT1_col8_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col8_exp = ;
%let _OUTPUT1_col8_input = ACCOUNT_TYPE_CD;
%let _OUTPUT1_col8_input_count = 1;
%let _OUTPUT1_col9_name = ACCOUNT_SUB_TYPE_CD;
%let _OUTPUT1_col9_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col9_length = 3;
%let _OUTPUT1_col9_type = $;
%let _OUTPUT1_col9_format = ;
%let _OUTPUT1_col9_informat = ;
%let _OUTPUT1_col9_label = %nrquote(_CD);
%let _OUTPUT1_col9_input0 = ACCOUNT_SUB_TYPE_CD;
%let _OUTPUT1_col9_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col9_exp = ;
%let _OUTPUT1_col9_input = ACCOUNT_SUB_TYPE_CD;
%let _OUTPUT1_col9_input_count = 1;
%let _OUTPUT1_col10_name = ACCOUNT_HIERARCHY_CD;
%let _OUTPUT1_col10_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col10_length = 3;
%let _OUTPUT1_col10_type = $;
%let _OUTPUT1_col10_format = ;
%let _OUTPUT1_col10_informat = ;
%let _OUTPUT1_col10_label = %nrquote(_CD);
%let _OUTPUT1_col10_input0 = ACCOUNT_HIERARCHY_CD;
%let _OUTPUT1_col10_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col10_exp = ;
%let _OUTPUT1_col10_input = ACCOUNT_HIERARCHY_CD;
%let _OUTPUT1_col10_input_count = 1;
%let _OUTPUT1_col11_name = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT1_col11_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col11_length = 3;
%let _OUTPUT1_col11_type = $;
%let _OUTPUT1_col11_format = ;
%let _OUTPUT1_col11_informat = ;
%let _OUTPUT1_col11_label = %nrquote(_CD);
%let _OUTPUT1_col11_input0 = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT1_col11_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col11_exp = ;
%let _OUTPUT1_col11_input = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT1_col11_input_count = 1;
%let _OUTPUT1_col12_name = FINANCIAL_PRODUCT_TYPE_CD;
%let _OUTPUT1_col12_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col12_length = 3;
%let _OUTPUT1_col12_type = $;
%let _OUTPUT1_col12_format = ;
%let _OUTPUT1_col12_informat = ;
%let _OUTPUT1_col12_label = %nrquote(_CD);
%let _OUTPUT1_col12_input0 = FINANCIAL_PRODUCT_TYPE_CD;
%let _OUTPUT1_col12_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col12_exp = ;
%let _OUTPUT1_col12_input = FINANCIAL_PRODUCT_TYPE_CD;
%let _OUTPUT1_col12_input_count = 1;
%let _OUTPUT1_col13_name = PRODUCT_SUB_TYPE_CD;
%let _OUTPUT1_col13_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col13_length = 3;
%let _OUTPUT1_col13_type = $;
%let _OUTPUT1_col13_format = ;
%let _OUTPUT1_col13_informat = ;
%let _OUTPUT1_col13_label = %nrquote(_CD);
%let _OUTPUT1_col13_input0 = PRODUCT_SUB_TYPE_CD;
%let _OUTPUT1_col13_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col13_exp = ;
%let _OUTPUT1_col13_input = PRODUCT_SUB_TYPE_CD;
%let _OUTPUT1_col13_input_count = 1;
%let _OUTPUT1_col14_name = TENURE_MTH_CNT;
%let _OUTPUT1_col14_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col14_length = 8;
%let _OUTPUT1_col14_type = ;
%let _OUTPUT1_col14_format = ;
%let _OUTPUT1_col14_informat = ;
%let _OUTPUT1_col14_label = %nrquote(TENURE_MTH_CNT);
%let _OUTPUT1_col14_input0 = TENURE_MTH_CNT;
%let _OUTPUT1_col14_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col14_exp = ;
%let _OUTPUT1_col14_input = TENURE_MTH_CNT;
%let _OUTPUT1_col14_input_count = 1;
%let _OUTPUT1_col15_name = TERM_TYPE_CD;
%let _OUTPUT1_col15_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col15_length = 3;
%let _OUTPUT1_col15_type = $;
%let _OUTPUT1_col15_format = ;
%let _OUTPUT1_col15_informat = ;
%let _OUTPUT1_col15_label = %nrquote(_CD);
%let _OUTPUT1_col15_input0 = TERM_TYPE_CD;
%let _OUTPUT1_col15_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col15_exp = ;
%let _OUTPUT1_col15_input = TERM_TYPE_CD;
%let _OUTPUT1_col15_input_count = 1;
%let _OUTPUT1_col16_name = UNUTILIZED_MTH_CNT;
%let _OUTPUT1_col16_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col16_length = 8;
%let _OUTPUT1_col16_type = ;
%let _OUTPUT1_col16_format = ;
%let _OUTPUT1_col16_informat = ;
%let _OUTPUT1_col16_label = %nrquote(UNUTILIZED_MTH_CNT);
%let _OUTPUT1_col16_input0 = UNUTILIZED_MTH_CNT;
%let _OUTPUT1_col16_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col16_exp = ;
%let _OUTPUT1_col16_input = UNUTILIZED_MTH_CNT;
%let _OUTPUT1_col16_input_count = 1;
%let _OUTPUT1_col17_name = UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT1_col17_table = &mybase..ACCOUNT_OTHER_INFO_BASE;
%let _OUTPUT1_col17_length = 8;
%let _OUTPUT1_col17_type = ;
%let _OUTPUT1_col17_format = NLNUM9.4;
%let _OUTPUT1_col17_informat = NLNUM9.4;
%let _OUTPUT1_col17_label = %nrquote(UNUTILIZED_MTH_TO_TENURE_PCT);
%let _OUTPUT1_col17_input0 = UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT1_col17_input0_table = &mybase..ACCOUNT_OTH_INF_BASETMP;
%let _OUTPUT1_col17_exp = ;
%let _OUTPUT1_col17_input = UNUTILIZED_MTH_TO_TENURE_PCT;
%let _OUTPUT1_col17_input_count = 1;


%let Explicit_Pass = %nrquote(N);

/* List of target columns to keep  */ 
%let _keep = ACCOUNT_RK PERIOD_LAST_DTTM LIMIT_INCR_CNT LIMIT_DECR_CNT 
        OD_LIMIT_INC_CNT OD_LIMIT_DEC_CNT CUSTOMER_RK CREDIT_FACILITY_RK 
        ACCOUNT_TYPE_CD ACCOUNT_SUB_TYPE_CD ACCOUNT_HIERARCHY_CD 
        CREDIT_FACILITY_TYPE_CD FINANCIAL_PRODUCT_TYPE_CD PRODUCT_SUB_TYPE_CD 
        TENURE_MTH_CNT TERM_TYPE_CD UNUTILIZED_MTH_CNT 
        UNUTILIZED_MTH_TO_TENURE_PCT;
/* List of target columns to keep  */ 
%let keep = ACCOUNT_RK PERIOD_LAST_DTTM LIMIT_INCR_CNT LIMIT_DECR_CNT 
        OD_LIMIT_INC_CNT OD_LIMIT_DEC_CNT CUSTOMER_RK CREDIT_FACILITY_RK 
        ACCOUNT_TYPE_CD ACCOUNT_SUB_TYPE_CD ACCOUNT_HIERARCHY_CD 
        CREDIT_FACILITY_TYPE_CD FINANCIAL_PRODUCT_TYPE_CD PRODUCT_SUB_TYPE_CD 
        TENURE_MTH_CNT TERM_TYPE_CD UNUTILIZED_MTH_CNT 
        UNUTILIZED_MTH_TO_TENURE_PCT;
%macro bankfdn_incremental_loader;

%let target_lib = %kscan(&_OUTPUT1,1,%str(.));
%let target_table = %kscan(&_OUTPUT1,2,%str(.));

%let source_lib = %kscan(&_INPUT1,1,%str(.));
%let source_table = %kscan(&_INPUT1,2,%str(.));

%let _OUTPUT1_engine=%upcase(&_OUTPUT1_engine);
%let _INPUT1_engine=%upcase(&_INPUT1_engine);

*==============================================================;
* Finding the Schema for Target Table ;
*==============================================================;

%if &_OUTPUT1_engine ne BASE %then %do;
	%let target_schema = ;
	/* Macro block present in ucmacro directory. */
	%bankfdn_getschemaname(libref = &target_lib,SchemaVarName = target_schema,
									databaseEngine = &_OUTPUT1_engine);

	%if target_schema eq %then %do;
/*	  ERROR: Error in getting the target table database;*/
	  %abort;
	%end;

%end;

*==============================================================;
* Finding the Schema for Source Table ;
*==============================================================;

%if &_INPUT1_engine ne BASE %then %do;
	%let source_schema = ;
	/* Macro block present in ucmacro directory. */
	%bankfdn_getschemaname(libref = &source_lib,SchemaVarName = source_schema,
									databaseEngine = &_INPUT1_engine);

	%if source_schema eq %then %do;
/*	  ERROR: Error in getting the source table database;*/
	  %abort;
	%end;

%end;


*==============================================================;
* Source and Target table both are in SAS ;
*==============================================================;

%if (&_INPUT1_engine eq BASE or &_INPUT1_engine eq  ) and &_OUTPUT1_engine = BASE %then %do;

	proc sql;
	      create table &mybase..source_mapped as
	         select
			 %do i = 0 %to %eval(&_OUTPUT1_col_count. - 1);
			   %if &&_OUTPUT1_col&i._input_count = 0 %then %do;
			     %if %nrquote(&&_OUTPUT1_col&i._exp) ne %str() %then %do;
	               %str(%()&&_OUTPUT1_col&i._exp%str(%))
				 %end;
	             %else %do;
	               %if &&_OUTPUT1_col&i._type  = %then %do;
	                 .
				   %end;
	               %else %do; 
	                 ''
	               %end;
	             %end;
	           %end;
			   %else %do;
	             %if %nrquote(&&_OUTPUT1_col&i._exp) ne %str() %then %do;
	               %str(%()&&_OUTPUT1_col&i._exp%str(%))
				 %end;
	             %else %do;
	               &&_OUTPUT1_col&i._input
				 %end;
	           %end;
			   AS &&_OUTPUT1_col&i._name
	             length = &&_OUTPUT1_col&i._length
	             %if &&_OUTPUT1_col&i._format ne %then format = &&_OUTPUT1_col&i._format;
	             %if &&_OUTPUT1_col&i._informat ne %then informat = &&_OUTPUT1_col&i._informat;
	           %if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
	             ,
			   %end;
			 %end;
			 from &_INPUT1.;
	quit;

	proc append base = &_OUTPUT1 data = &mybase..source_mapped;
	run;

%end;

*==============================================================;
* Source is SAS and Target table is in Other Database ;
*==============================================================;

%if (&_INPUT1_engine eq BASE or &_INPUT1_engine eq )  and &_OUTPUT1_engine. ne BASE %then %do;

	%if %length(&source_table) > 26 %then %do;    /* I18NOK:LINE */
		%let stg_ds = stg_%substr(&source_table.,1,26);    /* I18NOK:LINE */
  	%end;
  	%else %do;
    	%let stg_ds = stg_&source_table.;
  	%end;

	%if %sysfunc(exist(&target_lib..&stg_ds, DATA)) %then %do;
  		proc sql;
    		drop table &target_lib..&stg_ds.;
  		quit;
	%end;
	/* Macro block present in ucmacro directory. */
	%bankfdn_read_db_utility_config(target_table=&target_table,logtablename=&stg_ds);

		proc sql;
		create table &target_lib..&stg_ds. 
		  like  &target_lib..&target_table;		
		quit;

		proc append base=&target_lib..&stg_ds. &utility_load_option  data= &_INPUT1. force;
		run;

	proc sql;
		connect to &_OUTPUT1_engine. (&_OUTPUT1_connect.);
		execute 
	       ( 
	            insert into &target_schema..&target_table(
					%do i = 0 %to %eval(&_OUTPUT1_col_count. - 1);
						&&_OUTPUT1_col&i._name
						%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
	                       ,
				        %end;
					%end;
				)
				select %do i = 0 %to %eval(&_OUTPUT1_col_count. - 1);
						&&_OUTPUT1_col&i._name
						%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
	                       ,
				        %end;
					%end;
				from &target_schema..&stg_ds.
			 ) by &_OUTPUT1_engine.; 
		execute(commit) by &_OUTPUT1_engine.;
	  	disconnect from &_OUTPUT1_engine.;
	quit;
*==============================================================;
* Deleting Temporary Table ;
*==============================================================;

%let trans_rc = &syscc;

%if &trans_rc le 4 %then %do;
	%if %sysfunc(exist(&target_lib..&stg_ds, DATA)) %then %do;
  		proc sql;
    		drop table &target_lib..&stg_ds.;
  		quit;
	%end;
%end;
	
%end;

*==============================================================;
* Source and Target table both in Other Database ;
*==============================================================;

%if (&_INPUT1_engine ne BASE and &_INPUT1_engine ne ) and &_OUTPUT1_engine ne BASE %then %do;
	/* Macro block present in ucmacro directory. */
	%bankfdn_manage_constraint(libref = &target_lib.,tablename = &target_table.,constrainttype = PK, action = DISABLE);
	
	%if &Explicit_Pass = Y %then %do;

		proc sql;
			connect to &_OUTPUT1_engine. (&_OUTPUT1_connect.);
			%if &_OUTPUT1_engine = ORACLE %then %do;
			execute ( ALTER SESSION enable parallel dml) by ORACLE;
			%end;
			execute 
		       ( 
		            insert into &target_schema..&target_table(
						%do i = 0 %to %eval(&_OUTPUT1_col_count. - 1);
/*							%if &&_OUTPUT1_col&i._input_count gt 0 %then %do;*/
								&&_OUTPUT1_col&i._name
								%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
		                       	,
					        	%end;
/*							%end;*/
						%end;
					)
					select %do i = 0 %to %eval(&_OUTPUT1_col_count. - 1);
								%if &&_OUTPUT1_col&i._input_count gt 0 %then %do;
									%if %nrquote(&&_OUTPUT1_col&i._exp) ne %str() %then %do;
		               					%str(%()&&_OUTPUT1_col&i._exp%str(%))
					 				%end;
		             				%else %do;
		               					&&_OUTPUT1_col&i._input
					 				%end;
									%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
		                       			,
					        		%end;
								%end;
								%else %do;
									%if %nrquote(&&_OUTPUT1_col&i._exp) ne %str() %then %do;
										%str(%()&&_OUTPUT1_col&i._exp%str(%))
									%end;
									%else %do;
										NULL
									%end;
									%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
                       				,
			        				%end;
								%end;
							%end;
					from &source_schema..&source_table
				 ) by &_OUTPUT1_engine.; 
			execute(commit) by &_OUTPUT1_engine.;
		  	disconnect from &_OUTPUT1_engine.;
		quit;

	%end;
	%else %do;
		option dbidirectexec;

		proc sql;
			insert into &_OUTPUT1.(
					%if &_OUTPUT1_engine = ORACLE %then %do;
						orhints='/*+ APPEND PARALLEL */',    /* I18NOK:LINE */
					%end;
					%do i = 0 %to %eval(&_OUTPUT1_col_count. - 1);
/*						%if &&_OUTPUT1_col&i._input_count gt 0 %then %do;*/
							&&_OUTPUT1_col&i._name
							%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
	                       	,
				        	%end;
/*						%end;*/
					%end;
					)
			select 
				%do i = 0 %to %eval(&_OUTPUT1_col_count. - 1);
					%if &&_OUTPUT1_col&i._input_count gt 0 %then %do;
						%if %nrquote(&&_OUTPUT1_col&i._exp) ne %str() %then %do;
               				%str(%()&&_OUTPUT1_col&i._exp%str(%))
		 				%end;
             			%else %do;
               				&&_OUTPUT1_col&i._input
		 				%end;
						%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
                       		,
		        		%end;
					%end;
					%else %do;
						%if %nrquote(&&_OUTPUT1_col&i._exp) ne %str() %then %do;
							%str(%()&&_OUTPUT1_col&i._exp%str(%))
						%end;
						%else %do;
							NULL
						%end;
						%if &i ne %eval(&_OUTPUT1_col_count. - 1) %then %do;
                       		,
		        		%end;
					%end;
				%end;
			from &_INPUT1.
					%if &_OUTPUT1_engine = ORACLE %then %do;
						(orhints='/*+ PARALLEL */')    /* I18NOK:LINE */
					%end;
			;
		quit;
	%end;
	/* Macro block present in ucmacro directory. */
	%bankfdn_manage_constraint(libref = &target_lib.,tablename = &target_table.,constrainttype = PK, action = ENABLE);

%end;

%let trans_rc = &syscc;

%mend bankfdn_incremental_loader;

%bankfdn_incremental_loader;

/* Abort if any error occurs */
%let current_tbl=&mybase..ACCOUNT_OTHER_INFO_BASE;
%error(err=&syserr,stp=&current_tbl);

data _null_;run;

/********************************************************************************************************
   Module:  rmcr_fm_ext_credit_facility_default_dim.

   Function:  This macro performs Slowly Changing dimension type 2 on credit_facility_default_dim.

   Parameters: INPUT: bankcrfm.credit_facility_default_dim,bankcrfm.LOAD_CONTROL,bankcrfm.ACCOUNT_DIM,
					  bankcrfm.ACCOUNT_DETAIL,bankcrfm.DEFAULT_EVENT_DIM,bankcrfm.CREDIT_FACILITY_DIM
					  
			
   Mandatory inputs : All the Input tables are Mandatory.
	
   Processing:	This macro checks the staging observations of credit_facility_default_dim table 
				with final observations of specified columns and stores the changes as a 
				new observation. Finally appends the non matching observations to the 
				final table(credit_facility_default_dim) and Loads it to CAS.
				Tasks Being Performed:
					->* Start - Loading CAS table to staging(work);
					->* Creation of Intermediate tables with respective filters;
					->* Assigning all the SCD columns and table names to macros;
					->* Source and Target table both are in Teradata ;
					->* Source or Target table not in Teradata ;
					
*********************************************************************************************************/

*=============================================;
* Start - Loading CAS table to staging(work);
*=============================================;

/* Initiate cas session for loading fm extension */

/*%dabt_initiate_cas_session(cas_session_ref=fm_extension); */

*==========================================================;
* Creation of Intermediate tables with respective filters;
*==========================================================;

/*LOAD_CONTROL_CURRENT_CYCLE*/
/*Created in the previous step*/


/*DEFAULT_EVENT_DIM_EXT*/
/*Created in the previous step*/


/*ACCOUNT_DIM_EXT*/
/*Created in the previous step*/


/*DEFAULTED_CREDIT_FACILITY_EXT*/

%let _INPUT_count = 2;
%let _INPUT = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _INPUT_connect = null;
%let _INPUT_engine = BASE;
%let _INPUT_memtype = DATA;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
/* %let _INPUT_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Jobs/Dimensions Extract/Ext for Base Tables/DEFAULT_EVENT_DIM_EXT%(Table%)); */
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote(Table containing data extracted from DEFAULT_EVENT_DIM based on join with LOAD_CONTROL table .);
%let _INPUT_filetype = PhysicalTable;

%let _INPUT1 = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _INPUT1_connect = null;
%let _INPUT1_engine = BASE;
%let _INPUT1_memtype = DATA;
%let _INPUT1_options = %nrquote();
%let _INPUT1_alter = %nrquote();
/* %let _INPUT1_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Jobs/Dimensions Extract/Ext for Base Tables/DEFAULT_EVENT_DIM_EXT%(Table%)); */
%let _INPUT1_type = 1;
%let _INPUT1_label = %nrquote(Table containing data extracted from DEFAULT_EVENT_DIM based on join with LOAD_CONTROL table .);
%let _INPUT1_filetype = PhysicalTable;

%let _INPUT2 = &mybase..ACCOUNT_DIM_EXT;
%let _INPUT2_connect = null;
%let _INPUT2_engine = BASE;
%let _INPUT2_memtype = DATA;
%let _INPUT2_options = %nrquote();
%let _INPUT2_alter = %nrquote();
/*%let _INPUT2_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Jobs/Dimensions Extract/Ext for Base Tables/ACCOUNT_DIM_EXT%(Table%));*/
%let _INPUT2_type = 1;
%let _INPUT2_label = %nrquote();
%let _INPUT2_filetype = PhysicalTable;

%let _OUTPUT_count = 1;
%let _OUTPUT = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_connect = ;
%let _OUTPUT_engine = ;
%let _OUTPUT_memtype = DATA;
%let _OUTPUT_options = %nrquote();
%let _OUTPUT_alter = %nrquote();
/* %let _OUTPUT_path = %nrquote(/User Written_A5YP1KJX.BM00000Y%(WorkTable%)); */
%let _OUTPUT_type = 1;
%let _OUTPUT_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_keep = CREDIT_FACILITY_RK PERIOD_LAST_DTTM DEFAULT_EVENT_RK DEFAULT_EVENT_SK 
        ACCOUNT_RK DEFAULT_DT DEFAULT_STATUS_CD;
%let _OUTPUT_col_count = 7;
%let _OUTPUT_col0_name = CREDIT_FACILITY_RK;
%let _OUTPUT_col0_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_col0_length = 8;
%let _OUTPUT_col0_type = ;
%let _OUTPUT_col0_format = 12.;
%let _OUTPUT_col0_informat = 12.;
%let _OUTPUT_col0_label = %nrquote(Credit Facility Key);
%let _OUTPUT_col0_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT_col0_input0_table = &mybase..ACCOUNT_DIM_EXT;
%let _OUTPUT_col0_exp = ;
%let _OUTPUT_col0_input = CREDIT_FACILITY_RK;
%let _OUTPUT_col0_input_count = 1;
%let _OUTPUT_col1_name = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_col1_length = 8;
%let _OUTPUT_col1_type = ;
%let _OUTPUT_col1_format = NLDATM21.;
%let _OUTPUT_col1_informat = ;
%let _OUTPUT_col1_label = %nrquote();
%let _OUTPUT_col1_exp = ;
%let _OUTPUT_col1_input_count = 0;
%let _OUTPUT_col2_name = DEFAULT_EVENT_RK;
%let _OUTPUT_col2_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_col2_length = 8;
%let _OUTPUT_col2_type = ;
%let _OUTPUT_col2_format = 12.;
%let _OUTPUT_col2_informat = 12.;
%let _OUTPUT_col2_label = %nrquote(_RKorSK);
%let _OUTPUT_col2_input0 = DEFAULT_EVENT_RK;
%let _OUTPUT_col2_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT_col2_exp = ;
%let _OUTPUT_col2_input = DEFAULT_EVENT_RK;
%let _OUTPUT_col2_input_count = 1;
%let _OUTPUT_col3_name = DEFAULT_EVENT_SK;
%let _OUTPUT_col3_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_col3_length = 8;
%let _OUTPUT_col3_type = ;
%let _OUTPUT_col3_format = 12.;
%let _OUTPUT_col3_informat = 12.;
%let _OUTPUT_col3_label = %nrquote(_RKorSK);
%let _OUTPUT_col3_exp = ;
%let _OUTPUT_col3_input_count = 0;
%let _OUTPUT_col4_name = ACCOUNT_RK;
%let _OUTPUT_col4_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_col4_length = 8;
%let _OUTPUT_col4_type = ;
%let _OUTPUT_col4_format = 12.;
%let _OUTPUT_col4_informat = 12.;
%let _OUTPUT_col4_label = %nrquote(_RKorSK);
%let _OUTPUT_col4_input0 = ACCOUNT_RK;
%let _OUTPUT_col4_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT_col4_exp = ;
%let _OUTPUT_col4_input = ACCOUNT_RK;
%let _OUTPUT_col4_input_count = 1;
%let _OUTPUT_col5_name = DEFAULT_DT;
%let _OUTPUT_col5_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_col5_length = 8;
%let _OUTPUT_col5_type = ;
%let _OUTPUT_col5_format = NLDATM20.;
%let _OUTPUT_col5_informat = NLDATM20.;
%let _OUTPUT_col5_label = %nrquote(_DT);
%let _OUTPUT_col5_input0 = DEFAULT_DT;
%let _OUTPUT_col5_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT_col5_exp = ;
%let _OUTPUT_col5_input = DEFAULT_DT;
%let _OUTPUT_col5_input_count = 1;
%let _OUTPUT_col6_name = DEFAULT_STATUS_CD;
%let _OUTPUT_col6_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT_col6_length = 3;
%let _OUTPUT_col6_type = $;
%let _OUTPUT_col6_format = $3.;
%let _OUTPUT_col6_informat = $3.;
%let _OUTPUT_col6_label = %nrquote(_CD);
%let _OUTPUT_col6_input0 = DEFAULT_STATUS_CD;
%let _OUTPUT_col6_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT_col6_exp = ;
%let _OUTPUT_col6_input = DEFAULT_STATUS_CD;
%let _OUTPUT_col6_input_count = 1;
%let _OUTPUT_filetype = WorkTable;

%let _OUTPUT1 = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_connect = ;
%let _OUTPUT1_engine = ;
%let _OUTPUT1_memtype = DATA;
%let _OUTPUT1_options = %nrquote();
%let _OUTPUT1_alter = %nrquote();
/* %let _OUTPUT1_path = %nrquote(/User Written_A5YP1KJX.BM00000Y%(WorkTable%)); */
%let _OUTPUT1_type = 1;
%let _OUTPUT1_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT1_keep = CREDIT_FACILITY_RK PERIOD_LAST_DTTM DEFAULT_EVENT_RK DEFAULT_EVENT_SK 
        ACCOUNT_RK DEFAULT_DT DEFAULT_STATUS_CD;
%let _OUTPUT1_col_count = 7;
%let _OUTPUT1_col0_name = CREDIT_FACILITY_RK;
%let _OUTPUT1_col0_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_col0_length = 8;
%let _OUTPUT1_col0_type = ;
%let _OUTPUT1_col0_format = 12.;
%let _OUTPUT1_col0_informat = 12.;
%let _OUTPUT1_col0_label = %nrquote(Credit Facility Key);
%let _OUTPUT1_col0_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT1_col0_input0_table = &mybase..ACCOUNT_DIM_EXT;
%let _OUTPUT1_col0_exp = ;
%let _OUTPUT1_col0_input = CREDIT_FACILITY_RK;
%let _OUTPUT1_col0_input_count = 1;
%let _OUTPUT1_col1_name = PERIOD_LAST_DTTM;
%let _OUTPUT1_col1_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_col1_length = 8;
%let _OUTPUT1_col1_type = ;
%let _OUTPUT1_col1_format = NLDATM21.;
%let _OUTPUT1_col1_informat = ;
%let _OUTPUT1_col1_label = %nrquote();
%let _OUTPUT1_col1_exp = ;
%let _OUTPUT1_col1_input_count = 0;
%let _OUTPUT1_col2_name = DEFAULT_EVENT_RK;
%let _OUTPUT1_col2_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_col2_length = 8;
%let _OUTPUT1_col2_type = ;
%let _OUTPUT1_col2_format = 12.;
%let _OUTPUT1_col2_informat = 12.;
%let _OUTPUT1_col2_label = %nrquote(_RKorSK);
%let _OUTPUT1_col2_input0 = DEFAULT_EVENT_RK;
%let _OUTPUT1_col2_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT1_col2_exp = ;
%let _OUTPUT1_col2_input = DEFAULT_EVENT_RK;
%let _OUTPUT1_col2_input_count = 1;
%let _OUTPUT1_col3_name = DEFAULT_EVENT_SK;
%let _OUTPUT1_col3_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_col3_length = 8;
%let _OUTPUT1_col3_type = ;
%let _OUTPUT1_col3_format = 12.;
%let _OUTPUT1_col3_informat = 12.;
%let _OUTPUT1_col3_label = %nrquote(_RKorSK);
%let _OUTPUT1_col3_exp = ;
%let _OUTPUT1_col3_input_count = 0;
%let _OUTPUT1_col4_name = ACCOUNT_RK;
%let _OUTPUT1_col4_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_col4_length = 8;
%let _OUTPUT1_col4_type = ;
%let _OUTPUT1_col4_format = 12.;
%let _OUTPUT1_col4_informat = 12.;
%let _OUTPUT1_col4_label = %nrquote(_RKorSK);
%let _OUTPUT1_col4_input0 = ACCOUNT_RK;
%let _OUTPUT1_col4_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT1_col4_exp = ;
%let _OUTPUT1_col4_input = ACCOUNT_RK;
%let _OUTPUT1_col4_input_count = 1;
%let _OUTPUT1_col5_name = DEFAULT_DT;
%let _OUTPUT1_col5_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_col5_length = 8;
%let _OUTPUT1_col5_type = ;
%let _OUTPUT1_col5_format = NLDATM20.;
%let _OUTPUT1_col5_informat = NLDATM20.;
%let _OUTPUT1_col5_label = %nrquote(_DT);
%let _OUTPUT1_col5_input0 = DEFAULT_DT;
%let _OUTPUT1_col5_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT1_col5_exp = ;
%let _OUTPUT1_col5_input = DEFAULT_DT;
%let _OUTPUT1_col5_input_count = 1;
%let _OUTPUT1_col6_name = DEFAULT_STATUS_CD;
%let _OUTPUT1_col6_table = &mybase..DEFAULTED_CREDIT_FACILITY_EXT;
%let _OUTPUT1_col6_length = 3;
%let _OUTPUT1_col6_type = $;
%let _OUTPUT1_col6_format = $3.;
%let _OUTPUT1_col6_informat = $3.;
%let _OUTPUT1_col6_label = %nrquote(_CD);
%let _OUTPUT1_col6_input0 = DEFAULT_STATUS_CD;
%let _OUTPUT1_col6_input0_table = &mybase..DEFAULT_EVENT_DIM_EXT;
%let _OUTPUT1_col6_exp = ;
%let _OUTPUT1_col6_input = DEFAULT_STATUS_CD;
%let _OUTPUT1_col6_input_count = 1;
%let _OUTPUT1_filetype = WorkTable;

/*---- Start of User Written Code  ----*/ 

options dbidirectexec;

%MACRO Create_Table;

%if &fm_db_eng.=TERADATA %then %do;
	%let db_create_table_opts1 = (DBCREATE_TABLE_OPTS = 'unique primary index MIN_DEFAULT_EVENT_DIM_EXT ("CREDIT_FACILITY_RK","LOAD_END_DTTM")');   /* I18NOK:LINE */
	%let db_create_table_opts2 = (DBCREATE_TABLE_OPTS = 'unique primary index DEFAULTED_CREDIT_FACILITY_EXT ("CREDIT_FACILITY_RK","PERIOD_LAST_DTTM")');    /* I18NOK:LINE */
%end;
%else %if &fm_db_eng.=ORACLE %then %do;
		%let db_create_table_opts1 = (DBCREATE_TABLE_OPTS = /*'unique primary index MIN_DEFAULT_EVENT_DIM_EXT ("CREDIT_FACILITY_RK","LOAD_END_DTTM")'*/);
		%let db_create_table_opts2 = (DBCREATE_TABLE_OPTS = /*'unique primary index DEFAULTED_CREDIT_FACILITY_EXT ("CREDIT_FACILITY_RK","PERIOD_LAST_DTTM")'*/);
%end;
%else %if &fm_db_eng.=SAS %then %do;
	%let db_create_table_opts1 = ;
	%let db_create_table_opts2 = ;
%end;
/* Change Identified macro resolves to CAS */
%else %if &fm_db_eng.=CAS %then %do;
	%let db_create_table_opts1 = ;
	%let db_create_table_opts2 = ;
%end;

proc sql ; 
	create table  &mybase..MIN_DEFAULT_EVENT_DIM_EXT &db_create_table_opts1. as
	select distinct
      ACCOUNT_DIM_EXT.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',   /* I18NOK:LINE */
      ACCOUNT_DIM_EXT.LOAD_END_DTTM length = 8   
         format = NLDATM21.,
      MIN(DEFAULT_EVENT_DIM_EXT.DEFAULT_EVENT_RK ) as DEFAULT_EVENT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK'     /* I18NOK:LINE */
	from
     &_INPUT1, 
     &_INPUT2
   where
      DEFAULT_EVENT_DIM_EXT.ACCOUNT_RK = ACCOUNT_DIM_EXT.ACCOUNT_RK
      and DEFAULT_EVENT_DIM_EXT.LOAD_END_DTTM = ACCOUNT_DIM_EXT.LOAD_END_DTTM
      and DEFAULT_EVENT_DIM_EXT.DEFAULT_STATUS_CD IN (&DEFAULT_STATUS_CD.)
		and coalesce(ACCOUNT_DIM_EXT.CREDIT_FACILITY_RK,&DUMMY_RK.) > &DUMMY_RK.
	group by
      ACCOUNT_DIM_EXT.CREDIT_FACILITY_RK,
      ACCOUNT_DIM_EXT.LOAD_END_DTTM 
;	   
quit;

proc sql;
   create table  &mybase..DEFAULTED_CREDIT_FACILITY_EXT &db_create_table_opts2. as
   select distinct
      MIN_DEFAULT_EVENT_DIM_EXT.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',	/* I18NOK:LINE */
      MIN_DEFAULT_EVENT_DIM_EXT.LOAD_END_DTTM as PERIOD_LAST_DTTM length = 8   
         format = NLDATM21.,
      DEFAULT_EVENT_DIM_EXT.DEFAULT_EVENT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',   /* I18NOK:LINE */
      DEFAULT_EVENT_DIM_EXT.ACCOUNT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',    /* I18NOK:LINE */
      DEFAULT_EVENT_DIM_EXT.DEFAULT_EVENT_SK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',     /* I18NOK:LINE */
      DEFAULT_EVENT_DIM_EXT.DEFAULT_DT length = 8   
         format = NLDATE10.
         informat = NLDATE10.
         label = '_DT',    /* I18NOK:LINE */
      DEFAULT_EVENT_DIM_EXT.DEFAULT_STATUS_CD length = 3   
         format = $3.
         informat = $3.
         label = '_CD'    /* I18NOK:LINE */
   from
     &_INPUT1, 
	  &mybase..MIN_DEFAULT_EVENT_DIM_EXT
   where
		DEFAULT_EVENT_DIM_EXT.LOAD_END_DTTM = MIN_DEFAULT_EVENT_DIM_EXT.LOAD_END_DTTM
		and DEFAULT_EVENT_DIM_EXT.DEFAULT_EVENT_RK = MIN_DEFAULT_EVENT_DIM_EXT.DEFAULT_EVENT_RK
   ;
quit;

proc sql ;
 DROP TABLE &mybase..MIN_DEFAULT_EVENT_DIM_EXT ;
quit ; 

%MEND Create_Table;

%Create_Table;


/*CREDIT_FACILITY_DEFAULT_EXT*/
/*Created in the previous step*/


/*CREDIT_FACILITY_DEFAULT_DIM*/
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
   delete CREDIT_FAC_DEFAULT_DIM_TMP;
quit;

proc sql;
   create table &mybase..CREDIT_FAC_DEFAULT_DIM_TMP as
   select
      CREDIT_FACILITY_DIM_EXT.CREDIT_FACILITY_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key',    /* I18NOK:LINE */
      CREDIT_FACILITY_DIM_EXT.CREDIT_FACILITY_TYPE_CD length = 3   
         format = $3.
         informat = $3.
         label = 'Credit Facility Type Code',    /* I18NOK:LINE */
      CREDIT_FACILITY_DIM_EXT.CUSTOMER_RK length = 8   
         format = 12.
         informat = 12.
         label = 'Customer Key',    /* I18NOK:LINE */
      CREDIT_FACILITY_DIM_EXT.LOAD_END_DTTM as PERIOD_LAST_DTTM length = 8   
         format = NLDATM21.,
      DEFAULTED_CREDIT_FACILITY_EXT.DEFAULT_EVENT_RK length = 8   
         format = 12.
         informat = 12.
         label = '_RKorSK',    /* I18NOK:LINE */
      DEFAULTED_CREDIT_FACILITY_EXT.DEFAULT_DT length = 8   
         format = NLDATM20.
         informat = NLDATM20.
         label = '_DT',   /* I18NOK:LINE */
      DEFAULTED_CREDIT_FACILITY_EXT.DEFAULT_STATUS_CD length = 3   
         format = $3.
         informat = $3.
         label = '_CD',    /* I18NOK:LINE */
      CREDIT_FACILITY_DIM_EXT.LOAD_END_DTTM length = 8   
         format = NLDATM21.
         informat = NLDATM21.
         label = 'Load End Datetime',    /* I18NOK:LINE */
      CREDIT_FACILITY_DIM_EXT.CREDIT_FACILITY_SK length = 8   
         format = 12.
         informat = 12.
         label = 'Credit Facility Key'    /* I18NOK:LINE */
   from
      &mybase..CREDIT_FACILITY_DIM_EXT as CREDIT_FACILITY_DIM_EXT left join 
      &mybase..DEFAULTED_CREDIT_FACILITY_EXT as DEFAULTED_CREDIT_FACILITY_EXT
         on
         (
            CREDIT_FACILITY_DIM_EXT.CREDIT_FACILITY_RK = DEFAULTED_CREDIT_FACILITY_EXT.CREDIT_FACILITY_RK
            and CREDIT_FACILITY_DIM_EXT.LOAD_END_DTTM = DEFAULTED_CREDIT_FACILITY_EXT.PERIOD_LAST_DTTM
            and CREDIT_FACILITY_DIM_EXT.CREDIT_FACILITY_RK > &DUMMY_RK.
         )
   ;
quit;



/*Abort if errors occur*/
%let current_tbl=&mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%error(err=&syserr,stp=&current_tbl);

*==============================================================;
* Assigning all the SCD columns and table names to macros;
*==============================================================;

%let etls_table = %nrquote(CREDIT_FACILITY_DEFAULT_DIM);
%let etls_lib   = %nrquote(ABT Base Tables);

%let SYSLAST = %nrquote(&mybase..CREDIT_FAC_DEFAULT_DIM_TMP); 

%let _INPUT_count = 1; 
%let _INPUT = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _INPUT_connect = null;
%let _INPUT_engine = BASE;
%let _INPUT_memtype = DATA;
%let _INPUT_options = %nrquote();
%let _INPUT_alter = %nrquote();
%let _INPUT_path = %nrquote(/Join_A5YP1KJX.BM00000T%(WorkTable%));
%let _INPUT_type = 1;
%let _INPUT_label = %nrquote();

%let _INPUT0 = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _INPUT0_connect = null;
%let _INPUT0_engine = BASE;
%let _INPUT0_memtype = DATA;
%let _INPUT0_options = %nrquote();
%let _INPUT0_alter = %nrquote();
%let _INPUT0_path = %nrquote(/Join_A5YP1KJX.BM00000T%(WorkTable%));
%let _INPUT0_type = 1;
%let _INPUT0_label = %nrquote();

%let _OUTPUT_count = 1; 
%let _OUTPUT = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_connect = null;
%let _OUTPUT_engine = BASE;
%let _OUTPUT_memtype = DATA;
%let _OUTPUT_options = %nrquote();
%let _OUTPUT_alter = %nrquote();
/*%let _OUTPUT_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Data Sources/ABT Base Tables/CREDIT_FACILITY_DEFAULT_DIM%(Table%));*/
%let _OUTPUT_type = 1;
%let _OUTPUT_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT_keep = CREDIT_FACILITY_RK VALID_START_DTTM VALID_END_DTTM CUSTOMER_RK 
        DEFAULT_STATUS_CD CREDIT_FACILITY_TYPE_CD DEFAULT_DT PROCESSED_DTTM 
        DEFAULT_EVENT_RK;
%let _OUTPUT_col_count = 9;
%let _OUTPUT_col0_name = CREDIT_FACILITY_RK;
%let _OUTPUT_col0_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col0_length = 8;
%let _OUTPUT_col0_type = ;
%let _OUTPUT_col0_format = 12.;
%let _OUTPUT_col0_informat = 12.;
%let _OUTPUT_col0_label = %nrquote(_RKorSK);
%let _OUTPUT_col0_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT_col0_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT_col0_exp = ;
%let _OUTPUT_col0_input = CREDIT_FACILITY_RK;
%let _OUTPUT_col0_input_count = 1;
%let _OUTPUT_col1_name = VALID_START_DTTM;
%let _OUTPUT_col1_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col1_length = 8;
%let _OUTPUT_col1_type = ;
%let _OUTPUT_col1_format = NLDATM21.;
%let _OUTPUT_col1_informat = NLDATM21.;
%let _OUTPUT_col1_label = %nrquote(_DTTM);
%let _OUTPUT_col1_input0 = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT_col1_exp = Case When &FM_Grain=1 then INTNX("&Month_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')    /* I18NOK:LINE */     
        When &FM_Grain=2 then INTNX("&Week_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')         /* I18NOK:LINE */
        When &FM_Grain=3 then INTNX("&Day_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')       /* I18NOK:LINE */
	Else .         
End;
%let _OUTPUT_col1_input = PERIOD_LAST_DTTM;
%let _OUTPUT_col1_input_count = 1;
%let _OUTPUT_col2_name = VALID_END_DTTM;
%let _OUTPUT_col2_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col2_length = 8;
%let _OUTPUT_col2_type = ;
%let _OUTPUT_col2_format = NLDATM21.;
%let _OUTPUT_col2_informat = NLDATM21.;
%let _OUTPUT_col2_label = %nrquote(_DTTM);
%let _OUTPUT_col2_exp = ;
%let _OUTPUT_col2_input_count = 0;
%let _OUTPUT_col3_name = CUSTOMER_RK;
%let _OUTPUT_col3_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col3_length = 8;
%let _OUTPUT_col3_type = ;
%let _OUTPUT_col3_format = 12.;
%let _OUTPUT_col3_informat = 12.;
%let _OUTPUT_col3_label = %nrquote(_RKorSK);
%let _OUTPUT_col3_input0 = CUSTOMER_RK;
%let _OUTPUT_col3_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT_col3_exp = ;
%let _OUTPUT_col3_input = CUSTOMER_RK;
%let _OUTPUT_col3_input_count = 1;
%let _OUTPUT_col4_name = DEFAULT_STATUS_CD;
%let _OUTPUT_col4_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col4_length = 3;
%let _OUTPUT_col4_type = $;
%let _OUTPUT_col4_format = ;
%let _OUTPUT_col4_informat = ;
%let _OUTPUT_col4_label = %nrquote(_CD);
%let _OUTPUT_col4_input0 = DEFAULT_STATUS_CD;
%let _OUTPUT_col4_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT_col4_exp = ;
%let _OUTPUT_col4_input = DEFAULT_STATUS_CD;
%let _OUTPUT_col4_input_count = 1;
%let _OUTPUT_col5_name = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT_col5_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col5_length = 3;
%let _OUTPUT_col5_type = $;
%let _OUTPUT_col5_format = ;
%let _OUTPUT_col5_informat = ;
%let _OUTPUT_col5_label = %nrquote(_CD);
%let _OUTPUT_col5_input0 = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT_col5_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT_col5_exp = ;
%let _OUTPUT_col5_input = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT_col5_input_count = 1;
%let _OUTPUT_col6_name = DEFAULT_DT;
%let _OUTPUT_col6_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col6_length = 8;
%let _OUTPUT_col6_type = ;
%let _OUTPUT_col6_format = NLDATM21.;
%let _OUTPUT_col6_informat = NLDATM21.;
%let _OUTPUT_col6_label = %nrquote(_DT);
%let _OUTPUT_col6_input0 = DEFAULT_DT;
%let _OUTPUT_col6_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT_col6_exp = ;
%let _OUTPUT_col6_input = DEFAULT_DT;
%let _OUTPUT_col6_input_count = 1;
%let _OUTPUT_col7_name = PROCESSED_DTTM;
%let _OUTPUT_col7_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col7_length = 8;
%let _OUTPUT_col7_type = ;
%let _OUTPUT_col7_format = NLDATM21.;
%let _OUTPUT_col7_informat = NLDATM21.;
%let _OUTPUT_col7_label = %nrquote(_DTTM);
%let _OUTPUT_col7_exp = datetime();
%let _OUTPUT_col7_input_count = 0;
%let _OUTPUT_col8_name = DEFAULT_EVENT_RK;
%let _OUTPUT_col8_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT_col8_length = 8;
%let _OUTPUT_col8_type = ;
%let _OUTPUT_col8_format = 12.;
%let _OUTPUT_col8_informat = 12.;
%let _OUTPUT_col8_label = %nrquote(_RKorSK);
%let _OUTPUT_col8_input0 = DEFAULT_EVENT_RK;
%let _OUTPUT_col8_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT_col8_exp = ;
%let _OUTPUT_col8_input = DEFAULT_EVENT_RK;
%let _OUTPUT_col8_input_count = 1;

%let _OUTPUT0 = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_connect = null;
%let _OUTPUT0_engine = BASE;
%let _OUTPUT0_memtype = DATA;
%let _OUTPUT0_options = %nrquote();
%let _OUTPUT0_alter = %nrquote();
/*%let _OUTPUT0_path = %nrquote(/Products/SAS Credit Scoring For Banking Content/Foundation Mart Extensions/Data Sources/ABT Base Tables/CREDIT_FACILITY_DEFAULT_DIM%(Table%));*/
%let _OUTPUT0_type = 1;
%let _OUTPUT0_label = %nrquote();
/* List of target columns to keep  */ 
%let _OUTPUT0_keep = CREDIT_FACILITY_RK VALID_START_DTTM VALID_END_DTTM CUSTOMER_RK 
        DEFAULT_STATUS_CD CREDIT_FACILITY_TYPE_CD DEFAULT_DT PROCESSED_DTTM 
        DEFAULT_EVENT_RK;
%let _OUTPUT0_col_count = 9;
%let _OUTPUT0_col0_name = CREDIT_FACILITY_RK;
%let _OUTPUT0_col0_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col0_length = 8;
%let _OUTPUT0_col0_type = ;
%let _OUTPUT0_col0_format = 12.;
%let _OUTPUT0_col0_informat = 12.;
%let _OUTPUT0_col0_label = %nrquote(_RKorSK);
%let _OUTPUT0_col0_input0 = CREDIT_FACILITY_RK;
%let _OUTPUT0_col0_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT0_col0_exp = ;
%let _OUTPUT0_col0_input = CREDIT_FACILITY_RK;
%let _OUTPUT0_col0_input_count = 1;
%let _OUTPUT0_col1_name = VALID_START_DTTM;
%let _OUTPUT0_col1_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col1_length = 8;
%let _OUTPUT0_col1_type = ;
%let _OUTPUT0_col1_format = NLDATM21.;
%let _OUTPUT0_col1_informat = NLDATM21.;
%let _OUTPUT0_col1_label = %nrquote(_DTTM);
%let _OUTPUT0_col1_input0 = PERIOD_LAST_DTTM;
%let _OUTPUT0_col1_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT0_col1_exp = Case When &FM_Grain=1 then INTNX("&Month_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')      /* I18NOK:LINE */   
        When &FM_Grain=2 then INTNX("&Week_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')            /* I18NOK:LINE */
        When &FM_Grain=3 then INTNX("&Day_fmt.",PERIOD_LAST_DTTM ,0,'BEGINNING')        /* I18NOK:LINE */
	Else .         
End;
%let _OUTPUT0_col1_input = PERIOD_LAST_DTTM;
%let _OUTPUT0_col1_input_count = 1;
%let _OUTPUT0_col2_name = VALID_END_DTTM;
%let _OUTPUT0_col2_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col2_length = 8;
%let _OUTPUT0_col2_type = ;
%let _OUTPUT0_col2_format = NLDATM21.;
%let _OUTPUT0_col2_informat = NLDATM21.;
%let _OUTPUT0_col2_label = %nrquote(_DTTM);
%let _OUTPUT0_col2_exp = ;
%let _OUTPUT0_col2_input_count = 0;
%let _OUTPUT0_col3_name = CUSTOMER_RK;
%let _OUTPUT0_col3_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col3_length = 8;
%let _OUTPUT0_col3_type = ;
%let _OUTPUT0_col3_format = 12.;
%let _OUTPUT0_col3_informat = 12.;
%let _OUTPUT0_col3_label = %nrquote(_RKorSK);
%let _OUTPUT0_col3_input0 = CUSTOMER_RK;
%let _OUTPUT0_col3_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT0_col3_exp = ;
%let _OUTPUT0_col3_input = CUSTOMER_RK;
%let _OUTPUT0_col3_input_count = 1;
%let _OUTPUT0_col4_name = DEFAULT_STATUS_CD;
%let _OUTPUT0_col4_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col4_length = 3;
%let _OUTPUT0_col4_type = $;
%let _OUTPUT0_col4_format = ;
%let _OUTPUT0_col4_informat = ;
%let _OUTPUT0_col4_label = %nrquote(_CD);
%let _OUTPUT0_col4_input0 = DEFAULT_STATUS_CD;
%let _OUTPUT0_col4_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT0_col4_exp = ;
%let _OUTPUT0_col4_input = DEFAULT_STATUS_CD;
%let _OUTPUT0_col4_input_count = 1;
%let _OUTPUT0_col5_name = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT0_col5_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col5_length = 3;
%let _OUTPUT0_col5_type = $;
%let _OUTPUT0_col5_format = ;
%let _OUTPUT0_col5_informat = ;
%let _OUTPUT0_col5_label = %nrquote(_CD);
%let _OUTPUT0_col5_input0 = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT0_col5_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT0_col5_exp = ;
%let _OUTPUT0_col5_input = CREDIT_FACILITY_TYPE_CD;
%let _OUTPUT0_col5_input_count = 1;
%let _OUTPUT0_col6_name = DEFAULT_DT;
%let _OUTPUT0_col6_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col6_length = 8;
%let _OUTPUT0_col6_type = ;
%let _OUTPUT0_col6_format = NLDATM21.;
%let _OUTPUT0_col6_informat = NLDATM21.;
%let _OUTPUT0_col6_label = %nrquote(_DT);
%let _OUTPUT0_col6_input0 = DEFAULT_DT;
%let _OUTPUT0_col6_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT0_col6_exp = ;
%let _OUTPUT0_col6_input = DEFAULT_DT;
%let _OUTPUT0_col6_input_count = 1;
%let _OUTPUT0_col7_name = PROCESSED_DTTM;
%let _OUTPUT0_col7_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col7_length = 8;
%let _OUTPUT0_col7_type = ;
%let _OUTPUT0_col7_format = NLDATM21.;
%let _OUTPUT0_col7_informat = NLDATM21.;
%let _OUTPUT0_col7_label = %nrquote(_DTTM);
%let _OUTPUT0_col7_exp = datetime();
%let _OUTPUT0_col7_input_count = 0;
%let _OUTPUT0_col8_name = DEFAULT_EVENT_RK;
%let _OUTPUT0_col8_table = &mybase..CREDIT_FACILITY_DEFAULT_DIM;
%let _OUTPUT0_col8_length = 8;
%let _OUTPUT0_col8_type = ;
%let _OUTPUT0_col8_format = 12.;
%let _OUTPUT0_col8_informat = 12.;
%let _OUTPUT0_col8_label = %nrquote(_RKorSK);
%let _OUTPUT0_col8_input0 = DEFAULT_EVENT_RK;
%let _OUTPUT0_col8_input0_table = &mybase..CREDIT_FAC_DEFAULT_DIM_TMP;
%let _OUTPUT0_col8_exp = ;
%let _OUTPUT0_col8_input = DEFAULT_EVENT_RK;
%let _OUTPUT0_col8_input_count = 1;


%let Surrogate_key = ;
%let Surrogate_key_count = 0;
%let Surrogate_key0 = 0;
%let Business_key = CREDIT_FACILITY_RK;
%let Business_key_count = 1;
%let Business_key0 = 1;
%let Business_key1 = CREDIT_FACILITY_RK;
%let Tab_scd_def = %nrquote(&mybase..scddef_credit_facility_def_dim);
%let SCD2columns = CUSTOMER_RK DEFAULT_STATUS_CD DEFAULT_DT CREDIT_FACILITY_TYPE_CD 
DEFAULT_EVENT_RK;
%let SCD2columns_count = 5;
%let SCD2columns0 = 5;
%let SCD2columns1 = CUSTOMER_RK;
%let SCD2columns2 = DEFAULT_STATUS_CD;
%let SCD2columns3 = DEFAULT_DT;
%let SCD2columns4 = CREDIT_FACILITY_TYPE_CD;
%let SCD2columns5 = DEFAULT_EVENT_RK;
%let Etls_Xref = %nrquote(&mybase..xref_credit_facility_def_dim);

/* List of target columns to keep  */ 
%let _keep = CREDIT_FACILITY_RK VALID_START_DTTM VALID_END_DTTM CUSTOMER_RK 
        DEFAULT_STATUS_CD CREDIT_FACILITY_TYPE_CD DEFAULT_DT PROCESSED_DTTM 
        DEFAULT_EVENT_RK;
/* List of target columns to keep  */ 
%let keep = CREDIT_FACILITY_RK VALID_START_DTTM VALID_END_DTTM CUSTOMER_RK 
        DEFAULT_STATUS_CD CREDIT_FACILITY_TYPE_CD DEFAULT_DT PROCESSED_DTTM 
        DEFAULT_EVENT_RK;
%macro scd_type2_1_1_wrapper;

%let _OUTPUT0_engine=%upcase(&_OUTPUT0_engine);
%let _INPUT0_engine=%upcase(&_INPUT0_engine);

%let target_lib = %kscan(&_OUTPUT0,1,%str(.));
%let target_table = %kscan(&_OUTPUT0,2,%str(.));

*==============================================================;
* Source and Target table both are in Teradata ;
*==============================================================;

%if &_INPUT0_engine eq &_OUTPUT0_engine %then %do;

	%if &_INPUT0_engine eq TERADATA or &_INPUT0_engine eq ORACLE %then %do;

		option DBIDIRECTEXEC;

		proc sql;
	  		select count(*) into :load_cnt     /* I18NOK:LINE */
	  		from &mybase..load_control_current_cycle;
		quit;
		%let load_cnt = &load_cnt;

		proc sql;
	  		select load_end_dttm format = NLDATM20. into :load_dttm1-:load_dttm&load_cnt
	  		from &mybase..load_control_current_cycle
	  		order by load_end_dttm;
		quit;

		%do load_no = 1 %to &load_cnt;
	 
			%let load_dttm&load_no = &&load_dttm&load_no;
		
			%let scd_macro_name = bankfdn_scd_type2_1_1_%lowcase(&_INPUT0_engine.);
		
			%&scd_macro_name.;

		%end;

	%end;
	%else %do;

		%rmcr_fm_ext_scd_type2;

	%end;

%end;

*==============================================================;
* Source or Target table not in Teradata ;
*==============================================================;

%if &_INPUT0_engine ne &_OUTPUT0_engine %then %do;
	%rmcr_fm_ext_scd_type2;
%end;

%mend scd_type2_1_1_wrapper;
%scd_type2_1_1_wrapper;



/* Abort if error occurs */
%let current_tbl=&mybase..CREDIT_FACILITY_DEFAULT_DIM;
%error(err=&syserr,stp=&current_tbl);

data _null_;run;

/********************************************************************************************************
   Module:  rmcr_fm_ext_COUNTRY_SNAPSHOT_BASE.

   Function:  This macro performs incremental load of COUNTRY_SNAPSHOT_BASE.

   Parameters: INPUT: bankcrfm.COUNTRY_DETAIL,bankcrfm.LOAD_CONTROL
			
   Mandatory inputs : All the Input tables are Mandatory.
	
   Processing:	This macro loads the data from country_detail to country_snapshot_base input being
				the dates with flag as Running from LOAD_CONTROL.
				Tasks Being Performed:
					->* Start - Loading CAS table to staging(work);
					->* Creation of Intermediate tables with respective filters;
					->* Appending data to main staging(work.COUNTRY_SNAPSHOT_BASE) table;
					
*********************************************************************************************************/

*=============================================;
* Start - Loading CAS table to staging(work);
*=============================================;

/* Initiate cas session for loading fm extension */

/*%dabt_initiate_cas_session(cas_session_ref=fm_extension); */

*==========================================================;
* Creation of Intermediate tables with respective filters;
*==========================================================;

/* COUNTRY_SNAPSHOT_BASE_TMP */
proc datasets lib = &mybase. nolist nowarn memtype = (data view);
delete COUNTRY_SNAPSHOT_BASE_TMP;
quit;

proc sql;
create table &mybase..COUNTRY_SNAPSHOT_BASE_TMP as
select
  COUNTRY_DETAIL.COUNTRY_CD length = 3   
     format = $3.
     informat = $3.
     label = 'Country Code',      /* I18NOK:LINE */
  COUNTRY_DETAIL.INFLATION_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Inflation Rate',    /* I18NOK:LINE */
  COUNTRY_DETAIL.ANNUAL_GDP_GROWTH_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Annual GDP Growth Rate',    /* I18NOK:LINE */
  COUNTRY_DETAIL.EMPLOYMENT_GROWTH_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Employment growth rate',    /* I18NOK:LINE */
  COUNTRY_DETAIL.POPULATION_GROWTH_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Population Growth Rate',  /* I18NOK:LINE */
  COUNTRY_DETAIL.AVG_PERSONAL_INCOME_GROWTH_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Average Personal Growth Rate',    /* I18NOK:LINE */
  COUNTRY_DETAIL.HOUSE_PRICE_INDEX_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'House Price Index Rate',    /* I18NOK:LINE */
  COUNTRY_DETAIL.UNEMPLOYMENT_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Unemployment Rate',    /* I18NOK:LINE */
  COUNTRY_DETAIL.TREASURY_10_YR_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = '10-year Treasury rate',   /* I18NOK:LINE */
  COUNTRY_DETAIL.TREASURY_1_YEAR_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = '1-year Treasury rate',     /* I18NOK:LINE */
  COUNTRY_DETAIL.CASH_FLOW_DISCOUNT_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Cash flow discount rate',    /* I18NOK:LINE */
  COUNTRY_DETAIL.CONSUMER_PRICE_INDEX_RT length = 8   
     format = NLNUM9.4
     informat = NLNUM9.4
     label = 'Consumer Price Index',   /* I18NOK:LINE */
  LOAD_CONTROL.LOAD_END_DTTM as PERIOD_LAST_DTTM length = 8   
     format = NLDATM21.
     informat = NLDATM21.
     label = 'Load End Datetime'   /* I18NOK:LINE */
from
  &mybase..COUNTRY_DETAIL, 
  &mybase..LOAD_CONTROL
where
  LOAD_CONTROL.LOAD_END_DTTM >= COUNTRY_DETAIL.VALID_START_DTTM
  and LOAD_CONTROL.LOAD_END_DTTM <= COUNTRY_DETAIL.VALID_END_DTTM
  and LOAD_CONTROL.LOAD_FLG IN (&CHECK_FLAG_TRUE.)
  and LOAD_CONTROL.RUN_STATUS IN (&RUNNING_STATUS.)
;
quit;


proc datasets lib = &mybase. nolist nowarn memtype = (data view);
  delete W328YVM;
quit;

proc sql;
create view &mybase..W328YVM as
 select
    COUNTRY_CD,
    PERIOD_LAST_DTTM,
    INFLATION_RT,
    EMPLOYMENT_GROWTH_RT,
    ANNUAL_GDP_GROWTH_RT,
    POPULATION_GROWTH_RT,
    AVG_PERSONAL_INCOME_GROWTH_RT,
    HOUSE_PRICE_INDEX_RT,
    UNEMPLOYMENT_RT,
    TREASURY_10_YR_RT,
    TREASURY_1_YEAR_RT,
    CASH_FLOW_DISCOUNT_RT,
    CONSUMER_PRICE_INDEX_RT
from &mybase..COUNTRY_SNAPSHOT_BASE_TMP
;
quit;

%let SYSLAST = &mybase..W328YVM;

%let etls_lastTable = &SYSLAST; 
%let etls_tableOptions = ; 

*======================================================================;
* Appending data to main staging(work.COUNTRY_SNAPSHOT_BASE) table;
*======================================================================;

%let current_tbl=&mybase..country_snapshot_base;
%error(err=&syserr,stp=&current_tbl);

proc append base = &mybase..COUNTRY_SNAPSHOT_BASE
  data = &etls_lastTable (&etls_tableOptions)  force ; 
run; 



proc datasets lib = &mybase. nolist nowarn memtype = (data view);
delete 
ACCOUNT_DIM_EXT
DEFAULT_EVENT_DIM_EXT
ACCOUNT_DEFAULT_DIM_TEMP
CREDIT_FACILITY_DIM_EXT
FINANCIAL_PRODUCT_DIM_EXT
ACCOUNT_PRODUCT_CRD_TMP1
ACCOUNT_DIM_PREV_EXT
ACCOUNT_OTH_INF_BASETMP
CREDIT_FAC_DEFAULT_DIM_TMP
COUNTRY_SNAPSHOT_BASE_TMP
;
quit;

proc sql;
	update &mybase..load_control set RUN_STATUS="Finished"    /* I18NOK:LINE */
		where (datepart(load_end_dttm) between &m1_start. and &m1_end.) and 
			RUN_STATUS = "Running" and Cas_load_status="N";    /* I18NOK:LINE */
quit;

%dabt_terminate_cas_session(cas_session_ref=fm_ext); 

%mend rmcr_fm_ext_staging;

/*%rmcr_fm_ext_staging(lib_ref=,start_period=01/01/2011,end_period=10/31/2011);*/


