/********************************************************************************************************
   Module:  rmcr_fm_ext_scd_type2

   Function:  This macro performs Slowly changing dimension Type 2. where the whole history is stored in the work table.

   Parameters: INPUT: All the macros invoked are being created in the parent macro block(FM Extension macros).
			
   Mandatory inputs : This Macro is a part of the FM Extension macros. 
					  All the inputs are from global macros.
	
   Processing:	This macro checks the staging observations with final observations of specified columns
				and stores the changes as a new observation. Finally appends the non matching observations
				to the final table.
				Tasks Being Performed:
					->* Start -  Checking output engine of the tables (Primarily BASE);
					->* Droping Reference and Definition tables and creating dictionary indexes;
					->* REMOVE KEY & BEGIN/END DATE COLUMN NAMES FROM TYPE2 STRING ;
					->* Check the Digest definition table exists or not ;
					->* Update Digest Definition Table with new Digest Definition ;
					->* CREATE CROSS REFERRENCE TABLE;
					->* Process SOURCE table ;
					->* GENERATE MAX SURROGATE KEY ;
					->* Merging SOURCE data with XREF table ;
					->* UPDATE MAX SURROGATE KEY ;
					->* Perform type2 Close ;
					->* Perform type2 Append for new record ;
					->* Perform type2 Append for matched record ;
					->* Updating XREF table ;
					->* Inserting New Records in XREF table ;
					->* Delete intermediate table ;
					
*********************************************************************************************************/

%macro rmcr_fm_ext_scd_type2;

*========================================================================;
* Start -  Checking output engine of the tables (Primarily BASE);
*========================================================================;

%local trans_rc cnt_input;
%let trans_rc = 0;

%let _OUTPUT0_engine=%upcase(&_OUTPUT0_engine);
%let _INPUT_engine=%upcase(&_INPUT_engine);
%let business_key_comma_sep = %sysfunc(tranwrd(&business_key,%str( ),%str(,))); /* I18NOK:LINE */

data _null_;
  LOADTIME = put(datetime(),nldatm21.);
  call symput('ETLS_LOADTIME', LOADTIME);	/* I18NOK:LINE */
run;
%let ETLS_LOADTIME = &ETLS_LOADTIME;
%put %str(NOTE: The load time for PROCESSED_DTTM is &ETLS_LOADTIME);

%let target_lib = %kscan(&_OUTPUT0,1,%str(.));
%let target_table = %kscan(&_OUTPUT0,2,%str(.));

%if &_OUTPUT0_engine ne BASE %then %do;
	%let target_schema = ;
	%bankfdn_getschemaname(libref = &target_lib,SchemaVarName = target_schema,databaseEngine = &_OUTPUT0_engine);

	%if target_schema eq %then %do;
	  ERROR: Error in getting the target table database;
	  %abort;
	%end;

	%let dbscr_schema = ;
	%bankfdn_getschemaname(libref = work,SchemaVarName = dbscr_schema,databaseEngine = &_OUTPUT0_engine);

	%if dbscr_schema eq %then %do;
		ERROR: Error in getting the work database name;
		%abort;
	%end;
	
	%if &_OUTPUT0_engine = TERADATA %then %do;
		%let output_connect_string = &_OUTPUT0_connect DATABASE = &target_schema;
	%end;
	%else %do;
		%let output_connect_string = &_OUTPUT0_connect;
	%end;
%end;

%let input_connect_string = %sysfunc(tranwrd(&_INPUT0_connect,SCHEMA,DATABASE));		/* I18NOK:LINE */

*========================================================================;
* Droping Reference and Definition tables and creating dictionary indexes;
*========================================================================;
   
/* Droping XREF table */
%if %sysfunc(exist(&etls_xref, DATA)) ne 0 %then %do;
	proc sql;
		drop table &etls_xref;
	quit;
%end;

/* Droping SCDDEF table */
%if %sysfunc(exist(&tab_scd_def, DATA)) ne 0 %then %do;
	proc sql;
		drop table &tab_scd_def;
	quit;
%end;


%if &Surrogate_key. eq  and &_OUTPUT0_engine = BASE %then %do;

                proc sql;
                                create table dict_index as
                                select name, indxname, indxpos 
                                from dictionary.indexes
                                where libname = "%kupcase(&target_lib)" and	/* I18NOK:LINE */
                memname = "%kupcase(&target_table)" and				/* I18NOK:LINE */
                                                idxusage = "COMPOSITE"		/* I18NOK:LINE */
                                order by 2,3;
    quit;
    %local indexCount;
                data _null_;
                                set dict_index end = eof;
                                by indxname indxpos;
                                length col_lst $200;
                retain i 0 col_lst;

                                if first.indxname then do;
                i = i+1;
                                                col_lst = '';
                                end;

                                col_lst = compress(col_lst) || name;		/* I18NOK:LINE */

                                if last.indxname then do;
                                call symputx("indexColumns"||compress(put(i,2.)),col_lst);	/* I18NOK:LINE */
                                call symputx("indexName"||compress(put(i,2.)),indxname);	/* I18NOK:LINE */
                                end;

                                if eof then call symput("indexCount",i);		/* I18NOK:LINE */
                run;

                %local etls_targetIndex;
                %if &indexCount > 0 %then %do;
                %do i = 1 %to &indexCount;
                                                %if %kupcase(%sysfunc(kcompress(&Business_key. VALID_START_DTTM))) EQ
                                                %kupcase(&&indexColumns&i) %then %do;
                                %let etls_targetIndex = &&indexName&i;
                                                %end;
                %end;
                %end;

    %if &etls_targetIndex = %then %do;
                proc datasets lib = &target_lib. nolist; 
                modify &target_table; 
                                index create INDX_REF = (&Business_key. VALID_START_DTTM)
               / unique;
                quit;
                                %let etls_targetIndex = INDX_REF;
    %end;

%end;/* END - if Surrogate_key eq BLANK */

*==============================================================;
* REMOVE KEY & BEGIN/END DATE COLUMN NAMES FROM TYPE2 STRING ;
*==============================================================;
%if &SCD2Columns eq %then %do;

  proc sql noprint;
    select kupcase(name) into :SCD2Columns separated by ' ' 	/* I18NOK:LINE */
    from sashelp.vcolumn
    where libname = "%kupcase(&target_lib)" and			/* I18NOK:LINE */
          memname = "%kupcase(&target_table)" and		/* I18NOK:LINE */
          memtype = 'DATA';					/* I18NOK:LINE */
  quit;	

%end;
%else %do;
  %let SCD2Columns = %kupcase(&SCD2Columns);
%end;

%let i=1;
%let j=1;
%let exclude_lst = %kupcase(&Surrogate_Key. &Business_Key. VALID_START_DTTM
                            VALID_END_DTTM  CREATED_BY     CREATED_DTTM   
                            PROCESSED_DTTM);

proc sql noprint;
    select kupcase(name) into :SCD2Columns separated by ' '		/* I18NOK:LINE */ 
    from sashelp.vcolumn
    where libname = "%kupcase(&target_lib)" and		/* I18NOK:LINE */
          memname = "%kupcase(&target_table)" and	/* I18NOK:LINE */
          memtype = 'DATA'				/* I18NOK:LINE */
    and kupcase(name) in (%do %while(%scan(&SCD2Columns,&i,%str( )) ne );	/* I18NOK:LINE */
                              "%scan(&SCD2Columns,&i,%str( ))"			/* I18NOK:LINE */
                              %let i = %eval(&i + 1);
                            %end;)
    and kupcase(name) not in (%do %while(%scan(&exclude_lst,&j,%str( )) ne );	/* I18NOK:LINE */
                                "%scan(&exclude_lst,&j,%str( ))"		/* I18NOK:LINE */
                                %let j = %eval(&j + 1);	
                              %end;)
    ;
quit;

*=============================================================;
* Check the Digest definition table exists or not ;
*=============================================================;

%let etls_tableExist = %eval(%sysfunc(exist(&tab_scd_def, DATA)) or 
                             %sysfunc(exist(&tab_scd_def, VIEW))); 
   
/* if table does not exist  */ 
%if (&etls_tableExist eq 0 ) %then %do;  
  data &tab_scd_def;
                attrib table_name    length=$50.   format=$50.   informat=$50.
                       type2_digest  length=$32.   format=$32.   informat=$32.;
    stop;
  run;
%end;

*=============================================================;
* Check the Digest definition in Definition table ;
*=============================================================;

%local digestChanged;

%let cnt = %eval(%sysfunc(count(%sysfunc(compbl(&SCD2Columns)),%str( ))) + 1);		/* I18NOK:LINE */

data _null_;
  digest_value = put(md5(cats(%do i = 1 %to &cnt;					/* I18NOK:LINE */
                                "%kscan(&SCD2Columns,&i,%str( ))" 			/* I18NOK:LINE */
                                %if &i ne &cnt %then %do;
                                  ,
                                                                    %end;
                                                                  %end;
                             )),$hex32.) ;
  call symput('type2_digest',digest_value);						/* I18NOK:LINE */
run;

proc sql;
  select table_name into: digestChanged
  from &tab_scd_def
  where kcompress(type2_digest)= kcompress("&type2_digest");			/* I18NOK:LINE */
quit;       

*=============================================================;
* Update Digest Definition Table with new Digest Definition ;
*=============================================================;

/* if no record found or Digest definition not matching  */
%if (&digestchanged eq %str()) %then %do;
proc sql;
  delete from &tab_scd_def where trim(table_name) = trim("&_OUTPUT0");		/* I18NOK:LINE */

  insert into &tab_scd_def
    (table_name, type2_digest)
  values 
    ("&_OUTPUT0","&type2_digest");						/* I18NOK:LINE */
quit;
%end;

/*=============================================================*/
/*                                           CREATE CROSS REFERRENCE TABLE                                                                                                                          */
/*                                           This table will be created when                                                                                                                 */
/*                                           1) Table doesn't exists                                                                                                                                                                  */
/*                                           This table will be rebuilt when                                                                                                                    */
/*                                           2) Digest definition changed either by                                                                                    */
/*                                                           a) Columns added to / removed from Type 2 list                               */
/*                                                           b) Column Sequence changed in DI studio                                                            */
/*=============================================================*/

%let etls_tableExist = %eval(%sysfunc(exist(&etls_xref, DATA)) or 
                             %sysfunc(exist(&etls_xref, VIEW))); 
   
%if (&etls_tableExist eq 0 or &digestchanged eq ) %then 
%do;  /* if table does not exist  */ 
   
	%put %str(NOTE: Creating Cross Reference Table ...);

	data etls_xref_temp(keep = &surrogate_key &business_key digest_value valid_start_dttm valid_end_dttm);
		set &_OUTPUT0(	keep = &surrogate_key &business_key &SCD2Columns valid_start_dttm valid_end_dttm
						where = (valid_end_dttm = &valid_high_dttm)
							%if &_OUTPUT0_engine ne BASE %then %do;
								dbsliceparm = all
							%end;
					);

	attrib digest_value length = $32 format = $32. informat = $32.; 
	digest_value = put(md5(cats(of &SCD2Columns)),$hex32.) ;		/* I18NOK:LINE */

	run;

	proc sort data = etls_xref_temp out = &etls_xref tagsort;
		by &business_key valid_start_dttm;
	run;
	
	/* CREATE INDEXES ON THE XREF TABLE */
	proc sql;
		create unique index %if &Business_key_count = 1 %then 
								&business_key;
							%else 
								indx_business_key;
           on &etls_xref( %sysfunc(tranwrd(&business_key,%str( ),%str(,))) );		/* I18NOK:LINE */
	quit;

%end;

*==================================================================;
* Process SOURCE table ;
*==================================================================; 
proc sql ;
	select count(*) into: cnt_input from  &_INPUT0 ; /* I18NOK:LINE */
quit; 

%if &cnt_input = 0 %then %return ;;

proc sql;
	create table work.etls_source as
		select
			%do i = 0 %to %eval(&_OUTPUT0_col_count. - 1);
				%if &&_OUTPUT0_col&i._input_count = 0 %then %do;
					%if %nrquote(&&_OUTPUT0_col&i._exp) ne %str() %then %do;
						%str(%()&&_OUTPUT0_col&i._exp%str(%))
					%end;
					%else %do;
						%if &&_OUTPUT0_col&i._type  = %then %do;
							.
						%end;
						%else %do; 
							''
						%end;
					%end;
				%end;
				%else %do;
					%if %nrquote(&&_OUTPUT0_col&i._exp) ne %str() %then %do;
						%str(%()&&_OUTPUT0_col&i._exp%str(%))
					%end;
					%else %do;
						&&_OUTPUT0_col&i._input
					%end;
				%end;
				AS &&_OUTPUT0_col&i._name	length = &&_OUTPUT0_col&i._length
				%if &&_OUTPUT0_col&i._format ne %then format = &&_OUTPUT0_col&i._format;
				%if &&_OUTPUT0_col&i._informat ne %then informat = &&_OUTPUT0_col&i._informat;
				%if &i ne %eval(&_OUTPUT0_col_count. - 1) %then %do;
					,
				%end;
			%end;
		from &_INPUT0 %if &_INPUT_engine ne BASE and &_INPUT_engine ne %then (dbsliceparm=all);
		order by
			%do i=1 %to &Business_key_count;
				&&Business_key&i ,
			%end;
			valid_start_dttm
		;
quit;

/* Generate the default from date expression  */ 
%let etls_begdate = VALID_START_DTTM;

/* Generate the default to date expression  */ 
%let etls_enddate = &VALID_HIGH_DTTM;

/*---- Set beginning dates and close out any history records   ----*/ 
%let dsid=%sysfunc(open(etls_source));
%let obsCount=%sysfunc(attrn(&dsid,nobs));
%let rc=%sysfunc(close(&dsid));
data work.etls_source(drop = etls_var: etls_closedate ETLS_STGDIGEST_next etls_business_key:);
  retain currentobs 1;

  do while(currentobs <= &obsCount);

    set work.etls_source point = currentobs;

    length ETLS_STGDIGEST  ETLS_STGDIGEST_next $32.;

    ETLS_STGDIGEST =  put(md5(cats(of &scd2columns)),$hex32.);		/* I18NOK:LINE */

    do until(ETLS_STGDIGEST ne ETLS_STGDIGEST_next
                        or 
             %do i = 1 %to &Business_key_count;
             &&Business_key&i ne etls_business_key&i
                        or
             %end;
             currentobs > &obsCount);
                  currentobs = currentobs + 1;
                  if currentobs <= &obsCount then do;
        set work.etls_source(keep = &Business_key. &SCD2Columns valid_start_dttm
                             rename = (valid_start_dttm = etls_closedate
                                       %do i = 1 %to &Business_key_count;
                                       &&Business_key&i = etls_business_key&i
                                       %end;
                                       %do i = 1 %to &cnt;
                                       %kscan(&SCD2Columns,&i,%str( )) = etls_var&i
                                       %end;
                           ))
          point = currentobs;
        ETLS_STGDIGEST_next = put(md5(cats(of etls_var1-etls_var&cnt)),$hex32.);	/* I18NOK:LINE */
                  end;
    end;

    if %do i = 1 %to &Business_key_count;
       &&Business_key&i eq etls_business_key&i
             and
       %end;
       etls_closedate ne . 
             and
       currentobs <= &obsCount
    then 
      valid_end_dttm = etls_closedate - 1;

    output;

  end;

  
run;

*=============================================================;
* GENERATE MAX SURROGATE KEY ;
*=============================================================;
%local etls_maxkey etls_newkey;
%let etls_maxkey = 0;
%let etls_newkey = 0;

/* Surrogate_Key= ; so this macro will not be invoked */
%if &Surrogate_Key ne %then %do;

                %bankfdn_get_sk(input_lib     = &target_lib,
                                input_ds      = &target_table,
                                sk_value_var  = etls_maxkey);
%end;


*==================================================================;
* Merging SOURCE data with XREF table ;
*==================================================================; 
      
data work.etls_newrcds
             (drop = ETLS_STGDIGEST  NewMaxKey    ETLS_FROMDATE 
                     ETLS_CLSDATE    ETLS_TODATE  ETLS_LOADTIME
                     %if &Surrogate_Key ne %then %do;
                       ETLS_KEY
                     %end;
                     )
         work.etls_match
             (drop = ETLS_STGDIGEST  NewMaxKey    ETLS_FROMDATE 
                     ETLS_CLSDATE    ETLS_TODATE  ETLS_LOADTIME
                     %if &Surrogate_Key ne %then %do;
                       ETLS_KEY
                     %end;
                     )
         work.etls_close
             (keep = %if &Surrogate_Key ne %then %do;
                       ETLS_KEY
                     %end;
                     %else %do;
                                                                                   &Business_key. ETLS_FROMDATE
                                                                                %end;
                     ETLS_CLSDATE ETLS_LOADTIME)
         ;
         retain NewMaxKey &etls_maxkey;
         
         merge work.etls_source(in=inSort)
               &etls_xref(in=inXref rename=(%if &Surrogate_Key ne %then %do;
                                            &Surrogate_key. = ETLS_KEY
                                                                                                                                                                                %end;
                                            VALID_START_DTTM = ETLS_FROMDATE
                                            VALID_END_DTTM = ETLS_TODATE));
         by &Business_key.;
         /* bankfdn_dbms_dttm_format : no changes in this macro block */
         attrib ETLS_CLSDATE length=8 format = %bankfdn_dbms_dttm_format(&_OUTPUT0_engine.) informat = %bankfdn_dbms_dttm_format(&_OUTPUT0_engine.);		/* I18NOK:LINE */
         attrib ETLS_LOADTIME length=8 format = %bankfdn_dbms_dttm_format(&_OUTPUT0_engine.) informat = %bankfdn_dbms_dttm_format(&_OUTPUT0_engine.);		/* I18NOK:LINE */
         
         /* Process new records  */ 
         if inSort and not inXref then 
         do;

            %if &Surrogate_Key. ne %then %do;
            NewMaxKey = sum(NewMaxKey, 1);
            &Surrogate_key. = NewMaxKey;
                                                %end;
            digest_value = ETLS_STGDIGEST;
            
            if VALID_START_DTTM eq . then
               VALID_START_DTTM = &etls_begdate;
            if VALID_END_DTTM eq . then 
               VALID_END_DTTM = &etls_enddate;
               
            PROCESSED_DTTM = input("&etls_loadtime", NLDATM21.);
            
            output work.etls_newrcds;
            
         end;
                                /* Process changes made to existing records  */ 
         else if inSort and inXref then
         do;
         
            if ETLS_STGDIGEST NE DIGEST_VALUE then
            do;
            
               if VALID_START_DTTM eq . then
                  VALID_START_DTTM = &etls_begdate;
                  
               /* Ignore history records, only accept changes where the cross reference  */ 
               /*  beginning date is less than the source beginning date                 */ 
               if ETLS_FROMDATE < VALID_START_DTTM then
               do;
               
                  if VALID_END_DTTM eq . then
                     VALID_END_DTTM = &etls_enddate;
                     
                  PROCESSED_DTTM = input("&etls_loadtime", NLDATM21.);
                  ETLS_LOADTIME = input("&etls_loadtime", NLDATM21.);
                  
                  /* Save off the close date  */ 
                  if ETLS_TODATE = &etls_enddate or ETLS_TODATE > VALID_START_DTTM then
                  do;
                  
                     if ETLS_FROMDATE < VALID_START_DTTM then
                        ETLS_CLSDATE = VALID_START_DTTM - 1;
                     else ETLS_CLSDATE = ETLS_FROMDATE;
                     
                     output work.etls_close;
                     
                  end;
                  
                  %if &Surrogate_Key ne %then %do;
                  NewMaxKey = sum(NewMaxKey, 1);
                  &Surrogate_key. = NewMaxKey;
                  %end;
                  /* Overwrite the values to compare new records with from cross-reference   */ 
                  /* variable values to the new current values from source  */ 
                  DIGEST_VALUE = ETLS_STGDIGEST;
                  ETLS_FROMDATE = VALID_START_DTTM;
                  ETLS_TODATE = VALID_END_DTTM;
                  
                  output work.etls_match;
                  
               end;
               
            end;/*  End of digest values not equal   */ 
            
            
            else do;
            
               if VALID_END_DTTM ne . then
               do;
               
                  /* If the cross-reference end date is greater then the source end date then  */ 
                  /*  close out the target record with the source end date                     */ 
                  if last.&&business_key&business_key_count then
                     if ETLS_TODATE gt VALID_END_DTTM then
                     do;
                     
                        ETLS_CLSDATE = VALID_END_DTTM;
                        ETLS_LOADTIME = input("&etls_loadtime", NLDATM21.);
                        
                        output work.etls_close;
                        
                     end;
                     
               end;
               
               /* If the source beginning date is greater then both the cross-reference  */ 
               /*  beginning and end date, then reactivate the closed out record         */ 
               if VALID_START_DTTM gt ETLS_FROMDATE and
                  VALID_START_DTTM gt ETLS_TODATE then
               do;
               
                  if VALID_END_DTTM eq . then
                     VALID_END_DTTM = &etls_enddate;
                     
                  PROCESSED_DTTM = input("&etls_loadtime", NLDATM21.);

                                                                  %if &Surrogate_Key ne %then %do;
                  NewMaxKey = sum(NewMaxKey, 1);
                  &Surrogate_key. = NewMaxKey;
                  %end;
                  /* Update the cross-reference variables to the new current values  */ 
                  DIGEST_VALUE = ETLS_STGDIGEST;
                  ETLS_FROMDATE = VALID_START_DTTM;
                  ETLS_TODATE = VALID_END_DTTM;
                  
                  output work.etls_match;
                  
               end;
               
            end;  /* End of digest values are equal  */ 
            
         end;  /* End of inSort and inXref  */ 
         
                %if &Surrogate_key. ne %then %do;
                                call symput('etls_newkey',put(NewMaxKey,best.));	/* I18NOK:LINE */
                %end;

run;
*=============================================================;
* CHECK CUSTOMIZATIONS FOR TARGET TABLE ;
*=============================================================;

/* %bankfdn_update_column_values(fm_table=&target_table.,inputtable=work.etls_newrcds); */

*=============================================================;
* UPDATE MAX SURROGATE KEY ;
*=============================================================;

%if &Surrogate_key. ne and &etls_newkey ne %then %do;
                %bankfdn_update_sk(input_lib     = &target_lib,
                                input_ds      = &target_table,
                                sk_value_var  = etls_newkey);
%end;

*=============================================================;
* Perform type2 Close ;
*=============================================================;

%let dsid=%sysfunc(open(work.etls_Close));
%let cls_cnt=%sysfunc(attrn(&dsid,nobs));
%let rc=%sysfunc(close(&dsid));
   
%if &cls_cnt gt 0 and &rc eq 0  %then %do; 

	%if %length(&target_table) > 26 %then %do;					/* I18NOK:LINE */
		%let close_ds = cls_%substr(&target_table,1,26);		/* I18NOK:LINE */
	%end;
	%else %do;
		%let close_ds = cls_&target_table;
	%end;
	
	%if %length(&close_ds) > 26 %then %do; /* I18NOK:LINE */
		%let close_ds_index = pk_%substr(&close_ds,1,26); /* I18NOK:LINE */
	%end;
	%else %do;
			%let close_ds_index = pk_&close_ds.;
	%end;

	%if &_OUTPUT0_engine = BASE %then %do;

		data &_OUTPUT0;
			set work.etls_close 
				%if &Surrogate_key. ne %then %do;
					(rename = (ETLS_KEY = &Surrogate_key.))
				%end;
				%else %do;
					(rename = (ETLS_FROMDATE = VALID_START_DTTM))
				%end;
			;
			modify &_OUTPUT0 key = 
					%if &Surrogate_key. ne %then %do;
						&Surrogate_key.
					%end;
					%else %do;
					&etls_targetIndex.
					%end;
				/ unique;

			valid_end_dttm = etls_clsdate;
			processed_dttm = etls_loadtime;

			if %sysrc(_SOK) eq _iorc_ then replace;   

			_iorc_ = 0; 
			_error_ = 0;
		run; 

	%end;
	%else %do;

		%if %sysfunc(exist(work.&close_ds, DATA)) %then %do;
			proc sql;
				drop table work.&close_ds;
			quit;
		%end;
		
		data _null_;
			a="&Business_key";
			PI=KTRANSLATE(a,',',' ');			/* I18NOK:LINE */
			call symput('PI',PI);				/* I18NOK:LINE */
		run;

		data work.&close_ds 
			%if &_OUTPUT0_engine = TERADATA %then
				( 
					DBCREATE_TABLE_OPTS = "PRIMARY INDEX(&PI)" /* I18NOK:LINE */
					DBTYPE = (ETLS_CLSDATE = 'timestamp'  ETLS_LOADTIME  = 'timestamp' ) /* I18NOK:LINE */
				) ;		/* I18NOK:LINE */
			%else ;;
			
			set work.etls_close(obs = 0);
		run;
		
		/* bankfdn_read_db_utility_config: will not be invoked as all related tables are in work */
		%bankfdn_read_db_utility_config(target_table=&target_table,logtablename=&close_ds);  

		proc append data=work.etls_close base=work.&close_ds &utility_load_option force ;
		run;
		
		%if &_OUTPUT0_engine = ORACLE %then %do;
			proc sql;
				  connect to ORACLE (&_OUTPUT0_connect.);
				  execute 
					 ( 
						CREATE UNIQUE INDEX &dbscr_schema..&close_ds_index ON &dbscr_schema..&close_ds(%if &Surrogate_Key ne %then %do;
																										 ETLS_KEY
																									 %end;
																									 %else %do;
																										 &Business_key_comma_sep.,ETLS_FROMDATE
																									 %end;
																									 ) PARALLEL
					 ) by ORACLE;
					execute 
					 ( 
						ALTER TABLE &dbscr_schema..&close_ds ADD PRIMARY KEY (%if &Surrogate_Key ne %then %do;
																				 ETLS_KEY
																			 %end;
																			 %else %do;
																				 &Business_key_comma_sep.,ETLS_FROMDATE
																			 %end;
																			)
					 ) by ORACLE;
				  execute(commit) by ORACLE;
				  disconnect from ORACLE;
			quit;
		%end;

		proc sql;
			connect to &_OUTPUT0_engine (&_OUTPUT0_connect.);
			execute 
			( 
				%if &_OUTPUT0_engine = TERADATA %then %do;
					update t
						from &target_schema..&target_table as t, &dbscr_schema..&close_ds as s
						set processed_dttm = etls_loadtime,
							valid_end_dttm = etls_clsdate
						where 
							%if &Surrogate_Key ne %then %do;
								t.&Surrogate_key. = s.etls_key 
							%end;
							%else %do;
								%do i = 1 %to &Business_key_count;
									t.&&Business_key&i = s.&&Business_key&i and
								%end;
								t.valid_start_dttm = s.ETLS_FROMDATE
							%end;
				%end;
				%else %if &_OUTPUT0_engine = ORACLE %then %do;
									
					%let update_hint = %str(/)%str(*+) PARALLEL%str(%() &target_schema..&target_table.%str(%)) PARALLEL%str(%() &dbscr_schema..&close_ds. %str(%)) %str(*/);

					UPDATE /*+ PARALLEL(joinview) */
						(SELECT &update_hint.
							t.valid_end_dttm, t.processed_dttm,
							s.etls_clsdate, s.etls_loadtime
						  FROM &target_schema..&target_table t, &dbscr_schema..&close_ds s
						  WHERE %if &Surrogate_Key ne %then %do;
									t.&Surrogate_key. = s.etls_key 
								%end;
								%else %do;
									%do i = 1 %to &Business_key_count;
										t.&&Business_key&i = s.&&Business_key&i and
									%end;
									t.valid_start_dttm = s.ETLS_FROMDATE
								%end;
						) joinview
					SET valid_end_dttm = etls_clsdate, processed_dttm = etls_loadtime
		
				%end;
			) by &_OUTPUT0_engine; 
			execute(commit) by &_OUTPUT0_engine;
			disconnect from &_OUTPUT0_engine;
		quit;

	%end;

%end;
   
*=============================================================;
* Perform type2 Append for new record ;
*=============================================================;

%let dsid=%sysfunc(open(work.etls_newrcds));
%let new_cnt=%sysfunc(attrn(&dsid,nobs));
%let rc=%sysfunc(close(&dsid));
   
%if &new_cnt gt 0 and &rc eq 0  %then %do; 

	%if %length(&target_table) > 26 %then %do;				/* I18NOK:LINE */
		%let new_ds = new_%substr(&target_table,1,26);		/* I18NOK:LINE */
	%end;
	%else %do;
		%let new_ds = new_&target_table;
	%end;

	%if &_OUTPUT0_engine = BASE %then %do;
		proc append data=work.etls_newrcds(drop = digest_value) base=&_OUTPUT0 force;
		run;
	%end;
	%else %do;

		%if %sysfunc(exist(work.&new_ds, DATA)) %then %do;
			proc sql;
				drop table work.&new_ds;
			quit;
		%end;

		option DBIDIRECTEXEC;
		Proc Sql;
			create table  work.&new_ds as select * from &target_lib..&target_table where 1>2;
        quit;
		option NODBIDIRECTEXEC;
		
		%bankfdn_read_db_utility_config(target_table=&target_table,logtablename=&new_ds.);
		proc append
			data = work.etls_newrcds(drop = digest_value)
            base = work.&new_ds &utility_load_option force;
		run;

		proc sql;
			connect to &_OUTPUT0_engine. (&_OUTPUT0_connect.);
			execute 
			( 
				insert into &target_schema..&target_table(
					%do i = 0 %to %eval(&_OUTPUT0_col_count. - 1);
						&&_OUTPUT0_col&i._name
						%if &i ne %eval(&_OUTPUT0_col_count. - 1) %then %do;
							,
						%end;
					%end;
				)
				select
					%do i = 0 %to %eval(&_OUTPUT0_col_count. - 1);
						&&_OUTPUT0_col&i._name
						%if &i ne %eval(&_OUTPUT0_col_count. - 1) %then %do;
							,
						%end;
					%end;
				from &dbscr_schema..&new_ds
			) by &_OUTPUT0_engine; 
			execute(commit) by &_OUTPUT0_engine.;
			disconnect from &_OUTPUT0_engine.;
		quit;

	%end;
	
%end;

*=============================================================;
* Perform type2 Append for matched record ;
*=============================================================;

%let dsid=%sysfunc(open(work.etls_match));
%let match_cnt=%sysfunc(attrn(&dsid,nobs));
%let rc=%sysfunc(close(&dsid));
   
%if &match_cnt gt 0 and &rc eq 0  %then %do; 

	%if %length(&target_table) > 24 %then %do;				/* I18NOK:LINE */
		%let match_ds = match_%substr(&target_table,1,24);	/* I18NOK:LINE */
	%end;
	%else %do;
		%let match_ds = match_&target_table;
	%end;

	%if &_OUTPUT0_engine = BASE %then %do;

		proc append data=work.etls_match(drop = digest_value) base=&_OUTPUT0 force;
		run;

	%end;
	%else %do;

		%if %sysfunc(exist(work.&match_ds, DATA)) %then %do;
			proc sql;
				drop table work.&match_ds;
			quit;
		%end;
		
		option DBIDIRECTEXEC;
		Proc Sql;
			create table  work.&match_ds as select * from &target_lib..&target_table where 1>2;
		quit;
		option NODBIDIRECTEXEC;
		%bankfdn_read_db_utility_config(target_table=&target_table,logtablename=&match_ds.);
		
		proc append
			data = work.etls_match(drop = digest_value)
			base = work.&match_ds &utility_load_option force;
		run;

		proc sql;
			connect to &_OUTPUT0_engine (&_OUTPUT0_connect.);
			execute 
			( 
				insert into &target_schema..&target_table(
					%do i = 0 %to %eval(&_OUTPUT0_col_count. - 1);
						&&_OUTPUT0_col&i._name
						%if &i ne %eval(&_OUTPUT0_col_count. - 1) %then %do;
							,
						%end;
					%end;
				)
				select
					%do i = 0 %to %eval(&_OUTPUT0_col_count. - 1);
						&&_OUTPUT0_col&i._name
						%if &i ne %eval(&_OUTPUT0_col_count. - 1) %then %do;
							,
						%end;
					%end;
				from &dbscr_schema..&match_ds
			) by &_OUTPUT0_engine; 
			execute(commit) by &_OUTPUT0_engine;
			disconnect from &_OUTPUT0_engine;
		quit;

	%end;

%end;

*=============================================================;
* Updating XREF table ;
*=============================================================;

%let dsid=%sysfunc(open(&etls_xref));
%let num=%sysfunc(attrn(&dsid,nobs));
%let rc=%sysfunc(close(&dsid));

%if &num gt 0 and &rc eq 0  %then %do; 

	data &etls_xref;
		set work.etls_match(keep = 
								%if &Surrogate_key. ne %then %do;
									&surrogate_key.
								%end;
								&business_key. digest_value valid_end_dttm valid_start_dttm
							rename = 
								(
									%if &Surrogate_key. ne %then %do;
										&surrogate_key. = t_&surrogate_key.
									%end;
									digest_value = t_digest_value
									valid_end_dttm = t_valid_end_dttm
									valid_start_dttm = t_valid_start_dttm
								)
							);
		modify &etls_xref key = 
			%if &Business_key_count = 1 %then 
				&business_key;
			%else 
				indx_business_key;
			/ unique;

		%if &Surrogate_key. ne %then %do;
			&surrogate_key. = t_&surrogate_key.;
		%end;
		valid_end_dttm = t_valid_end_dttm;
		valid_start_dttm = t_valid_start_dttm;
		digest_value   = t_digest_value;

		if %sysrc(_SOK) eq _iorc_ then replace;   

		_iorc_ = 0; 
		_error_ = 0;
	run;
	
%end;

*=============================================================;
* Inserting New Records in XREF table ;
*=============================================================;

proc append data=work.etls_newrcds(keep = &surrogate_key &business_key digest_value valid_start_dttm valid_end_dttm
                                    where = (valid_end_dttm = &valid_high_dttm))
     base=&etls_xref force;
run;

*=============================================================;
* Delete intermediate table ;
*=============================================================;

%let trans_rc = &syscc;

%if &_OUTPUT0_engine ne BASE and &trans_rc <= 4 %then %do;
	  
	%if &new_cnt gt 0 %then %do;
		proc sql;
			drop table work.&new_ds;
		quit;
	%end;
	%if &match_cnt gt 0 %then %do;
		proc sql;
			drop table work.&match_ds;
		quit;
	%end;
	%if &cls_cnt gt 0 %then %do;
		proc sql;
			drop table work.&close_ds;
		quit;
	%end;

%end;

%mend rmcr_fm_ext_scd_type2;
