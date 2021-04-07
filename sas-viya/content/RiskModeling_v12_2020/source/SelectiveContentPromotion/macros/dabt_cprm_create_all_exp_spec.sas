/*****************************************************************/
/* NAME: dabt_cprm_create_all_exp_spec.sas                       */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to export all entities in to excel sheet   */
/*                                                               */
/* Parameters :  export_spec_file_path:without ending slash      */
/* 			     export_spec_file_nm:file name without extension */
/* Macro variables:                                              */
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Called by Admin Users     		                     */
/*          %dabt_cprm_create_all_exp_spec(export_spec_file_path=, 
			export_spec_file_nm=);  					         */
/*                                                               */
/*****************************************************************/

/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*4May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/

%macro dabt_cprm_create_all_exp_spec(export_spec_file_path=, export_spec_file_nm=);
	
	%let syscc = 0;
	%if ("&export_spec_file_path." eq "") %then %do;
		%let export_spec_file_path=%str(/&m_file_srvr_job_folder_path/&m_job_sk);
	%end;
	%if ("&export_spec_file_nm." eq "") %then %do;
		%let export_spec_file_nm=all_exp_spec_data;
	%end;
	/*Check for export_spec_file_path ,export_spec_file_nm :should not be blank */

	%if ("&export_spec_file_path." eq "") %then %do;
		/* Export specification file path cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_PATH_NOT_SPECIFIED, noquote));
		%let syscc=99;
		%retun;
	%end;

	%if ("&export_spec_file_nm." eq "") %then %do;
		/* Export specification file name cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_NM_NOT_SPECIFIED, noquote));	
		%let syscc=99;
		%return ;
	%end;
	
	/**** check existence of user specified SAS content folder loaction ***/
	filename resp temp;
	filename resp_hdr temp;
	%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

	proc http url="&BASE_URI/folders/folders/@item?path=&export_spec_file_path." /* i18nOK:Line */
		method='get'/* i18nOK:Line */
		oauth_bearer=sas_services out=resp headerout=resp_hdr headerout_overwrite 
			ct="application/json";		/* i18nOK:Line */
		DEBUG LEVEL=3;
	run;
	quit;
	
	
	%put &SYS_PROCHTTP_STATUS_CODE.;
	%if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %do;
		%let syscc=99;
		%put ERROR: &export_spec_file_path - &SYS_PROCHTTP_STATUS_PHRASE.;
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_PATH_NOT_EXIST, noquote, &export_spec_file_path.) );
		%return ;
	%end;

	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;

	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

	%local export_specs_ds_lib
		   export_specs_ds_nm
		   m_exprt_rec_cnt
		   ;

	/*Assigning created path to library cpspcscr */

	%let export_specs_ds_lib =  work ; /* i18NOK:LINE */

	*libname &export_specs_ds_lib. "&export_spec_file_path.";

	/*Creating input dataset which will append all entites to be exported*/

	%let export_specs_ds_nm = cprm_export_specification; /* i18NOK:LINE */

	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_specs_ds_nm.);

	%let cprm_entity_type_cd_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_CD_LABEL, noquote)); 
	%let cprm_entity_type_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_NM_LABEL, noquote)); 
	%let cprm_entity_key_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_KEY_LABEL, noquote)); 
	%let cprm_entity_desc_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_DESC_LABEL, noquote)); 
	%let cprm_promote_flg_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_PROMOTE_FLG_LABEL, noquote)); 
	%let cprm_entity_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_NM_LABEL, noquote)); 
	%let cprm_owner_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_OWNER_NM_LABEL, noquote)); 
	proc sql noprint;
		create table &export_specs_ds_lib..&export_specs_ds_nm. ( 
			entity_type_cd  character(10) not null 
					label="&cprm_entity_type_cd_label.", 
			entity_type_nm character(360) not null
					label="&cprm_entity_type_nm_label.", 
			entity_key  numeric(10) not null
				label="&cprm_entity_key_label.", 
			entity_nm character(360) not null 
				label="&cprm_entity_nm_label.", 
			entity_desc character(1800) 
				label="&cprm_entity_desc_label.", 
			promote_flg character(1) not null 
				label="&cprm_promote_flg_label.", 
			owner character(32) not null
				label="&cprm_owner_nm_label.",  /**** need l10n *****/
			constraint entity_type_cd_key primary key (entity_type_cd,entity_key));
	quit;
	
	%let Pool_scheme_entity_sk=;
	Proc sql noprint;
		Select  entity_type_sk into :Pool_scheme_entity_sk
		From &lib_apdm..cprm_entity_master
		Where trim(left(upcase(entity_type_cd)))="POOL";			/*i18NOK:LINE*/ 
	Quit;
	
	%let master_scale_entity_sk=;
	Proc sql noprint;
		Select  entity_type_sk into :master_scale_entity_sk
		From &lib_apdm..cprm_entity_master
		Where trim(left(upcase(entity_type_cd)))="MS";				/*i18NOK:LINE*/
	Quit;
	
	

	%dabt_err_chk(type=SQL);

	/* Start of macro call to create export specifications . */

	%dabt_cprm_export_ext_code_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm = &export_specs_ds_nm., m_ext_cd_sk_lst=* );

	%dabt_cprm_export_library_spec(export_specs_ds_lib = &export_specs_ds_lib., export_specs_ds_nm = &export_specs_ds_nm., m_lib_sk_lst=* ) ;
	
	%dabt_cprm_export_purpose_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., purpose_sk_lst= * );
	
	%dabt_cprm_export_table_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., table_sk_lst=*);
	
	%dabt_cprm_export_sbj_grp_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., subject_group_sk_lst=*);
	
	%dabt_cprm_export_soa_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., level_sk_lst=*);
	
	%dabt_cprm_exprt_time_period_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., time_period_sk_lst=* );

	%dabt_cprm_exprt_as_of_time_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., as_of_time_sk_lst=* );

	%dabt_cprm_export_sbstmp_spec(	export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_sbstmp_sk_lst=*); 

	%dabt_cprm_export_project_spec(	export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_prj_sk_lst=*);
	
	%dabt_cprm_export_model_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_model_sk_lst=* );
	
	%if  &Pool_scheme_entity_sk ne %then %do;
	
	%*csbmva_cprm_export_pool_sch_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_pool_sch_sk_lst=* );
	
	%end;
	%if  &master_scale_entity_sk ne %then %do;
	
	%*csbmva_cprm_export_ms_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_ms_no_lst=* );
	
	%end;

	/* End of macro call to create export specifications . */
	
	%if &syscc. > 4 %then %do;
		/* Some issue occured while creationn of export specifcation table */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_DS_NOT_CREATED, noquote));	
		%let syscc=99;
		%return;
	%end;

	/* Macro variable created to count observations count in  &export_specs_ds_lib..&export_specs_ds_nm. */

	proc sql noprint; 
		select count(*) into: m_exprt_rec_cnt /* i18nOK:Line */
			from &export_specs_ds_lib..&export_specs_ds_nm.;
	quit;
	
	%dabt_err_chk(type=SQL);

	%let m_exprt_rec_cnt = &m_exprt_rec_cnt.;

	/* If &export_specs_ds_lib..&export_specs_ds_nm. contains observations only furthur steps will execute.*/

	%if &m_exprt_rec_cnt. gt 0 %then %do;
		
		proc sort data=&export_specs_ds_lib..&export_specs_ds_nm. out=&export_specs_ds_lib..&export_specs_ds_nm.;
			by entity_type_nm entity_nm;
		run;
				
		/* S1436878 - Handling newline character in <entity>_description - translating to dummy (space) */
		
		data &export_specs_ds_lib..&export_specs_ds_nm.;
			set &export_specs_ds_lib..&export_specs_ds_nm.;
			entity_desc = tranwrd(entity_desc,'0D0A'x,' ');	/*i18NOK:LINE*/ 
			entity_desc = tranwrd(compress(tranwrd(kstrip(entity_desc),' ','!`!'),,'S'),'!`!',' ');	/*i18NOK:LINE*/
		run;
		
		*filename exp_file "&export_spec_file_path./&export_spec_file_nm..csv" &dabt_csv_export_encoding_option;			/*i18NOK:LINE*/

 filename exp_file filesrvc folderpath="&export_spec_file_path./" filename= "&export_spec_file_nm..csv" encoding='utf-8' debug=http; /* i18nOK:Line */
		
		/*Data present in cpspcscr.cprm_export_specification table will be exported to csv format*/
		proc export
			data=&export_specs_ds_lib..&export_specs_ds_nm.
			outfile = exp_file  /* i18NOK:LINE */
			DBMS= csv
			replace label;
		run;
		
		/*Export check : If export is not successful. Put message in log and return.*/
		%if &syserr. ne 0 %then %do;
			/* Some issue occured while creation of export specifcation excel file */
			%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_EXCEL_NOT_CREATED noquote));	
			%let syscc=99;
			%return;
		%end;
	%end;
	%else %do;
		/* Noting found to export to entity specification CSV file.  */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_EMPTY_DS, noquote));	
		%let syscc=99;
		%return;
	%end;

	%if &DABT_DEBUG_FLG. eq Y %then %do;
		filename cportout filesrvc folderpath="&export_spec_file_path./" filename= "debug_all_exp_spec_&export_specs_ds_lib._lib.xpt"  debug=http; /* i18nOK:Line */
		proc cport library = &export_specs_ds_lib. file=cportout memtype=data; /* i18NOK:LINE */
		run;
			
	 	filename cportout clear;
	%end;
	
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_specs_ds_nm.);

	*libname &export_specs_ds_lib. clear;

%mend dabt_cprm_create_all_exp_spec ;
