/*****************************************************************/
/* NAME: dabt_cprm_create_mdl_exp_spec.sas                       */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to export all dependent entities          */
/* (purpose,soa,datasource,extenal_code,subject_group, subset map */ 
/* Time periods, as of time ) for a project or list 			 */                                                  
/* of project specified by the user   							 */
/*                                                               */
/* Parameters :  export_spec_file_path:without ending slash      */
/* 			     export_spec_file_nm:file name without extension */
/*				 model_id_lst: comma separated project id list */                               
/*   Example:  %dabt_cprm_create_mdl_exp_spec(export_spec_file_path=C:\Export, export_spec_file_nm=test_data3,model_id_lst=%quote(200));*/                                       
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Called by Admin Users     		                     */
/*          %dabt_cprm_create_mdl_exp_spec(export_spec_file_path=, 
			export_spec_file_nm=,model_id_lst=);  			 */
/*                                                               */
/*****************************************************************/

/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*1May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/
%macro dabt_cprm_create_mdl_exp_spec(export_spec_file_path=, export_spec_file_nm=,model_id_lst=,log_divert_flg=N);


	/**** Assigning job folder path ***/
	%if ("&export_spec_file_path." eq "") %then %do;
		%let export_spec_file_path=%str(/&m_file_srvr_job_folder_path/&m_job_sk);
	%end;
	%if ("&export_spec_file_nm." eq "") %then %do;
		%let export_spec_file_nm=mdl_exp_spec_data;
	%end;

%if ("&export_spec_file_path." eq "") %then %do;
		/* Export specification file path cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_PATH_NOT_SPECIFIED, noquote));
		%let syscc=99;
		%return;
	%end;
	
	filename resp temp;
	filename resp_hdr temp;
	%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

	proc http url="&BASE_URI/folders/folders/@item?path=&export_spec_file_path." /* i18nOK:Line */
		method='get'/* i18nOK:Line */
		oauth_bearer=sas_services out=resp headerout=resp_hdr headerout_overwrite 
			ct="application/json";			/* i18nOK:Line */
		DEBUG LEVEL=3;
	run;
	quit;
	
	
	%put &SYS_PROCHTTP_STATUS_CODE.;
	%if &SYS_PROCHTTP_STATUS_CODE. ne 200 %then %do;
		%put ERROR: &export_spec_file_path - &SYS_PROCHTTP_STATUS_PHRASE.;
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_PATH_NOT_EXIST, noquote, &export_spec_file_path.) );
		%let syscc=99;
		%return ;
	%end;

	%if ("&export_spec_file_nm." eq "") %then %do;
		/* Export specification file name cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_NM_NOT_SPECIFIED, noquote));	
		%let syscc=99;
		%return;
	%end;
	
	%if ("&model_id_lst." eq "") %then %do;
		/* Model id list cannot be blank */
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_MODEL_ID_NOT_SPECIFIED, noquote));
		%let syscc=99;
		%return;
	%end;
	
	%else %do;
		%let m_model_id_cnt=;
		%let m_model_id_cnt = %eval(%sysfunc(countc(%quote(&model_id_lst),","))+1);			/*i18NOK:LINE*/
		%do i=1 %to &m_model_id_cnt;
			%let model_id=;
			%let model_id =  %scan(%quote(&model_id_lst),&i,%str(,));						/*i18NOK:LINE*/
			%let cnt=;
			proc sql noprint;
				select count(model_id) into :cnt											/*i18NOK:LINE*/
				from &lib_apdm..model_master
				where  model_id="&model_id.";
			quit;
			%if &cnt. eq 0 %then %do;
				/* The specified Model ID does not exist */
				%let syscc=99;
				%put %sysfunc(sasmsg(work.DABT_CPRM_MISC, ERR_MODEL_ID_NOT_VALID,N,&model_id));
				%put ERROR;
				%return;
			%end;
		%end;
		
	%end;
	
	*%dabt_dir_delete(dirname = &export_spec_file_path./logs,deleteMode=FILE); 

*%let path_of_log =;
*%dabt_make_work_area(dir=&export_spec_file_path., create_dir=logs, path=path_of_log);

*%let cprm_log_path=;
*%let cprm_log_path=&export_spec_file_path./logs;
				
/*Redirecting the logs.*/
/* %if &log_divert_flg=Y %then %do; */
/* 	*proc printto log = "&cprm_log_path./dabt_cprm_create_mdl_exp_spec.log" new;			/*i18NOK:LINE */
/* 	*run; */
/* 	filename mdl_log filesrvc folderpath="&export_spec_file_path./" filename= "dabt_cprm_create_mdl_exp_spec.log"  debug=http; /* i18nOK:Line */
/* 	proc printto log = mdl_log; /* i18nOK:Line */
/* 	run; */
/* %end; */
/* 	 */
	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;

	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

	%local export_specs_ds_lib export_specs_ds_nm m_exprt_rec_cnt m_model_cnt export_model_ds_nm i;

	/*Assigning created path to library cpspcscr */

	%let export_specs_ds_lib =  work ; /* i18NOK:LINE */

	*libname &export_specs_ds_lib. "&export_spec_file_path.";

	
	%dabt_err_chk(type=SQL);
	
	/*Creating input dataset which will append all entites related to a model to be exported. this will be used in a loop*/
	%let export_model_ds_nm = cprm_export_mdl_data ; /* i18NOK:LINE */
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_model_ds_nm.);
	
	proc sql noprint;
		create table &export_specs_ds_lib..&export_model_ds_nm. ( 
			model_sk numeric(10) not null, 
			entity_type_cd  character(10) not null , 
			entity_sk  numeric(10) not null 
			);
	quit;
	
	
	/**Get model_sks corresponding to model_ids specified***/
	
	%let m_model_cnt = %eval(%sysfunc(countc(%quote(&model_id_lst),","))+1);		/*i18NOK:LINE*/
		%do i=1 %to &m_model_cnt;
			%let model_id=;
			%let model_id =  %scan(%quote(&model_id_lst),&i,%str(,));				/*i18NOK:LINE*/
			
			proc sql noprint;
				select model_sk into :model_sk
				from &lib_apdm..model_master
				where model_id = "&model_id.";
			quit;
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_model_ds_nm.
				select &model_sk. ,"PROJECT" ,project_sk							/*i18NOK:LINE*/
				from &lib_apdm..model_master  
			where model_sk = &model_sk.;
			quit;
			/* Following macro will add all the dependent entities required for a subset map to be imported */
			%dabt_cprm_get_subset_map_spec (export_specs_ds_lib= &export_specs_ds_lib.,entity_sk=&model_sk.,export_ds_nm=&export_model_ds_nm.,entity_type_cd=MDL);
		%end;
	
	
	proc sql noprint;
		select distinct(project_id) into :project_id_lst separated by ","
		from &export_specs_ds_lib..&export_model_ds_nm. in_ds inner join &lib_apdm..project_master prj_master on (in_ds.entity_sk = prj_master.project_sk)
		where upcase(trim(in_ds.entity_type_cd)) = "PROJECT"; ;						/*i18NOK:LINE*/
	quit;	
	
	proc sql noprint;
		select distinct(model_sk) into :model_sk_list separated by ","
		from &export_specs_ds_lib..&export_model_ds_nm.;
	quit;
	
	
	
%let export_prj_spec_file_nm= prj_out;
%dabt_cprm_create_prj_exp_spec(export_spec_file_path=&export_spec_file_path., export_spec_file_nm=&export_prj_spec_file_nm.,project_id_lst=%quote(&project_id_lst.),log_divert_flg=N);
	
%let export_specs_ds_nm = cprm_export_specification; /* i18NOK:LINE */

%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_specs_ds_nm.);
%let export_specs_prj_ds_nm=project_data;
/* %let exportfile=&export_spec_file_path./&export_prj_spec_file_nm..csv; */

filename prj_file filesrvc folderpath="&export_spec_file_path./" filename= "&export_prj_spec_file_nm..csv" encoding='utf-8' debug=http; /* i18nOK:Line */			

proc import OUT= &export_specs_ds_lib..&export_specs_prj_ds_nm. DATAFILE= prj_file DBMS=csv REPLACE; guessingrows=32767;  
run;

/* filename myfile "&export_spec_file_path.\&export_prj_spec_file_nm..csv";			/*i18NOK:LINE */
data _null_;
rc=fdelete("prj_file");																/*i18NOK:LINE*/
run;

%let cprm_entity_type_cd_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_CD_LABEL, noquote)); 
%let cprm_entity_type_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_NM_LABEL, noquote)); 
%let cprm_entity_key_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_KEY_LABEL, noquote)); 
%let cprm_entity_desc_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_DESC_LABEL, noquote)); 
%let cprm_promote_flg_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_PROMOTE_FLG_LABEL, noquote)); 
%let cprm_entity_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_NM_LABEL, noquote)); 
%let cprm_owner_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_OWNER_NM_LABEL, noquote)); 

%let m_filename= dabt_cprm_export_specification.sas;
*%dabt_check_and_create_table( libref = &export_specs_ds_lib.,tablename = &export_specs_ds_nm.  ,filename = &m_filename. , replace_flag = Y);
	proc sql;
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
	


proc sql;
	insert into &export_specs_ds_lib..&export_specs_ds_nm.
	select * from &export_specs_ds_lib..&export_specs_prj_ds_nm.;
quit;


%let m_sub_map_lst_mdl=;
%let m_table_lst_mdl=;
%let m_soa_lst_mdl=;
%let m_lib_lst_mdl=;

proc sql noprint;
		select distinct (entity_sk) into :m_sub_map_lst_mdl separated by ","
		from &export_specs_ds_lib..&export_model_ds_nm. in_ds
		where upcase(trim(in_ds.entity_type_cd)) = "SUBSET_MAP" and entity_sk not in (select entity_key from &export_specs_ds_lib..&export_specs_ds_nm. where upcase(trim(entity_type_cd)) = "SUBSET_MAP"  );	/*i18NOK:LINE*/
quit;
proc sql noprint;
		select distinct (entity_sk) into :m_table_lst_mdl separated by ","
		from &export_specs_ds_lib..&export_model_ds_nm. in_ds
		where upcase(trim(in_ds.entity_type_cd)) = "DATASOURCE" and entity_sk not in (select entity_key from &export_specs_ds_lib..&export_specs_ds_nm. where upcase(trim(entity_type_cd)) = "DATASOURCE"  );	/*i18NOK:LINE*/
quit;
proc sql noprint;
		select distinct (entity_sk) into :m_soa_lst_mdl separated by ","
		from &export_specs_ds_lib..&export_model_ds_nm. in_ds
		where upcase(trim(in_ds.entity_type_cd)) = "SOA" and entity_sk not in (select entity_key from &export_specs_ds_lib..&export_specs_ds_nm. where upcase(trim(entity_type_cd)) = "SOA"  );					/*i18NOK:LINE*/
quit;
proc sql noprint;
		select distinct (entity_sk) into :m_lib_lst_mdl separated by ","
		from &export_specs_ds_lib..&export_model_ds_nm. in_ds
		where upcase(trim(in_ds.entity_type_cd)) = "LIBRARY" and entity_sk not in (select entity_key from &export_specs_ds_lib..&export_specs_ds_nm. where upcase(trim(entity_type_cd)) = "LIBRARY"  );			/*i18NOK:LINE*/
quit;
	
	
	%if &m_sub_map_lst_mdl ne %then %do;
		%dabt_cprm_export_sbstmp_spec(	export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_sbstmp_sk_lst=%quote(&m_sub_map_lst_mdl.)); 
	%end;
	
	%if &m_table_lst_mdl ne %then %do;
		%dabt_cprm_export_table_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., table_sk_lst=%quote(&m_table_lst_mdl.));
	%end;
	
	%if &m_soa_lst_mdl ne %then %do;
		%dabt_cprm_export_soa_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., level_sk_lst=%quote(&m_soa_lst_mdl.));
	%end;
	
	%if &m_lib_lst_mdl ne %then %do;
		%dabt_cprm_export_library_spec(export_specs_ds_lib = &export_specs_ds_lib., export_specs_ds_nm = &export_specs_ds_nm., m_lib_sk_lst=%quote(&m_lib_lst_mdl. ) );
	%end;	


%dabt_cprm_export_model_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  = &export_specs_ds_nm., m_model_sk_lst= %quote(&model_sk_list.));

%if &syscc. > 4 %then %do;
		/* Some issue occured while creationn of export specifcation table */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_DS_NOT_CREATED, noquote));
/* 		%if &log_divert_flg=Y %then %do; */
/* 			proc printto; */
/* 			run; */
/* 		%end; */
		%let syscc=99;
		%return;
	%end;
	
		/*Sorting final dataset which will be exported to spreadsheet.*/
		
		proc sort data=&export_specs_ds_lib..&export_specs_ds_nm. out=&export_specs_ds_lib..&export_specs_ds_nm.;
			by entity_type_nm entity_nm;
		run;
		
		/* S1436878 - Handling newline character in <entity>_description - translating to dummy (space) */
		
		data &export_specs_ds_lib..&export_specs_ds_nm.;
			set &export_specs_ds_lib..&export_specs_ds_nm.;
			entity_desc = tranwrd(entity_desc,'0D0A'x,' ');	/*i18NOK:LINE*/ 
			entity_desc = tranwrd(compress(tranwrd(kstrip(entity_desc),' ','!`!'),,'S'),'!`!',' '); /*i18NOK:LINE*/
		run;
		
		*filename exp_file "&export_spec_file_path./&export_spec_file_nm..csv" &dabt_csv_export_encoding_option;			/*i18NOK:LINE*/
filename exp_file filesrvc folderpath="&export_spec_file_path./" filename= "&export_spec_file_nm..csv" encoding='utf-8' debug=http; /* i18nOK:Line */			
		
		/*Data present in cpspcscr.cprm_export_specification table will be exported to csv sheet*/
		proc export
			data=&export_specs_ds_lib..&export_specs_ds_nm.
			outfile = exp_file  /* i18NOK:LINE */
			DBMS= csv
			replace label;
		run;

		/*Export check : If export is not successful. Put message in log and return.*/
		%if &syserr. ne 0 %then %do;
			/* Some issue occured while creation of export specifcation csv file */
			%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_EXCEL_NOT_CREATED noquote));	
			%if &log_divert_flg=Y %then %do;
				proc printto;
				run;
			%end;
			%let syscc=99;
			%return;
		%end;

	%if &DABT_DEBUG_FLG. eq Y %then %do;
		filename cportout filesrvc folderpath="&export_spec_file_path./" filename= "debug_cprm_create_mdl_exp_spec_&export_specs_ds_lib._lib.xpt"  debug=http; /* i18nOK:Line */
		proc cport library = &export_specs_ds_lib. file=cportout memtype=data; /* i18NOK:LINE */
		run;
			
	 	filename cportout clear;
	%end;
	
	/* Cleaning up scratch data sets */
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_specs_ds_nm.);
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_model_ds_nm.);
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_specs_prj_ds_nm.);

%if &log_divert_flg=Y %then %do;	
	proc printto;
	run;
%end;	



%mend dabt_cprm_create_mdl_exp_spec;
