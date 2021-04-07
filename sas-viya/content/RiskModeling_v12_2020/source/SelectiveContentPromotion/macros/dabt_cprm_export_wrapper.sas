/*****************************************************************/
/* NAME: dabt_cprm_export_wrapper.sas                      		 */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to export all entities in to excel sheet   */
/*                                                               */
/* Parameters :  export_spec_file_path:Export Specification 	
						file path						         */
/* 			     export_spec_file_nm:Excel file name that 
						contains export specification.			 */
/*				 export_ouput_folder_path: Folder location 
					to which export package to create		 	 */
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Called by Admin User								 */
/*          %dabt_cprm_export_wrapper(export_spec_file_path=, 
			export_spec_file_nm=,export_ouput_folder_path=,log_divert_flg=);	 */
/*                                                               */
/*****************************************************************/
 
/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*5May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/

%macro dabt_cprm_export_wrapper(export_spec_file_path=,export_spec_file_nm=, export_ouput_folder_path=,log_divert_flg=N);

	%let syscc = 0;
	/**** Assigning job folder path ***/
	
	%if ("&export_ouput_folder_path." eq "") %then %do;
		%let export_ouput_folder_path=%str(/&m_file_srvr_job_folder_path/&m_job_sk);
	%end;
	%if ("&export_spec_file_path." eq "") %then %do;
		/* Export specification file path cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_PATH_NOT_SPECIFIED, noquote));	
		%let syscc=99;
		%return;
	%end;

	%if ("&export_spec_file_nm." eq "" ) %then %do;
		/* Export specification file name cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_NM_NOT_SPECIFIED, noquote));	
		%let syscc=99;
		%return;
	%end;

	%if ("&export_ouput_folder_path." eq "" ) %then %do;
		/* Exported package folder path cannot be blank. */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_PACKAGE_FOLDER_PATH_NOT_SPECIFIED, noquote));	
		%let syscc=99;
		%return;
	%end;
	
	filename spec filesrvc 
              folderpath="&export_spec_file_path."      /* I18NOK:LINE */
              filename="&export_spec_file_nm..csv" debug=http 	/* i18nOK:Line */
              CD="attachment; filename=&export_spec_file_nm..csv" recfm=n;	/* i18nOK:Line */
	
	%if %sysfunc(fexist(spec)) eq 0 %then %do;
		%put %sysfunc(sasmsg(work.dabt_cprm_misc, ERR_EXPORT_SPEC_FILE_NM_NOT_FOUND, noquote,&export_spec_file_nm.));	
		%return;
	%end;
	
	 filename spec clear;
	 
	/*Redirecting the logs.*/
/* 	%if &log_divert_flg=Y %then %do; */
/* 	 */
/* 		*%local path_of_log cprm_log_path ;  */
/* 		 */
/* 		*%let path_of_log =; */
/* 		*%dabt_make_work_area(dir=&export_ouput_folder_path., create_dir=logs, path=path_of_log); */
/*  */
/* 		*%let cprm_log_path	=	&export_ouput_folder_path./logs; */
/* 	 */
/* 		*proc printto log = "&cprm_log_path./dabt_cprm_export_wrapper.log" new; /* i18nOK:Line */
/* 		filename wrp_log filesrvc folderpath="&export_spec_file_path./" filename= "dabt_cprm_export_wrapper.log"  debug=http; /* i18nOK:Line */
/* 		proc printto log = wrp_log; /* i18nOK:Line */
/* 		run; */
/* 	%end; */

	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;

	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

	
	%local m_rel_path
		   mm_rel_path
		   export_folder_nm
		   export_scratch_ds_lib
		   export_specs_ds_nm
		   import_spec_file_nm
		   ;

	/*Folder name which will be created next to folder provided by user.This will contain entire export package*/

	*%let export_folder_nm = cprm_export_package; /* i18NOK:LINE */

	/*Macro variable to store full path of export package folder */
	%let m_rel_path=;
	%let m_rel_path =&export_ouput_folder_path.;

	*%dabt_dir_delete(dirname=&export_ouput_folder_path./&export_folder_nm.);
	%let mm_rel_path=;
	/*This will create cprm_export_package folder next to export_ouput_folder_path  */
	%let m_work_lib_path = %sysfunc(pathname(work));
	%dabt_make_work_area(dir=&m_work_lib_path., create_dir=&export_folder_nm., path=mm_rel_path); 

	%let mm_rel_path = &mm_rel_path.;
	

	/*Assigning created path to library cpexpscr */

	%let export_scratch_ds_lib =  cpexpscr ; /* i18NOK:LINE */

	libname &export_scratch_ds_lib. "&mm_rel_path.";

	/*Creating input dataset which will contains only those entites for which user has set promote_flg = Y*/

	%let export_specs_ds_nm = cprm_export_specification;


	%dabt_drop_table(m_table_nm=&export_scratch_ds_lib..&export_specs_ds_nm.);

	%let cprm_entity_type_cd_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_CD_LABEL, noquote)); 
	%let cprm_entity_type_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_NM_LABEL, noquote)); 
	%let cprm_entity_key_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_KEY_LABEL, noquote)); 
	%let cprm_entity_desc_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_DESC_LABEL, noquote)); 
	%let cprm_promote_flg_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_PROMOTE_FLG_LABEL, noquote)); 
	%let cprm_entity_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_NM_LABEL, noquote)); 
	%let cprm_owner_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_OWNER_NM_LABEL, noquote)); 
	
	proc sql noprint;
		create table &export_scratch_ds_lib..&export_specs_ds_nm. ( 
			entity_type_cd  character(40) not null 
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

	%dabt_err_chk(type=SQL);

	/*Dumping entire APDM to other location and dropping all views from apdm and creating xpt for apdm.*/

	%let m_scr_apdm_path =;

	*%dabt_dir_delete(dirname=&m_rel_path./apdm);

	/*This will create apdm folder as a scratch next to &m_rel_path.  */
	
	%dabt_make_work_area(dir=&m_work_lib_path., create_dir=apdm, path=m_scr_apdm_path); 

	%let m_scr_apdm_path = &m_scr_apdm_path.;

	libname scr_apdm "&m_scr_apdm_path.";
	
	*%let scr_apdm=work;
	
	/*To exclude EXPORT_TEMPLATE_MASTER from apdm xpt*/
	%let exc_table_nm = ;
	%if %sysfunc(exist(&lib_apdm..export_template_master)) %then %do; /* i18NOK:LINE */
		%let exc_table_nm = export_template_master; 		/* i18NOK:LINE */
	%end;
	%let m_view_tbl_nm=;

	proc sql ;
	quit;

	proc sql noprint;
		select memname 
				into  :m_view_tbl_nm  separated by ' '  /* i18NOK:LINE */
		from dictionary.tables
			where kupcase(libname)=kupcase("&lib_apdm.")
				and kupcase(memname) like "VW_%";		/* i18NOK:LINE */
	quit;
	
	%let m_view_tbl_nm =&m_view_tbl_nm.;

	/*Copying entire content in to tmpapdm library and excluding all views starting with vw_ */	

	options bufsize=4096;

	proc datasets nolist;
		copy in=&lib_apdm. out=scr_apdm;
		%if &m_view_tbl_nm. ne %then %do;
			exclude &m_view_tbl_nm. &exc_table_nm.;
		%end;
	quit;

	/*Creating xpt file for entire APDM into export_ouput_folder_path/scratch folder.*/

	*filename cportout "&m_rel_path./source_apdm.xpt"; /* i18NOK:LINE */

	filename cportout filesrvc folderpath="&export_ouput_folder_path./" filename= "source_apdm.xpt"  debug=http; /* i18nOK:Line */
	proc cport library = scr_apdm file=cportout memtype=data; /* i18NOK:LINE */
	run;

	*%dabt_dir_delete(dirname=&m_scr_apdm_path., deleteParentFolderFlg=Y); /*S1438342 - parent folder 'APDM' should be deleted */
	
	/*Importing entities specification provided by user */
filename wrp_file filesrvc folderpath="&export_spec_file_path./" filename= "&export_spec_file_nm..csv" encoding='utf-8' debug=http; /* i18nOK:Line */			
	proc import datafile=wrp_file  /* i18NOK:LINE */
		out=&export_scratch_ds_lib..user_exprt_specs /* i18NOK:LINE */
		dbms=csv
		replace;
		guessingrows=32767;
		datarow=2;
		getnames=no;
	run;
	
	%let m_pool_promote_cnt=;
	proc sql noprint;
		select count(*)  /* i18NOK:LINE */
				into :m_pool_promote_cnt
		from &export_scratch_ds_lib..user_exprt_specs 
		where kupcase(var1) eq "POOL"; /* i18NOK:LINE */
	quit;
		
	%if &m_pool_promote_cnt >0 %then %do;
		%let pool_scheme_lst=;
		proc sql noprint;
			select pool_scheme_sk into :pool_scheme_lst separated by ","
			from &lib_apdm..pool_scheme 
			where status_cd='OBSLT' or pool_scheme_sk in (select pool_scheme_sk from &lib_apdm..pool_scheme_lock_info); /* i18NOK:LINE */
		quit;
		%if &pool_scheme_lst. ne %then %do;
			
			proc sql noprint;
				delete from &export_scratch_ds_lib..user_exprt_specs 
				where var3 in (&pool_scheme_lst.)
				and kupcase(var1) eq "POOL"; /* i18nOK:Line */
			quit;
				%let pool_scheme_lst = %quote(&pool_scheme_lst);
			 %put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, WARN_INVALID_POOL_SCHEMES,noquote,&pool_scheme_lst));
			
		%end;
				
	
	%end;

	/*populating cpexpscr.cprm_export_specification with only those enties which user has marked to be  promoted.*/

	proc sql noprint ;
		insert into &export_scratch_ds_lib..&export_specs_ds_nm. 
			(entity_type_cd,
				entity_type_nm,
				entity_key,
				entity_nm,
				entity_desc,
				promote_flg,owner ) 
			select var1 as entity_type_cd,
				var2 as entity_type_nm,
				var3 as entity_key,
				var4 as entity_nm,
				var5 as entity_desc,
				&check_flag_true. as promote_flg,
				var7 as owner
			from 
				&export_scratch_ds_lib..user_exprt_specs
			where kupcase(var6) = kupcase(&check_flag_true.);
	quit;

	%dabt_err_chk(type=SQL);

	%dabt_drop_table(m_table_nm=&export_scratch_ds_lib..user_exprt_specs);

	/*Creating files package that will also move to target machine for import*/

	%local m_ext_cd_promote_cnt;
	%let m_ext_cd_promote_cnt = 0; 

	/*Check if external code has been set to promote or not */

	proc sql noprint;
		select count(*)  /* i18NOK:LINE */
				into: m_ext_cd_promote_cnt
		from &export_scratch_ds_lib..&export_specs_ds_nm. 
		where kupcase(entity_type_cd) eq "EXT_CODE";  /* i18NOK:LINE */
	quit;

	/*If promote flg Y is set for external code then export package for external code will be created.*/


	%if &m_ext_cd_promote_cnt. gt 0 %then %do;
		%dabt_cprm_export_external_code(export_spec_ds_lib= &export_scratch_ds_lib.,export_spec_ds_nm= &export_specs_ds_nm., export_ouput_folder_path=&m_rel_path.);
	%end;	
	
	
	/*Check if model has been set to promote or not */
	%let m_mdl_promote_cnt=;
	proc sql noprint;
		select count(*)   /* i18NOK:LINE */
				into: m_mdl_promote_cnt
		from &export_scratch_ds_lib..&export_specs_ds_nm. 
		where kupcase(entity_type_cd) eq "MODEL";  /* i18NOK:LINE */
	quit;

	/*If promote flg Y is set for a model then export package for model will be created.*/


	%if &m_mdl_promote_cnt. gt 0 %then %do;
		%dabt_cprm_export_model(export_spec_ds_lib= &export_scratch_ds_lib.,export_spec_ds_nm= &export_specs_ds_nm., export_ouput_folder_path=&m_rel_path.);
	%end;
	
	
	/*If promote flg Y is set for pool scheme then export package for pool scheme will be created.*/

	%if &m_pool_promote_cnt. gt 0 %then %do;
		%csbmva_cprm_export_pool_sch(export_spec_ds_lib= &export_scratch_ds_lib.,export_spec_ds_nm= &export_specs_ds_nm., export_ouput_folder_path=&m_rel_path.);
	%end;	

	%let import_spec_file_nm = cprm_import_specification_file; /* i18NOK:LINE */
	
	*filename exp_file "&m_rel_path./&import_spec_file_nm..csv" &dabt_csv_export_encoding_option;   /* i18NOK:LINE */
filename exp_file filesrvc folderpath="&export_ouput_folder_path./" filename= "&import_spec_file_nm..csv" encoding='utf-8' debug=http; /* i18nOK:Line */			

	proc export
		data=&export_scratch_ds_lib..&export_specs_ds_nm. 
		outfile = exp_file  /* i18NOK:LINE */
		DBMS= csv
		replace label;
	run;

	/*Export check : If export is not successful. Put message in log and return.*/
	%if &syserr. ne 0 %then %do;
		/* Some issue occured while creation of import specifcation csv file */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_IMPORT_SPEC_EXCEL_NOT_CREATED noquote));	
		%let syscc=99;
		%return;
	%end;

	%if &DABT_DEBUG_FLG. eq Y %then %do;
		filename cprtout filesrvc folderpath="&export_ouput_folder_path./" filename= "debug_cprm_export_wrapper_&export_scratch_ds_lib._lib.xpt"  debug=http; /* i18nOK:Line */
		proc cport library = &export_scratch_ds_lib. file=cprtout memtype=data; /* i18NOK:LINE */
		run;
			
	 	filename cprtout clear;
	%end;	
	%dabt_drop_table(m_table_nm=&export_scratch_ds_lib..&export_specs_ds_nm.);

/* 	 */
/* 	%if &log_divert_flg=Y %then %do;	 */
/* 		proc printto; */
/* 		run; */
/* 	%end;	 */

%mend dabt_cprm_export_wrapper;
