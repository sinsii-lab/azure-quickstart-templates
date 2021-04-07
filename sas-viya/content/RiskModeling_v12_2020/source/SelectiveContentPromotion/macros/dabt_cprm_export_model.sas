/*****************************************************************/
/* NAME: dabt_cprm_export_model.sas               		 */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to move model realted sas files  
				and dev data to specified location								 */
/*                                                               */
/* Parameters :  export_spec_ds_lib						         */
/* 			     export_spec_ds_nm:								 */
/*				 export_ouput_folder_path: Folder location 
					to which export package to create		 	 */
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Called Internally by dabt_cprm_export_wrapper macro  */
/*          dabt_cprm_export_model(export_spec_ds_lib =,
				export_spec_ds_nm= ,export_ouput_folder_path=)	 */
/*                                                               */
/*****************************************************************/
 
/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*24May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/
%macro dabt_cprm_export_model(export_spec_ds_lib= ,export_spec_ds_nm= , export_ouput_folder_path=);

	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;

	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

	/********Defining local variables ********/
	%local  export_folder_nm m_mdl_path  m_astore_path m_entity_type_cd m_entity_key m_mdl_cd_cnt  m_score_code_file_loc m_score_code_file_nm m_score_grp_code_file_nm m_model_sk m_model_id j;
	
			
	/*****Folder name where all model related files will be copied.*/

	%let export_folder_nm = model; /* i18NOK:LINE */

	/****Macro variable to store full path of export package folder***** */

	%let m_mdl_path =;
	
	/* Deleeting all contents of &export_ouput_folder_path./&export_folder_nm. */

	*%dabt_dir_delete(dirname=&export_ouput_folder_path./&export_folder_nm.);

	%let m_entity_type_cd =MODEL;  /* i18NOK:LINE */
	
	/*Counting number of models */

	proc sql noprint;
		select
			cp_exp_spec.entity_key , count(*)    /*i18NOK:LINE*/
			into
				:m_entity_key separated by '#', /* i18NOK:LINE */
				:m_mdl_cd_cnt 
		from 
				&export_spec_ds_lib..&export_spec_ds_nm. as cp_exp_spec 
			inner join &lib_apdm..cprm_entity_master
				on(cp_exp_spec.entity_type_cd = cprm_entity_master.entity_type_cd)
		where trim(left(upcase(cprm_entity_master.entity_type_cd)))="&m_entity_type_cd." /* i18NOK:LINE */
				and kupcase(promote_flg) = kupcase(&check_flag_true.)
		order by cp_exp_spec.entity_key ; /* i18NOK:LINE */
	quit;

	%dabt_err_chk(type=SQL);

	%let m_entity_key = &m_entity_key.;
	%let m_mdl_cd_cnt = &m_mdl_cd_cnt.;
	
	/*This will create cprm_export_package folder next to export_ouput_folder_path  */
	%let m_work_lib_path = %sysfunc(pathname(work));
		%dabt_make_work_area(dir=&m_work_lib_path., create_dir=&export_folder_nm., path=m_mdl_path); 

		%let m_mdl_path = &m_mdl_path.;/*Accurate path upto &export_folder_nm */
		
		libname mdl_data "&m_mdl_path."; /* Creating a temporary library for data movement*/
		
		%dabt_initiate_cas_session(cas_session_ref=cprm_rmmdl_path);
		
		proc cas;
		table.caslibinfo result=ast / caslib="&DABT_MODELING_ABT_LIBREF." verbose="TRUE"; /* i18nOK:Line */
		exist_ast=findtable(ast);
		if exist_ast then
			saveresult ast dataout=work.ast_info;
		quit;

		%let ast_caslib_path= ;

		proc sql noprint;
			select path into :ast_caslib_path from work.ast_info ;
		quit;
		%dabt_terminate_cas_session(cas_session_ref=cprm_rmmdl_path);
		
		%do j=1 %to &m_mdl_cd_cnt.;
			%let m_model_sk = %scan(&m_entity_key.,&j,%str(#));			/*i18NOK:LINE*/

			/*Finding model score  code file name and external code file path from external code master.*/
			
	   				%let m_mdl_spec_scoring_code_path = &m_file_srvr_mdl_folder_path./&m_model_sk.;
					%let m_scoring_code_path = &m_file_srvr_mdl_folder_path.;
					
 
			/*This will create cprm_export_package folder next to export_ouput_folder_path  */
/* 			%dabt_make_work_area(dir=&export_ouput_folder_path., create_dir=&export_folder_nm., path=m_rel_path);  */
/* 			%let m_rel_path = &m_rel_path.;/*Accurate path upto &export_folder_nm		 */

		
			/*selecting model_id using model_sk*/
			proc sql noprint;
				select model_id into :m_model_id
				from &lib_apdm..model_master
				where model_sk=&m_model_sk.;
			quit;	 
			
			
			%let m_model_id=%sysfunc(kstrip(&m_model_id));
					 
			%let m_score_code_file_nm=  score_&m_model_id..sas;
			%let m_astore_file_nm=  MDL_ASTORE_&m_model_id..sashdat;
			%let m_score_grp_code_file_nm= scorecard_grouping_code_&m_model_id..sas ;
		
			/*Defining source file and desination external file name and it's path.*/

			filename src filesrvc folderpath= "/&m_mdl_spec_scoring_code_path./" filename="score.sas" recfm=n; /* i18NOK:LINE */
			*filename dest "&m_rel_path./&m_score_code_file_nm." recfm=n; /* i18NOK:LINE */
filename des filesrvc folderpath="&export_ouput_folder_path./" filename= "&m_score_code_file_nm." recfm=n debug=http; /* i18nOK:Line */			
			
			filename sr_gr filesrvc folderpath= "/&m_scoring_code_path./" filename="&m_score_grp_code_file_nm." recfm=n; /* i18NOK:LINE */
			*filename des_gr "&m_rel_path./&m_score_grp_code_file_nm." recfm=n; /* i18NOK:LINE */
filename des_gr filesrvc folderpath="&export_ouput_folder_path./" filename= "&m_score_grp_code_file_nm." recfm=n debug=http; /* i18nOK:Line */			
			
		/*Copying scoring and scorecard_group code files from source to destination.*/
			
		 %if %sysfunc(fexist(src)) eq 1 %then %do;

			data _null_;
				rc=fcopy('src', 'des'); /* i18NOK:LINE */
     			format msg $1000.;
	             msg=sysmsg();
	             put rc=msg=;				
			run;
			
		%end;
		%if %sysfunc(fexist(sr_gr)) eq 1 %then %do;

			data _null_;
				rc=fcopy('sr_gr', 'des_gr'); /* i18NOK:LINE */
     			format msg $1000.;
	             msg=sysmsg();
	             put rc=msg=;				
			run;
			
		%end;
			

		/* CPRM CSB-24639 Begin: Extract report_specification_sk for deployed version to export Dev Data Tables */
/* 		proc sql; */
/* 		select report_specification_sk into :m_src_rpt_spec_sk_dv  	 */
/* 			from &lib_apdm..mm_report_specification */
/* 				where deployed_flg = &CHECK_FLAG_TRUE. 			 		/* i18nOK:Line	 */
/* 				and model_sk = &m_model_sk. ; */
/* 		 */
/* 		quit; */
/*  */
/*  */
/* 		%if %symexist(m_src_rpt_spec_sk_dv) %then %do ; */
/* 			%let m_src_rpt_spec_sk_dv = &m_src_rpt_spec_sk_dv; */
/* 		%end; */
/* 		 */
/* 		%dabt_initiate_cas_session(cas_session_ref=cprm_export_model); */
/* 		%let cas_tableExist=; */
/* 		%let cas_tableExist=%eval( %sysfunc(exist(&cs_fact_lib.._&m_model_id._&m_src_rpt_spec_sk_dv._dev_fact,DATA))); */
/* 		%if &cas_tableExist. eq 1 or &cas_tableExist. eq 2 %then %do;/*1 -local scope 2-global scope */
/* 			proc copy in=&cs_fact_lib. out=mdl_data memtype=data; */
/* 				select _&m_model_id._&m_src_rpt_spec_sk_dv._dev_fact ; */
/* 			run; */
/* 		%end; */
/* 		%dabt_terminate_cas_session(cas_session_ref=cprm_export_model); */
		/* CPRM CSB-24639 End: Extract report_specification_sk for deployed version to export Dev Data Tables */
		
		/* CPRM CSB-24639 NEEDS TO BE UPDATED AFTER DEV OF TABLE */
/*		%if %sysfunc(exist(&cs_fact_lib.._&m_model_id._sc_range_fact)) eq 1 %then %do; 
			proc copy in=&cs_fact_lib. out=mdl_data memtype=data;
				select _&m_model_id._sc_range_fact ;
			run;
		%end;
*/		

/****** logic to extract python SWAT models's astore files saved in rm_mdl caslib ***/
		%dabt_initiate_cas_session(cas_session_ref=cprm_export_model_astore);
	
		proc cas;
			table.fileInfo result=Files / caslib="&DABT_MODELING_ABT_LIBREF." 
					path="&m_astore_file_nm.";
			exist_Files=findtable(Files);
		
			if exist_Files then
				saveresult Files dataout=work.ast_files;
		quit;
		%let m_ast_cnt=;
		proc sql noprint;
			select count(*) into :m_ast_cnt from ast_files;
		quit;
		
		%if &m_ast_cnt. ge 1 %then %do;

			filename ast_in "&ast_caslib_path./&m_astore_file_nm." recfm=n; /*i18NOK:LINE*/
			filename ast_out filesrvc 
				folderpath="&export_ouput_folder_path./"      /* I18NOK:LINE */
				filename="&m_astore_file_nm." debug=http 
				CD="attachment; filename=&m_astore_file_nm." recfm=n;

			/* i18nOK:Line */
			data _null_;
				rc=fcopy('ast_in', 'ast_out');
			format msg $1000.;
				/* I18NOK:LINE */
				msg=sysmsg();
				put rc=msg=;
			run;

		%end;
		%dabt_terminate_cas_session(cas_session_ref=cprm_export_model_astore);

		%let m_published_model_nm=;	
		%let m_model_short_nm=;	
		proc sql noprint;
			select last_registered_model_nm,model_short_nm into :m_published_model_nm ,:m_model_short_nm
			from &lib_apdm..model_master where model_sk =&m_model_sk. and model_source_type_sk=3;
		quit;

		%if "&m_published_model_nm." NE "" %then %do;
			%dabt_initiate_cas_session(cas_session_ref=cprm_cdmml_model_check);
			%let m_published_model_nm=%sysfunc(kstrip(&m_published_model_nm.));
			proc cas;
			table.fetch result=vdml / fetchVars={{name="ModelName"}} 
				sortby={{name="ModelName"}} table={caslib="&RM_PUBLISHED_DEST_CAS_LIB.", 
				name="&RM_PUBLISHED_DEST_CAS_TABLE_NM.",
				where="ModelName='&m_published_model_nm.'"};
			exist_Files=findtable(vdml);
		
			if exist_Files then
				saveresult vdml dataout=work.vdml;
			run;
			quit;
			
			proc sql noprint;
				select * from vdml;
			quit;


			%if &sqlobs. lt 1 %then %do;
				%let syscc=99;
				%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_EXPORT_MODEL1.1, noquote,&m_model_short_nm.,&m_published_model_nm.,&RM_PUBLISHED_DEST_CAS_LIB.,&RM_PUBLISHED_DEST_CAS_TABLE_NM.));
				%put  %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_EXPORT_MODEL1.2, noquote));	
				%return;
			%end;
		
			%dabt_terminate_cas_session(cas_session_ref=cprm_cdmml_model_check);
			
		%end;
	%end;/*loop end for all models */
	
	
	
	*filename cportout "&export_ouput_folder_path./dev_data.xpt"; /* i18NOK:LINE */
/* filename cportout filesrvc folderpath="&export_ouput_folder_path./" filename= "dev_data.xpt"  debug=http; /* i18nOK:Line			 */
/* 	proc cport library=mdl_data file=cportout memtype=data; /* i18NOK:LINE */
/* 	run; */
/* 	filename cportout clear; */
/*  */
/* 	proc datasets library=mdl_data kill nodetails nolist; */
/* 	quit; */
/***** Copy all vdmml model metadata as sashdat ***/ 

%let mm_entity_key= %sysfunc(tranwrd(&m_entity_key.,%str(#),%str(,)));

%let m_all_pub_nm=;

proc sql noprint;
	select "'"||kstrip(last_registered_model_nm)||"'" into :m_all_pub_nm separated by ',' 
	from &lib_apdm..model_master where model_source_type_sk=3 and model_sk in (&mm_entity_key.);
quit;

%put m_all_pub_nm=&m_all_pub_nm.;

%if "&m_all_pub_nm." ne "" %then %do; 

%dabt_initiate_cas_session(cas_session_ref=cprm_vdmml_model_xprt);


%let tmp_caslib=PBDST_TEMP;
caslib &tmp_caslib. task=add type=path path="&m_mdl_path" desc='Flat files' ;

%let src_pub_cas_tbl_nm=SRC_RM_PUB_CAS_TBL;
proc cas;
	table.partition / casout={caslib="&tmp_caslib.", name=kupcase("&src_pub_cas_tbl_nm."), 	/* I18NOK:LINE */
		promote="FALSE", replace="TRUE"} table={caslib=kupcase("&RM_PUBLISHED_DEST_CAS_LIB.") , 
		name=kupcase("&RM_PUBLISHED_DEST_CAS_TABLE_NM."), 
		where="ModelName IN (&m_all_pub_nm.)"};
	run;
quit;

proc cas;
	table.save / caslib="&tmp_caslib." name=kupcase("&src_pub_cas_tbl_nm.") replace=True 
		table={caslib="&tmp_caslib." name=kupcase("&src_pub_cas_tbl_nm.")};
quit;

			filename vd_in "&m_mdl_path./&src_pub_cas_tbl_nm..sashdat" recfm=n; /*i18NOK:LINE*/
			filename vd_out filesrvc 
				folderpath="&export_ouput_folder_path./"      /* I18NOK:LINE */
				filename="&src_pub_cas_tbl_nm..sashdat" debug=http 
				CD="attachment; filename=&src_pub_cas_tbl_nm..sashdat" recfm=n;

			/* i18nOK:Line */
			data _null_;
				rc=fcopy('vd_in', 'vd_out');
			format msg $1000.;
				/* I18NOK:LINE */
				msg=sysmsg();
				put rc=msg=;
			run;

	%dabt_terminate_cas_session(cas_session_ref=cprm_vdmml_model_xprt);
%end;

%mend dabt_cprm_export_model;
