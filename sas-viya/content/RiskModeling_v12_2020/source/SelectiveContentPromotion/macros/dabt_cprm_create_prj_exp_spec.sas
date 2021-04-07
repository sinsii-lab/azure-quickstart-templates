/*****************************************************************/
/* NAME: dabt_cprm_create_prj_exp_spec.sas                       */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to export all dependent entities          */
/* (purpose,soa,datasource,extenal_code,subject_group, subset map */ 
/* Time periods, as of time ) for a project or list 			 */                                                  
/* of project specified by the user   							 */
/*                                                               */
/* Parameters :  export_spec_file_path:without ending slash      */
/* 			     export_spec_file_nm:file name without extension */
/*				 project_id_lst: comma separated project id list */                               
/*                                            */
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Called by Admin Users     		                     */
/*          %dabt_cprm_create_prj_exp_spec(export_spec_file_path=, 
			export_spec_file_nm=,project_id_lst=);  			 */
/*                                                               */
/*****************************************************************/

/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*1May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/

%macro dabt_cprm_create_prj_exp_spec(export_spec_file_path=, export_spec_file_nm=,project_id_lst=,log_divert_flg=N);
	%let log_divert_flg=&log_divert_flg;
	%let project_id_lst=&project_id_lst;
	%let syscc = 0;

	/**** Assigning job folder path ***/
	%if ("&export_spec_file_path." eq "") %then %do;
		%let export_spec_file_path=%str(/&m_file_srvr_job_folder_path/&m_job_sk);
	%end;
	%if ("&export_spec_file_nm." eq "") %then %do;
		%let export_spec_file_nm=prj_exp_spec_data;
	%end;

/*Check for export_spec_file_path ,export_spec_file_nm :should not be blank */
	%if ("&export_spec_file_path." eq "") %then %do;
		/* Export specification file path cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_PATH_NOT_SPECIFIED, noquote));
		%let syscc=99;
		%return ;
	%end;
	
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
		%put ERROR: &export_spec_file_path - &SYS_PROCHTTP_STATUS_PHRASE.;
		%let syscc=99;
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_PATH_NOT_EXIST, noquote, &export_spec_file_path.) );
		%return ;
	%end;

	%if ("&export_spec_file_nm." eq "") %then %do;
		/* Export specification file name cannot be blank */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_FILE_NM_NOT_SPECIFIED, noquote));	
		%let syscc=99;
		%return;
	%end;
	
	%if ("&project_id_lst." eq "") %then %do;
		/* Project id list cannot be blank */
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_PROJECT_ID_NOT_SPECIFIED, noquote));
		%let syscc=99;
		%return;
	%end;
	
	%else %do;
		%let m_project_id_cnt=;
		%let m_project_id_cnt = %eval(%sysfunc(countc(%quote(&project_id_lst),","))+1); 		/*i18NOK:LINE*/
		%do i=1 %to &m_project_id_cnt;
			%let project_id=;
			%let project_id =  %scan(%quote(&project_id_lst),&i,%str(,));						/*i18NOK:LINE*/
			%let cnt=;
			proc sql noprint;
				select count(project_id) into :cnt												/*i18NOK:LINE*/
				from &lib_apdm..project_master
				where  project_id="&project_id.";
			quit;
			%if &cnt. eq 0 %then %do;
				/* The specified project ID does not exist */
				%let syscc=99;
				%put %sysfunc(sasmsg(work.DABT_CPRM_MISC, ERR_PROJECT_ID_NOT_VALID,N,&project_id));
				%put ERROR;
				%return;
			%end;
		%end;
		
	%end;
	
	
/*  */
/* %if &log_divert_flg. ne N %then %do; */
/* *%dabt_dir_delete(dirname = &export_spec_file_path./logs,deleteMode=FILE);  */
/*  */
/* *%let path_of_log =; */
/* *%dabt_make_work_area(dir=&export_spec_file_path., create_dir=logs, path=path_of_log); */
/*  */
/*  */
/* 	*%let cprm_log_path=; */
/* 	*%let cprm_log_path=&export_spec_file_path./logs; */
/* 					 */
/* 	Redirecting the logs.	 */
/* 	*proc printto log = "&cprm_log_path./dabt_cprm_create_prj_exp_spec.log" new;  /*i18NOK:LINE	 */
/*  */
/* 	filename prj_log filesrvc folderpath="&export_spec_file_path./" filename= "dabt_cprm_create_prj_exp_spec.log"  debug=http; /* i18nOK:Line */
/* 	proc printto log = prj_log; /* i18nOK:Line */
/* 	run;	 */
/* %end; */
	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;

	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

	%local export_specs_ds_lib export_specs_ds_nm m_exprt_rec_cnt m_project_cnt export_project_ds_nm i;

	/*Assigning created path to library cpspcscr */

	%let export_specs_ds_lib =  work ; /* i18NOK:LINE */

	*libname &export_specs_ds_lib. "&export_spec_file_path.";

	
	%dabt_err_chk(type=SQL);
	
	/*Creating input dataset which will append all entites related to a project to be exported. this will be used in a loop*/
	%let export_project_ds_nm = cprm_export_prj_data ; /* i18NOK:LINE */
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_project_ds_nm.);
	
	proc sql noprint;
		create table &export_specs_ds_lib..&export_project_ds_nm. ( 
			project_sk numeric(10) not null, 
			entity_type_cd  character(10) not null , 
			entity_sk  numeric(10) not null 
			);
	quit;
	
	
	/**Get project_sks corresponding to project_ids specified***/
	
	%let m_project_cnt = %eval(%sysfunc(countc(%quote(&project_id_lst),","))+1);		/*i18NOK:LINE*/
		%do i=1 %to &m_project_cnt;
			%let project_id=;
			%let project_id =  %scan(%quote(&project_id_lst),&i,%str(,));				/*i18NOK:LINE*/
			
			proc sql noprint;
				select project_sk into :project_sk
				from &lib_apdm..project_master
				where project_id = "&project_id.";
			quit;
	
	
	/*PURPOSE:Inserting purpose info into cprm_export_prj_data table */
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				  select &project_sk.,"PURPOSE",purpose_sk 								/*i18NOK:LINE*/
				  from &lib_apdm..project_master where project_sk= &project_sk.;
			quit;
			
	/*SUBJECT_GROUP:Inserting subject_group info into cprm_export_prj_data table */
		
			%let SBJCT_GRP_SK=;
			proc sql noprint;
				select subject_group_sk into :SBJCT_GRP_SK
				from &lib_apdm..project_master where project_sk= &project_sk.;
			quit;
			%if &SBJCT_GRP_SK ne . %then %do;
				proc sql noprint;
					insert into &export_specs_ds_lib..&export_project_ds_nm. 
					  select &project_sk.,"SBJCT_GRP",subject_group_sk 					/*i18NOK:LINE*/
					  from &lib_apdm..project_master where project_sk= &project_sk.;
				quit;
			%end;
			
	/*SUBSET_MAP:Inserting subset_map and its dependent objects info into cprm_export_prj_data table */
			
			
			/* Following macro will add all the dependent entities required for a subset map to be imported */
			%dabt_cprm_get_subset_map_spec (export_specs_ds_lib=&export_specs_ds_lib.,entity_sk=&project_sk.,export_ds_nm=&export_project_ds_nm.,entity_type_cd=PRJ);
			
			/** Inserting subset map information using subject Group**/
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				  select &project_sk.,"SUBSET_MAP", subset_from_path_sk 				/*i18NOK:LINE*/
				  from &lib_apdm..subject_group_master sub_grp_master 
					inner join &lib_apdm..target_query_master tgt_query_master
						on (sub_grp_master.target_query_sk= tgt_query_master.target_query_sk)
				  where subject_group_sk= &SBJCT_GRP_SK.;
			quit;
						
	/*DATA SOURCES:Inserting datasource info into cprm_export_prj_data table */	
		
			
			/**Inserting data sources associated with variables**/
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"DATASOURCE", src_tab_master.source_table_sk		/*i18NOK:LINE*/
				from &lib_apdm..modeling_abt_master mdl_abt_master  inner join 
				&lib_apdm..modeling_abt_x_variable mdl_abt_x_var on (mdl_abt_master.abt_sk=mdl_abt_x_var.abt_sk) inner join &lib_apdm..variable_master var_master on 
				(mdl_abt_x_var.variable_sk=var_master.variable_sk) inner join &lib_apdm..source_table_master src_tab_master on (var_master.source_table_sk=src_tab_master.source_table_sk)
				where mdl_abt_master.project_sk= &project_sk.;
			quit;
			
			/**Inserting data sources associated with levels corresponding to the project**/
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"DATASOURCE", lvl_master.master_source_table_sk		/*i18NOK:LINE*/
				from &lib_apdm..project_master prj_master inner join &lib_apdm..level_master lvl_master on (prj_master.level_sk= lvl_master.level_sk) 
				where project_sk= &project_sk.;
			quit;	
			
			
			
			/**Inserting data sources associated with associated levels of the levels corresponding to the project**/
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"DATASOURCE", lvl_assoc.mapping_source_table_sk		/*i18NOK:LINE*/
				from &lib_apdm..project_master prj_master inner join &lib_apdm..level_master lvl_master on (prj_master.level_sk= lvl_master.level_sk) inner join 
				&lib_apdm..level_assoc lvl_assoc on (lvl_assoc.level_sk=lvl_master.level_sk) 
				where project_sk= &project_sk.;
			quit;	
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"DATASOURCE", lvl_master.master_source_table_sk		/*i18NOK:LINE*/
				from &lib_apdm..project_master prj_master inner join &lib_apdm..level_master lvl_master on (prj_master.level_sk= lvl_master.level_sk) inner join 
				&lib_apdm..level_assoc lvl_assoc on (lvl_assoc.level_sk=lvl_master.level_sk) 
				where project_sk= &project_sk.;
			quit;	

		
			
	/*LEVEL:Inserting subject of analysis info into cprm_export_prj_data table */	
		
			/**Inserting levels associated with the data sources**/
			
			proc sql noprint;
				select entity_sk into :datasource_lst separated by ","
				from &export_specs_ds_lib..&export_project_ds_nm. in_ds
				where kupcase(ktrim(in_ds.entity_type_cd)) = "DATASOURCE";				/*i18NOK:LINE*/
			quit;
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"SOA", level_sk 									/*i18NOK:LINE*/
				from &lib_apdm..source_table_x_level src_tab_x_level 
				where src_tab_x_level.source_table_sk in ( &datasource_lst.);
			quit;
			
		
			/**Inserting levels associated with the project**/
		
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				  select &project_sk.,"SOA",level_sk 									/*i18NOK:LINE*/
				  from &lib_apdm..project_master where project_sk= &project_sk.;
			quit;
		
			/**Inserting levels associated with levels of the project**/
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"SOA", assoc_level_sk								/*i18NOK:LINE*/
				from &lib_apdm..project_master prj_master inner join &lib_apdm..level_master lvl_master on (prj_master.level_sk= lvl_master.level_sk) inner join 
				&lib_apdm..level_assoc lvl_assoc on (lvl_assoc.level_sk=lvl_master.level_sk) 
				where project_sk= &project_sk.;
			quit;
		
			/**Inserting levels associated with subject_group of the project**/
			

			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"SOA", sub_grp_spec_dtl.level_sk					/*i18NOK:LINE*/
				from &lib_apdm..project_master prj_master inner join &lib_apdm..Subject_group_master sub_grp_master on (prj_master.subject_group_sk= sub_grp_master.subject_group_sk) inner join 
				&lib_apdm..Subject_group_spcfcn_dtl sub_grp_spec_dtl on (sub_grp_master.subject_group_sk=sub_grp_spec_dtl.subject_group_sk)
				where project_sk= &project_sk.;
			quit;	
			
			
			
			
			
			
	/*DATA SOURCES:Inserting data sources info into cprm_export_prj_data table */			
		/**Inserting data sources associated with all the levels associated with the project derived above **/
		
		
		%let level_sk_lst=;
			proc sql noprint;
				select distinct(entity_sk) into :level_sk_lst separated by ","
				from &export_specs_ds_lib..&export_project_ds_nm. in_ds
				where kupcase(ktrim(in_ds.entity_type_cd)) = "SOA";					/*i18NOK:LINE*/
			quit;
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"DATASOURCE", lvl_master.master_source_table_sk	/*i18NOK:LINE*/
				from  &lib_apdm..level_master lvl_master  
				where lvl_master.level_sk in ( &level_sk_lst.);
			quit;
			
					
			
		/*LIBRARY:Inserting library info into cprm_export_prj_data table */
			
			proc sql noprint;
				select entity_sk into :datasource_lst separated by ","
				from &export_specs_ds_lib..&export_project_ds_nm. in_ds
				where kupcase(ktrim(in_ds.entity_type_cd)) = "DATASOURCE";			/*i18NOK:LINE*/
			quit;
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"LIBRARY", library_sk 							/*i18NOK:LINE*/
				from &lib_apdm..source_table_master src_tab_master 
				where src_tab_master.source_table_sk in ( &datasource_lst.);
			quit;	
		/* TIME PERIOD:Inserting Time period info into cprm_export_prj_data table */	
		
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"TIME_PRD", beh_var.time_period_sk				/*i18NOK:LINE*/
				from &lib_apdm..modeling_abt_master mdl_abt_master  inner join 
				&lib_apdm..modeling_abt_x_variable mdl_abt_x_var on (mdl_abt_master.abt_sk=mdl_abt_x_var.abt_sk) inner join &lib_apdm..variable_master var_master on 
				(mdl_abt_x_var.variable_sk=var_master.variable_sk) inner join &lib_apdm..behavioral_variable beh_var on (var_master.variable_sk=beh_var.variable_sk)
				where mdl_abt_master.project_sk= &project_sk.;
			quit;
			
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"TIME_PRD", recnt_var.time_period_sk			/*i18NOK:LINE*/
				from &lib_apdm..modeling_abt_master mdl_abt_master  inner join 
				&lib_apdm..modeling_abt_x_variable mdl_abt_x_var on (mdl_abt_master.abt_sk=mdl_abt_x_var.abt_sk) inner join &lib_apdm..variable_master var_master on 
				(mdl_abt_x_var.variable_sk=var_master.variable_sk) inner join &lib_apdm..Recent_variable recnt_var on (var_master.variable_sk=recnt_var.variable_sk)
				where mdl_abt_master.project_sk= &project_sk.;
			quit;
		
		/*AS OF TIME: Inserting Time point (As of time) info into cprm_export_prj_data table */	
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"AS_OF_TIME", sup_var.as_of_time_sk				/*i18NOK:LINE*/
				from &lib_apdm..modeling_abt_master mdl_abt_master  inner join 
				&lib_apdm..modeling_abt_x_variable mdl_abt_x_var on (mdl_abt_master.abt_sk=mdl_abt_x_var.abt_sk) inner join &lib_apdm..variable_master var_master on 
				(mdl_abt_x_var.variable_sk=var_master.variable_sk) inner join &lib_apdm..supplementary_variable sup_var on (var_master.variable_sk=sup_var.variable_sk)
				where mdl_abt_master.project_sk= &project_sk.;
			quit;
		
		
		/*EXTERNAL CODE:Inserting External code info into cprm_export_prj_data table */	
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm. 
				select &project_sk.,"EXT_CODE", ext_cd_master.external_code_sk		/*i18NOK:LINE*/
				from &lib_apdm..modeling_abt_master mdl_abt_master  inner join 
				&lib_apdm..modeling_abt_x_variable mdl_abt_x_var on (mdl_abt_master.abt_sk=mdl_abt_x_var.abt_sk) inner join &lib_apdm..variable_master var_master on 
				(mdl_abt_x_var.variable_sk=var_master.variable_sk) inner join &lib_apdm..external_variable  ext_var on (ext_var.variable_sk=var_master.variable_sk) 
				inner join &lib_apdm..external_variable_master ext_var_master on (ext_var_master.external_variable_sk=ext_var.external_variable_sk) inner join &lib_apdm..external_code_master ext_cd_master
				on (ext_cd_master.external_code_sk=ext_var_master.external_code_sk) 
				where mdl_abt_master.project_sk= &project_sk.;
			quit;
			
			
		
			
		/*PROJECT:Inserting project info into cprm_export_prj_data table */
			proc sql noprint;
				insert into &export_specs_ds_lib..&export_project_ds_nm.( project_sk,entity_type_cd,entity_sk)
				values( &project_sk.,"PROJECT",&project_sk. );						/*i18NOK:LINE*/
			quit;
		
		%end;
	
	
	/* End of loop to create individual project related export specifications .*/
	
	
	%let m_purpose_lst=;
	%let m_sub_grp_lst=;
	%let m_sub_map_lst=;
	%let m_table_lst=;
	%let m_soa_lst=;
	%let m_lib_lst=;
	%let m_time_prd_lst=;
	%let m_as_of_time_lst=;
	%let m_ext_code_lst=;
	%let m_proj_lst=;
	
	proc sql noprint;
		select distinct (entity_sk) into :m_purpose_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "PURPOSE";						/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_sub_grp_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "SBJCT_GRP";					/*i18NOK:LINE*/
	quit;
		
	proc sql noprint;
		select distinct (entity_sk) into :m_sub_map_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "SUBSET_MAP";					/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_table_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "DATASOURCE";					/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_soa_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "SOA";							/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_lib_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "LIBRARY";						/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_time_prd_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "TIME_PRD";						/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_as_of_time_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "AS_OF_TIME";					/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_ext_code_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "EXT_CODE";						/*i18NOK:LINE*/
	quit;
	proc sql noprint;
		select distinct (entity_sk) into :m_proj_lst separated by ","
		from &export_specs_ds_lib..&export_project_ds_nm. in_ds
		where upcase(ktrim(in_ds.entity_type_cd)) = "PROJECT";						/*i18NOK:LINE*/
	quit;
			
		
	
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
	
			%if &m_purpose_lst ne %then %do;
				%dabt_cprm_export_purpose_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., purpose_sk_lst=%quote(&m_purpose_lst. ));
			%end;
			
			%if &m_sub_grp_lst ne %then %do;
				%dabt_cprm_export_sbj_grp_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., subject_group_sk_lst=%quote(&m_sub_grp_lst.));
			%end;
			
			%if &m_sub_map_lst ne %then %do;
				%dabt_cprm_export_sbstmp_spec(	export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_sbstmp_sk_lst=%quote(&m_sub_map_lst.)); 
			%end;
			
			%if &m_table_lst ne %then %do;
				%dabt_cprm_export_table_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., table_sk_lst=%quote(&m_table_lst.));
			%end;
			
			%if &m_soa_lst ne %then %do;
				%dabt_cprm_export_soa_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., level_sk_lst=%quote(&m_soa_lst.));
			%end;
			
			%if &m_lib_lst ne %then %do;
				%dabt_cprm_export_library_spec(export_specs_ds_lib = &export_specs_ds_lib., export_specs_ds_nm = &export_specs_ds_nm., m_lib_sk_lst=%quote(&m_lib_lst. ) );
			%end;
			
			
			%if &m_time_prd_lst ne %then %do;
				%dabt_cprm_exprt_time_period_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., time_period_sk_lst=%quote(&m_time_prd_lst.) );
			%end;
			
			%if &m_as_of_time_lst ne %then %do;
				%dabt_cprm_exprt_as_of_time_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., as_of_time_sk_lst=%quote(&m_as_of_time_lst.) );
			%end;
			
						
			%if &m_ext_code_lst ne %then %do;
				%dabt_cprm_export_ext_code_spec(export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm = &export_specs_ds_nm., m_ext_cd_sk_lst=%quote(&m_ext_code_lst. ));
			%end;
			
				
			%if &m_proj_lst ne %then %do;
				%dabt_cprm_export_project_spec(	export_specs_ds_lib=&export_specs_ds_lib., export_specs_ds_nm  =&export_specs_ds_nm., m_prj_sk_lst=%quote(&m_proj_lst.));
			%end;
			
	
	
	%if &syscc. > 4 %then %do;
		/* Some issue occured while creationn of export specifcation table */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_SPEC_DS_NOT_CREATED, noquote));
/* 		%if &log_divert_flg. ne N %then %do; */
/* 			proc printto; */
/* 			run; */
/* 		%end;	 */
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
		
		*filename exp_file "&export_spec_file_path./&export_spec_file_nm..csv" &dabt_csv_export_encoding_option;						/*i18NOK:LINE*/
		
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
/* 			%if &log_divert_flg. ne N %then %do; */
/* 				proc printto; */
/* 				run; */
/* 			%end; */
			%let syscc=99;
			%return;
		%end;
	%if &DABT_DEBUG_FLG. eq Y %then %do;
		filename cportout filesrvc folderpath="&export_spec_file_path./" filename= "debug_cprm_create_prj_exp_spec_&export_specs_ds_lib._lib.xpt"  debug=http; /* i18nOK:Line */
		proc cport library = &export_specs_ds_lib. file=cportout memtype=data; /* i18NOK:LINE */
		run;
			
	 	filename cportout clear;
	%end;
	/* Cleaning up scratch data sets */
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_specs_ds_nm.);
	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_project_ds_nm.);

	
/* %if &log_divert_flg. ne N %then %do; */
/* 	proc printto; */
/* 	run; */
/* %end; */

%mend dabt_cprm_create_prj_exp_spec;
