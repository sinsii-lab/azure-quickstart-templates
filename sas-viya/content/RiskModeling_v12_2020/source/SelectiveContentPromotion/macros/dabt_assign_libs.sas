   /* This macro assings the library for ADM, log path and temporary datasets */
   
	/*Note By: sinsls*/
   /*Note: m_project_sk is removed from this macro as parameter. Till CS 6.3, each and every folder were creating in project_id folder which is being derived from project_sk*/
   /*As of now, i've corrected it's regression impact for m_workspace_type=(PRJ/MDL). For other will correct when we will work on it. */
   
%macro dabt_assign_libs(tmp_lib=,src_lib=, log_path= ,tmp_file_path=dummy, 
					   m_workspace_type=PRJ, m_called_for_abt_processing_flg=N,
					   m_abt_or_template_sk=, m_crt_by_usr=,
					   stg_lib=, scr_path=, query_file_path=, stg_path=, scoring_code_path=, m_deploy_flg=N,
						m_abt_tbl_nm=, deployed_code_path=, m_related_path=, 
						m_called_for_audit_rpt=N, report_path=,m_scr_control_dtl_sk=,
						import_analysis_report_path =, m_cprm_src_apdm_lib=, m_cprm_ctl_lib  = , m_report_spec_sk=
						, m_job_sk=,
						dep_code_fs_path=,dep_scr_fs_path=,build_scr_fs_path=,
						entity_type_sk = , job_fs_path=
						);

	/** Dummy value is assigned as default for tmp_file_path. This is to take care of existing call to this 
		macro which might not be passing htis parameter **/

	/* m_workspace_type is assigned default value of PRJ to avoid making changes in calls in Java UI code 
		where they are already calling this macro for Project workspace.*/

	/* project_sk has significance only in case of PRJ workspace and modeling ABT build. 
		In case of SCM workspace, this has value of model_sk. Else it can remain null*/

	/* 
	For MVA macros, called from JAVA UI, m_called_for_abt_processing_flg=N. It will mean log and scratch will go in 
	application folder

	m_called_for_abt_processing_flg is set to Y when macro is called for ABT related processing.
	Hence log of ABT related wrapper macros won't go under Application folder.
	It is applicable for PRJ and SCR workspace. Not applicable for ADM workspace.

	Typical macros which call it with m_called_for_abt_processing_flg=Y are
		%dabt_build_act_abt_wrapper
		%dabt_build_mdl_abt_wrapper
		%dabt_build_scr_abt_wrapper
		%dabt_export_job_wrapper
	*/

	/*
	m_abt_or_template_sk has significance only in case m_called_for_abt_processing_flg=Y.
	In that case, it decides,instead of application folder, log and scratch will go in which folder.
	In case m_workspace_type= PRJ, it will contain modeling abt sk
	In case of workspace type SCR/ACT it will contain scoring template sk
	In case of workspace type PLG/BIN/CHRSTC it will contain model sk
	*/
	/* 
	dep_code_fs_path = This variable will contain content server path under <project_id> folder where all deployed codes will be present.
	dep_scr_fs_path = This variable will contain content server path under <job_sk> folder where all logs of deploy ABT  will be present.
	build_scr_fs_path = This variable will contain content server path under <job_sk> folder where all logs of Build ABT  will be present.
	*/
	%let dabt_apdm_lib_ref = &lib_apdm;
	%let &src_lib = &dabt_apdm_lib_ref;
	


	%let m_deploy_flg = %kupcase(&m_deploy_flg);
	%let m_workspace_type = %kupcase(&m_workspace_type);
	%let m_abt_ds_nm_asgn = &m_abt_tbl_nm;
	
	%if &m_workspace_type = MDL %then %do;
		%let m_workspace_type = PRJ; /* i18nOK:Line */
	%end;
	
	%if &m_workspace_type ne ADM and  &m_workspace_type ne GEN_DEV_STATS and &m_workspace_type ne CPRM_IMP %then %do;
		/*To extract entity_type_sk from entity_type_master based on ABT type*/
		proc sql noprint;
			select entity_type_sk 
				into:m_entity_type_sk
			from 
				&dabt_apdm_lib_ref..entity_type_master
			where 1 = 1	
				%if &m_workspace_type eq PRJ %then %do;
					and %kupcase(entity_type_cd) = %kupcase("PROJECT")  /* i18nOK:Line */
				%end;
				%else %if &m_workspace_type eq BCK %then %do;	/* RM81 : For BCK */
					and %kupcase(entity_type_cd) = %kupcase("RPT_SPEC")  /* i18nOK:Line */
				%end;
				%else %if &m_workspace_type eq SCR %then %do;	/* RM81 : For BCK */
					and %kupcase(entity_type_cd) = %kupcase("MODEL")  /* i18nOK:Line */
				%end;
			/*....For other m_workspace_type ADD condition here...*/
			;
		quit;
		
		%let &entity_type_sk = &m_entity_type_sk.;
		/*%let m_related_path_dabt_tmp = &m_related_path;*/ /* Need to remove this variable as we will move ahead with more m_workspace_type.Currently handled for PRJ/MDL/ADM*/
		%if &m_workspace_type ne BCK %then %do;
			%let m_rel_path =;
			%let wokr_lib_path = %sysfunc(pathname(work)); 
			%dabt_make_work_area(dir=&wokr_lib_path., create_dir=&m_job_sk., path=m_rel_path);
			%let m_job_sk_folder_path = &m_rel_path.;
		%end;
	%end;/* m_workspace_type ne ADM condition ends here*/	
	
	
	/* Assign dummy macro variable value, if not passed via macro*/
	%if &m_workspace_type ne CHRSTC and &deployed_code_path eq %then %do;
		%let deployed_code_path = m_deployed_code_path;
	%end;
	
	%if &m_workspace_type = PRJ %then %do;  /* i18nOK:Line */
		
		%local m_project_sk;
		proc sql noprint;
			select
			project_sk into :m_project_sk
			from &dabt_apdm_lib_ref..modeling_abt_master
			where abt_sk = &m_abt_or_template_sk.;
		quit;
		%let m_project_sk = &m_project_sk.;
	
		%if &m_deploy_flg = Y %then %do;
			/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
			%dabt_make_work_area(dir=&m_job_sk_folder_path., create_dir=deploy_modeling_abt/scratch, path=&scr_path);

			/*% to create create_dir folder below dir. Then to stg_path get assigned vaalue of dir/create_dir path*/
			%dabt_make_work_area(dir=&m_job_sk_folder_path., create_dir=deploy_modeling_abt/scratch/stg, path=&stg_path);

			/*Creating File content server path macro variables*/
			%let m_deploy_filesrv_path = %str(&m_file_srvr_job_folder_path/Deploy Data Set Building Jobs/Modeling Data Set); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
			%if &dep_scr_fs_path ne %then %do;
				%let &dep_scr_fs_path = &m_deploy_filesrv_path;
			%end;
		%end;/*m_deploy_flg condition ends here*/ 
		%else %do;
			%if &m_abt_ds_nm_asgn eq %then %do;
				%let m_abt_ds_nm_asgn = ;
				proc sql noprint;
					select abt_table_nm into :m_abt_ds_nm_asgn
					from &dabt_apdm_lib_ref..modeling_abt_master (where = (abt_sk=&m_abt_or_template_sk.));
				quit;
				%let m_abt_ds_nm_asgn = &m_abt_ds_nm_asgn.;
			%end;

			/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
			%dabt_make_work_area(dir=&m_job_sk_folder_path., create_dir=build_modeling_abt/&m_abt_ds_nm_asgn./scratch, path=&scr_path);

			/*% to create create_dir folder below dir. Then to stg_path get assigned vaalue of dir/create_dir path*/
			%dabt_make_work_area(dir=&m_job_sk_folder_path., create_dir=build_modeling_abt/&m_abt_ds_nm_asgn./scratch/stg, path=&stg_path);

			%let path_of_log =;
			%dabt_make_work_area(dir=&m_job_sk_folder_path., create_dir=build_modeling_abt/&m_abt_ds_nm_asgn./log, path=path_of_log);
			%let &log_path = &path_of_log/;
			/*% &log_path get assigned dir/create_dir path*/
			
			/*Creating File content server path macro variables*/
			
			%let m_build_filesrv_path = %str(&m_file_srvr_job_folder_path/Build Data Set/Modeling Data Set); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
			%if "&dep_scr_fs_path" ne " " %then %do;
				%let &dep_scr_fs_path = &m_build_filesrv_path.;
			%end;
			
		%end;/*Build Flag condition ends here*/
			/* &wrk_area_lib means folder which will conatin temp tables other than staging tables. */
			libname tmp_lib "&&&scr_path."; /*i18NOK:LINE*/
			libname stg_lib "&&&stg_path."; /*i18NOK:LINE*/

			%let &stg_lib = stg_lib;
			%let &tmp_lib = tmp_lib;


		%if &m_called_for_audit_rpt = Y %then %do;
				%let path_of_report =;
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=build_modeling_abt/audit_report, path=path_of_report);
				%let &report_path = &path_of_report/;
		%end;
		
		/*Common File Content server path required for both Deploy and Build ABT*/
		
		%let m_dep_code_filesrv_path = &m_file_srvr_prj_folder_path/&m_project_sk/&m_file_srvr_depl_code_folder_nm; /*Will be returned to calling macro*/ /*i18NOK:LINE*/
		%if "&dep_code_fs_path" ne " " %then %do;
			%let &dep_code_fs_path = &m_dep_code_filesrv_path.;
		%end;
		
		
	%end;/*&m_workspace_type = PRJ */

	%else %if &m_workspace_type = SCR %then %do; /* i18nOK:Line */
	
		%local m_model_sk;
		proc sql noprint;
			select
			model_sk into :m_model_sk
			from &dabt_apdm_lib_ref..scoring_model
			where scoring_template_sk = &m_abt_or_template_sk.;
		quit;
		%let m_model_sk = &m_model_sk.;
	
		%if &m_called_for_abt_processing_flg = Y %then %do;
			%if &m_deploy_flg = Y %then %do;				
				%dabt_make_work_area(dir=&m_job_sk_folder_path., create_dir=scoring_run/deploy_scoring_job/scratch, path=&scr_path);

				/*% to create create_dir folder below dir. Then to stg_path get assigned value of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=scoring_run/deploy_scoring_job/scratch/stg, path=&stg_path);

				/*Creating File content server path macro variables*/
				%let m_deploy_filesrv_path = %str(&m_file_srvr_job_folder_path/Deploy Data Set Building Jobs/Scoring Data Set); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
				%if &dep_scr_fs_path ne %then %do;
					%let &dep_scr_fs_path = &m_deploy_filesrv_path;
				%end;
				
			%end; /* m_deploy_flg condition ends here */ 
			%else %do;	
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=scoring_run/build_scoring_job/scratch, path=&scr_path);

				/*% to create create_dir folder below dir. Then to stg_path get assigned value of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=scoring_run/build_scoring_job/scratch/stg, path=&stg_path);

				%let path_of_log =;
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=scoring_run/build_scoring_job/log, path=path_of_log);
				%let &log_path = &path_of_log/;
				
				/*Creating File content server path macro variables*/			
				%let m_build_filesrv_path = %str(&m_file_srvr_job_folder_path/Build Data Set/Scoring Data Set); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
				%if "&dep_scr_fs_path" ne " " %then %do;
					%let &dep_scr_fs_path = &m_build_filesrv_path.;
				%end; /*Build Flag condition ends here*/
				
			%end;			

			/*% to create create_dir folder below dir. Then to deployed_code_path get assigned value of dir/create_dir path*/
			/*%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&export_code_folder_nm./scoring_deployed_code, path=&deployed_code_path);*/


			/* &wrk_area_lib means folder which will conatin temp tables other than staging tables. */
			libname tmp_lib "&&&scr_path."; /*i18NOK:LINE*/
			libname stg_lib "&&&stg_path."; /*i18NOK:LINE*/

			%let &stg_lib = stg_lib;
			%let &tmp_lib = tmp_lib;
		%end;/* &m_called_for_abt_processing_flg = Y*/
		%if &m_called_for_audit_rpt = Y %then %do;
				%let path_of_report =;
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=scoring_run/audit_report, path=path_of_report);
				%let &report_path = &path_of_report/;
		%end;
		
		/*Common File Content server path required for both Deploy and Build ABT*/		
		%let m_dep_code_filesrv_path = &m_file_srvr_mdl_folder_path/&m_model_sk./Scoring/&m_file_srvr_depl_code_folder_nm; /*Will be returned to calling macro*/ /*i18NOK:LINE*/
		%if "&dep_code_fs_path" ne " " %then %do;
			%let &dep_code_fs_path = &m_dep_code_filesrv_path.;
		%end;
		
	%end; /* &m_workspace_type = SCR */
 
	%else %if &m_workspace_type = ACT %then %do;   /* i18nOK:Line */
		
		%local m_model_sk;
		proc sql noprint;
			select
			model_sk into :m_model_sk
			from &dabt_apdm_lib_ref..scoring_model
			where scoring_template_sk = &m_abt_or_template_sk.;
		quit;
		%let m_model_sk = &m_model_sk.;				
		
		%if &m_called_for_abt_processing_flg = Y %then %do;
		
			%if &m_deploy_flg = Y %then %do;
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=actual_run/deploy_actual_calc_job/scratch, path=&scr_path);
				
				/*% to create create_dir folder below dir. Then to stg_path get assigned vaalue of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=actual_run/deploy_actual_calc_job/scratch/stg, path=&stg_path);

				/*Creating File content server path macro variables*/
				%let m_deploy_filesrv_path = %str(&m_file_srvr_job_folder_path/Deploy Data Set Building Jobs/Actual Data Set); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
				%if &dep_scr_fs_path ne %then %do;
					%let &dep_scr_fs_path = &m_deploy_filesrv_path;
				%end;
			%end; /* m_deploy_flg condition ends here */ 
			%else %do;
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=actual_run/build_actual_calc_job/scratch, path=&scr_path);
				
				%if &m_scr_control_dtl_sk ne %then %do;
					/*% to create create_dir folder below dir. Then to stg_path get assigned vaalue of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=actual_run/build_actual_calc_job/scratch/&m_scr_control_dtl_sk/stg, path=&stg_path);
				%end;

				%let path_of_log =;
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=actual_run/build_actual_calc_job/log, path=path_of_log);
				%let &log_path = &path_of_log./;
				
				/*Creating File content server path macro variables*/			
				%let m_build_filesrv_path = %str(&m_file_srvr_job_folder_path/Build Data Set/Actual Data Set); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
				%if "&dep_scr_fs_path" ne " " %then %do;
					%let &dep_scr_fs_path = &m_build_filesrv_path.;
				%end; /*Build Flag condition ends here*/					
			%end;

				/*% to create create_dir folder below dir. Then to deployed_code_path get assigned vaalue of dir/create_dir path*/
				/* %dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&export_code_folder_nm./actual_deployed_code, path=&deployed_code_path); */
				/*% to create create_dir folder below dir. Then to query_file_path get assigned vaalue of dir/create_dir path*/
				/* %dabt_make_work_area(dir=&&&stg_path., create_dir=query_files, path=&query_file_path); */
				
				
				/* &wrk_area_lib means folder which will conatin temp tables other than staging tables. */
				libname tmp_lib "&&&scr_path."; /*i18NOK:LINE*/
				libname stg_lib "&&&stg_path."; /*i18NOK:LINE*/

				%let &stg_lib = stg_lib;
				%let &tmp_lib = tmp_lib;
				
			%end;/* &m_called_for_abt_processing_flg = Y*/
			
		%if &m_called_for_audit_rpt = Y %then %do;
				%let path_of_report =;
				%dabt_make_work_area(dir=&m_job_sk_folder_path, create_dir=actual_run/audit_report, path=path_of_report);
				%let &report_path = &path_of_report/;
		%end;
		
		/*Common File Content server path required for both Deploy and Build ABT*/		
		%let m_dep_code_filesrv_path = &m_file_srvr_mdl_folder_path/&m_model_sk./Actual/&m_file_srvr_depl_code_folder_nm; /*Will be returned to calling macro*/ /*i18NOK:LINE*/
		%if "&dep_code_fs_path" ne " " %then %do;
			%let &dep_code_fs_path = &m_dep_code_filesrv_path.;
		%end;

	%end;/*&m_workspace_type = ACT*/
	/* Code added for CAC53 - start - by sinsur/27Jul2011 */

	/* Assigning tmp lib & log path for the  models for creating subset criteria in the model specification area */

	%else %if &m_workspace_type = SCM %then %do;  /* i18nOK:Line */
			
			/* In case of SCM, in m_project_sk actually the model_sk is passed. */
			%let m_project_id = ;
			%let m_model_id = ;
			proc sql noprint;
				select project_id, model_id into :m_project_id, :m_model_id
				from &dabt_apdm_lib_ref..model_master(where=(model_sk in (&m_project_sk.))) as mm
						inner join &dabt_apdm_lib_ref..project_master as pm
						on (mm.project_sk = pm.project_sk);
			quit;
			%let m_project_id = &m_project_id.;
			%let m_model_id = &m_model_id.;
			
			%if %quote(&m_related_path_dabt_tmp) eq %then %do;
				%let m_rel_path = ;
				/*% to create create_dir folder below dir. */
				%dabt_make_work_area(dir=&project_path., create_dir=&m_project_id./model/&m_model_id., path=m_rel_path);
				%let m_related_path_dabt_tmp = &project_path/&m_project_id./model/&m_model_id.;
			%end;
			
			/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
			%let scr_path = ;
			%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=scratch, path=scr_path);
			/* &wrk_area_lib means folder which will conatin temp tables other than staging tables. */
			libname tmp_lib "&scr_path."; /*i18NOK:LINE*/
			
			%let &tmp_file_path = &scr_path;
			
			%let &tmp_lib = tmp_lib;
			
			%if &log_path ne %then %do;
				%let path_of_log =;
				%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=log, path=path_of_log);
			
				%let &log_path = &path_of_log/;
			%end;
			%if &m_called_for_audit_rpt. = Y %then %do;
				%let &report_path = &path_of_log/;
			%end;
	%end;
	%else %if &m_workspace_type = BCK %then %do;  /* i18nOK:Line */
	
		%let m_project_id = ;
		%let m_model_id = ;
		
		proc sql noprint;
			select project_id, model_id into :m_project_id, :m_model_id
			from &dabt_apdm_lib_ref..model_master(where=(model_sk in (&m_abt_or_template_sk.))) as mm
					inner join &dabt_apdm_lib_ref..project_master as pm
					on (mm.project_sk = pm.project_sk);
		quit;
		%let m_project_id = &m_project_id.;
		%let m_model_id = &m_model_id.;
		
		/*Value is passsd from parameter and in case of backtsting it is job_sk folder path inside work library.*/
		
		%let m_related_path = &m_related_path.; 
		
		%let m_rpt_spec_sk_path = ;
		/* To create report specification folder inside job_sk which is created at physical path of work library */
		%dabt_make_work_area(dir=&m_related_path., create_dir=report_specification/&m_report_spec_sk., path=m_rpt_spec_sk_path);  /*i18NOK:LINE*/
		%let m_rpt_spec_sk_path = &m_rpt_spec_sk_path.;
		
		/*In case of BCK,m_abt_or_template_sk will have value of model_sk */
		
		%let m_mdl_sk_scr_fs_path = %str(&m_file_srvr_job_folder_path/&m_job_sk./Model/&m_abt_or_template_sk);  /*i18NOK:LINE*/ 
		%let m_rpt_spec_sk_scr_fs_path = %str(&m_mdl_sk_scr_fs_path/Report Specification/&m_report_spec_sk.);  /*i18NOK:LINE*/
		
		%if &m_called_for_abt_processing_flg = N %then %do;
			/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
			%dabt_make_work_area(dir=&m_rpt_spec_sk_path., create_dir=scoring_code, path=&scoring_code_path);

		%end;
		%else %do;
			%if &m_deploy_flg = Y %then %do;
				
				/*% to create create_dir folder below dir. Then to &scr_path get assigned value of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_rpt_spec_sk_path., create_dir=abt/deployed_job/scratch, path=&scr_path);

				/*% to create create_dir folder below dir. Then to stg_path get assigned value of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_rpt_spec_sk_path., create_dir=abt/deployed_job/scratch/stg, path=&stg_path);

				/* To get deployed code folder location from content server*/
				/*Creating File content server path macro variables*/
				
				%let m_deploy_bck_abt_scr_fs_path  = %str(&m_rpt_spec_sk_scr_fs_path/Deploy Data Set Building Jobs); /*i18NOK:LINE*/
				%let &dep_scr_fs_path = &m_deploy_bck_abt_scr_fs_path;
			%end;
			%else %do;
				%if &m_abt_ds_nm_asgn eq %then %do;
					%let m_abt_ds_nm_asgn = ;
					proc sql noprint;
						select abt_table_nm into :m_abt_ds_nm_asgn
						from &dabt_apdm_lib_ref..model_master(where=(model_sk in (&m_abt_or_template_sk.))) t1
								inner join &dabt_apdm_lib_ref..model_x_modeling_abt t2
								on (t1.model_sk = t2.model_sk)
								inner join &dabt_apdm_lib_ref..modeling_abt_master t3
								on (t2.abt_sk = t3.abt_sk)
						;
					quit;
					%let m_project_id = &m_project_id.;
					%let m_abt_ds_nm_asgn = &m_abt_ds_nm_asgn.;
				%end;

				/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_rpt_spec_sk_path., create_dir=abt/build_abt/&m_abt_ds_nm_asgn/scratch, path=&scr_path);

				/*% to create create_dir folder below dir. Then to stg_path get assigned vaalue of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_rpt_spec_sk_path., create_dir=abt/build_abt/&m_abt_ds_nm_asgn/scratch/stg, path=&stg_path);
				
				%let path_of_log =;
				%dabt_make_work_area(dir=&m_rpt_spec_sk_path., create_dir=abt/build_abt/&m_abt_ds_nm_asgn./log, path=path_of_log);
				%let &log_path = &path_of_log/;
				
				/*Creating File content server path macro variables*/
				
				%let m_build_bck_abt_scr_fs_path  = %str(&m_rpt_spec_sk_scr_fs_path./Build Data Set); /*i18NOK:LINE*/
				%let &dep_scr_fs_path = &m_build_bck_abt_scr_fs_path;
				
			%end;

				/* &wrk_area_lib means folder which will conatin temp tables other than staging tables. */
				libname tmp_lib "&&&scr_path."; /*i18NOK:LINE*/
				libname stg_lib "&&&stg_path."; /*i18NOK:LINE*/

				%let &stg_lib = stg_lib;
				%let &tmp_lib = tmp_lib;
		%end;
	
		%if &m_called_for_audit_rpt = Y %then %do;
				%let path_of_report =;
				%dabt_make_work_area(dir=&m_rpt_spec_sk_path, create_dir=abt/build_abt/audit_report, path=path_of_report);
				%let &report_path = &path_of_report/;
		%end;
		
		/*Common File Content server path required for both Deploy and Build ABT*/
		
		%let m_deploy_cd_fs_path = %str(&m_rpt_spec_sk_scr_fs_path./Deploy Data Set Building Jobs/Deployed Code);/*Will be returned to calling macro*/ /*i18NOK:LINE*/
		%let &dep_code_fs_path = &m_deploy_cd_fs_path.;
		
	%end;/*m_workspace_type=BCK*/
	

	/* Code added for CAC53 - end - by sinsur/27Jul2011 */
	%else %if &m_workspace_type = PLG or &m_workspace_type = BIN or &m_workspace_type = CHRSTC %then %do;
	
			%if &m_workspace_type = PLG %then %do;
				%let m_folder_nm = pooling;
			%end;
			%else %if &m_workspace_type = BIN %then %do;
				%let m_folder_nm = bin;
			%end;
			%else %if &m_workspace_type = CHRSTC %then %do;
				%let m_folder_nm = characteristics;
			%end;
			
			%let m_project_id = ;
			%let m_model_id = ;
			proc sql noprint;
				select project_id, model_id into :m_project_id, :m_model_id
				from &dabt_apdm_lib_ref..model_master(where=(model_sk in (&m_abt_or_template_sk.))) as mm
						inner join &dabt_apdm_lib_ref..project_master as pm
						on (mm.project_sk = pm.project_sk);
			quit;
			%let m_project_id = &m_project_id.;
			%let m_model_id = &m_model_id.;
			
			%if %quote(&m_related_path_dabt_tmp) eq %then %do;
				%let m_rel_path = ;
				/*% to create create_dir folder below dir. */
				%dabt_make_work_area(dir=&project_path., create_dir=&m_project_id./model/&m_model_id., path=m_rel_path);
				%let m_related_path_dabt_tmp = &project_path/&m_project_id./model/&m_model_id.;
			%end;
			
			%if &m_called_for_abt_processing_flg = N %then %do;

				/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
				%dabt_make_work_area(dir=&project_path., create_dir=&m_project_id./model/&m_model_id./scoring_code, path=&scoring_code_path);

				/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
				%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=scratch, path=&scr_path);
				/* &wrk_area_lib means folder which will conatin temp tables other than staging tables. */
				libname tmp_lib "&&&scr_path."; /*i18NOK:LINE*/
				
				%if &deployed_code_path ne %then %do;
					/*% to create create_dir folder below dir. Then to deployed_code_path get assigned vaalue of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/deployed_job/deployed_code, path=&deployed_code_path);
				%end;
					
				%let &tmp_file_path = &&&scr_path;
				
				%let &tmp_lib = tmp_lib;
				
				%if &log_path ne %then %do;
					%let path_of_log =;
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=log, path=path_of_log);
				
					%let &log_path = &path_of_log/;
				%end;
			%end;
			%else %do;
				%if &m_deploy_flg = Y %then %do;
					/*% to create create_dir folder below dir. Then to &scr_path get assigned value of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/deployed_job/scratch, path=&scr_path);

					/*% to create create_dir folder below dir. Then to stg_path get assigned value of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/deployed_job/scratch/stg, path=&stg_path);

					/*% to create create_dir folder below dir. Then to query_file_path get assigned vaalue of dir/create_dir path*/
					%dabt_make_work_area(dir=&&&stg_path., create_dir=query_files, path=&query_file_path);
					/*% to create create_dir folder below dir. Then to deployed_code_path get assigned vaalue of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/deployed_job/deployed_code, path=&deployed_code_path);

					%let path_of_log =;
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/deployed_job/log, path=path_of_log);
					%let &log_path = &path_of_log/;
					/*% &log_path get assigned dir/create_dir path*/
				%end;
				%else %do;
					%if &m_abt_ds_nm_asgn eq %then %do;
						%let m_abt_ds_nm_asgn = ;
						proc sql noprint;
							select abt_table_nm into :m_abt_ds_nm_asgn
							from &dabt_apdm_lib_ref..model_master(where=(model_sk in (&m_abt_or_template_sk.))) t1
									inner join &dabt_apdm_lib_ref..model_x_modeling_abt t2
									on (t1.model_sk = t2.model_sk)
									inner join &dabt_apdm_lib_ref..modeling_abt_master t3
									on (t2.abt_sk = t3.abt_sk)
							;
						quit;
						%let m_project_id = &m_project_id.;
						%let m_abt_ds_nm_asgn = &m_abt_ds_nm_asgn.;
					%end;

					/*% to create create_dir folder below dir. Then to &scr_path get assigned vaalue of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/&m_abt_ds_nm_asgn/scratch, path=&scr_path);


					/*% to create create_dir folder below dir. Then to stg_path get assigned vaalue of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/&m_abt_ds_nm_asgn/scratch/stg, path=&stg_path);
					
					/*% to create create_dir folder below dir. Then to deployed_code_path get assigned value of dir/create_dir path*/
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/deployed_job/deployed_code, path=&deployed_code_path);

					%let path_of_log =;
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp., create_dir=&m_folder_nm/&m_abt_ds_nm_asgn/log, path=path_of_log);
					%let &log_path = &path_of_log/;
					/*% &log_path get assigned dir/create_dir path*/
				%end;

				/* &wrk_area_lib means folder which will conatin temp tables other than staging tables. */
				libname tmp_lib "&&&scr_path."; /*i18NOK:LINE*/
				libname stg_lib "&&&stg_path."; /*i18NOK:LINE*/

				%let &stg_lib = stg_lib;
				%let &tmp_lib = tmp_lib;
			%end;/* &m_called_for_abt_processing_flg = Y*/
			%if &m_called_for_audit_rpt = Y %then %do;
					%let path_of_report =;
					%dabt_make_work_area(dir=&m_related_path_dabt_tmp, create_dir=&m_folder_nm/audit_report, path=path_of_report);
					%let &report_path = &path_of_report/;
			%end;
	%end;/*&m_workspace_type = CHRSTC or BIN or PLG */
	%else %if &m_workspace_type = ADM %then %do;
		
			%let &tmp_lib = WORK;
			%let path_of_log = %sysfunc(pathname(work)); 
						
			/* Assign a path if log_path is requested through the call. 
			If parameter is not used, do not assign. */
			%if &log_path ne %then %do;
				%let &log_path = &path_of_log./;
			%end;
			
			%if &job_fs_path ne %then %do;
				%let &job_fs_path = %str(&m_file_srvr_job_folder_path/&m_job_sk); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
			%end;
			
			/*Creating File content server path macro variables*/
			%let m_deploy_filesrv_path = %str(&m_file_srvr_job_folder_path); /*Will be returned to calling macro*/ /*i18NOK:LINE*/
			%if &dep_scr_fs_path ne %then %do;
				%let &dep_scr_fs_path = &m_file_srvr_job_folder_path.;
			%end;
	
	%end;
	%else %if &m_workspace_type = CPRM_IMP %then %do;
	
			%let path_of_cprm_scratch_lib =;
			%dabt_make_work_area(dir=&import_analysis_report_path., create_dir=scratch, path=path_of_cprm_scratch_lib);
			libname cprmpscr "&path_of_cprm_scratch_lib"; /*i18NOK:LINE*/
			%let &tmp_lib = cprmpscr;


			%let path_of_cprm_source_apdm_lib =; 
			%dabt_make_work_area(dir=&import_analysis_report_path/scratch, create_dir=source_apdm, path=path_of_cprm_source_apdm_lib);
			libname cprmapdm "&path_of_cprm_source_apdm_lib"; /*i18NOK:LINE*/
			%let &m_cprm_src_apdm_lib = cprmapdm;


			%let path_of_cprm_control_lib =; 
			%dabt_make_work_area(dir=&import_analysis_report_path., create_dir=control, path=path_of_cprm_control_lib);
			libname cprm_ctl "&path_of_cprm_control_lib"; /*i18NOK:LINE*/
			%let &m_cprm_ctl_lib = cprm_ctl;
			
			/*%if &log_path. ne %then %do;
				%let path_of_log =;
				%dabt_make_work_area(dir=&import_analysis_report_path., create_dir=logs, path=path_of_log);
				%let &log_path = &path_of_log./;
			%end;*/
	%end;
	%else %if &m_workspace_type = GEN_DEV_STATS %then %do;
		%let path_of_work_lib = %sysfunc(pathname(work)); 
		%let lcreate_status = ;
		%csbmva_create_dir_and_libref(	path = &path_of_work_lib., 
										dirname = model/&m_abt_or_template_sk./dev_stats/scratch, 
										libref= csbscr,status_var = lcreate_status
									);
		%if %quote(&lcreate_status) eq %then %do;
			%put ERROR: User does not have permission to create directory; /* I18NOK:LINE */
			%abort ABEND;
		%end;
		
		libname csdebug "%sysfunc(pathname(csbscr))"; /* I18NOK:LINE */
		
		%let &src_lib = csbscr;
		%let &tmp_lib = csdebug;
	%end;
	
%mend dabt_assign_libs;


