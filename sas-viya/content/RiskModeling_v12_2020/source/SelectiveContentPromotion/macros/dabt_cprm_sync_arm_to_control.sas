/*********************************************************************************************************************************************
dabt_cprm_sync_arm_to_control: This macro intends to sync arm to control tables for sites that want to perform scoring actual in prod env
								and Ongoing in dev environment.
								
Pre-requisites:
The model should be deployed where Ongoing is to be run.
The CAS data from RM_ARM and RM_SCR should be copied on source machine for models whose ongoing is to be performed.
While copying CAS tables from prod to source the tables should be renamed according to source model id.


Overall processing:
1)Create a table of all deployed models,version,scoring_template_sk,scoring_model_sk and purpose_sk from mm_report_specification deployed_flg='Y'

2)For each record in above table -Call dabt_get_scr_act_table_name to fetch scoring and actual table name for model. 

3)See if prdctd table present in rm_arm. If not load from sashdat. If not do nothing.If present
	Select distinct scoring_as_of_dttm from rm arm.predicted table
		For scorecard model check if scored abt is present in rm_scr caslib.
		Check if record with all three status(build_abt_status_sk,scoring_status_sk,load_arm_status_sk) as success is present in scoring_control_detail table.
		If yes do nothing else call dabt_update_adm_for_scr( with RUNNING).Call dabt_update_adm_for_scr APPLY SCR to update scored_abt_name in table
		Update build_abt_status_sk,scoring_status_sk and load_arm_status_sk set to 1. Set load_csmart_status_sk =0.
		Call Post action macro
		

4)For actual see if actual table present in rm_arm. If not load from sashdat. If not do nothing.If present
	Select distinct scoring_as_of_dttm ,actual_as_of_dttm from actual arm table
		For each scoring,actual date check if combination of scoring_control_sk and actual_as_of_dttm present in actual control table with both status(build_abt_status_sk ,load_arm_status_sk)as success.
		If present do nothing. If not
			Call dabt_update_adm_for_act( with RUNNING).Set build_abt_status_sk and load_arm_status_sk set to 1. Set load_csmart_status_sk =0.
******************************************************************************************************************************************************/

%macro dabt_cprm_sync_arm_to_control;

/*Start of step 1:Create a table of all deployed models,version,scoring_template_sk,scoring_model_sk and purpose_sk*/
proc sql noprint;
	create table deployed_mdl_list
		as select mm_rpt.model_sk,mm_rpt.report_specification_sk ,
		scr_mdl.scoring_template_sk,scr_mdl.scoring_model_sk,
		pm.purpose_sk
		from &lib_apdm..mm_report_specification mm_rpt
		inner join &lib_apdm..scoring_model scr_mdl
		on mm_rpt.model_sk=scr_mdl.model_sk
		inner join &lib_apdm..model_master mm
		on mm_rpt.model_sk=mm.model_sk
		inner join &lib_apdm..project_master pm
		on mm.project_sk=pm.project_sk
		where mm_rpt.deployed_flg='Y';		/*i18NOK:LINE*/
	quit;
	
proc sql noprint;
select count(*) into :mdl_cnt from deployed_mdl_list;	/*i18NOK:LINE*/	
quit;
/*End of step 1:Create a table of all deployed models,version,scoring_template_sk,scoring_model_sk and purpose_sk*/

%dabt_initiate_cas_session(cas_session_ref=prfrm_ong_dev);

%if &mdl_cnt gt 0 %then %do;

	%do mdl=1 %to &mdl_cnt;
		
		data _null_;
		obs=&mdl;
		set deployed_mdl_list point=obs;
		call symputx('m_model_sk',model_sk);	   /*i18NOK:LINE*/
		call symputx('m_report_spec_sk',report_specification_sk); 	/*i18NOK:LINE*/
		call symputx('m_scoring_template_sk',scoring_template_sk);	/*i18NOK:LINE*/
		call symputx('m_scoring_model_sk',scoring_model_sk);	/*i18NOK:LINE*/
		call symputx('m_purpose_sk',purpose_sk);	/*i18NOK:LINE*/
		stop;
		run;

		%let m_scoring_template_id=&m_scoring_template_sk;
		
		/*Start of logic to insert in scoring control detail*/
		%let m_mining_algorithm = ;     
   
		proc sql ;
				select model_mining_algorithm into :m_mining_algorithm from &lib_apdm..model_master
				where model_sk = &m_model_sk
				and kupcase(model_mining_algorithm) in (&DABT_VALID_SCRCRD_ALGRTHM_VAL.) ; /*i18NOK:LINE*/
		quit;
		
		%let m_mining_algorithm = &m_mining_algorithm.;
		
		/*Start of step 2:Call dabt_get_scr_act_table_name to fetch scoring and actual table name for model*/
		%let out_scr_ds_nm=;
		%let out_act_ds_nm=;
		%dabt_get_scr_act_table_name(m_scoring_model_sk = &m_scoring_model_sk., m_scr_arm_table_nm = out_scr_ds_nm, m_act_arm_table_nm = out_act_ds_nm);
		
		%let out_scr_ds_nm=&out_scr_ds_nm;
		%let out_act_ds_nm=&out_act_ds_nm;
		
		/*End of step 2:Call dabt_get_scr_act_table_name to fetch scoring and actual table name for model*/	
		
		/*Start of Step 3: For predicted arm tables*/
		
		/*-------------Check if predicted table exist---------*/
		%let prdctd_tableExist = %eval(%sysfunc(exist(&RM_ANALYTICAL_RSLT_MART_LIBREF..&out_scr_ds_nm., DATA)));
		%if &prdctd_tableExist. eq 0 %then %do;
				%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_SYNC_ARM1.1, noquote,&out_scr_ds_nm.,&m_model_sk.));
				%goto mdl_end;
		%end;
		
		/*CAS action to update model id in predicted table.
			Scenario:User copies ARM tables from target to source and rename them with source model idBut the model_sk column still has targetmodel_sk*/
		proc cas;
		table.update/
		set={
		{var="model_sk",value="&m_model_sk"}	/*i18NOK:LINE*/
		}
		table={
		caslib="&RM_ANALYTICAL_RSLT_MART_LIBREF",
		name="&out_scr_ds_nm."
		};
		quit;
		
		proc fedsql sessref=prfrm_ong_dev;
		create table &RM_ANALYTICAL_RSLT_MART_LIBREF..DSTNCT_SCR_DT {options replace=true}
		as select distinct scoring_as_of_dttm from 
		&RM_ANALYTICAL_RSLT_MART_LIBREF..&out_scr_ds_nm.;
		quit;
		
		
		data DSTNCT_SCR_DT;
		set &RM_ANALYTICAL_RSLT_MART_LIBREF..DSTNCT_SCR_DT;
		run;
		
		proc sql noprint;
		select count(*) into :m_cnt_scr from DSTNCT_SCR_DT;		/*i18NOK:LINE*/
		quit;
		
		/*For each distinct scoring date in RM_ARM predicted table for a model 
		1)For scorecard model check if scored abt is present in rm_scr caslib.
		2)Check if record with all three status(build_abt_status_sk,scoring_status_sk,load_arm_status_sk) as success is present in scoring_control_detail table.
		3)If yes do nothing else call dabt_update_adm_for_scr( with RUNNING).Call dabt_update_adm_for_scr APPLY SCR to update scored_abt_name in table
		4)Update build_abt_status_sk,scoring_status_sk and load_arm_status_sk set to 1. Set load_csmart_status_sk =0.
		5)Call Post action macro
		*/
		%if &m_cnt_scr gt 0 %then %do;
			%do scr_dt=1 %to &m_cnt_scr;
				data _null_;
				obs=&scr_dt;
				set DSTNCT_SCR_DT point=obs;
				call symputx('m_scoring_dt',scoring_as_of_dttm);	/*i18NOK:LINE*/
				stop;
				run;
				
				
				%let m_scrd_abt_lib = ;
				%let m_scrd_abt_tbl_nm = ;
				/* Obtain the scored abt name */
				%dabt_intrfc_get_scrd_abt_info(m_abt_type=SCR,m_model_id=&m_model_sk.,m_scoring_template_id=&m_scoring_template_sk, m_scoring_dttm=&m_scoring_dt.,
												m_out_lib_ref=m_scrd_abt_lib,m_out_tbl_nm=m_scrd_abt_tbl_nm);
				%let m_scrd_abt_lib = &m_scrd_abt_lib;
				%let m_scrd_abt_tbl_nm = %sysfunc(kupcase(&m_scrd_abt_tbl_nm));
				
				
				/*-------------Check if scored abt table exist---------*/
				%let scr_tableExist = %eval(%sysfunc(exist(&DABT_SCORING_ABT_LIBREF..&m_scrd_abt_tbl_nm., DATA)));
				%if &scr_tableExist. eq 0 %then %do;
						%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_SYNC_ARM2.1, noquote,&m_scrd_abt_tbl_nm.,&m_model_sk.));
						%goto mdl_end;
				%end;
			
				
				/*Check if record with all three status_sk as success is already present in scoring control detail*/
				proc sql noprint;
				select count(*) into :prev_scr_cnt from &lib_apdm..scoring_control_detail where scoring_template_sk=&m_scoring_template_sk and	/*i18NOK:LINE*/
				scoring_as_of_dttm = &m_scoring_dt. and build_abt_status_sk=1 and scoring_status_sk=1 and load_arm_status_sk=1;
				quit;
				
				%if &prev_scr_cnt eq 0 %then %do;
				
					%let m_scr_abt_bld_status = ;

					/*Call dabt_update_adm_for_scr to insert in scoring control detail table*/
					%dabt_update_adm_for_scr(m_scoring_template_sk = &m_scoring_template_sk, m_scoring_bld_dttm = &m_scoring_dt, 
												m_scr_sts = &m_scr_abt_bld_status., m_scoring_process_cd = BUILD_ABT);
					/*Call again for process code=APPLY_SCR to update scored abt name*/							
					%dabt_update_adm_for_scr(m_scoring_template_sk=&m_scoring_template_sk, m_scoring_bld_dttm=&m_scoring_dt, 
												m_scr_sts=&m_scr_abt_bld_status., m_scoring_process_cd=APPLY_SCR);
									
					proc sql;
						update &lib_apdm..scoring_control_detail set build_abt_status_sk=1 ,
						scoring_status_sk=1,load_arm_status_sk=1,load_csmart_status_sk=0
						where scoring_template_sk=&m_scoring_template_sk and scoring_as_of_dttm = &m_scoring_dt.;
					quit;
					
					%let post_action_err_flg=;
					%let m_src_lib=apdm;
					%let m_scoring_as_of_dt=&m_scoring_dt.;/*m_scoring_as_of_dt was passed to post action macro*/
					%dabt_call_post_action_macro(m_action_cd=SCR_JOB, m_purpose_sk=&m_purpose_sk,
													m_scoring_template_sk=&m_scoring_template_sk, m_post_action_flg=post_action_err_flg);
					%if &post_action_err_flg = Y %then %do;
						%let job_rc = 1012;
					%end;
					
				%end;
												
			%end;
		%end;
		
		/*End of Step 3: For predicted arm tables*/
		
		
		/*Start of Step 4: For actual arm tables*/
		
		/*-------------Check if actual table exist---------*/
		%let actual_tableExist = %eval(%sysfunc(exist(&RM_ANALYTICAL_RSLT_MART_LIBREF..&out_act_ds_nm., DATA)));
		%if &actual_tableExist. eq 0 %then %do;
				%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_SYNC_ARM3.1, noquote,&out_act_ds_nm.,&m_model_sk.));
				%goto mdl_end;
		%end;
		
		/*CAS action to update model id in predicted table.
			Scenario:User copies ARM tables from target to source and rename them with source model idBut the model_sk column still has targetmodel_sk*/
		proc cas;
		table.update/
		set={
		{var="model_sk",value="&m_model_sk"}		/*i18NOK:LINE*/
		}
		table={
		caslib="&RM_ANALYTICAL_RSLT_MART_LIBREF",
		name="&out_act_ds_nm."
		};
		quit;
		
		/*TODO:Save this table again*/
		
		proc fedsql sessref=prfrm_ong_dev;
		create table &RM_ANALYTICAL_RSLT_MART_LIBREF..DSTNCT_SCR_ACT_DT {options replace=true}
		as select distinct scoring_as_of_dttm,actual_as_of_dttm from 
		&RM_ANALYTICAL_RSLT_MART_LIBREF..&out_act_ds_nm.;
		quit;
		
		data DSTNCT_SCR_ACT_DT;
		set &RM_ANALYTICAL_RSLT_MART_LIBREF..DSTNCT_SCR_ACT_DT;
		run;
		
		proc sql noprint;
		select count(*) into :m_cnt_scr_act from DSTNCT_SCR_ACT_DT;		/*i18NOK:LINE*/
		quit;
		
		/*For each distinct scoring,actual date in RM_ARM actaul table:
		1)Check if combination of scoring_control_sk and actual_as_of_dttm present in actual control table with both status(build_abt_status_sk ,load_arm_status_sk)as success.
		2)If present do nothing. If not	Call dabt_update_adm_for_act( with RUNNING).
		3)Set build_abt_status_sk and load_arm_status_sk set to 1. Set load_csmart_status_sk =0.*/
		%if &m_cnt_scr_act gt 0 %then %do;
			%do scr_act_dt=1 %to &m_cnt_scr_act;
			
				data _null_;
				obs=&scr_act_dt;
				set DSTNCT_SCR_ACT_DT point=obs;
				call symputx('m_scoring_dt',scoring_as_of_dttm);	/*i18NOK:LINE*/
				call symputx('m_actual_dt',actual_as_of_dttm);		/*i18NOK:LINE*/	
				stop;
				run;
				
				%let m_scoring_control_detail_sk=;
				proc sql;
					select put(scoring_control_detail_sk,12.)
							into :m_scoring_control_detail_sk
						from &lib_apdm..scoring_control_detail
						where scoring_template_sk = &m_scoring_template_sk.
						and	scoring_as_of_dttm = &m_scoring_dt.
						and build_abt_status_sk=1 and scoring_status_sk=1 and load_arm_status_sk=1;
				quit;
				
				proc sql noprint;
				select count(*) into :prev_act_cnt from &lib_apdm..actual_result_control_detail		/*i18NOK:LINE*/
				where scoring_control_detail_sk=&m_scoring_control_detail_sk
				and build_abt_status_sk=1 and load_arm_status_sk=1 and actual_result_as_of_dttm=&m_actual_dt.;
				quit;
				
				%if &prev_act_cnt eq 0 %then %do;
				
					%let m_act_abt_sts = ;
					
					/*Start of Calculate m_actual_calc_abt_nm as was externally passed to dabt_update_adm_for_act macro  */
					%let m_scr_dttm = ;
					%let m_act_dttm = ;
					data _null_;
						scr_date = put(datepart(&m_scoring_dt.), date9.); /*i18NOK:LINE*/
						act_date = put(datepart(&m_actual_dt.), date9.); /*i18NOK:LINE*/
						call symput('m_scr_dttm',scr_date); /*i18NOK:LINE*/
						call symput('m_act_dttm',act_date); /*i18NOK:LINE*/
					run;
					%let m_scr_dttm = &m_scr_dttm.;
					%let m_act_dttm = &m_act_dttm;
			
					%let m_actual_calc_abt_nm = ACT_&m_model_sk._&m_scr_dttm._&m_act_dttm;
					%let m_actual_calc_abt_nm = &m_actual_calc_abt_nm.;
					/*End of Calculate m_actual_calc_abt_nm as was externally passed to dabt_update_adm_for_act macro*/
					
					/*Insert in acutal result control detail*/
					%dabt_update_adm_for_act(m_scoring_control_detail_sk = &m_scoring_control_detail_sk., 
								m_actual_bld_dttm = &m_actual_dt., m_act_sts = &m_act_abt_sts, 
								m_actual_process_cd = BUILD_ABT);
					
					/*Update  build_abt_status_sk=1 ,load_arm_status_sk=1,load_csmart_status_sk=0*/
					proc sql;
						update &lib_apdm..actual_result_control_detail set build_abt_status_sk=1 ,
						load_arm_status_sk=1,load_csmart_status_sk=0
						where scoring_control_detail_sk=&m_scoring_control_detail_sk and actual_result_as_of_dttm = &m_actual_dt.;
					quit;
				
				%end;
			%end;
		%end;
		
		
		
		/*End of Step 4: For actual arm tables*/
	%mdl_end:
	%end;/*End of do loop for deployed model*/

%end;/*End of model count gt 0*/
%dabt_terminate_cas_session(cas_session_ref=prfrm_ong_dev);
%mend dabt_cprm_sync_arm_to_control;