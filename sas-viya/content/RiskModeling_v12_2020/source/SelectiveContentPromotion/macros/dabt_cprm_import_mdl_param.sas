/***************************************************************************************
* Macro Name : dabt_cprm_import_model
* Function   : As content promotion this code will import Models. 
*  				1) Required Projects, ABT variables, Purpose, Level, Subject Group
*				   should present on Target Machine. 
*				2) If a model is already present on target machine then it will be updated. 
****************************************************************************************/

%macro dabt_cprm_import_mdl_param(	 entity_sk=,
							  		 import_analysis_report_path=,
									 varmap_ds_lib=,
 								     varmap_ds_nm  = ,
								     m_scratch_ds_prefix = ,
							  		 mode = 
								  ) ;
 
%local m_apdm_lib m_cprm_src_apdm m_cprm_scr_lib m_cprm_imp_ctl; 
%local m_entity_type_cd m_mdl_ret_tgt_sk m_entity_nm m_mdl_different_defn_flg; 
%local m_valid_flg m_src_prj_sk m_tgt_prj_sk m_mdl_prj_in_tgt_flg m_mdl_prj_in_pck_flg m_prj_different_defn_flg; 
%local m_cnt_var m_src_subgrp_sk m_rel_path m_tgt_abt_sk; 

%global m_tgt_mpng_sk;   /**** for CSBCR-14112 source target IDs mapping ***/

%let m_entity_type_cd = MODEL;
%let m_valid_flg = &check_flag_true ; 

/* CPRM CSB-24614 : creating current timestamp for inserting into apdm */
%let imported_dttm = %sysfunc(datetime());
/********************************************************
	Assign Libraries for source package and Analysis.
********************************************************/
		%let m_apdm_lib=;
		%let m_cprm_src_apdm=; 
		%let m_cprm_scr_lib=;  
		%let m_cprm_imp_ctl=;  

		%dabt_assign_libs(	tmp_lib=m_cprm_scr_lib,
							m_workspace_type=CPRM_IMP,
							src_lib = m_apdm_lib,
			                import_analysis_report_path = &import_analysis_report_path., 	/* I18NOK:LINE */
							m_cprm_src_apdm_lib= m_cprm_src_apdm, 
			                m_cprm_ctl_lib = m_cprm_imp_ctl
						  ); 
/********************************************************
	MODEL ANALYSE: Master Entity. 
********************************************************/
	*=================== check model within a project ===============; 
		proc sql noprint; 
		  select tgt_mm.model_sk into : m_mdl_ret_tgt_sk 
		  from &m_cprm_src_apdm..model_master src_mm
		  inner join &m_cprm_src_apdm..project_master src_pm
		  	on src_mm.project_sk = src_pm.project_sk 
		  inner join &m_apdm_lib..model_master tgt_mm 
		  	on kupcase(tgt_mm.model_short_nm) = kupcase(src_mm.model_short_nm)
		  inner join &m_apdm_lib..project_master tgt_pm
		  	on kupcase(tgt_pm.project_short_nm) = kupcase(src_pm.project_short_nm) 
		  	
		  where src_mm.model_sk  = &entity_sk 
		  	and tgt_mm.project_sk = tgt_pm.project_sk 	; 
		quit; 


		%let m_mdl_ret_tgt_sk = &m_mdl_ret_tgt_sk;
		%let m_ref_flg= ;
		%let m_mdl_different_defn_flg=;
		%let import_publish_mdl_add_info= ;
		%let m_mdl_different_defn_cnt=;

		%if &m_mdl_ret_tgt_sk ne %then %do; 

			%let m_mdl_in_tgt_flg = &check_flag_true ;
			proc sql noprint ; 
				select count(*) into : m_mdl_different_defn_cnt		/*i18NOK:LINE*/
				from &m_cprm_src_apdm..model_master a, &m_apdm_lib..model_master b
				 where a.model_sk = &entity_sk 
				 and   b.model_sk = &m_mdl_ret_tgt_sk 
				 and   a.model_source_type_sk = b.model_source_type_sk 
				 ;
			quit;

			%let scr_mdl_sk=;
			%let source_scr_mdl_sk =;
			
					proc sql noprint;
						select scoring_model_sk into : scr_mdl_sk
						from &lib_apdm..scoring_model
						where model_sk=&m_mdl_ret_tgt_sk.;
					quit;
					
					proc sql noprint;
						select scoring_model_sk into :source_scr_mdl_sk
						from &m_cprm_src_apdm..scoring_model
						where model_sk=&entity_sk.;
					quit;
			

			%let import_publish_mdl_add_info= ;

			%if &source_scr_mdl_sk eq and &scr_mdl_sk. ne %then %do;	
				%let m_ref_flg= &CHECK_FLAG_TRUE.;
				%let m_mdl_different_defn_flg= &CHECK_FLAG_TRUE.;
				%let m_valid_flg = &check_flag_false ; 
				%let import_publish_mdl_add_info= %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, PUB_MDL_IMPORT_ERR.PUBLSHED_TGT, noquote));
			%end;
		

			%if &m_mdl_different_defn_cnt ne 0 and &m_mdl_different_defn_flg ne &CHECK_FLAG_TRUE. %then %do ; 
				%let m_mdl_different_defn_flg = &check_flag_false; 
			%end; 
			%else %do ; 
				%let m_mdl_different_defn_flg = &check_flag_true;
				%let m_valid_flg = &check_flag_false ; 
			%end; 

		%end; 
		%else %do ; 
			%let m_mdl_in_tgt_flg = &check_flag_false ;
			%let m_mdl_different_defn_flg=;
		%end;

		proc sql noprint ; 
			select model_short_nm into : m_entity_nm 
			from &m_cprm_src_apdm..model_master
			where model_sk = &entity_sk; 
		quit; 

		%let m_entity_nm = %superq(m_entity_nm);
	%if &mode = ANALYSE %then %do ; 

		%dabt_cprm_ins_pre_analysis_dtl (
								m_promotion_entity_nm= &m_entity_nm,
								m_promotion_entity_type_cd=&m_entity_type_cd,
								m_assoc_entity_nm= &m_entity_nm,
								m_assoc_entity_type_cd=&m_entity_type_cd,
								m_unique_constr_violation_flg=,
								m_present_in_tgt_flg=&m_mdl_in_tgt_flg,
								m_present_in_import_package_flg=&check_flag_true,
								m_referred_in_other_entity_flg= &m_ref_flg.,
								m_different_defn_flg=&m_mdl_different_defn_flg,
								m_addnl_info= &import_publish_mdl_add_info.
								);
	%end; 
/********************************************************
	MODEL ANALYSE: Asssociated Entities 
********************************************************/

	%local m_src_level_cd m_src_level_sk m_tgt_level_cd m_tgt_level_sk ; 
	%local m_src_prps_cd m_src_prps_sk m_tgt_prps_cd m_tgt_prps_sk ;
	%local m_src_subgrp_cd m_src_subgrp_sk m_tgt_subgrp_cd m_tgt_subgrp_sk;

	*=============================== Project =============================; 
		proc sql noprint ; 
			select project_sk 
			into : m_src_prj_sk 
			from &m_cprm_src_apdm..model_master 
			where model_sk = &entity_sk ; 
		quit; 
	 	proc sql noprint ; 
				select c.project_sk
				into : m_tgt_prj_sk 
				from &m_cprm_src_apdm..model_master a
				INNER JOIN  &m_cprm_src_apdm..project_master b  
				   ON a.project_sk = b.project_sk 
				INNER JOIN  &m_apdm_lib..project_master c
				   ON upcase(b.project_short_nm) = upcase(c.project_short_nm)	/*i18NOK:LINE*/
				where a.model_sk = &entity_sk; 
		quit; 

		%let m_tgt_prj_sk = &m_tgt_prj_sk ; 

		%if &m_tgt_prj_sk ne %then %do ; 

			%let m_mdl_prj_in_tgt_flg = &check_flag_true ;

			*---level for project --- ; 
			proc sql noprint ;
				select b.level_cd, b.level_sk 
			    into : m_src_level_cd, :m_src_level_sk
				from &m_cprm_src_apdm..project_master a, 
					 &m_cprm_src_apdm..level_master b
				where a.project_sk = &m_src_prj_sk 
				and   a.level_sk = b.level_sk ; 
			quit; 

			proc sql noprint ;
				select b.level_cd, b.level_sk 
			    into : m_tgt_level_cd, :m_tgt_level_sk
				from &m_apdm_lib..project_master a, 
					 &m_apdm_lib..level_master b
				where a.project_sk = &m_tgt_prj_sk 
				and   a.level_sk = b.level_sk ; 
			quit; 

			*--purpose for project --- ; 
			proc sql noprint ;
				select b.purpose_cd, b.purpose_sk
			    into : m_src_prps_cd, :m_src_prps_sk
				from &m_cprm_src_apdm..project_master a, 
					 &m_cprm_src_apdm..purpose_master b
				where a.project_sk = &m_src_prj_sk 
				and   a.purpose_sk = b.purpose_sk ; 
			quit; 

			proc sql noprint ;
				select b.purpose_cd, b.purpose_sk 
			    into : m_tgt_prps_cd, :m_tgt_prps_sk
				from &m_apdm_lib..project_master a, 
					 &m_apdm_lib..purpose_master b
				where a.project_sk = &m_tgt_prj_sk 
				and   a.purpose_sk = b.purpose_sk ;
			quit; 

			*--subject group for project ---; 
			proc sql noprint ;
				select b.subject_group_cd, b.subject_group_sk
			    into : m_src_subgrp_cd, :m_src_subgrp_sk
				from &m_cprm_src_apdm..project_master a, 
					 &m_cprm_src_apdm..subject_group_master b
				where a.project_sk = &m_src_prj_sk 
				and   a.subject_group_sk = b.subject_group_sk ; 
			quit; 

			proc sql noprint ;
				select b.subject_group_cd, b.subject_group_sk
			    into : m_tgt_subgrp_cd, :m_tgt_subgrp_sk
				from &m_apdm_lib..project_master a, 
					 &m_apdm_lib..subject_group_master b
				where a.project_sk = &m_tgt_prj_sk 
				and   a.subject_group_sk = b.subject_group_sk ; 
			quit; 


			%if ( &m_src_level_cd ne &m_tgt_level_cd ) OR 
				( &m_src_prps_cd ne &m_tgt_prps_cd ) OR 
				( &m_src_subgrp_cd ne &m_tgt_subgrp_cd ) %then %do;  
				%let m_prj_different_defn_flg = &check_flag_true ; 
				%let m_valid_flg = &check_flag_false ; 
			%end; 
			%else %do ; 
				%let m_prj_different_defn_flg = &check_flag_false ; 
			%end; 
		%end; 
		%else %do ; 
			%let m_mdl_prj_in_tgt_flg = &check_flag_false ;
			%let m_prj_different_defn_flg = ; 
		%end; 

		proc sql noprint ; 
			select count(*) into : m_cnt_prj_pck	/*i18NOK:LINE*/
			from &m_cprm_imp_ctl..CPRM_IMPORT_PARAM_LIST_TMP a 
			where kupcase(a.entity_type_cd) = "PROJECT" 	/*i18NOK:LINE*/
				and a.entity_key =  &m_src_prj_sk 
				and kupcase(a.promote_flg) = &CHECK_FLAG_TRUE;
		quit; 

		%if &m_cnt_prj_pck ne 0 %then %do ;
			%let m_prj_in_import_package_flg =  &CHECK_FLAG_TRUE;
		%end; 
		%else %do ; 
			%let m_prj_in_import_package_flg =  &CHECK_FLAG_FALSE;
		%end; 

		%if &m_prj_in_import_package_flg = &check_flag_false && &m_mdl_prj_in_tgt_flg = &check_flag_false %then %do ;
			%let m_valid_flg = &check_flag_false ;  
		%end; 

		%if &mode = ANALYSE %then %do ;
			proc sql noprint ; 
				select b.project_short_nm
				into : m_assoc_entity_nm 
				from &m_cprm_src_apdm..model_master a
				INNER JOIN  &m_cprm_src_apdm..project_master b  
				   ON a.project_sk = b.project_sk 
				where model_sk = &entity_sk; 
			quit; 
			
			%let m_assoc_entity_nm = %superq(m_assoc_entity_nm);
			
			%let m_assoc_entity_type_cd = PROJECT ; 
			
					%dabt_cprm_ins_pre_analysis_dtl (
								m_promotion_entity_nm= &m_entity_nm,
								m_promotion_entity_type_cd= &m_entity_type_cd,
								m_assoc_entity_nm= &m_assoc_entity_nm,
								m_assoc_entity_type_cd= &m_assoc_entity_type_cd,
								m_unique_constr_violation_flg=,
								m_present_in_tgt_flg=&m_mdl_prj_in_tgt_flg,
								m_present_in_import_package_flg=&m_prj_in_import_package_flg,
								m_different_defn_flg = &m_prj_different_defn_flg 
								);
		%end; 

	*=============================== VARIABLES =============================; 
		proc sql noprint ; 
			CREATE TABLE &m_cprm_scr_lib..&m_scratch_ds_prefix._var_map
			AS 
			select  a.variable_sk,  
					b.src_var_sk, 
					b.tgt_var_sk,
					b.present_in_tgt_flg,
					c.variable_column_nm,
					b.present_in_import_package_flg 
			from  &m_cprm_src_apdm..MODEL_X_ACT_OUTCOME_VAR a
			inner join &m_cprm_src_apdm..VARIABLE_MASTER c
				ON a.variable_sk = c.variable_sk 
			LEFT JOIN &varmap_ds_lib..&varmap_ds_nm b
				ON a.variable_sk = b.src_var_sk 
			where a.model_sk = &entity_sk; 

		quit; 
		
		proc sql noprint ; 
			select count(*) into : m_cnt_var 	/*i18NOK:LINE*/
			from &m_cprm_scr_lib..&m_scratch_ds_prefix._var_map;
		quit; 

		%do i=1 %to &m_cnt_var ; 
			DATA _NULL_ ; 
				data_point = &i; 
				SET &m_cprm_scr_lib..&m_scratch_ds_prefix._var_map POINT=data_point ;
				CALL SYMPUT("m_src_var_sk",variable_sk) ;  				/*i18NOK:LINE*/
				CALL SYMPUT("m_var_in_tgt_flg",present_in_tgt_flg) ;	/*i18NOK:LINE*/
				CALL SYMPUT("m_var_column_nm",variable_column_nm);		/*i18NOK:LINE*/
				CALL SYMPUT("m_var_in_imp_pck_flg",present_in_import_package_flg);	/*i18NOK:LINE*/
				STOP;  
			RUN ;
 
			%if  &m_var_in_tgt_flg eq Y %then %do ; 
				%let m_var_in_tgt_flg = &check_flag_true; 
			%end;
			%else %do ; 
				%let m_var_in_tgt_flg = &check_flag_false ; 
			%end; 
				
			%if  &m_var_in_imp_pck_flg eq Y %then %do ; 
				%let m_var_in_imp_pck_flg = &check_flag_true; 
			%end;
			%else %do ; 
				%let m_var_in_imp_pck_flg = &check_flag_false ; 
			%end; 

			%let m_assoc_entity_type_cd = VARIABLE ;
			%let m_assoc_entity_nm = &m_var_column_nm ;

		 		%if &mode = ANALYSE %then %do ;
					%dabt_cprm_ins_pre_analysis_dtl (
										m_promotion_entity_nm= &m_entity_nm,
										m_promotion_entity_type_cd= &m_entity_type_cd,
										m_assoc_entity_nm=&m_assoc_entity_nm,
										m_assoc_entity_type_cd= &m_assoc_entity_type_cd,
										m_unique_constr_violation_flg=,
										m_present_in_tgt_flg=&m_var_in_tgt_flg,
										m_present_in_import_package_flg=&m_var_in_imp_pck_flg
										);
				%end;

			%if (&m_var_in_tgt_flg eq &CHECK_FLAG_FALSE AND  &m_var_in_imp_pck_flg eq &CHECK_FLAG_FALSE ) %then %do ;  
				%let m_valid_flg = &CHECK_FLAG_FALSE ; 
			%end;
		%end ; 

	*=============================== REFERENCE EVENT CHECK =============================; 
	/* Code change begin: FIX S1437851	*/
		%local src_event_sk ;

		proc sql noprint;
			select event_sk into :src_event_sk 
			from &m_cprm_src_apdm..model_master 
			where model_sk = &entity_sk ;
		quit;

		%local tgt_event_sk event_vld_rslt_flg;

		%if &src_event_sk ne . %then %do;
			%dabt_cprm_check_parent_entity ( 	entity_sk 					= &entity_sk,
												entity_type_cd 				= &m_entity_type_cd,
												assoc_entity_sk 			= &src_event_sk,
												assoc_entity_type_cd 		= EVENT,
												src_apdm_lib 				= &m_cprm_src_apdm,
												tgt_apdm_lib 				= &m_apdm_lib,
												mode 						= &mode,
												return_assoc_entity_tgt_sk 	= tgt_event_sk,
												return_validation_rslt_flg 	= event_vld_rslt_flg
											);
		%end;
		%else %do;
			%let tgt_event_sk = .;
			%let event_vld_rslt_flg = &CHECK_FLAG_TRUE.;
		%end;
	
	%if &event_vld_rslt_flg eq &CHECK_FLAG_FALSE %then %do ; 
		%let m_valid_flg = &CHECK_FLAG_FALSE ; 
	%end; 

/********************************************************
	MODEL EXECUTE:
*********************************************************/
	%if &mode = EXECUTE AND &m_valid_flg = &check_flag_true AND &syscc le 4 %then %do ; 
		*=============================== MODEL_MASTER & MM_REPORT_SPECIFICATION =============================;
			%if &m_mdl_in_tgt_flg eq &check_flag_true %then %do; 
				%let updt_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=MODEL_MASTER, 
										m_src_lib_nm= &m_cprm_src_apdm, 
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= level_sk project_sk model_sk model_id event_sk authorization_rule_id owned_by_user job_sk, 
										m_col_lst=, 
										m_prim_col_nm=model_sk, 
										m_prim_col_val=&entity_sk,
										m_upd_cols_lst=updt_cols_lst
								  );

				proc sql noprint ; 	
					update &m_apdm_lib..model_master 
					set &updt_cols_lst , event_sk = &tgt_event_sk. /* Code change event_sk added to list: FIX S1437851	*/
					where model_sk = &m_mdl_ret_tgt_sk ; 
				quit; 
				%let m_tgt_mpng_sk=&m_mdl_ret_tgt_sk.;
				
				%let trgt_owned_by=;
				proc sql noprint;
				select owned_by_user into :trgt_owned_by from &m_apdm_lib..model_master
				where model_sk = &m_mdl_ret_tgt_sk ;
				quit;
				
				%if &trgt_owned_by ne &owned_by %then %do;			
					%dabt_change_model_ownership(model_id=&m_mdl_ret_tgt_sk, change_owner_to=&owned_by);
				%end;
			%end; 
			%else %do ; 
				*========================= Importing a new Model ===========================;
 
				

				%let m_next_model_sk = .;

				proc sql;
					&apdm_connect_string.;
					select 
						temp into :m_next_model_sk
					from 
						connection to postgres 
							( select nextval( %nrbquote('&apdm_schema..model_master_model_sk_seq') ) as temp );   /* i18nOK:Line */
					    &apdm_disconnect_string.;
				quit;
				%let m_next_model_sk = &m_next_model_sk ;
				
				
				%let m_tgt_mpng_sk=&m_next_model_sk.;
			
				*%dabt_make_work_area(dir=&project_path/&m_tgt_prj_sk, create_dir=/model/&m_next_model_sk, path=m_rel_path);
 
				%let ins_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=MODEL_MASTER, 
										m_src_lib_nm= &m_cprm_src_apdm, 
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= level_sk project_sk model_sk model_id event_sk job_sk, 
										m_col_lst=, 
										m_prim_col_nm=model_sk, 
										m_prim_col_val=&entity_sk,
										m_ins_cols_lst=ins_cols_lst
								  );
				
				/*insert empty job_sk in model_master*/
				%let job_sk_blank=.;
			
				proc sql noprint ; 
					insert into &m_apdm_lib..model_master 
					(&ins_cols_lst, model_sk, level_sk, project_sk, event_sk, model_id,job_sk) 
					select 	&ins_cols_lst, &m_next_model_sk.,
							&m_tgt_level_sk as level_sk, 
							&m_tgt_prj_sk as project_sk,
							%if &src_event_sk ne . %then %do; 
							&tgt_event_sk
							%end;
							%else %do ;
							&src_event_sk 
							%end; as event_sk,
							"&m_next_model_sk" as model_id,
							&job_sk_blank as job_sk
					from &m_cprm_src_apdm..model_master 
					where model_sk = &entity_sk ; 
				quit;
				
				/*Start-To create model folder on content server*/
				
				proc sql noprint;
				update &m_apdm_lib..model_master set owned_by_user='NA'
				where model_sk=&m_next_model_sk;
				quit;
				
				%dabt_change_model_ownership(model_id=&m_next_model_sk, change_owner_to=&owned_by);
				
				/*End-To create model folder on content server*/

				proc sql noprint; 
					  select tgt_mm.model_sk into : m_mdl_ret_tgt_sk 
					  from &m_cprm_src_apdm..model_master src_mm
					  inner join &m_cprm_src_apdm..project_master src_pm
					  on src_mm.project_sk = src_pm.project_sk 
					  inner join &m_apdm_lib..model_master tgt_mm 
					  on kupcase(tgt_mm.model_short_nm) = kupcase(src_mm.model_short_nm)
					  inner join &m_apdm_lib..project_master tgt_pm
					  on kupcase(tgt_pm.project_short_nm) = kupcase(src_pm.project_short_nm) 
					  where src_mm.model_sk  = &entity_sk 
						and tgt_mm.project_sk = tgt_pm.project_sk 	; 	/* Added check for	S1438979 and S1441259 */
				quit;
				%let m_mdl_ret_tgt_sk = &m_mdl_ret_tgt_sk.;
				
				
				/* CPRM CSB23670 */
				proc sql;
					&apdm_connect_string.;

					select 
						temp into :m_next_rptspec_sk
					from 
						connection to postgres 
							( select nextval( %nrbquote('&apdm_schema..mm_report_specification_report_specification_sk_seq') ) as temp );    /* i18nOK:Line */
					    &apdm_disconnect_string.;
				quit;


				%put &m_next_rptspec_sk;
		%let m_next_rptspec_sk = &m_next_rptspec_sk.;
		
		%let ins_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=MM_REPORT_SPECIFICATION , 
										m_src_lib_nm= &m_cprm_src_apdm, 
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= report_specification_sk model_sk, 
										m_col_lst=, 
										m_prim_col_nm=report_specification_sk, 
										m_prim_col_val=,			/* update does not happen */
										m_ins_cols_lst=ins_cols_lst
								  );
								  
	/* CPRM CSB23670 - Extract SRC report specification sk */
		proc sql;
			select report_specification_sk into :m_src_rpt_spec_sk
			from &m_cprm_src_apdm..mm_report_specification MMRS
			inner join &m_cprm_src_apdm..mm_rpt_spec_type_master  MRST
				on MMRS.report_specification_type_sk = MRST.report_specification_type_sk
				and MRST.report_specification_type_cd = 'MS'            /* i18nOK:Line */
				where model_sk = &entity_sk. ;
		quit;
		
		
		
				proc sql noprint ; 	
					insert into &m_apdm_lib..mm_report_specification 
					(&ins_cols_lst , report_specification_sk, model_sk) 
					select 	&ins_cols_lst, &m_next_rptspec_sk., &m_next_model_sk.
					from &m_cprm_src_apdm..mm_report_specification 
					where model_sk = &entity_sk and report_specification_sk in (&m_src_rpt_spec_sk.); 
				quit; 
			
			%let m_tgt_rpt_spec_sk = &m_next_rptspec_sk. ;
		
		
 			%end; 
		
		*=============================== IMPORTING VERSIONS INTO APDM.MM_REPORT_SPECIFICATION =============================; 
		/* CPRM CSB-24614: Deployed Version, if present on Source, should be added to the target as a new version */		
		proc sql noprint;
	/* CPRM CSB-24614: Extract SRC report specification sk for Deployed Version */
			select report_specification_sk into :m_src_rpt_spec_sk_dv  	
			from &m_cprm_src_apdm..mm_report_specification 
				where deployed_flg = &CHECK_FLAG_TRUE. 			 		/* i18nOK:Line */	
				and model_sk = &entity_sk. ;	
	
	/* CPRM CSB-24614: Extract sequential report specification sk  */
	%if %symexist(m_src_rpt_spec_sk_dv) %then %do;
	&apdm_connect_string.;

					select 
						temp into :m_next_rptspec_sk_dv
					from 
						connection to postgres 
							( select nextval( %nrbquote('&apdm_schema..mm_report_specification_report_specification_sk_seq') ) as temp );    /* i18nOK:Line */
					  
					&apdm_disconnect_string.;
						
					
	/* CPRM CSB-24614: Extract version no for Deployed Version (is current running version no +1)  */		
					select coalesce(max(version_no),0)+1 into :nxt_vrsn_no
						from &m_apdm_lib..mm_report_specification
						where model_sk = &m_mdl_ret_tgt_sk. and report_specification_type_sk not in 
								(select report_specification_type_sk 
							from &m_apdm_lib..mm_rpt_spec_type_master 
							where report_specification_type_cd eq 'MS'); /* I18NOK:LINE  */
				quit;
	%let ins_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=MM_REPORT_SPECIFICATION , 
										m_src_lib_nm= &m_cprm_src_apdm, 
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= report_specification_sk report_specification_nm model_sk version_no deployed_flg ready_for_deployment_flg created_dttm last_processed_dttm, 
										m_col_lst=, 
										m_prim_col_nm=report_specification_sk, 
										m_prim_col_val=&m_src_rpt_spec_sk_dv.,			
										m_ins_cols_lst=ins_cols_lst
								  );
	
	/* CPRM CSB-24614: Reset Ready For Deployment Flag, and Insert record into target mm_report_specification for Deployed Version */				
				proc sql noprint nowarn; 	/* S1443811 : nowarn added to suppress warning: insert with report_specification_nm will result in a warning, which is not data-dependent */
					update &m_apdm_lib..mm_report_specification 
					set ready_for_deployment_flg = '' 
					where model_sk = &m_mdl_ret_tgt_sk ;
					
					insert into &m_apdm_lib..mm_report_specification 
						(&ins_cols_lst , 
						report_specification_sk, 
						report_specification_nm, 
						model_sk, 
						version_no, 
						deployed_flg, 
						ready_for_deployment_flg, 
						created_dttm, 
						last_processed_dttm)  
					select 	
						&ins_cols_lst, 
						&m_next_rptspec_sk_dv., 
						cat("Version ",kstrip("&nxt_vrsn_no.")), 		/* i18nOK:Line */ /*S1440261 - inserting sequential version name */
						&m_mdl_ret_tgt_sk., 
						&nxt_vrsn_no. , 
						' ', 											/* i18nOK:Line */
						'Y'	, 											/* i18nOK:Line */
						&imported_dttm. , 
						&imported_dttm.								
					from &m_cprm_src_apdm..mm_report_specification 
					where model_sk = &entity_sk. and report_specification_sk in (&m_src_rpt_spec_sk_dv.); 
				quit; 
	
		*========================= Insert into Report_spec_model_param if source has a deployed version  ===========================;

	/* CPRM CSB-24614: Insert record into target Report_spec_model_param for Deployed Version */
				%let ins_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=REPORT_SPEC_MODEL_PARAM , 
										m_src_lib_nm= &m_cprm_src_apdm, 
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= report_specification_sk event_sk,
										m_col_lst=, 
										m_prim_col_nm=report_specification_sk, 
										m_prim_col_val=,			/* update does not happen */
										m_ins_cols_lst=ins_cols_lst
								  );
			

				proc sql;
					select event_sk into :src_event_sk_dv
					from &m_cprm_src_apdm..report_spec_model_param 
					where report_specification_sk in (&m_src_rpt_spec_sk_dv.); 
				quit;

				%local tgt_event_sk_dv event_vld_rslt_flg_dv;

			%if &src_event_sk_dv ne . %then %do;
			%dabt_cprm_check_parent_entity ( 	entity_sk 					= &entity_sk,
												entity_type_cd 				= &m_entity_type_cd,
												assoc_entity_sk 			= &src_event_sk_dv,
												assoc_entity_type_cd 		= EVENT,
												src_apdm_lib 				= &m_cprm_src_apdm,
												tgt_apdm_lib 				= &m_apdm_lib,
												mode 						= &mode,
												return_assoc_entity_tgt_sk 	= tgt_event_sk_dv,
												return_validation_rslt_flg 	= event_vld_rslt_flg_dv
											);
											%end;
			
										%else %do;
			%let tgt_event_sk_dv = .;
			%let event_vld_rslt_flg_dv = &CHECK_FLAG_TRUE.;
										%end;
			%if &event_vld_rslt_flg eq &CHECK_FLAG_FALSE %then %do ; 
				%let m_valid_flg = &CHECK_FLAG_FALSE ; 
			%end; 


			%if &mode = EXECUTE AND &m_valid_flg = &check_flag_true AND &syscc le 4 %then %do ; 
				proc sql noprint ; 	
					insert into &m_apdm_lib..report_spec_model_param 
					(&ins_cols_lst , report_specification_sk, event_sk)  
					select 	&ins_cols_lst, &m_next_rptspec_sk_dv., &tgt_event_sk_dv.
					from &m_cprm_src_apdm..report_spec_model_param 
					where report_specification_sk in (&m_src_rpt_spec_sk_dv.); 
				quit; 	
			%end;
			%let m_tgt_rpt_spec_sk_dv = &m_next_rptspec_sk_dv. ;				
				
%end;
		
		
			
		*=============================== MODEL_MASTER_EXTENSION =============================; 
		%let etls_tableExist = %eval(	%sysfunc(exist(&m_apdm_lib..MODEL_MASTER_EXTENSION, DATA)) or 
         					             %sysfunc(exist(&m_apdm_lib..MODEL_MASTER_EXTENSION, VIEW))
						            ); 
						
						
		%if (&etls_tableExist ne 0) %then %do; 
			%if &m_mdl_in_tgt_flg eq &check_flag_true %then %do; 
				%let updt_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=MODEL_MASTER_EXTENSION, 
										m_src_lib_nm= &m_cprm_src_apdm, 
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= level_sk purpose_sk model_key model_id, /* S1457320 - added model_id */
										m_col_lst=, 
										m_prim_col_nm=model_key, 
										m_prim_col_val=&entity_sk,
										m_upd_cols_lst=updt_cols_lst
								  );

				proc sql noprint ; 	
					update &m_apdm_lib..model_master_EXTENSION
					set &updt_cols_lst, 
						level_sk= &m_tgt_level_sk, 
						purpose_sk= &m_tgt_prps_sk
					where model_key = &m_mdl_ret_tgt_sk ; 
				quit; 
			%end; 
			%else %do ; 
				%let ins_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=MODEL_MASTER_EXTENSION, 
										m_src_lib_nm= &m_cprm_src_apdm,  
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= level_sk purpose_sk model_key model_id, 
										m_col_lst=, 
										m_prim_col_nm=model_key, 
										m_prim_col_val=&entity_sk,
										m_ins_cols_lst=ins_cols_lst
								  );
				proc sql noprint ; 
					insert into &m_apdm_lib..model_master_extension 
					(&ins_cols_lst, level_sk, purpose_sk, model_key, model_id) 
					select 	&ins_cols_lst,
							&m_tgt_level_sk as level_sk, 
							&m_tgt_prps_sk as purpose_sk, 
							&m_mdl_ret_tgt_sk as model_key,
							"&m_mdl_ret_tgt_sk" as model_id
					from &m_cprm_src_apdm..model_master_extension
					where model_key = &entity_sk ; 
				quit;

			%end;
		%end;
		*=============================== MODEL_X_MODELING_ABT =============================;
			%if &m_mdl_in_tgt_flg eq &check_flag_true %then %do; 
				proc sql noprint; 
					delete from &m_apdm_lib..MODEL_X_MODELING_ABT 
					where model_sk = &m_mdl_ret_tgt_sk ; 
				quit; 
			%end; 

			%let ins_cols_lst = ;
			%dabt_cprm_get_col_lst(	
									m_ds_nm=MODEL_X_MODELING_ABT, 
									m_src_lib_nm= &m_cprm_src_apdm,  
									m_tgt_lib_nm= &m_apdm_lib, 
									m_exclued_col= model_sk abt_sk, 
									m_col_lst=, 
									m_prim_col_nm=model_sk, 
									m_prim_col_val=&entity_sk,
									m_ins_cols_lst=ins_cols_lst
							  );

			proc sql noprint ; 
				select abt_sk into : m_tgt_abt_sk
				from &m_apdm_lib..modeling_abt_master b
				where b.project_sk = &m_tgt_prj_sk;
			quit; 

			proc sql noprint ; 
				insert into &m_apdm_lib..model_x_modeling_abt
				(&ins_cols_lst, model_sk, abt_sk) 
				select &ins_cols_lst,
					&m_mdl_ret_tgt_sk as model_sk, 
					&m_tgt_abt_sk  as abt_sk
				from  &m_cprm_src_apdm..model_x_modeling_abt
				where model_sk = &entity_sk ; 
			quit; 
		*=============================== MODEL_X_ACT_OUTCOME_VAR =============================;
			%if &m_mdl_in_tgt_flg eq &check_flag_true %then %do; 
				proc sql noprint; 
					delete from &m_apdm_lib..MODEL_X_ACT_OUTCOME_VAR 
					where model_sk = &m_mdl_ret_tgt_sk ; 
				quit; 
			%end;  
				%let ins_cols_lst = ;
				%dabt_cprm_get_col_lst(	
										m_ds_nm=MODEL_X_ACT_OUTCOME_VAR, 
										m_src_lib_nm= &m_cprm_src_apdm,  
										m_tgt_lib_nm= &m_apdm_lib, 
										m_exclued_col= model_sk variable_sk , 
										m_col_lst=, 
										m_prim_col_nm=model_sk, 
										m_prim_col_val=&entity_sk,
										m_ins_cols_lst=ins_cols_lst
								  );
				proc sql noprint ; 
					insert into &m_apdm_lib..MODEL_X_ACT_OUTCOME_VAR 
					(&ins_cols_lst, model_sk, variable_sk) 
					select 	&ins_cols_lst,
							&m_mdl_ret_tgt_sk as model_sk, 
							tgt_var_sk as variable_sk
					from &m_cprm_scr_lib..&m_scratch_ds_prefix._var_map a 
					inner join &m_cprm_src_apdm..model_x_act_outcome_var b 
					on a.variable_sk = b.variable_sk 
					where model_sk = &entity_sk ; 
				quit;
	%end; 

	%if &m_valid_flg = &check_flag_false AND &syscc le 4 %then %do ; 
		%let syscc = 9999 ; 
	%end; 	
	%put syscc = &syscc ; 
%mend dabt_cprm_import_mdl_param; 
