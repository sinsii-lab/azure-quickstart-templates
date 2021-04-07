/********************************************************************************************************
   Module:  dabt_cprm_import_project_master

   Called by: dabt_cprm_import_project job
   Function : This macro validates if the project can be imported 
			  from the source machine to the TGT. If yes, then import the project_master.

   Parameters: INPUT: 
			1. entity_sk                   : table sk of the source machine, to be imported.
			2. import_spec_ds_nm           : dataset which contains the entity details to be imported.
			3. import_package_path 	       : Path containing the import package.
			4. import_analysis_report_path : Path where the impor analysis report is generated.
			5. import_analysis_report_ds_nm: The dataset where individual macros will insert the analysis result.
			6. mode						   : ANALYSE/EXECUTE
*********************************************************************************************************/

%macro dabt_cprm_import_project_master(	entity_sk=&enity_key., 
								import_spec_ds_nm=CPRM_IMPORT_PARAM_LIST_TMP,
								import_package_path=, 
								import_analysis_report_path=, 
								import_analysis_report_ds_nm = cprm_import_analysis_dtl,
								mode = 
								);

	/*Declare local macro variables*/
%global m_tgt_mpng_sk;   /**** for CSBCR-14112 source target IDs mapping ***/
	%local m_valid_flg
		m_prj_short_nm
		m_tgt_project_sk
		m_src_level_sk
		m_tgt_level_sk
		m_valid_level_flg
		m_src_purpose_sk
		m_tgt_purpose_sk
		m_valid_purpose_flg
		m_src_subject_group_sk
		m_tgt_subject_group_sk
		m_valid_subject_group_flg
		m_tgt_level_sk_check
		m_tgt_purpose_sk_check
		m_tgt_sbjct_grp_sk_check
		m_same_sbjct_grp_flg
		m_same_purpose_flg
		m_same_level_flg;

	%let import_analysis_report_path = &import_analysis_report_path.;
	%let import_analysis_report_ds_nm = &import_analysis_report_ds_nm.;
	%let mode = &mode.;

	%let m_valid_flg= &CHECK_FLAG_TRUE.;

	%local m_entity_type_nm m_entity_type_cd ; 

	%let m_entity_type_nm= ;
	%let m_entity_type_cd=PROJECT;
	
	proc sql noprint;
		select entity_type_nm 
			into :m_entity_type_nm
		from &lib_apdm..CPRM_ENTITY_MASTER
		where ktrim(kleft(kupcase(entity_type_cd)))=%upcase("&m_entity_type_cd.");	/* I18NOK:LINE */
	quit; 

	/*m_cprm_src_apdm- Stores libref of source apdm. dabt_assign_lib macro will assign value to this*/
	/*m_cprm_scr_lib- Stores libref for scratch. dabt_assign_lib macro will assign value to this*/
	/*m_cprm_imp_ctl- Stores libref for control library. This lib will has CPRM_IMPORT_PARAM_LIST_TMP, . dabt_assign_lib macro will assign value to this*/

	%local m_cprm_src_apdm m_cprm_scr_lib m_cprm_imp_ctl m_apdm_lib;
	%dabt_assign_libs(tmp_lib=m_cprm_scr_lib,m_workspace_type=CPRM_IMP,src_lib = m_apdm_lib,
	                                import_analysis_report_path = &import_analysis_report_path., m_cprm_src_apdm_lib= m_cprm_src_apdm, 
	                                m_cprm_ctl_lib = m_cprm_imp_ctl);


	/****************************************START OF ANALYSIS MODE*****************************************/

	%let m_prj_short_nm = ;
	proc sql noprint;
		select src_prj.project_short_nm
				into :m_prj_short_nm
			from &m_cprm_src_apdm..project_master src_prj 
			where src_prj.project_sk = &entity_sk.;
	quit;
	
	%let m_present_in_tgt= ;
	%let m_present_in_import_package= ;
	%let m_prj_short_nm = %superq(m_prj_short_nm);
	

	/*Get the project_sk on the TGT machine if the project is already present on the TGT.*/
	%let m_tgt_project_sk = ;	

	%dabt_cprm_get_entity_tgt_sk(	entity_sk = &entity_sk.,
									entity_type_cd = &m_entity_type_cd. , 
									src_apdm_lib = &m_cprm_src_apdm., 
									tgt_apdm_lib = &lib_apdm., 
									return_entity_tgt_sk = m_tgt_project_sk);


	/*Start of Validation for SOA: Validating if the SOA required for the project is 
									   present on the target.*/

	/*Get the level_sk for the project, from the source package.*/

	%let m_src_level_sk = ;

	proc sql noprint;
		select src_lvl.level_sk
				into :m_src_level_sk
			from &m_cprm_src_apdm..project_master src_prj 
				inner join &m_cprm_src_apdm..level_master src_lvl
					on src_prj.level_sk = src_lvl.level_sk
			where src_prj.project_sk = &entity_sk.;
	quit;
	%let m_src_level_sk = &m_src_level_sk.;

	/*Call macro to check if the level is present on the target or in the source package.
	  If yes, then return the target level_sk.*/
	%let m_present_in_tgt= ;
	%let m_present_in_import_package= ;

	%let m_tgt_level_sk = ;
	%let m_valid_level_flg = ;
	%dabt_cprm_check_parent_entity( 	entity_sk = &entity_sk., 
										entity_type_cd = &m_entity_type_cd., 
										assoc_entity_sk = &m_src_level_sk., 
										assoc_entity_type_cd = SOA, 
										src_apdm_lib = &m_cprm_src_apdm., 
										tgt_apdm_lib = &m_apdm_lib.,   
										mode = &mode.,
										return_assoc_entity_tgt_sk = m_tgt_level_sk,
										return_validation_rslt_flg = m_valid_level_flg);

	
	/*End of Validation for SOA*/

	/*Start of Validation for Purpose: Validating if the Purpose required for the project is 
									   present on the target.*/

	/*Get the purpose_sk for the project, from the source package.*/

	%let m_src_purpose_sk = ;

	proc sql noprint;
		select src_purp.purpose_sk
				into :m_src_purpose_sk
			from &m_cprm_src_apdm..project_master src_prj 
				inner join &m_cprm_src_apdm..purpose_master src_purp
					on src_prj.purpose_sk = src_purp.purpose_sk
			where src_prj.project_sk = &entity_sk.;
	quit;
	%let m_src_purpose_sk = &m_src_purpose_sk.;

	/*Call macro to check if the purpose is present on the target or in the source package.
	  If yes, then return the target purpose_sk.*/
	%let m_present_in_tgt= ;
	%let m_present_in_import_package= ;

	%let m_tgt_purpose_sk = ;
	%let m_valid_purpose_flg = ;
	%dabt_cprm_check_parent_entity( 	entity_sk = &entity_sk., 
										entity_type_cd = &m_entity_type_cd., 
										assoc_entity_sk = &m_src_purpose_sk., 
										assoc_entity_type_cd = PURPOSE, 
										src_apdm_lib = &m_cprm_src_apdm., 
										tgt_apdm_lib = &m_apdm_lib.,   
										mode = &mode.,
										return_assoc_entity_tgt_sk = m_tgt_purpose_sk,
										return_validation_rslt_flg = m_valid_purpose_flg);

	
	/*End of Validation for Purpose*/

	/*Start of Validation for Subject Group: Validating if the Subject Group required for the project is 
									   present on the target.*/

	/*Get the subject_group_sk for the project, from the source package.*/

	%let m_src_subject_group_sk = .;

	proc sql noprint;
		select src_sbj.subject_group_sk
				into :m_src_subject_group_sk
			from &m_cprm_src_apdm..project_master src_prj 
				inner join &m_cprm_src_apdm..subject_group_master src_sbj
					on src_prj.subject_group_sk = src_sbj.subject_group_sk
			where src_prj.project_sk = &entity_sk.;
	quit;

	/*If subject group is linked to the project then call macro to check if the Subject Group is 
		present on the target or in the source package. If yes, then return the target subject_group_sk.*/

	%if &m_src_subject_group_sk. ne . %then %do;

		%let m_present_in_tgt= ;
		%let m_present_in_import_package= ;

		%let m_tgt_subject_group_sk = .;
		%let m_valid_subject_group_flg = ;
		%dabt_cprm_check_parent_entity( 	entity_sk = &entity_sk., 
							entity_type_cd = &m_entity_type_cd., 
							assoc_entity_sk = &m_src_subject_group_sk., 
							assoc_entity_type_cd = SBJCT_GRP, 
							src_apdm_lib = &m_cprm_src_apdm., 
							tgt_apdm_lib = &m_apdm_lib.,   
							mode = &mode.,
							return_assoc_entity_tgt_sk = m_tgt_subject_group_sk,
							return_validation_rslt_flg = m_valid_subject_group_flg);

	%end;
	%else %do;
		%let m_tgt_subject_group_sk = .;
	%end;

	/*End of Validation for Subject Group*/

	/*Start: Validations if the project is already present on TGT.*/
	%if &m_tgt_project_sk. ne  %then %do;
		/*Check if level associated with the project on TGT is also the same as on SRC.*/

		%let m_tgt_level_sk_check = ;
		
		proc sql noprint;
			select tgt_lvl.level_sk
					into :m_tgt_level_sk_check
				from &m_cprm_src_apdm..project_master src_prj 
					inner join &m_cprm_src_apdm..level_master src_lvl
						on src_prj.level_sk = src_lvl.level_sk
					inner join &lib_apdm..level_master tgt_lvl
						on src_lvl.level_cd = tgt_lvl.level_cd
					inner join &lib_apdm..project_master tgt_prj
						on tgt_prj.level_sk = tgt_lvl.level_sk
							and tgt_prj.project_sk = &m_tgt_project_sk.
				where src_prj.project_sk = &entity_sk.;
		quit;

		%let m_same_level_flg = &CHECK_FLAG_FALSE;
		%if &m_tgt_level_sk_check. ne %then %do;
			%let m_same_level_flg = &CHECK_FLAG_TRUE;
		%end;

		/*Check if purpose associated with the project on TGT is also the same as on SRC.*/
		%let m_tgt_purpose_sk_check = ;
		
		proc sql noprint;
			select tgt_prp.purpose_sk
					into :m_tgt_purpose_sk_check
				from &m_cprm_src_apdm..project_master src_prj 
					inner join &m_cprm_src_apdm..purpose_master src_prp
						on src_prj.purpose_sk = src_prp.purpose_sk
					inner join &lib_apdm..purpose_master tgt_prp
						on src_prp.purpose_cd = tgt_prp.purpose_cd
					inner join &lib_apdm..project_master tgt_prj
						on tgt_prj.purpose_sk = tgt_prp.purpose_sk
							and tgt_prj.project_sk = &m_tgt_project_sk.
				where src_prj.project_sk = &entity_sk.;
		quit;

		%let m_same_purpose_flg = &CHECK_FLAG_FALSE;
		%if &m_tgt_purpose_sk_check. ne %then %do;
			%let m_same_purpose_flg = &CHECK_FLAG_TRUE;
		%end;

		/*Check if subject group associated with the project on TGT is also the same as on SRC.*/

		%let m_tgt_sbjct_grp_sk_check = ;
		%let m_same_sbjct_grp_flg = &CHECK_FLAG_FALSE;

		%let m_tgt_subject_group_present = ;

		proc sql noprint;
			select tgt_prj.subject_group_sk
					into :m_tgt_subject_group_present
				from &m_cprm_src_apdm..project_master src_prj 
					inner join &lib_apdm..project_master tgt_prj 
						on src_prj.project_short_nm = tgt_prj.project_short_nm
				where src_prj.project_sk = &entity_sk.;
		quit;

		%if (&m_src_subject_group_sk. ne . and 
				&m_tgt_subject_group_present. ne .) %then %do;
		
			proc sql noprint;
				select tgt_sbj.subject_group_sk
						into :m_tgt_sbjct_grp_sk_check
					from &m_cprm_src_apdm..project_master src_prj 
						inner join &m_cprm_src_apdm..subject_group_master src_sbj
							on src_prj.subject_group_sk = src_sbj.subject_group_sk
						inner join &lib_apdm..subject_group_master tgt_sbj
							on src_sbj.subject_group_cd = tgt_sbj.subject_group_cd
						inner join &lib_apdm..project_master tgt_prj
							on tgt_prj.subject_group_sk = tgt_sbj.subject_group_sk
								and tgt_prj.project_sk = &m_tgt_project_sk.
					where src_prj.project_sk = &entity_sk.;
			quit;

			%if &m_tgt_sbjct_grp_sk_check. ne %then %do;
				%let m_same_sbjct_grp_flg = &CHECK_FLAG_TRUE;
			%end;
		%end;
		%else %if (&m_src_subject_group_sk. eq . and 
					&m_tgt_subject_group_present. eq .) %then %do;

				%let m_same_sbjct_grp_flg = &CHECK_FLAG_TRUE;

		%end;

		%if &m_same_level_flg = &CHECK_FLAG_TRUE 
			and &m_same_purpose_flg = &CHECK_FLAG_TRUE
			and &m_same_sbjct_grp_flg = &CHECK_FLAG_TRUE %then %do;

				%let m_valid_flg= &CHECK_FLAG_TRUE.;
				%let m_present_in_tgt = &CHECK_FLAG_TRUE.;
				%let m_different_defn = &CHECK_FLAG_FALSE.;	

		%end;
		%else %do;

				%let m_valid_flg= &CHECK_FLAG_FALSE.;
				%let m_present_in_tgt = &CHECK_FLAG_TRUE.;
				%let m_different_defn = &CHECK_FLAG_TRUE.;	

		%end;

	%end; /*End: Validations if the project is already present on TGT.*/

	%else %do;
		%let m_present_in_tgt = &CHECK_FLAG_FALSE.;
		%let m_different_defn = ;
	%end;
	

	%if &mode = ANALYSE %then %do;
		%dabt_cprm_ins_pre_analysis_dtl (
							m_promotion_entity_nm=&m_prj_short_nm,
							m_promotion_entity_type_cd=&m_entity_type_cd,
							m_assoc_entity_nm=&m_prj_short_nm ,
							m_assoc_entity_type_cd=&m_entity_type_cd,
							m_present_in_tgt_flg= &m_present_in_tgt.,
							m_present_in_import_package_flg= &CHECK_FLAG_TRUE.,
							m_different_defn_flg = &m_different_defn.
						);
	%end;

	%if &m_valid_level_flg. = &CHECK_FLAG_FALSE or
		&m_valid_purpose_flg. = &CHECK_FLAG_FALSE or
		&m_valid_subject_group_flg. = &CHECK_FLAG_FALSE %then %do;

			%let m_valid_flg= &CHECK_FLAG_FALSE.;

	%end;
	/****************************************END OF ANALYSIS MODE*****************************************/


	/****************************************START OF EXECUTE MODE****************************************/

	/*If the mode is execute and all the validations have passed then execute the below block*/

	%if &mode = EXECUTE and &m_valid_flg. = &CHECK_FLAG_TRUE. and &syscc le 4 %then %do;

		%if &m_tgt_project_sk. ne %then %do; 
 
			%let upd_cols_project_master = ;
			%dabt_cprm_get_col_lst(	m_ds_nm=project_master, 
									m_src_lib_nm=&m_cprm_src_apdm, 
									m_tgt_lib_nm=&lib_apdm, 
									m_exclued_col= project_sk purpose_sk level_sk 
													subject_group_sk project_id project_short_nm authorization_rule_id owned_by_user, 
									m_col_lst=, 
									m_prim_col_nm=project_sk, 
									m_prim_col_val=&entity_sk,
									m_upd_cols_lst= upd_cols_project_master
								  );

			/*Delete the records from source_table_x_level*/
			proc sql;
				update &lib_apdm..project_master
					set &upd_cols_project_master 
					where project_sk = &m_tgt_project_sk.; 
			quit;
			%let m_tgt_mpng_sk=&m_tgt_project_sk.;   /*** CSBCR-14112****/
			
			%let trgt_owned_by=;
			proc sql noprint;
			select owned_by_user into :trgt_owned_by from &lib_apdm..project_master 
			where project_sk=&m_tgt_project_sk.;
			quit;
			
			%if &trgt_owned_by ne &owned_by %then %do;
				%dabt_change_project_ownership(project_id=&m_tgt_project_sk., change_owner_to=&owned_by);
			%end;
		%end;

		%else %do;
	
			/*Start: Insert records in the project_master.*/
			%let ins_cols_project_master = ;
			%dabt_cprm_get_col_lst(	m_ds_nm=project_master, 
									m_src_lib_nm=&m_cprm_src_apdm, 
									m_tgt_lib_nm=&lib_apdm, 
									m_exclued_col= project_sk purpose_sk level_sk 
													subject_group_sk project_id, 
									m_col_lst=, 
									m_prim_col_nm=project_sk, 
									m_prim_col_val=&entity_sk,
									m_ins_cols_lst= ins_cols_project_master
								  );
			
			%let m_next_project_sk = .;
			proc sql;
				&apdm_connect_string.; 
					select 
						temp into :m_next_project_sk
					from 
						connection to postgres 
						( 
							select nextval( %nrbquote('&apdm_schema..project_master_project_sk_seq') ) as temp
						);
				&apdm_disconnect_string.; 
			quit;

			%let m_next_project_sk = &m_next_project_sk.;

			%let m_tgt_project_sk = &m_next_project_sk.;
			
			
			%let m_tgt_mpng_sk=&m_next_project_sk.;
			
			proc sql noprint;
		       insert into &lib_apdm..project_master 
				(project_sk, &ins_cols_project_master., purpose_sk, level_sk, 
					subject_group_sk, project_id) 
		       select &m_next_project_sk., &ins_cols_project_master., &m_tgt_purpose_sk., &m_tgt_level_sk., 
					&m_tgt_subject_group_sk., "&m_next_project_sk."
		       	from &m_cprm_src_apdm..project_master src
		       	where src.project_sk = &entity_sk.; 
			quit; 

		

		/*???We need to check and call api that would create a folder on content under project*/
		
		/*%dabt_make_work_area(dir=&project_path., create_dir=&m_tgt_project_sk./application/log, path=prj_path);
		%dabt_make_work_area(dir=&project_path., create_dir=&m_tgt_project_sk./application/scratch, path=prj_path);*/
		proc sql noprint;
		update &lib_apdm..project_master set owned_by_user='NA'
		where project_sk=&m_tgt_project_sk.;
		quit;
		
		%dabt_change_project_ownership(project_id=&m_tgt_project_sk., change_owner_to=&owned_by);
		%end;

	%end;
	%if &m_valid_flg. = &CHECK_FLAG_FALSE. and &syscc. le 4 %then %do;
		%let syscc = 999 ;
	%end;


	/****************************************END OF EXECUTE MODE*****************************************/
%mend dabt_cprm_import_project_master;
