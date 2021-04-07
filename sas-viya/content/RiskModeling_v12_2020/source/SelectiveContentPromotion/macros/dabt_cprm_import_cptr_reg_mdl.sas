/********************************************************************************************************
   Module:  dabt_cprm_import_cptr_reg_mdl

   Called by: dabt_cprm_import_mdl_wrapper.sas
   Function : This macro imports the model scorecard for the source model_sk (entity_sk) from the SRC to
			  TGT machine.

   Parameters: INPUT: 
			1. entity_sk                   : model_sk of the source machine, whose info to be imported.
			2. import_spec_ds_nm           : dataset which contains the entity details to be imported.
			3. import_package_path 	       : Path containing the import package.
			4. import_analysis_report_path : Path where the impor analysis report is generated.
			5. import_analysis_report_ds_nm: The dataset where individual macros will insert the analysis result.
			6. varmap_ds_nm              : Dataset contating the mapping of SRC and TGT variable_sk.
			7. scratch_ds_prefix		   : Prefix used to uniquely identify the scratch datasets.
            8. model_out_clmn_map_ds_nm    : Dataset contating the mapping of SRC and TGT model_output_column_sk.
			9. mode						   : ANALYZE/EXECUTE
*********************************************************************************************************/

%macro dabt_cprm_import_cptr_reg_mdl(
											entity_sk=, 
											import_spec_ds_nm=CPRM_IMPORT_PARAM_LIST_TMP,
											import_package_path=, 
											import_analysis_report_path=, 
											import_analysis_report_ds_nm = cprm_import_analysis_dtl,
											varmap_ds_lib = ,
											varmap_ds_nm = ,
											scratch_ds_prefix = ,
											model_out_clmn_map_ds_nm =,
											mode = 
									);
	
	%let import_analysis_report_path = &import_analysis_report_path.;
	%let import_analysis_report_ds_nm = &import_analysis_report_ds_nm.;

	/*m_cprm_src_apdm- Stores libref of source apdm. dabt_assign_lib macro will assign value to this*/
	/*m_cprm_scr_lib- Stores libref for scratch. dabt_assign_lib macro will assign value to this*/
	/*m_cprm_imp_ctl- Stores libref for control library. This lib will has CPRM_IMPORT_PARAM_LIST_TMP, . dabt_assign_lib macro will assign value to this*/

	%local m_cprm_src_apdm m_cprm_scr_lib m_cprm_imp_ctl m_apdm_lib;
	%dabt_assign_libs(tmp_lib=m_cprm_scr_lib,m_workspace_type=CPRM_IMP,src_lib = m_apdm_lib,
	                                import_analysis_report_path = &import_analysis_report_path., m_cprm_src_apdm_lib= m_cprm_src_apdm, 
	                                m_cprm_ctl_lib = m_cprm_imp_ctl);


	/****************************************START OF ANALYSIS MODE*****************************************/

	%local m_mdl_short_nm
			m_present_in_tgt
			m_present_in_import_package
			m_valid_flg
			m_tgt_model_sk
			m_cnt_sig_var 
			m_entity_type_cd;

	%let m_mdl_short_nm = ;

	%let m_entity_type_cd = MODEL;
	proc sql noprint;
		select src_mdl.model_short_nm
				into :m_mdl_short_nm
			from &m_cprm_src_apdm..model_master src_mdl 
			where src_mdl.model_sk = &entity_sk.;
	quit;
	%let m_mdl_short_nm = %superq(m_mdl_short_nm);

	%let m_present_in_tgt= ;
	%let m_present_in_import_package= ;

	%let m_valid_flg= &CHECK_FLAG_TRUE.;

	/*Start: Check that the model significant variables on SRC are also available on TGT.*/

	proc sql noprint ; 

	  create table &m_cprm_scr_lib..&scratch_ds_prefix._sig_var_map
	 	as select  a.variable_sk,  
	            b.src_var_sk, 
	            b.tgt_var_sk,
	            b.present_in_tgt_flg,
	            c.variable_column_nm,
	            b.present_in_import_package_flg 
	  from  &m_cprm_src_apdm..model_x_scr_input_variable a
	  inner join &m_cprm_src_apdm..variable_master c
	       on a.variable_sk = c.variable_sk 
	  left join &varmap_ds_lib..&varmap_ds_nm b
	       on a.variable_sk = b.src_var_sk 
	  where a.model_sk = &entity_sk; 

	quit; 

	%let m_cnt_sig_var = ;

	proc sql noprint ; 

	  select count(*) into :m_cnt_sig_var 			/*i18NOK:LINE*/
	  	from &m_cprm_scr_lib..&scratch_ds_prefix._sig_var_map;

	quit; 

	%local m_src_var_sk m_var_in_tgt_flg m_var_column_nm 
			m_var_in_imp_pck_flg m_assoc_entity_type_cd m_assoc_entity_nm;

	%do i=1 %to &m_cnt_sig_var ; 

	  data _null_ ; 
	       data_point = &i; 
	       set &m_cprm_scr_lib..&scratch_ds_prefix._sig_var_map POINT=data_point ;
	       call symput("m_src_var_sk",src_var_sk) ;  				/*i18NOK:LINE*/
	       call symput("m_var_in_tgt_flg",present_in_tgt_flg) ;		/*i18NOK:LINE*/
	       call symput("m_tgt_var_sk",tgt_var_sk);					/*i18NOK:LINE*/
	       call symput("m_var_in_imp_pck_flg",present_in_import_package_flg);	/*i18NOK:LINE*/
	       stop;  
	  run;

	  %if  &m_var_in_tgt_flg eq Y %then %do ; 
	       %let m_var_in_tgt_flg = &check_flag_true.; 
	  %end;
	  %else %do ; 
	       %let m_var_in_tgt_flg = &check_flag_false. ; 
	  %end; 
	       
	  %if  &m_var_in_imp_pck_flg eq Y %then %do ; 
	       %let m_var_in_imp_pck_flg = &check_flag_true.; 
	  %end;
	  %else %do ; 
	       %let m_var_in_imp_pck_flg = &check_flag_false. ; 
	  %end; 

	  %let m_var_column_nm = ;

	  proc sql noprint;
	  	select variable_column_nm into :m_var_column_nm 
			from &m_cprm_src_apdm..variable_master 
			where variable_sk = &m_src_var_sk.;
	  quit;

	  %let m_assoc_entity_type_cd = VARIABLE ;
	  %let m_assoc_entity_nm = &m_var_column_nm ;

		%if &mode. = ANALYSE %then %do;
			%dabt_cprm_ins_pre_analysis_dtl (
			                      m_promotion_entity_nm= &m_mdl_short_nm.,
			                      m_promotion_entity_type_cd= &m_entity_type_cd.,
			                      m_assoc_entity_nm= &m_assoc_entity_nm.,
			                      m_assoc_entity_type_cd= &m_assoc_entity_type_cd.,
			                      m_unique_constr_violation_flg=,
			                      m_present_in_tgt_flg=&m_var_in_tgt_flg,
			                      m_present_in_import_package_flg=&m_var_in_imp_pck_flg
			                      );
		%end;

		%if &m_var_in_tgt_flg. eq &check_flag_false. and &m_var_in_imp_pck_flg. = &check_flag_false. and &m_valid_flg. = &CHECK_FLAG_TRUE.
			%then %let m_valid_flg= &CHECK_FLAG_FALSE.;

	%end ;
	%if &m_valid_flg. = &CHECK_FLAG_FALSE. %then %let syscc = 999;
	
	/*End: Check that the model significant variables on SRC are also available on TGT.*/

	/****************************************END OF ANALYSIS MODE*****************************************/

	%if &mode. = EXECUTE and &m_valid_flg = &CHECK_FLAG_TRUE. %then %do;

		/*Get the target model sk*/
		%let m_tgt_model_sk = ;	
		%let m_entity_type_cd = MODEL;
		%dabt_cprm_get_entity_tgt_sk(	entity_sk = &entity_sk.,
										entity_type_cd = &m_entity_type_cd. , 
										src_apdm_lib = &m_cprm_src_apdm., 
										tgt_apdm_lib = &lib_apdm., 
										return_entity_tgt_sk = m_tgt_model_sk);

		%if &m_tgt_model_sk. eq %then %do;
			%let syscc = 999;
			%return;
		%end;

		%local m_out_column_sk_to_reserve m_seq_start_val ins_cols_model_output_column 
				ins_cols_model_x_scr_inp ;
		/*Reserve the number of sk based on the count of records to be inserted from SRC to TGT.*/
		%let m_out_column_sk_to_reserve = ;
		proc sql noprint;
			Select count(*) into :m_out_column_sk_to_reserve 								/*i18NOK:LINE*/
				from &m_cprm_src_apdm..model_output_column where model_sk = &entity_sk.;
		quit;

		%let m_seq_start_val = ;
		%dabt_reserve_sequence_values(m_table_nm= model_output_column, m_no_of_values_to_reserve= &m_out_column_sk_to_reserve. , m_out_starting_sequence_value = m_seq_start_val);
		
		/*Create a table in scratch with scorecard_bin_group_sk mapping. The name of the dataset(model_out_clmn_map)
			will be passed by the wrapper macro.*/

		data &m_cprm_scr_lib..&model_out_clmn_map_ds_nm.;
			set &m_cprm_src_apdm..model_output_column(where=(model_sk=&entity_sk.));
			if _n_ = 1 then do;
				tgt_mdl_output_column_sk + &m_seq_start_val.;
			end;
			else do;
				tgt_mdl_output_column_sk + 1;
			end;
		run;

		/*Start: Insert records in the model_output_column.*/
		%let ins_cols_model_output_column = ;
		%dabt_cprm_get_col_lst(	m_ds_nm=model_output_column, 
								m_src_lib_nm=&m_cprm_src_apdm, 
								m_tgt_lib_nm=&lib_apdm, 
								m_exclued_col= model_output_column_sk model_sk,
								m_col_lst=, 
								m_prim_col_nm=, 
								m_prim_col_val=,
								m_ins_cols_lst= ins_cols_model_output_column
							  );

		proc sql noprint;
		   insert into &lib_apdm..model_output_column 
			(model_output_column_sk, &ins_cols_model_output_column., model_sk) 
		   select tgt_mdl_output_column_sk, &ins_cols_model_output_column., &m_tgt_model_sk.
		       from &m_cprm_scr_lib..&model_out_clmn_map_ds_nm.; 
		quit; 

		/*End: Insert records in the model_output_column.*/

		/*Start: Insert records in the model_x_scr_input_variable.*/

		%let ins_cols_model_x_scr_inp = ;
		%dabt_cprm_get_col_lst(	m_ds_nm=model_x_scr_input_variable, 
								m_src_lib_nm=&m_cprm_src_apdm, 
								m_tgt_lib_nm=&lib_apdm, 
								m_exclued_col= model_sk variable_sk,
								m_col_lst=, 
								m_prim_col_nm=, 
								m_prim_col_val=,
								m_ins_cols_lst= ins_cols_model_x_scr_inp
							  );

		proc sql noprint;
		   insert into &lib_apdm..model_x_scr_input_variable 
			(model_sk, &ins_cols_model_x_scr_inp., variable_sk) 
		   select &m_tgt_model_sk., &ins_cols_model_x_scr_inp., b.tgt_var_sk 
				from &m_cprm_src_apdm..model_x_scr_input_variable a 
			   		inner join &varmap_ds_lib..&varmap_ds_nm. b
						on (a.variable_sk = b.src_var_sk 
							and a.model_sk = &entity_sk.); 
		quit; 
		/*End: Insert records in the scorecard_bin.*/

		/*Start: Code to copy scoring_code and scorecard_grouping_code from SRC to TGT */
		%local m_rel_path_src;
		%let m_rel_path_src = ;
		%dabt_make_work_area(dir=&import_package_path., create_dir=model, path=m_rel_path_src); 

		%let m_rel_path_src = &m_rel_path_src.;/*Absolute path upto &import_folder_nm/model */
		
		
		%local m_rel_path_src_xml;
		%let m_rel_path_src_xml = ;
		%dabt_make_work_area(dir=&m_rel_path_src., create_dir=&entity_sk., path=m_rel_path_src_xml); 

		%let m_rel_path_src_xml = &m_rel_path_src_xml.;/*Absolute path upto &import_folder_nm/model */
		

		/* obtain the apdm library and log and scratch path and library */

		%if %symexist(job_rc)=0 %then %do;
			%global job_rc;
		%end;

		%if %symexist(sqlrc)=0 %then %do;
			%global sqlrc;
		%end;

/* 		%let m_tmp_lib = ; */
/* 		%let m_src_lib = ; */
/* 		%let m_log_path = ; */
/*  */
/* 		%let m_scr_path = ; */
/* 		%let m_scoring_code_path = ; */
/*  */
/* 		%dabt_assign_libs(tmp_lib=m_tmp_lib,src_lib=m_src_lib, log_path= m_log_path,  */
/* 						  m_workspace_type=CHRSTC, m_called_for_abt_processing_flg=N, */
/*  						  m_abt_or_template_sk=&m_tgt_model_sk., scr_path=m_scr_path, */
/* 						  scoring_code_path=m_scoring_code_path); */
/*  */
/*  */
/* 		%let m_scr_path = &m_scr_path.; */
/* 		%let m_tmp_lib = &m_tmp_lib.; */
/* 		%let m_log_path = &m_log_path.; */
/* 		%let m_scoring_code_path = &m_scoring_code_path.; */
/* 		%let m_src_lib = &m_src_lib.; */

		/*selecting model_id using model_sk from TGT machine*/
		proc sql noprint;
			select model_id into :m_tgt_model_id
				from &lib_apdm..model_master
				where model_sk=&m_tgt_model_sk.;
		quit;	 
			
		%let m_tgt_model_id=&m_tgt_model_id;
				 
		%let m_tgt_score_code_file_nm = scoring_code_&m_tgt_model_id..sas;
		%let m_tgt_ds2_score_code_file_nm = cs_rtdm_sas_process_&m_tgt_model_id..ds2;
		%let m_tgt_score_grp_code_file_nm = scorecard_grouping_code_&m_tgt_model_id..sas ;

		/*selecting model_id using model_sk from TGT machine*/
		proc sql noprint;
			select model_id into :m_src_model_id
			from &m_cprm_src_apdm..model_master
			where model_sk=&entity_sk.;
		quit;	 
			
		%let m_src_model_id=&m_src_model_id;
				 
		%let m_src_score_code_file_nm = score_&m_src_model_id..sas;
		%let m_src_score_grp_code_file_nm = scorecard_grouping_code_&m_src_model_id..sas ;

		filename src_scor filesrvc folderpath="&import_package_path" filename= "&m_src_score_code_file_nm." debug=http CD="attachment; filename=&m_src_score_code_file_nm.";/* i18nOK:Line */
		filename tgt_scor filesrvc folderpath="/&m_file_srvr_mdl_folder_path./&m_tgt_model_id." filename= "score.sas" debug=http CD="attachment; filename=score.sas";/* i18nOK:Line */
		
		filename src_grp filesrvc folderpath="&import_package_path" filename= "&m_src_score_grp_code_file_nm." debug=http CD="attachment; filename=&m_src_score_grp_code_file_nm.";/* i18nOK:Line */
		filename tgt_grp filesrvc folderpath="/&m_file_srvr_mdl_folder_path." filename= "&m_tgt_score_grp_code_file_nm" debug=http CD="attachment; filename=&m_tgt_score_grp_code_file_nm";/* i18nOK:Line */


		 %if %sysfunc(fexist(src_scor)) eq 1 and %sysfunc(fexist(tgt_scor)) eq 0 %then %do;

			data _null_;
				rc=fcopy('src_scor', 'tgt_scor'); /* i18NOK:LINE */
				if rc ne 0 then do;
					call symput ('syscc',99); /* i18NOK:LINE */
				end;
			run;
			
		%end;
		%if %sysfunc(fexist(src_grp)) eq 1 and %sysfunc(fexist(tgt_grp)) eq 0 %then %do;

			data _null_;
				rc=fcopy('src_grp', 'tgt_grp'); /* i18NOK:LINE */
				if rc ne 0 then do;
					call symput ('syscc',99); /* i18NOK:LINE */
				end;
			run;
			
		%end;
		
		/*End: Code to copy scoring_code and scorecard_grouping_code from SRC to TGT */

		/*Call macro to populate CS model extension tables.*/
		%if %sysfunc(exist(&lib_apdm..model_master_extension)) %then %do;
			%csbmva_post_capt_reg_mdl(m_key_sk =  &m_tgt_model_sk., src_apdm_lib = &m_cprm_src_apdm., m_called_from=CPRM);
		%end;

	%end;

%mend dabt_cprm_import_cptr_reg_mdl;
