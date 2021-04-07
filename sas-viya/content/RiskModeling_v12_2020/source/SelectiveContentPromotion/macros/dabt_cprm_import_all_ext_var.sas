/*****************************************************************/
/* NAME: dabt_cprm_import_all_ext_var.sas                    	 */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to import external code varib;e
					on target machine  							 */
/*                                                               */
/* Parameters :  entity_sk: External code id of source machine   
				 import_spec_ds_nm: Dataset having
					import specification
				 import_package_path: location of import
					package
				import_analysis_report_path: Location where 
					import analysis report will be created
				import_analysis_report_ds_nm: Name of
						import analysis report 
			    mode: EXECUTE/ANALYSE							 */
/* 			 													 */
/*															 	 */
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Called by dabt_cprm_import_ext_cd_wrapper			 */
/*   															 */
/*                                                               */
/*****************************************************************/
 
/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*10May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/

%macro dabt_cprm_import_all_ext_var(entity_sk=, 
								import_spec_ds_nm=,
								import_package_path=, 
								import_analysis_report_path=, 
								import_analysis_report_ds_nm =,
								mode = ,
								valid_flg_for_ext_code = ,
								valid_flg_for_ext_cd_wapper = 
								);
								
	%let m_valid_flg = &valid_flg_for_ext_code.;/*Valid flag returned by processing of importing external code*/							
	
	/*List of variables used in dabt_assign_libs */
	
	%local m_cprm_scr_lib 
		   m_apdm_lib 
		   m_cprm_src_apdm 
		   m_cprm_imp_ctl;	/* I18NOK:LINE */


	%let m_apdm_lib=;/*Stores libref of target apdm. dabt_assign_lib macro will assign value to this*/
	%let m_cprm_src_apdm=; /*Stores libref of source apdm. dabt_assign_lib macro will assign value to this*/
	%let m_cprm_scr_lib=;  /*Stores libref for scratch. dabt_assign_lib macro will assign value to this*/
	%let m_cprm_imp_ctl=; /*Stores libref for control library. This lib will has CPRM_IMPORT_PARAM_LIST_TMP,dabt_assign_lib macro will assign value to this*/ 

	%dabt_assign_libs(tmp_lib=m_cprm_scr_lib,
						m_workspace_type=CPRM_IMP,
						src_lib = m_apdm_lib,
		                import_analysis_report_path = &import_analysis_report_path., 	/* I18NOK:LINE */
						m_cprm_src_apdm_lib= m_cprm_src_apdm, 
		                m_cprm_ctl_lib = m_cprm_imp_ctl
					 );
	
	%let m_ent_type_cd=EXT_CODE; /* I18NOK:LINE */
	
	%let m_assoc_ent_type_cd=EXT_VAR; /* I18NOK:LINE */

	/*Finding target external_code_sk based on SOA and physical file nm on target machine */

	%local m_tgt_ext_cd_sk 
		   m_src_ext_cd_short_nm;
	
	proc sql noprint;
		select src_ext_cd.external_code_short_nm
			into :m_src_ext_cd_short_nm
		from 
			&m_cprm_src_apdm..external_code_master src_ext_cd
		where src_ext_cd.external_code_sk=&entity_sk.;
	quit;

	%let m_src_ext_cd_short_nm = %superq(m_src_ext_cd_short_nm);/*Name of external code present on Source */

	/* Finding external_code_sk on target machine based on file name and soa on source machine */	
	
	proc sql noprint;
		select tgt_ext_cd.external_code_sk				
			into :m_tgt_ext_cd_sk
			from 
				&m_apdm_lib..external_code_master tgt_ext_cd

			inner join &m_cprm_src_apdm..external_code_master src_ext_cd
					on(tgt_ext_cd.external_code_file_nm=src_ext_cd.external_code_file_nm)

			inner join &m_cprm_src_apdm..level_master src_lvl_master
				on(src_lvl_master.level_sk=src_ext_cd.level_sk)

			inner join &m_apdm_lib..level_master tgt_lvl_master
				on(tgt_lvl_master.level_sk=tgt_ext_cd.level_sk
					and tgt_lvl_master.level_cd=src_lvl_master.level_cd)

			where src_ext_cd.external_code_sk=&entity_sk.;
	quit;

	%let m_tgt_ext_cd_sk = &m_tgt_ext_cd_sk.;/*External code sk of target for the same file name and soa as of source */
	

	%if &m_tgt_ext_cd_sk. eq  %then %do;
		%let m_ext_cd_present_in_tgt_flag = &CHECK_FLAG_FALSE.;
	%end;
	%else %do;
		%let m_ext_cd_present_in_tgt_flag = &CHECK_FLAG_TRUE.;
	%end;

	%if &m_tgt_ext_cd_sk. ne  %then %do; /*External code present in target*/

		/*Start: Validation for external code variable, if the external code variable exists only on TGT.*/

		%let m_ext_var_nm_only_in_tgt = ;
		%let m_ext_var_sk_only_in_tgt = ;

		proc sql noprint;
			select tgt.external_variable_column_nm, tgt.external_variable_sk 
					into 
						:m_ext_var_nm_only_in_tgt separated by ',',			/*i18NOK:LINE*/
						:m_ext_var_sk_only_in_tgt separated by ','			/*i18NOK:LINE*/
				from 
					&m_apdm_lib..external_variable_master tgt
				where 
					tgt.external_variable_column_nm not in
													(select src.external_variable_column_nm from
														&m_cprm_src_apdm..external_variable_master src
															where src.external_code_sk = &entity_sk.)
				and tgt.external_code_sk = &m_tgt_ext_cd_sk.;
		quit;

		%if &m_ext_var_sk_only_in_tgt. ne %then %do;

		%let m_tgt_count = %eval(%sysfunc(countc(%quote(&m_ext_var_sk_only_in_tgt.), ','))+1);		/*i18NOK:LINE*/										

			%let m_present_in_tgt_flag = &CHECK_FLAG_TRUE.;
			%let m_present_in_import_package = &CHECK_FLAG_FALSE.;
			
			%do count = 1 %to &m_tgt_count;

				%let m_tgt_ext_var_nm_tkn = %scan(%quote(&m_ext_var_nm_only_in_tgt.),&count,%str(,)); /* i18nOK:Line */
				%let m_tgt_ext_var_sk_tkn = %scan(%quote(&m_ext_var_sk_only_in_tgt.),&count,%str(,)); /* i18nOK:Line */

				%let m_referred_in_other_entity_flg = ;

				%dabt_cprm_chk_child_ref_exist(parent_table_nm=external_variable_master,
									 parent_column_nm=external_variable_sk,
									 child_column_value=&m_tgt_ext_var_sk_tkn,
									 m_return_child_exists_flg=m_referred_in_other_entity_flg
										 );

				%let m_referred_in_other_entity_flg = &m_referred_in_other_entity_flg.;

				%if &m_referred_in_other_entity_flg eq &CHECK_FLAG_TRUE. %then %do;
					%let m_valid_flg = &CHECK_FLAG_FALSE.;
				%end;

				%if &mode. = ANALYSE %then %do;
					%dabt_cprm_ins_pre_analysis_dtl (
											m_promotion_entity_nm=&m_src_ext_cd_short_nm,
											m_promotion_entity_type_cd=&m_ent_type_cd,
											m_assoc_entity_nm=&m_tgt_ext_var_nm_tkn,
											m_assoc_entity_type_cd=&m_assoc_ent_type_cd,
											m_present_in_tgt_flg= &m_present_in_tgt_flag,
											m_present_in_import_package_flg=&m_present_in_import_package,
											m_referred_in_other_entity_flg=&m_referred_in_other_entity_flg,
											m_unique_constr_violation_flg=,
											m_different_defn_flg= 
										);
				%end;
			%end;/*End: Loop for the list of external code variables present in  TGT*/

		%end ; /*End: If external code variable, if the external code variable exists only on TGT.*/

		/*End: Processing for the list of columns present only in TGT.*/


		/*Start: Processing for the list of external code variables present in both source and TGT.*/
		
		%let m_ext_var_nm_src_and_tgt = ; 
		%let m_ext_var_sk_src_and_tgt = ;

		proc sql noprint;
			select tgt.external_variable_column_nm, tgt.external_variable_sk 
					into :m_ext_var_nm_src_and_tgt separated by ',', :m_ext_var_sk_src_and_tgt separated by ','		/*i18NOK:LINE*/
				from &m_apdm_lib..external_variable_master tgt
				where tgt.external_variable_column_nm in (select src.external_variable_column_nm from
													&m_cprm_src_apdm..external_variable_master src
													where src.external_code_sk = &entity_sk.)
				and tgt.external_code_sk = &m_tgt_ext_cd_sk.;
		quit; 

		%if &m_ext_var_nm_src_and_tgt. ne %then %do;
			%let m_src_and_tgt_count = %eval(%sysfunc(countc(%quote(&m_ext_var_nm_src_and_tgt.), ','))+1);			/*i18NOK:LINE*/

			%let m_present_in_tgt_flag = &CHECK_FLAG_TRUE.;
			%let m_present_in_import_package = &CHECK_FLAG_TRUE;

			%do count = 1 %to &m_src_and_tgt_count;

				%let m_src_tgt_ext_var_nm_tkn = %scan(%quote(&m_ext_var_nm_src_and_tgt.),&count,%str(,)); /* i18nOK:Line */
				%let m_src_tgt_ext_var_sk_tkn = %scan(%quote(&m_ext_var_sk_src_and_tgt.),&count,%str(,)); /* i18nOK:Line */
				
				%if &mode. = ANALYSE %then %do;
					%dabt_cprm_ins_pre_analysis_dtl (
											m_promotion_entity_nm=&m_src_ext_cd_short_nm,
											m_promotion_entity_type_cd=&m_ent_type_cd,
											m_assoc_entity_nm=&m_src_tgt_ext_var_nm_tkn,
											m_assoc_entity_type_cd=&m_assoc_ent_type_cd,
											m_present_in_tgt_flg= &m_present_in_tgt_flag,
											m_present_in_import_package_flg=&m_present_in_import_package,
											m_referred_in_other_entity_flg= ,
											m_unique_constr_violation_flg= ,
											m_different_defn_flg= 
										);
				%end;
			%end;/*End: Loop for the list of external code variables present in both source and TGT*/
		%end;/*End: if for the list of external code variables present in both source and TGT*/

		/*End: Processing for the list of external code variables present in both source and TGT.*/

		/*Start: Processing for the list of external code variables present only in source.*/

		%let m_ext_var_nm_only_in_src = ;
		%let m_ext_var_sk_only_in_src = ;
		
		proc sql noprint;
			select src.external_variable_column_nm, src.external_variable_sk 
					into :m_ext_var_nm_only_in_src separated by ',', :m_ext_var_sk_only_in_src separated by ','		/*i18NOK:LINE*/
				from &m_cprm_src_apdm..external_variable_master src
					where src.external_variable_column_nm not in (select tgt.external_variable_column_nm from
														&m_apdm_lib..external_variable_master tgt
													where tgt.external_code_sk = &m_tgt_ext_cd_sk.)
						and src.external_code_sk = &entity_sk.;
		quit;

		%if &m_ext_var_nm_only_in_src. ne %then %do;
			%let m_src_count = %eval(%sysfunc(countc(%quote(&m_ext_var_nm_only_in_src.), ','))+1);					/*i18NOK:LINE*/
			%do count = 1 %to &m_src_count;
				%let m_src_ext_var_nm_tkn = %scan(%quote(&m_ext_var_nm_only_in_src.),&count,%str(,)); /* i18nOK:Line */
				%let m_src_ext_var_sk_tkn = %scan(%quote(&m_ext_var_sk_only_in_src.),&count,%str(,)); /* i18nOK:Line */

				%let m_present_in_tgt_flag = &CHECK_FLAG_FALSE.;
				%let m_present_in_import_package = &CHECK_FLAG_TRUE;
				
				%if &mode. = ANALYSE %then %do;
					%dabt_cprm_ins_pre_analysis_dtl (
											m_promotion_entity_nm=&m_src_ext_cd_short_nm,
											m_promotion_entity_type_cd=&m_ent_type_cd,
											m_assoc_entity_nm=&m_src_ext_var_nm_tkn,
											m_assoc_entity_type_cd=&m_assoc_ent_type_cd,
											m_present_in_tgt_flg= &m_present_in_tgt_flag,
											m_present_in_import_package_flg=&m_present_in_import_package,
											m_referred_in_other_entity_flg= ,
											m_unique_constr_violation_flg= ,
											m_different_defn_flg= 
										);
				%end;
			%end;/*End:Loop for the list of columns present only in source*/
		%end;/*End: If for the list of columns present only in source*/

		/*End: Processing for the list of external code variables present only in source.*/

	%end;/*End:Validation for external code variables, if the external code  already exists on TGT.*/
	
	%if &m_tgt_ext_cd_sk. eq  %then %do; /*External code present in target*/
		/*Start: Processing for the list of external code variables present only in source.*/

		%let m_ext_var_nm_only_in_src = ;
		%let m_ext_var_sk_only_in_src = ;
		
		proc sql noprint;
			select src.external_variable_column_nm, src.external_variable_sk 
					into :m_ext_var_nm_only_in_src separated by ',', :m_ext_var_sk_only_in_src separated by ','			/*i18NOK:LINE*/
				from &m_cprm_src_apdm..external_variable_master src
				where src.external_code_sk = &entity_sk.;
		quit;

		%if &m_ext_var_nm_only_in_src. ne %then %do;
			%let m_src_count = %eval(%sysfunc(countc(%quote(&m_ext_var_nm_only_in_src.), ','))+1);						/*i18NOK:LINE*/
			%do count = 1 %to &m_src_count;
				%let m_src_ext_var_nm_tkn = %scan(%quote(&m_ext_var_nm_only_in_src.),&count,%str(,)); /* i18nOK:Line */
				%let m_src_ext_var_sk_tkn = %scan(%quote(&m_ext_var_sk_only_in_src.),&count,%str(,)); /* i18nOK:Line */

				%let m_present_in_tgt_flag = &CHECK_FLAG_FALSE.;
				%let m_present_in_import_package = &CHECK_FLAG_TRUE;
				
				%if &mode. = ANALYSE %then %do;
					%dabt_cprm_ins_pre_analysis_dtl (
											m_promotion_entity_nm=&m_src_ext_cd_short_nm,
											m_promotion_entity_type_cd=&m_ent_type_cd,
											m_assoc_entity_nm=&m_src_ext_var_nm_tkn,
											m_assoc_entity_type_cd=&m_assoc_ent_type_cd,
											m_present_in_tgt_flg= &m_present_in_tgt_flag,
											m_present_in_import_package_flg=&m_present_in_import_package,
											m_referred_in_other_entity_flg= ,
											m_unique_constr_violation_flg= ,
											m_different_defn_flg= 
										);
				%end;

			%end;/*End:Loop for the list of columns present only in source*/

		%end;/*End: If for the list of columns present only in source*/

	%end;/*End: Processing for the list of external code variables present only in source.*/

/*Validation is completed for only in tgt , src and tgt, only in src.*/

	%if &mode. = EXECUTE and &m_valid_flg. = &CHECK_FLAG_TRUE. and &syscc le 4 %then %do;
		
		/*Start: If the external code already exists on TGT, 
					then perform following actions.*/
			/*
			1.) Delete those external code variables which are present only on TGT.
			2.) Insert those external code variables which are present only on source.
			3.) Updae those external code variables which are present in both source and TGT.
			*/
					
		%if &m_tgt_ext_cd_sk. ne %then %do;

			/*Execution starts for variables m_ext_var_sk_only_in_tgt */
			%if &m_ext_var_sk_only_in_tgt. ne %then %do; 
				proc sql noprint;
					delete from &m_apdm_lib..external_variable_master tgt 
						where tgt.external_variable_sk in (&m_ext_var_sk_only_in_tgt.);
				quit;
			%end;
			/*Execution starts for variables m_ext_var_sk_only_in_tgt */

			/*Execution starts for variables m_ext_var_sk_src_and_tgt */	
			%if &m_ext_var_sk_src_and_tgt. ne %then %do;

				%let m_src_count = %eval(%sysfunc(countc(%quote(&m_ext_var_sk_src_and_tgt.), ','))+1);			/*i18NOK:LINE*/	

				%do count = 1 %to &m_src_count;
					%let m_src_tgt_ext_var_sk_tkn = %scan(%quote(&m_ext_var_sk_src_and_tgt.),&count,%str(,)); /* i18nOK:Line */
					%let m_src_tgt_ext_var_nm_tkn = %scan(%quote(&m_ext_var_nm_src_and_tgt.),&count,%str(,)); /* i18nOK:Line */
					
					proc sql;
					select src.external_variable_sk into :m_src_ext_var_sk from
						&m_cprm_src_apdm..external_variable_master src
						where src.external_code_sk = &entity_sk. and src.external_variable_column_nm="&m_src_tgt_ext_var_nm_tkn";
					quit;
					
					/*Get the list of external code variables to be updated, from external_variable_master*/
					%let upd_cols_table = ;
					%dabt_cprm_get_col_lst(	m_ds_nm=external_variable_master, 
											m_src_lib_nm=&m_cprm_src_apdm, 
											m_tgt_lib_nm=&m_apdm_lib, 
											m_exclued_col= external_variable_sk external_code_sk, 
											m_col_lst=, 
											m_prim_col_nm=external_variable_sk, 
											m_prim_col_val=&m_src_ext_var_sk, 
											m_upd_cols_lst= upd_cols_table
										  );

					proc sql noprint;
						update &m_apdm_lib..external_variable_master tgt 
							set &upd_cols_table. ,external_code_sk=&m_tgt_ext_cd_sk
							where tgt.external_variable_sk = &m_src_tgt_ext_var_sk_tkn.;
					quit;
				%end;/*Loop end for processing of variables m_ext_var_sk_src_and_tgt */
			%end; /*End:Conditional processing of  variables present on both source and target */

			/*Execution ends for variables m_ext_var_sk_src_and_tgt */

			/*Execution Starts  for variables m_ext_var_sk_only_in_src */

			%if &m_ext_var_sk_only_in_src. ne %then %do;
			
					%let m_src_count = %eval(%sysfunc(countc(%quote(&m_ext_var_sk_only_in_src.), ','))+1);		/*i18NOK:LINE*/

					%do count = 1 %to &m_src_count;
						%let m_src_ext_var_sk_tkn = %scan(%quote(&m_ext_var_sk_only_in_src.),&count,%str(,)); /* i18nOK:Line */

						/*Get the list of external code variables to be updated, from external_variable_master*/
						%let ins_cols_lst = ;

						%dabt_cprm_get_col_lst(	m_ds_nm=external_variable_master, 
												m_src_lib_nm=&m_cprm_src_apdm, 
												m_tgt_lib_nm=&m_apdm_lib, 
												m_exclued_col= external_variable_sk external_code_sk, 
												m_col_lst=, 
												m_prim_col_nm=external_variable_sk, 
												m_prim_col_val=&m_src_ext_var_sk_tkn, 
												m_ins_cols_lst= ins_cols_lst
											  );
											  
						%let m_next_ext_var_sk = .;
						proc sql;
							&apdm_connect_string.; 
								select 
									temp into :m_next_ext_var_sk
								from 
									connection to postgres 
									( 
										select nextval( %nrbquote('&apdm_schema..external_variable_master_external_variable_sk_seq') ) as temp
									);
							&apdm_disconnect_string.; 
						quit;

						%let m_next_ext_var_sk = &m_next_ext_var_sk.;
						
						proc sql noprint ;
							insert into &m_apdm_lib..external_variable_master 
									(external_variable_sk,&ins_cols_lst , external_code_sk)
								select &m_next_ext_var_sk.,&ins_cols_lst, &m_tgt_ext_cd_sk as external_code_sk
										from  &m_cprm_src_apdm..external_variable_master src
									where src.external_variable_sk eq &m_src_ext_var_sk_tkn.;
						quit;
					%end;/*Loop end for processing of variables m_ext_var_sk_only_in_src */
				%end; /*End:Conditional processing of  variables present only on src */
			%end;	
		/*End: If the external code already exists on TGT, 
				then perform following actions.*/

		/*Start: If the external code does not exists on TGT, 
					then insert the external code variables from source machine to the TGT machine.*/
		%else %do;
			%let syscc = 99 ;
		%end;
		/*End: If the external code does not exists on TGT, raise the value of syscc to 99.*/
	%end;/*Execution mode end*/
		

	/**********************************************************************************
	 Error for Import External Code variables on Target machine If validation Failed 
	***********************************************************************************/
	%let &valid_flg_for_ext_cd_wapper = &m_valid_flg.;

	%if (&m_valid_flg = &CHECK_FLAG_FALSE) and (&syscc. le 4) %then %do;
		%let syscc = 9999 ;
	%end; 

%mend dabt_cprm_import_all_ext_var;
