/********************************************************************************************************
   	Module		:  dabt_cprm_imprt_var_only_in_tgt

   	Function	:  This macro for a given project validates all those variables which are present in
					target machine.
					It deletes all those variables which are present on targte machine and are not used anywhere.
					For the variable present only on target machine and used somewhere, it will give error.
					Returns the validation result in a macro variable based on following logic:
						For ANALYSE mode, validation will be false if the variable present only on target machine and used somewhere.
						For EXECUTE mode, validation will be false if the variable present only on target machine and used somewhere.
					Return return_validation_rslt_flg as Y/N 

   	Parameters	:	NAME							TYPE		DESC
					src_prject_sk					INPUT		-> Parent Entity(Project) that need to be imported .Its Key on the source machine
					m_only_in_tgt_var_sk			INPUT		-> Variable Sk.Its key on the target machine
					mode							INPUT		-> ANALYSE / EXECUTE
					return_var_validation_rslt_flg	OUTPUT		-> Name of the macro variable in validation result will be returned.
																	Possible values: 
																		Y - Validation successful
																		N - Validation failed
*********************************************************************************************************/

%macro dabt_cprm_imprt_var_only_in_tgt(	src_project_sk=,
										m_only_in_tgt_var_sk=,
										mode =,
										return_var_validation_rslt_flg=
									);

	%local 	m_referred_in_other_entity_flg
			m_only_in_tgt_valid_flag ;

	/*Finding project_short_nm for specified project_sk*/

	proc sql noprint;
		select project_short_nm 
			into :src_project_short_nm
			from &m_cprm_src_apdm..project_master
			where project_sk=&src_project_sk.;
	quit;
	
	%let src_project_short_nm = %superq(src_project_short_nm);

	/*Finding variable_short_nm for specified variable*/

	proc sql noprint;
		select variable_short_nm 
			into :tgt_variable_short_nm
			from &m_apdm_lib..modeling_abt_x_variable
			where variable_sk=&m_only_in_tgt_var_sk.;
	quit;

	%let tgt_variable_short_nm = %superq(tgt_variable_short_nm);

	%let m_only_in_tgt_valid_flag = ;

	%let m_referred_in_other_entity_flg = &CHECK_FLAG_FALSE. ;

	%dabt_cprm_chk_child_ref_exist(parent_table_nm=VARIABLE_MASTER,
									 parent_column_nm=VARIABLE_SK,
									 child_column_value=&m_only_in_tgt_var_sk,
									 m_return_child_exists_flg=m_referred_in_other_entity_flg
									 );

	%let m_referred_in_other_entity_flg = &m_referred_in_other_entity_flg.;

	%if &mode. = ANALYSE %then %do;
		%dabt_cprm_ins_pre_analysis_dtl (
								m_promotion_entity_nm=&src_project_short_nm,
								m_promotion_entity_type_cd=PROJECT,
								m_assoc_entity_nm=&tgt_variable_short_nm,
								m_assoc_entity_type_cd=VARIABLE,		/*Need to Discuss ?????*/
								m_present_in_tgt_flg= &CHECK_FLAG_TRUE,
								m_present_in_import_package_flg=&CHECK_FLAG_FALSE,
								m_referred_in_other_entity_flg=&m_referred_in_other_entity_flg
								);
	%end;

	%let m_referred_in_other_entity_flg = &m_referred_in_other_entity_flg.;

	%if &m_referred_in_other_entity_flg ne &CHECK_FLAG_TRUE. %then %do;
		%let m_only_in_tgt_valid_flag = &CHECK_FLAG_TRUE.;
	%end;
	%else %do;
		%let m_only_in_tgt_valid_flag = &CHECK_FLAG_FALSE.;	
	%end;

	***********************************************************;
	*Execution Starts for Variables Present only in target.
	***********************************************************;
	
	%if %kupcase("&mode.") eq "EXECUTE" and  &m_only_in_tgt_valid_flag. eq &CHECK_FLAG_TRUE  and &syscc le 4 %then %do;		/*i18NOK:LINE*/
		/*List of tables from which data has to delete*/

	/*				1) Delete from BEHAVIORAL_VARIABLE*/
	/*				2) Delete from DERIVED_VAR_X_EXPRESSION_VAR*/
	/*				3) Delete from DERIVED_VARIABLE*/
	/*				4) Delete from EXTERNAL_VARIABLE*/
	/*				5) Delete from RECENT_VARIABLE*/
	/*				6) Delete from SUPPLEMENTARY_VARIABLE*/
	/*				7) Delete from MODELING_ABT_X_VARIABLE*/
	/*				8) Delete from VARIABLE_DIM_ATTRIBUTE_FILTER*/
	/*				9) Delete from Derived_var_all_expression_var*/
	

		/*Delete from BEHAVIORAL_VARIABLE*/

		proc sql noprint;
			delete from &m_apdm_lib..behavioral_variable
			where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

		/*Delete from DERIVED_VAR_X_EXPRESSION_VAR*/

		proc sql noprint;
			delete from &m_apdm_lib..derived_var_x_expression_var
			where derived_variable_sk = &m_only_in_tgt_var_sk.;
		quit;
		
		/*expression_variable_sk as Foreign key.To delete variable_sk from variable_master delete from this table*/
		proc sql noprint;
			delete from &m_apdm_lib..derived_var_x_expression_var
			where expression_variable_sk = &m_only_in_tgt_var_sk.;
		quit;

		/*Delete from derived_var_all_expression_var*/

		proc sql noprint;
			delete from &m_apdm_lib..derived_var_all_expression_var
			where derived_variable_sk = &m_only_in_tgt_var_sk.;
		quit;
		
		/*expression_variable_sk as Foreign key.To delete variable_sk from variable_master delete from this table*/
		proc sql noprint;
			delete from &m_apdm_lib..derived_var_all_expression_var
			where expression_variable_sk = &m_only_in_tgt_var_sk.;
		quit;
		

		/*Delete from DERIVED_VARIABLE*/

		proc sql noprint;
			delete from &m_apdm_lib..derived_variable
			where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

		/*Delete from EXTERNAL_VARIABLE*/

		proc sql noprint;
			delete from &m_apdm_lib..external_variable
			where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

		/*Delete from RECENT_VARIABLE*/

		proc sql noprint;
			delete from &m_apdm_lib..recent_variable
			where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

		/*Delete from SUPPLEMENTARY_VARIABLE*/

		proc sql noprint;
			delete from &m_apdm_lib..supplementary_variable
			where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

		/*Delete from MODELING_ABT_X_VARIABLE*/

		proc sql noprint;
			delete from &m_apdm_lib..Modeling_abt_x_variable
			where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

		
		/*Delete from VARIABLE_DIM_ATTRIBUTE_FILTER*/

		proc sql noprint;
			delete from &m_apdm_lib..variable_dim_attribute_filter
			where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

		/*Delete from VARIABLE_MASTER*/

		proc sql noprint;
			delete from &m_apdm_lib..variable_master
				where variable_sk=&m_only_in_tgt_var_sk.;
		quit;

	%end;/*Execution Mode ends...*/


	
*==================================================================;
* Set the output parameters ;
*==================================================================;

%let &return_var_validation_rslt_flg. = &m_only_in_tgt_valid_flag.;



%mend dabt_cprm_imprt_var_only_in_tgt;
