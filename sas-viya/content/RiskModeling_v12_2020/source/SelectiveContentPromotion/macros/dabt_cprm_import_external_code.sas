/*****************************************************************/
/* NAME: dabt_cprm_import_external_code.sas                    	 */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to import external code on target machine  */
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
/* USAGE: 	Called by dabt_cprm_import_ext_cd_wrapper  			 */
/*   															 */
/*                                                               */
/*****************************************************************/
 
/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*10May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/

%macro dabt_cprm_import_external_code(entity_sk=, 
								import_spec_ds_nm=,
								import_package_path=, 
								import_analysis_report_path=, 
								import_analysis_report_ds_nm =,
								mode = ,
								valid_flg_for_ext_code =
								);
	
	%local  m_src_lvl_cd
			m_external_code_file_nm
			m_external_code_file_loc 
			tgt_external_code_sk 
			m_level_short_nm 
			m_external_code_shrt_nm
			m_valid_flg	
			m_tgt_level_sk ;	
			
	/*List of variables used in dabt_assign_libs */
	
	%local m_cprm_scr_lib 
		   m_apdm_lib
		   m_cprm_src_apdm
		   m_cprm_imp_ctl;	

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

	%let m_assoc_ent_type_cd = SOA;/* I18NOK:LINE */
	
	/****************************************************************************************
		Check if level_cd associated with external code in source also exists in target.	
	****************************************************************************************/ 
	
	
	/*	Finding level_cd,external_code_file_nm,external_code_short_nm and  level_short_nm for sepecified external code */

	proc sql noprint; 
		select 
			src_lvl.level_cd,ktrim(kleft(src_ext.external_code_file_nm)),
			ktrim(kleft(src_ext.external_code_short_nm)),ktrim(kleft(src_lvl.level_short_nm))
				into : m_src_lvl_cd,
					 :m_external_code_file_nm,
					 :m_external_code_shrt_nm,
					 :m_level_short_nm
		from 
			&m_cprm_src_apdm..external_code_master src_ext
		inner join 
			&m_cprm_src_apdm..level_master src_lvl
				on(src_lvl.level_sk=src_ext.level_sk)
		where 
			src_ext.external_code_sk = &entity_sk.; 
	quit; 

	%let m_src_lvl_cd = &m_src_lvl_cd.;
	%let m_external_code_file_nm = %superq(m_external_code_file_nm);
	%let m_external_code_shrt_nm = %superq(m_external_code_shrt_nm);
	%let m_level_short_nm = %superq(m_level_short_nm);
	%let m_tgt_level_sk=;
	
	%let m_external_code_file_nm=%sysfunc(kstrip(&m_external_code_file_nm));

	
	%let m_soa_exist_in_tgt_flag= ; /*To check SOA in present in target machine */
	%let m_soa_exist_in_imprt_pkg_flag= ; /*To check SOA in present in import package */

	%dabt_cprm_check_existance(	m_ent_type_cd= SOA, 
								m_ent_unique_identifier_col_val= &m_src_lvl_cd. , 
								m_present_in_tgt_flg= m_soa_exist_in_tgt_flag, 
								m_present_in_import_package_flg= m_soa_exist_in_imprt_pkg_flag,
								m_mode= &mode., 
								m_tgt_apdm_lib= &m_apdm_lib., 
								m_cprm_src_apdm_lib= &m_cprm_src_apdm., 
								m_cprm_ctl_lib= &m_cprm_imp_ctl.,
								m_return_tgt_entity_sk= m_tgt_level_sk);

	%let m_soa_exist_in_imprt_pkg_flag= &m_soa_exist_in_imprt_pkg_flag.;
	%let m_soa_exist_in_tgt_flag= &m_soa_exist_in_tgt_flag.;
	%let m_tgt_level_sk = &m_tgt_level_sk.;
	
	%if (&m_soa_exist_in_tgt_flag. = &CHECK_FLAG_TRUE. or &m_soa_exist_in_imprt_pkg_flag. = &CHECK_FLAG_TRUE.)%then %do;
		%let m_valid_flg= &CHECK_FLAG_TRUE.;
	%end;
	%else %do;
		%let m_valid_flg= &CHECK_FLAG_FALSE.;
		/*Level code doesnot exists.*/
	%end;
	
	%if &mode. = ANALYSE %then %do;
		%dabt_cprm_ins_pre_analysis_dtl (m_promotion_entity_nm=&m_external_code_shrt_nm,
									m_promotion_entity_type_cd=&m_ent_type_cd,
									m_assoc_entity_type_cd=&m_assoc_ent_type_cd,
									m_assoc_entity_nm=&m_level_short_nm,
									m_present_in_tgt_flg=&m_soa_exist_in_tgt_flag,
									m_present_in_import_package_flg= &m_soa_exist_in_imprt_pkg_flag,
									m_referred_in_other_entity_flg=,
									m_unique_constr_violation_flg=,
									m_different_defn_flg=
									);
	%end;
	
	/****************************************************************************************
		Check if ext code with same  physical file nm and same soa already exists in target.	
	****************************************************************************************/ 
	
	%local m_tgt_entity_sk;
	%let m_tgt_level_cd =;
	%let m_external_code_short_nm=;

	proc sql noprint;
		select tgt_ext_cd.external_code_sk
			into :m_tgt_entity_sk
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
	%let m_tgt_entity_sk = &m_tgt_entity_sk.;
	
	%if &m_tgt_entity_sk. eq  %then %do;
		%let m_file_soa_exist_in_tgt_flag = &CHECK_FLAG_FALSE.;
	%end;
	%else %do;
		%let m_file_soa_exist_in_tgt_flag = &CHECK_FLAG_TRUE.;
	%end;

	/************************************************************************************************************
		Unique constraint for External Code Master of Source and Target based on external code display name
	*************************************************************************************************************/
 	
	%let m_unique_constr_violation_flag = &CHECK_FLAG_FALSE.;
	
		%let m_ent_constraint_col_val = ;

		proc sql noprint; 
			select tgt_tbl.external_code_sk 
				into :m_ent_constraint_col_val
			from 
				&m_cprm_src_apdm..external_code_master src_tbl
			inner join &m_apdm_lib..external_code_master tgt_tbl 
				on kupcase(src_tbl.external_code_short_nm) = kupcase(tgt_tbl.external_code_short_nm)
				and src_tbl.external_code_sk eq &entity_sk.
				%if &m_tgt_entity_sk. ne  %then %do;
					and tgt_tbl.external_code_sk ne &m_tgt_entity_sk.
				%end;
			;
		quit; 

		%let m_ent_constraint_col_val = &m_ent_constraint_col_val.;

		%if &m_ent_constraint_col_val. ne  %then %do;
			%let m_unique_constr_violation_flag = &CHECK_FLAG_TRUE.;
			%let m_valid_flg= &CHECK_FLAG_FALSE.;
			/*unique constraint fails and insert in table.*/
		%end;
		
		%if &mode. = ANALYSE %then %do;
			%dabt_cprm_ins_pre_analysis_dtl (m_promotion_entity_nm=&m_external_code_shrt_nm,
											m_promotion_entity_type_cd=&m_ent_type_cd,
											m_assoc_entity_type_cd=&m_ent_type_cd,
											m_assoc_entity_nm=&m_external_code_shrt_nm ,
											m_present_in_tgt_flg=&m_file_soa_exist_in_tgt_flag,
											m_present_in_import_package_flg= &CHECK_FLAG_TRUE,
											m_referred_in_other_entity_flg=,
											m_unique_constr_violation_flg=	&m_unique_constr_violation_flag,
											m_different_defn_flg=
											);
		%end;

	/*EXECUTION STARTS*/
	
	%if &mode = EXECUTE and &m_valid_flg. = &CHECK_FLAG_TRUE. and &syscc le 4 %then %do;
	
		/*Defining source file and desination external file name and it's path.*/

		*filename src "&import_package_path./external_code/&m_external_code_file_nm." ; /* i18NOK:LINE */
		filename src filesrvc folderpath="&import_package_path." filename= "&m_external_code_file_nm." debug=http CD="attachment; filename=&m_external_code_file_nm.";  /* i18nOK:Line */
		*filename dest "&DABT_EXTERNAL_CODE_PATH_LOCATION./&m_external_code_file_nm." ; /* i18NOK:LINE */
		filename dest filesrvc folderpath="&DABT_EXTERNAL_CODE_PATH_LOCATION." filename= "&m_external_code_file_nm." debug=http CD="attachment; filename=&m_external_code_file_nm.";  /* i18nOK:Line */
		
		/*Copying external code file from source to destination.*/

		data _null_;
			rc=fcopy('src', 'dest'); /* i18NOK:LINE */
		run;
		
		%if &m_soa_exist_in_tgt_flag. eq &CHECK_FLAG_TRUE. AND &m_file_soa_exist_in_tgt_flag. eq &CHECK_FLAG_TRUE. %then %do;
		
			%let upd_cols_lst = ;
			%dabt_cprm_get_col_lst(	m_ds_nm=external_code_master, 
									m_src_lib_nm=&m_cprm_src_apdm, 
									m_tgt_lib_nm=&m_apdm_lib, 
									m_exclued_col= external_code_file_loc external_code_sk external_code_id level_sk,
									m_col_lst=, 
									m_prim_col_nm=external_code_sk, 
									m_prim_col_val=&entity_sk,
									m_ins_cols_lst=,
									m_upd_cols_lst=upd_cols_lst
			);

			/*Updating already present record &m_apdm_lib..external_code_master*/

			proc sql noprint ; 
				update &m_apdm_lib..external_code_master tgt	
				set &upd_cols_lst ,
					external_code_file_loc = '&DABT_EXTERNAL_CODE_PATH_LOCATION' /* i18nok:line */
				where tgt.external_code_sk = &m_tgt_entity_sk  ; 
			quit;

		%end;/*Ending block for updation in &m_apdm_lib..external_code_master*/

		%else %do;/*Inserting new record in &m_apdm_lib..external_code_master */
		
			%let ins_cols_lst = ;
			%dabt_cprm_get_col_lst(	m_ds_nm=external_code_master, 
									m_src_lib_nm=&m_cprm_src_apdm, 
									m_tgt_lib_nm=&m_apdm_lib, 
									m_exclued_col= external_code_file_loc external_code_sk external_code_id level_sk,
									m_col_lst=, 
									m_prim_col_nm=, 
									m_prim_col_val=,
									m_ins_cols_lst=ins_cols_lst,
									m_upd_cols_lst=
			);
			
			proc sql noprint ;
			       insert into &m_apdm_lib..external_code_master
					(&ins_cols_lst, external_code_file_loc,external_code_id,level_sk) 
				       select 
						 &ins_cols_lst, 
							'&DABT_EXTERNAL_CODE_PATH_LOCATION' as external_code_file_loc, /* i18nok:line */
							"src_&entity_sk." as external_code_id,  /* i18nok:line */
						/*External code id is hardcoded "src_&entity_sk." to avoid unique constraint failure on tgt machine*/
							&m_tgt_level_sk. as level_sk
				       from 
							&m_cprm_src_apdm..external_code_master src 
				       where 
							src.external_code_sk = &entity_sk ; 
			quit;

			/*Updating external_code_id based on last insert record in external_code_master on tgt machine */

			proc sql noprint;
				select tgt.external_code_sk  into :tgt_external_code_sk
					from &m_apdm_lib..external_code_master tgt
					where tgt.external_code_id = "src_&entity_sk.";  /*i18NOK:LINE*/
			quit;

			%let tgt_external_code_sk = &tgt_external_code_sk.;

			proc sql noprint;
				update &m_apdm_lib..external_code_master
					set external_code_id = "&tgt_external_code_sk." 
				where external_code_sk = &tgt_external_code_sk;
			quit;

		%end;/*Ending block for Insertion in &m_apdm_lib..external_code_master*/

	%end;/*Ending block of insertion or updation in tgt machine*/

	/*****************************************************************************
		 Error for Import External CODE on Target machine If validation Failed 
	******************************************************************************/

	%let &valid_flg_for_ext_code = &m_valid_flg.;/*Returning m_valid_flg to continue execution for external code variable */

	%if (&m_valid_flg = &CHECK_FLAG_FALSE) and (&syscc. le 4) %then %do;
		%let syscc = 9999 ;
	%end; 

%mend dabt_cprm_import_external_code;
