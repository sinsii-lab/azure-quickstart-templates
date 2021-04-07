/********************************************************************************************************
   	Module		:  dabt_cprm_imprt_var_only_in_src

   	Function	:  This macro for a given project validates all those variables which are present only in
					source machine.
					It validates following prerequistes for promoting variable present only on source machine.
						1: Check if the corresponding data source is available on the TGT or in the import package;
						2: Check if the corresponding column is available on the TGT or in the import package;
						3:Check if the corresponding column  values are available on the TGT or in the import package;
						4:Check if corresponding time period is available on the TGT or in the import package;
						5: Check if corresponding as of time is available on the TGT or in the import package;
						6: Check if corresponding external variable is available on the TGT or in the import package;
						7: Check if associated level with variable is present o on the TGT or in the import package;

					Returns the validation result in a macro variable based on following logic:
						For ANALYSE mode, validation will be false if the prerequistes are not present on target machine
						For EXECUTE mode, validation will be false if  the prerequistes are not present on target machine
					Return return_validation_rslt_flg as Y/N 

   	Parameters	:	NAME							TYPE		DESC
					src_project_sk					INPUT		-> 	Parent Entity(Project) that need to be imported .Its Key on the source machine
					m_only_in_tgt_var_sk			INPUT		-> 	Variable Sk.Its key on the target machine
					tgt_project_sk						INPUT		-> 	Project_sk on target  machine
					varmap_ds_lib 					INPUT		->	Libref of table having src and tgt variable map table
					varmap_ds_nm 					INPUT		->	Name of table having src and tgt variable map table								
					mode							INPUT		-> 	ANALYSE / EXECUTE
					return_var_validation_rslt_flg	OUTPUT		-> 	Name of the macro variable in validation result will be returned.
																	Possible values: 
																		Y - Validation successful
																		N - Validation failed
*********************************************************************************************************/


%macro dabt_cprm_imprt_var_only_in_src(	src_project_sk	=,
										m_only_src_var_sk =, 
										mode =,
										tgt_project_sk=,
										varmap_ds_lib =	,
										varmap_ds_nm =	,
										return_var_validation_rslt_flg =
									);
	%local	m_only_in_src_valid_flag
			m_var_type_cd 
			m_src_prj_shrt_nm 
			src_variable_short_nm
			; 

	%let m_only_in_src_valid_flag=;

	/*Finding project_short_nm for specified project_sk*/
	%let m_src_prj_shrt_nm = ;
	proc sql noprint;
		select project_short_nm into:
			m_src_prj_shrt_nm 
			from &m_cprm_src_apdm..project_master src_prj
		where src_prj.project_sk=&src_project_sk.;

	quit ;

	%let m_src_prj_shrt_nm = %superq(m_src_prj_shrt_nm);

	/*Finding variable_short_nm for specified variable*/
	
	%let src_variable_short_nm = ;
	proc sql noprint;
		select variable_short_nm 
			into :src_variable_short_nm
			from &m_cprm_src_apdm..modeling_abt_x_variable
			where variable_sk=&m_only_src_var_sk.;
	quit;

	%let src_variable_short_nm = %superq(src_variable_short_nm);

	
	**************************************************************************************************************;
	*Pre Analysis Check 1: Check if the corresponding data source is available on the TGT or in the import package;
	**************************************************************************************************************;
	%local 	m_src_master_table_sk
		master_table_vld_rslt_flg
		m_tgt_master_table_sk
		m_master_table_sk_ds_nm;

	/*Finding out source_table_sk associated with variable. */

	proc sql noprint;
		select src_tbl.source_table_sk 
				into: m_src_master_table_sk
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..source_table_master src_tbl
					on(src_var.source_table_sk=src_tbl.source_table_sk)
			where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	%let m_src_master_table_sk = &m_src_master_table_sk.;
	%let master_table_vld_rslt_flg = ;
	%let m_tgt_master_table_sk = ;

	/*Table stores master table sk onn target machine*/
	%let m_master_table_sk_ds_nm = _&src_project_sk._src_tbl_map ;
	
	%if &m_src_master_table_sk. ne  %then %do;
		
		%dabt_cprm_check_parent_entity (entity_sk 					= &src_project_sk.,
										entity_type_cd 				= PROJECT,
										assoc_entity_sk 			= &m_src_master_table_sk.,
										assoc_entity_type_cd 		= DATASOURCE,
										src_apdm_lib 				= &m_cprm_src_apdm.,
										tgt_apdm_lib 				= &m_apdm_lib.,
										mode 						= &mode.,
										return_assoc_entity_tgt_sk 	= m_tgt_master_table_sk,
										return_validation_rslt_flg 	= master_table_vld_rslt_flg
										);
		%let master_table_vld_rslt_flg 	= &master_table_vld_rslt_flg.;
		%let m_tgt_master_table_sk 		= &m_tgt_master_table_sk.;

		%if &master_table_vld_rslt_flg. ne &CHECK_FLAG_TRUE. %then %do;
			%let master_table_vld_rslt_flg = &CHECK_FLAG_FALSE.;
			%let m_tgt_master_table_sk 	=	.;
		%end;

		/*Creating maping table of source_table_sk on source and target machine*/
/*i18NOK:BEGIN*/
		%if %kupcase("&mode.") eq "EXECUTE" and &master_table_vld_rslt_flg. eq &CHECK_FLAG_TRUE %then %do;
			data &m_cprm_scr_lib..&m_master_table_sk_ds_nm.;
				src_master_tbl_sk = symgetn('m_src_master_table_sk');
				src_variable_sk = symgetn('m_only_src_var_sk');
				tgt_master_tbl_sk = symgetn('m_tgt_master_table_sk');
			run;
		%end;
/*i18NOK:END*/
	%end;/*Condition check for source_table_sk ne blank*/

	**********************************************************************************************************;
	*Pre Analysis Check 2: Check if the corresponding column is available on the TGT or in the import package;
	**********************************************************************************************************;
	%local 	m_dis_source_column_sk
			m_tbl_col_vld_rslt_flg
			m_tgt_tbl_col_sk
			m_tbl_col_sk_ds_nm
			m_col_exist_in_tgt_flg	;



	/*Creating src_clmn_sk_tmp table to store distinct source column sk.*/
	proc sql noprint;
		create table &m_cprm_scr_lib.._&src_project_sk._src_clmn_sk_tmp
			(source_column_sk num(11));
	quit;

	/*For Behavioral Variable - MEASURE_SOURCE_COLUMN_SK*/

	proc sql noprint;
		insert into &m_cprm_scr_lib.._&src_project_sk._src_clmn_sk_tmp 
				select src_col.source_column_sk
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..behavioral_variable src_beh_var
					on(src_beh_var.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..Source_column_master src_col
					on(src_beh_var.measure_source_column_sk=src_col.source_column_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	/*For SUPPLEMENTARY VARIABLE - SELECT_SOURCE_COLUMN_SK*/

	proc sql noprint;
		insert into &m_cprm_scr_lib.._&src_project_sk._src_clmn_sk_tmp 
				select src_col.source_column_sk
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..supplementary_variable src_sup_var
					on(src_sup_var.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..Source_column_master src_col
					on(src_sup_var.SELECT_SOURCE_COLUMN_SK=src_col.source_column_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	/*For RECENT VARIABLE - ORDER_BY_DATE_SOURCE_COLUMN_SK*/

	proc sql noprint;
		insert into &m_cprm_scr_lib.._&src_project_sk._src_clmn_sk_tmp
				select src_col.source_column_sk
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..recent_variable src_rec_var
					on(src_rec_var.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..Source_column_master src_col
					on(src_rec_var.order_by_date_source_column_sk=src_col.source_column_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	/*For RECENT VARIABLE - SELECT_SOURCE_COLUMN_SK*/

	proc sql noprint;
		insert into &m_cprm_scr_lib.._&src_project_sk._src_clmn_sk_tmp 
				select src_col.source_column_sk
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..recent_variable src_rec_var
					on(src_rec_var.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..Source_column_master src_col
					on(src_rec_var.select_source_column_sk=src_col.source_column_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	%let m_dis_source_column_sk = .;

	/*Selecting distinct source_column_sk to analyze*/
	proc sql noprint;
		select distinct(source_column_sk) 
			into :m_dis_source_column_sk  separated by ',' /* I18NOK:LINE */
			from &m_cprm_scr_lib.._&src_project_sk._src_clmn_sk_tmp;
	quit;
	%let m_dis_source_column_sk = &m_dis_source_column_sk.;/*List of distinct source_column_sk associated with variable*/
	
	%dabt_drop_table(m_table_nm=&m_cprm_scr_lib.._&src_project_sk._src_clmn_sk_tmp);

	/*For every source column check available on the TGT or in the import package*/

			/*Table stores  maping table of source_column_sk on source and target machine*/
		%let m_tbl_col_sk_ds_nm = _&src_project_sk._src_col_map ;

		/*Creating maping table of source_column_sk on source and target machine*/
		%if %kupcase("&mode.") eq "EXECUTE" %then %do;	/*i18NOK:LINE*/
			proc sql noprint;
				create table &m_cprm_scr_lib..&m_tbl_col_sk_ds_nm.
					(src_tbl_col_sk num(11) ,src_variable_sk num(11),tgt_tbl_col_sk num(11));
			quit; 
		%end;

	%if &m_dis_source_column_sk. ne %str(.) %then %do;

		%let m_src_clmn_cnt = %eval(%sysfunc(countc(%quote(&m_dis_source_column_sk.), %str(,)))+1);	/*i18NOK:LINE*/



		%let m_tbl_col_vld_rslt_flg = ;
		%let m_tgt_tbl_col_sk = ; /*source_column_sk of same column of source machine on target machine*/

		
		/*Set flag to insert into pre analysis detail table*/

		%if %kupcase("&mode.") eq "ANALYSE" %then %do;			/*i18NOK:LINE*/
			%let m_populate_pre_anlys_ds_flg = &CHECK_FLAG_TRUE;
		%end;
		%else %do;
			%let m_populate_pre_anlys_ds_flg = &CHECK_FLAG_FALSE;
		%end;

		%do col_cnt = 1 %to &m_src_clmn_cnt.;

			%let m_src_clmn_sk_tkn = %scan(%quote(&m_dis_source_column_sk.),&col_cnt,%str(,)); /* i18nOK:Line */

			%dabt_cprm_validate_column(master_entity_sk 			= &src_project_sk.,
										master_entity_type_cd 		= PROJECT ,
										column_src_sk 				= &m_src_clmn_sk_tkn.,
										src_apdm_lib 				= &m_cprm_src_apdm.,
										tgt_apdm_lib 				= &m_apdm_lib.,
										mode						= &mode.,
										populate_pre_anlys_ds_flg	= &m_populate_pre_anlys_ds_flg.,
										return_column_tgt_sk 		= m_tgt_tbl_col_sk,
										return_validation_rslt_flg 	= m_tbl_col_vld_rslt_flg
										);

			%let m_tgt_tbl_col_sk = &m_tgt_tbl_col_sk.;
			%let m_tbl_col_vld_rslt_flg = &m_tbl_col_vld_rslt_flg.;

			%if &m_tbl_col_vld_rslt_flg. ne &CHECK_FLAG_TRUE. %then %do;
				%let m_tbl_col_vld_rslt_flg = &CHECK_FLAG_FALSE.;
				%let m_tgt_tbl_col_sk = .;
			%end;

			%if %kupcase("&mode.") eq "EXECUTE" and  &m_tbl_col_vld_rslt_flg. eq &CHECK_FLAG_TRUE  %then %do;	/*i18NOK:LINE*/
				proc sql noprint;
					insert into &m_cprm_scr_lib..&m_tbl_col_sk_ds_nm.  
						values (&m_src_clmn_sk_tkn.,&m_only_src_var_sk.,&m_tgt_tbl_col_sk.);
				quit;
			%end;
		%end;/*Loop end for every source_column_sk*/

	%end;/*Condition check for source_column_sk ne blank*/
	
	****************************************************************************************************************;
	*Pre Analysis Check 3:Check if the corresponding column  values are available on the TGT or in the import package;
	*****************************************************************************************************************;

	%local 	m_src_dim_attrib_sk
			m_dim_attrib_vld_rslt_flg
			m_tgt_dim_attrib_sk
			m_dim_attrib_sk_ds_nm
			;
	/*For Variable_dim_attribute_filter*/
	%let m_src_dim_attrib_sk = .;

	proc sql noprint;
			select distinct(src_dim_atrb.dim_attribute_value_sk)
					into :m_src_dim_attrib_sk separated by ',' /* I18NOK:LINE */
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..variable_dim_attribute_filter src_dim_fltr
					on(src_dim_fltr.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..source_dim_attrib_value_master src_dim_atrb
					on(src_dim_fltr.dim_attribute_value_sk=src_dim_atrb.dim_attribute_value_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	%let m_src_dim_attrib_sk = &m_src_dim_attrib_sk.;
	/*Table stores  maping table of dim_attrib_sk on source and target machine*/
	%let m_dim_attrib_sk_ds_nm = _&src_project_sk._src_dim_map;

	%if &m_src_dim_attrib_sk. ne %str(.) %then %do;

		%let m_src_dim_atrb_cnt = %eval(%sysfunc(countc(%quote(&m_src_dim_attrib_sk.), %str(,)))+1);	/*i18NOK:LINE*/

		%if %kupcase("&mode.") eq "ANALYSE" %then %do;		/*i18NOK:LINE*/
			%let m_populate_pre_anlys_ds_flg = &CHECK_FLAG_TRUE;
		%end;
		%else %do;
			%let m_populate_pre_anlys_ds_flg = &CHECK_FLAG_FALSE;
		%end;

		%let m_tgt_dim_attrib_sk = ; /*dim_attribute_value_sk of same as of column value on target machine*/
		%let m_dim_attrib_vld_rslt_flg = ;

		

		/*Creating maping table of dim_attrib_sk on source and target machine*/
		%if %kupcase("&mode.") eq "EXECUTE" %then %do;		/*i18NOK:LINE*/
			proc sql noprint;
				create table &m_cprm_scr_lib..&m_dim_attrib_sk_ds_nm.
					(src_dim_attrib_sk num(11) ,src_variable_sk num(11),tgt_dim_attrib_sk num(11));
			quit; 
		%end;

		%do dim_atrb_cnt = 1 %to &m_src_dim_atrb_cnt.;

			%let m_src_dim_attrib_sk_tkn = %scan(%quote(&m_src_dim_attrib_sk.),&dim_atrb_cnt,%str(,)); /* i18nOK:Line */
			
			%dabt_cprm_validate_column_value ( 	master_entity_sk 		= &src_project_sk. ,
											master_entity_type_cd 		= PROJECT,
											column_value_src_sk 		= &m_src_dim_attrib_sk_tkn.,
											src_apdm_lib 				= &m_cprm_src_apdm.,
											tgt_apdm_lib 				= &m_apdm_lib.,
											mode 						= &mode.,
											populate_pre_anlys_ds_flg 	= &m_populate_pre_anlys_ds_flg.,
											return_column_value_tgt_sk 	= m_tgt_dim_attrib_sk,
											return_validation_rslt_flg 	= m_dim_attrib_vld_rslt_flg 
											);
			
			%let m_tgt_dim_attrib_sk = &m_tgt_dim_attrib_sk.;
			%let m_dim_attrib_vld_rslt_flg = &m_dim_attrib_vld_rslt_flg.;

			%if &m_dim_attrib_vld_rslt_flg. ne &CHECK_FLAG_TRUE. %then %do;
				%let m_dim_attrib_vld_rslt_flg = &CHECK_FLAG_FALSE.;
				%let m_tgt_dim_attrib_sk = .;
			%end;

			%if %kupcase("&mode.") eq "EXECUTE"  and &m_dim_attrib_vld_rslt_flg. eq &CHECK_FLAG_TRUE  %then %do;	/*i18NOK:LINE*/
				proc sql noprint;
					insert into &m_cprm_scr_lib..&m_dim_attrib_sk_ds_nm.  
						values (&m_src_dim_attrib_sk_tkn.,&m_only_src_var_sk.,&m_tgt_dim_attrib_sk.);
				quit;
			%end;
		%end;/*Loop end for every dim_attribute_value_sk*/

	%end;/*Condition check for dim_attribute_value_sk ne blank*/
	
	*********************************************************************************************************;
	*Pre Analysis Check 4:Check if corresponding time period is available on the TGT or in the import package;
	*********************************************************************************************************;
	%local 	m_dis_tm_prd_sk
			m_tm_prd_vld_rslt_flg
			m_tgt_tm_prd_sk
			m_tm_prd_sk_ds_nm
			m_tm_prd_exist_in_tgt_flg;
			

	proc sql noprint;
		create table &m_cprm_scr_lib.._&src_project_sk._tm_prd_sk_tmp 
			(time_period_sk num(11));
	quit;

	/*For Behavioral Variable*/

	proc sql noprint;
		insert into &m_cprm_scr_lib.._&src_project_sk._tm_prd_sk_tmp
				select src_tm.time_period_sk
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..behavioral_variable src_beh_var
					on(src_beh_var.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..source_time_period src_tm
					on(src_beh_var.time_period_sk=src_tm.time_period_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	/*For RECENT  VARIABLE*/

	proc sql noprint;
		insert into &m_cprm_scr_lib.._&src_project_sk._tm_prd_sk_tmp
				select src_tm.time_period_sk
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..recent_variable src_rec_var
					on(src_rec_var.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..source_time_period src_tm
					on(src_rec_var.time_period_sk=src_tm.time_period_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	
	%let m_dis_tm_prd_sk = .;

	/*Selecting distinct time_period_sk to analyze*/
	proc sql noprint;
		select distinct(time_period_sk) 
			into :m_dis_tm_prd_sk  separated by ',' /* I18NOK:LINE */
			from &m_cprm_scr_lib.._&src_project_sk._tm_prd_sk_tmp;
	quit;
	
	%dabt_drop_table(m_table_nm=&m_cprm_scr_lib.._&src_project_sk._tm_prd_sk_tmp);

	%let m_dis_tm_prd_sk = &m_dis_tm_prd_sk.; /*List of distinct time_period_sk associated with variable*/

	/*Table stores  maping table of time_period_sk on source and target machine*/
	%let m_tm_prd_sk_ds_nm = _&src_project_sk._src_tm_prd_map; 

	%if &m_dis_tm_prd_sk. ne %str(.) %then %do;

		%let m_tm_prd_cnt = %eval(%sysfunc(countc(%quote(&m_dis_tm_prd_sk.), %str(,)))+1);	/*i18NOK:LINE*/
	
		/*Creating maping table of time_period_sk on source and target machine*/
		%if %kupcase("&mode.") eq "EXECUTE" %then %do;		/*i18NOK:LINE*/			
			proc sql noprint;
				create table &m_cprm_scr_lib..&m_tm_prd_sk_ds_nm.
					(src_tbl_tm_prd_sk num(11) ,src_variable_sk num(11),tgt_tbl_tm_prd_sk num(11));
			quit;
		%end;
		%let m_tm_prd_vld_rslt_flg =;
		%let m_tgt_tm_prd_sk = ; /*time_period_sk of same time_period on target machine*/

		%do tm_count = 1 %to &m_tm_prd_cnt.;

			%let m_src_tm_prd_tkn = %scan(%quote(&m_dis_tm_prd_sk.),&tm_count,%str(,)); /* i18nOK:Line */

			%dabt_cprm_check_parent_entity (entity_sk 				= &src_project_sk.,
											entity_type_cd 				= PROJECT,
											assoc_entity_sk 			= &m_src_tm_prd_tkn.,
											assoc_entity_type_cd 		= TIME_PRD,
											src_apdm_lib 				= &m_cprm_src_apdm.,
											tgt_apdm_lib 				= &m_apdm_lib.,
											mode 						= &mode.,
											return_assoc_entity_tgt_sk 	= m_tgt_tm_prd_sk,
											return_validation_rslt_flg 	= m_tm_prd_vld_rslt_flg
											);
			%let m_tgt_tm_prd_sk = &m_tgt_tm_prd_sk.;
			%let m_tm_prd_vld_rslt_flg = &m_tm_prd_vld_rslt_flg.;

			
			%if &m_tm_prd_vld_rslt_flg. ne &CHECK_FLAG_TRUE. %then %do;
				%let m_tm_prd_vld_rslt_flg = &CHECK_FLAG_FALSE.;
				%let m_tgt_tm_prd_sk = .;
			%end;

			/*Inserting observations in table of time_period_sk on source and target machine*/

			%if %kupcase("&mode.") eq "EXECUTE" and &m_tm_prd_vld_rslt_flg. eq  &CHECK_FLAG_TRUE %then %do;		/*i18NOK:LINE*/
				proc sql noprint;
					insert into &m_cprm_scr_lib..&m_tm_prd_sk_ds_nm.
						values (&m_src_tm_prd_tkn.,&m_only_src_var_sk.,&m_tgt_tm_prd_sk.);
				quit;
			%end;	
		
		%end;/*Loop end for every time_period_sk*/

	%end;/*Condition check for time_period_sk ne blank*/
	
	*********************************************************************************************************;
	*Pre Analysis Check 5: Check if corresponding as of time is available on the TGT or in the import package;
	*********************************************************************************************************;
	%local 	m_src_as_of_tm_sk
			m_as_of_tm_vld_rslt_flg
			m_tgt_as_of_tm_sk
			m_as_of_tm_exist_in_tgt_flg
			;

	/*For SUPPLEMENTARY VARIABLE*/

	proc sql noprint;
			select src_tm.as_of_time_sk
					into :m_src_as_of_tm_sk 
			from &m_cprm_src_apdm..variable_master src_var
				inner join &m_cprm_src_apdm..supplementary_variable src_sup_var
					on(src_sup_var.variable_sk=src_var.variable_sk)
				inner join &m_cprm_src_apdm..source_as_of_time_master src_tm
					on(src_sup_var.as_of_time_sk=src_tm.as_of_time_sk)
				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	%if &m_src_as_of_tm_sk. ne  %then %do;
		%let m_src_as_of_tm_sk = &m_src_as_of_tm_sk.;
	%end;
	
	%let m_as_of_tm_vld_rslt_flg =;
	%let m_tgt_as_of_tm_sk = ; /*as_of_time_sk of same as of time period on target machine*/

	/*Table stores  maping table of as_of_time_sk on source and target machine*/
	%let m_as_of_tm_sk_ds_nm = _&src_project_sk._src_as_of_tm_map;

	%if &m_src_as_of_tm_sk. ne  %then %do;

		%dabt_cprm_check_parent_entity (entity_sk 					= &src_project_sk.,
										entity_type_cd 				= PROJECT,
										assoc_entity_sk 			= &m_src_as_of_tm_sk.,
										assoc_entity_type_cd 		= AS_OF_TIME,
										src_apdm_lib 				= &m_cprm_src_apdm.,
										tgt_apdm_lib 				= &m_apdm_lib.,
										mode 						= &mode.,
										return_assoc_entity_tgt_sk 	= m_tgt_as_of_tm_sk,
										return_validation_rslt_flg 	= m_as_of_tm_vld_rslt_flg
										);
		%let m_tgt_as_of_tm_sk = &m_tgt_as_of_tm_sk.;
		%let m_as_of_tm_vld_rslt_flg = &m_as_of_tm_vld_rslt_flg.;

		%if &m_as_of_tm_vld_rslt_flg. ne &CHECK_FLAG_TRUE. %then %do;
				%let m_as_of_tm_vld_rslt_flg = &CHECK_FLAG_FALSE.;
				%let m_tgt_as_of_tm_sk = .;
		%end;


		/*Creating maping table of as of time on source and target machine*/	/*i18NOK:BEGIN*/
		%if %kupcase("&mode.") eq "EXECUTE" and  &m_as_of_tm_vld_rslt_flg. eq &CHECK_FLAG_TRUE %then %do;	
			data &m_cprm_scr_lib..&m_as_of_tm_sk_ds_nm.; 
				src_as_of_tm_sk = symgetn('m_src_as_of_tm_sk');
				src_variable_sk = symgetn('m_only_src_var_sk');
				tgt_as_of_tm_sk = symgetn('m_tgt_as_of_tm_sk');
			run;	/*i18NOK:END*/
		%end;

	%end;/*Condition check for as_of_time_sk ne blank*/

	****************************************************************************************************************;
	*Pre Analysis Check 6: Check if corresponding external variable is available on the TGT or in the import package;
	****************************************************************************************************************;

	%local	m_src_ext_var_sk
			m_src_ext_code_sk
			m_src_ext_cd_shrt_nm
			m_src_ext_var_shrt_nm
			m_tgt_ext_cd_sk
			m_ext_cd_vld_rslt_flg
			m_ext_var_vld_rslt_flg
			m_tgt_ext_var_sk
			m_ext_cd_exist_in_tgt_flg
			m_ext_var_exist_in_tgt_flg
			;
	
	*******************************************************************************;
	*Finding master.external_variable_sk , external_variable_short_nm , external_code_sk from source
				for a given variable which  is present only in source.				
	*******************************************************************************;

	proc sql noprint;
			select external_variable_master.external_variable_sk,
					external_variable_master.external_variable_short_nm,
					src_ext_cd.external_code_short_nm,
					src_ext_cd.external_code_sk
				into :m_src_ext_var_sk ,
					 :m_src_ext_var_shrt_nm,
					 :m_src_ext_cd_shrt_nm,
					 :m_src_ext_code_sk
			from &m_cprm_src_apdm..variable_master src_var

				inner join &m_cprm_src_apdm..external_variable src_ext_var
					on(src_ext_var.variable_sk=src_var.variable_sk)

				inner join &m_cprm_src_apdm..external_variable_master
					on(src_ext_var.external_variable_sk=external_variable_master.external_variable_sk)

				inner join &m_cprm_src_apdm..external_code_master src_ext_cd
					on(src_ext_cd.external_code_sk=external_variable_master.external_code_sk)

				where src_var.variable_sk=&m_only_src_var_sk.;
	quit;

	%let m_src_ext_var_sk = &m_src_ext_var_sk.;
	%let m_src_ext_code_sk = &m_src_ext_code_sk.;	
	%let m_src_ext_cd_shrt_nm = &m_src_ext_cd_shrt_nm.;
	%let m_src_ext_var_shrt_nm = &m_src_ext_var_shrt_nm.;
	
	%let m_ext_var_vld_rslt_flg = ;
	%let m_tgt_ext_var_sk =; /*external_variable_sk of same as of external variable on target machine*/

	/*Table stores  maping table of external_variable_sk on source and target machine*/
	%let m_ext_var_sk_ds_nm = _&src_project_sk._src_ext_sk_map;
	
	%if &m_src_ext_var_sk. ne  %then %do;

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

				where src_ext_cd.external_code_sk=&m_src_ext_code_sk.;
		quit;

		%let m_tgt_ext_cd_sk = &m_tgt_ext_cd_sk.;

		%if &m_tgt_ext_cd_sk. eq  %then %do;
			%let m_ext_cd_exist_in_tgt_flg = &CHECK_FLAG_FALSE.;
		%end;/*Condition End for external code not present on target machine*/
		%else %do;
			%let m_ext_cd_exist_in_tgt_flg = &CHECK_FLAG_TRUE.;
		%end;

		/*Finding External Code is present in import package or not*/
		%let m_cnt_imp_pack_ext_cd = 0;	

		proc sql noprint ; 
			select count(*)			/*i18NOK:LINE*/ 
				into :m_cnt_imp_pack_ext_cd
			from 
				&m_cprm_imp_ctl..cprm_import_param_list_tmp a 
			where 
				kupcase(a.entity_type_cd) = "EXT_CODE" 		/*i18NOK:LINE*/
					and a.entity_key =  &m_src_ext_code_sk.
					and kupcase(a.promote_flg) = &CHECK_FLAG_TRUE. ;
		quit; 

		%let m_cnt_imp_pack_ext_cd = &m_cnt_imp_pack_ext_cd.;

		%if &m_cnt_imp_pack_ext_cd ne 0 %then %do ;
			%let m_ext_cd_in_import_package_flg =  &CHECK_FLAG_TRUE;
		%end; 
		%else %do ; 
			%let m_ext_cd_in_import_package_flg =  &CHECK_FLAG_FALSE;
		%end; 

		%if &mode = ANALYSE %then %do;
			%if (&m_ext_cd_exist_in_tgt_flg. = &CHECK_FLAG_TRUE. or &m_ext_cd_in_import_package_flg. = &CHECK_FLAG_TRUE.) %then %do;
				%let m_ext_cd_vld_rslt_flg  = &CHECK_FLAG_TRUE.;
			%end;
			%else %do;
				%let m_ext_cd_vld_rslt_flg  = &CHECK_FLAG_FALSE.;
			%end;
		%end;

		%else %if &mode = EXECUTE %then %do;
			%if (&m_ext_cd_exist_in_tgt_flg. = &CHECK_FLAG_TRUE.) %then %do;
				%let m_ext_cd_vld_rslt_flg = &CHECK_FLAG_TRUE.;
			%end;
			%else %do;
				%let m_ext_cd_vld_rslt_flg  = &CHECK_FLAG_FALSE.;
			%end;
		%end;

		%if &mode. = ANALYSE %then %do;
			%dabt_cprm_ins_pre_analysis_dtl (
												m_promotion_entity_nm=&m_src_prj_shrt_nm,
												m_promotion_entity_type_cd=PROJECT,
												m_assoc_entity_nm=&m_src_ext_cd_shrt_nm,
												m_assoc_entity_type_cd= EXT_CODE,
												m_present_in_tgt_flg= &m_ext_cd_exist_in_tgt_flg,
												m_present_in_import_package_flg=&m_ext_cd_in_import_package_flg.
											);
		%end;
	
		%if &m_ext_cd_exist_in_tgt_flg. eq &CHECK_FLAG_TRUE. %then %do;
			
			/*Finding external variable sk on target machine same as of shortname on source machine.*/

			proc sql noprint;
				select tgt_ext_var.external_variable_sk
					into:m_tgt_ext_var_sk
				from 
					&m_cprm_src_apdm..external_variable_master src_ext_var

				inner join &m_apdm_lib..external_variable_master tgt_ext_var
					on(src_ext_var.external_variable_column_nm=tgt_ext_var.external_variable_column_nm)

				where tgt_ext_var.external_code_sk=&m_tgt_ext_cd_sk. 
					and src_ext_var.external_variable_sk=&m_src_ext_var_sk.;

			quit;

			%let m_tgt_ext_var_sk = &m_tgt_ext_var_sk.;

			%if &m_tgt_ext_var_sk. eq  %then %do;
				%let m_ext_var_exist_in_tgt_flg = &CHECK_FLAG_FALSE.;
			%end;/*Condition End for external variable not present on target machine*/
			%else %do;
				%let m_ext_var_exist_in_tgt_flg = &CHECK_FLAG_TRUE.;
			%end;

			%if &mode = ANALYSE %then %do;
				%if (&m_ext_var_exist_in_tgt_flg. = &CHECK_FLAG_TRUE. or &m_ext_cd_vld_rslt_flg. = &CHECK_FLAG_TRUE.) %then %do;
					%let m_ext_var_vld_rslt_flg  = &CHECK_FLAG_TRUE.;
				%end;
				%else %do;
					%let m_ext_var_vld_rslt_flg  = &CHECK_FLAG_FALSE.;
				%end;
			%end;

			%else %if &mode = EXECUTE %then %do;
				%if (&m_ext_var_exist_in_tgt_flg. = &CHECK_FLAG_TRUE.) %then %do;
					%let m_ext_var_vld_rslt_flg = &CHECK_FLAG_TRUE.;
				%end;
				%else %do;
					%let m_ext_var_vld_rslt_flg  = &CHECK_FLAG_FALSE.;
				%end;
			%end;

			%if &mode. = ANALYSE %then %do;
				%dabt_cprm_ins_pre_analysis_dtl (
													m_promotion_entity_nm=&m_src_prj_shrt_nm,
													m_promotion_entity_type_cd=PROJECT,
													m_assoc_entity_nm=&m_src_ext_var_shrt_nm,
													m_assoc_entity_type_cd= EXT_VAR,
													m_present_in_tgt_flg= &m_ext_var_exist_in_tgt_flg,
													m_present_in_import_package_flg=&m_ext_cd_in_import_package_flg.
												);
			%end;
		
		%end;/*Condition end for valdidation flag of external code is true*/
/*i18NOK:BEGIN*/
		%if %kupcase("&mode.") eq "EXECUTE" and &m_ext_cd_vld_rslt_flg. eq &CHECK_FLAG_TRUE and &m_ext_var_vld_rslt_flg. eq &CHECK_FLAG_TRUE  %then %do;
			data &m_cprm_scr_lib..&m_ext_var_sk_ds_nm.;
				src_ext_var_sk = symgetn('m_src_ext_var_sk');
				src_variable_sk = symgetn('m_only_src_var_sk');
				tgt_ext_var_sk = symgetn('m_tgt_ext_var_sk');
			run;	/*i18NOK:END*/
		%end;

	%end;/*Condition check for source external_variable_sk ne blank*/

	****************************************************************************************************************;
	*Pre Analysis Check 7: Check if associated level with variable is present on on the TGT or in the import package;
	****************************************************************************************************************;
	%local	m_src_var_lvl_sk
			m_tgt_var_lvl_sk
			m_var_lvl_vld_rslt_flg
			;

		* Finding level_sk on target machine for the same level code of source machine ;
		
	proc sql noprint;
		select level_sk
			into:m_src_var_lvl_sk
		from &m_cprm_src_apdm..variable_master src_var
			where variable_sk=&m_only_src_var_sk.;
	quit;
		
	%let m_src_var_lvl_sk = &m_src_var_lvl_sk.;

	%let m_var_lvl_vld_rslt_flg =;
	%let m_tgt_var_lvl_sk = ; /*level_sk of same as of level_cd on target machine*/

	/*Table stores  maping table of level_sk on source and target machine*/
	%let m_var_lvl_sk_ds_nm = _&src_project_sk._src_lvl_sk_map; 
	
	%if &m_src_var_lvl_sk. ne and &m_src_var_lvl_sk. ne . %then %do;

		%dabt_cprm_check_parent_entity (entity_sk 					= &src_project_sk.,
										entity_type_cd 				= PROJECT,
										assoc_entity_sk 			= &m_src_var_lvl_sk.,
										assoc_entity_type_cd 		= SOA,
										src_apdm_lib 				= &m_cprm_src_apdm.,
										tgt_apdm_lib 				= &m_apdm_lib.,
										mode 						= &mode.,
										return_assoc_entity_tgt_sk 	= m_tgt_var_lvl_sk,
										return_validation_rslt_flg 	= m_var_lvl_vld_rslt_flg
										);
		%let m_tgt_var_lvl_sk = &m_tgt_var_lvl_sk.;
		%let m_var_lvl_vld_rslt_flg = &m_var_lvl_vld_rslt_flg.;

		%if &m_var_lvl_vld_rslt_flg. ne &CHECK_FLAG_TRUE. %then %do;
				%let m_var_lvl_vld_rslt_flg = &CHECK_FLAG_FALSE.;
				%let m_tgt_var_lvl_sk = .;
		%end;

		/*Creating maping table of as of time on source and target machine*/	/*i18NOK:BEGIN*/
		%if %kupcase("&mode.") eq "EXECUTE" and  &m_var_lvl_vld_rslt_flg. eq &CHECK_FLAG_TRUE %then %do;
			data &m_cprm_scr_lib..&m_var_lvl_sk_ds_nm.; 
				src_var_lvl_sk = symgetn('m_src_var_lvl_sk');
				src_variable_sk = symgetn('m_only_src_var_sk');
				tgt_var_lvl_sk = symgetn('m_tgt_var_lvl_sk');
			run;	/*i18NOK:END*/
		%end;

	%end;/*Condition check for source level_sk ne blank*/

		
	*==================================================;
	* Check if all validations are successful or not ;
	*==================================================;

	%if 
		&master_table_vld_rslt_flg = &CHECK_FLAG_FALSE. 
				or 
		&m_tbl_col_vld_rslt_flg = &CHECK_FLAG_FALSE. 
				or 
		&m_dim_attrib_vld_rslt_flg = &CHECK_FLAG_FALSE. 
				or 
		&m_tm_prd_vld_rslt_flg = &CHECK_FLAG_FALSE. 
				or
		&m_as_of_tm_vld_rslt_flg = &CHECK_FLAG_FALSE. 
				or
		&m_ext_cd_vld_rslt_flg = &CHECK_FLAG_FALSE.
			or
		&m_ext_var_vld_rslt_flg = &CHECK_FLAG_FALSE.
				or
		&m_var_lvl_vld_rslt_flg = &CHECK_FLAG_FALSE. 
	%then %do;
		%let m_only_in_src_valid_flag = &CHECK_FLAG_FALSE;
	%end;
	%else %do;
		%let m_only_in_src_valid_flag = &CHECK_FLAG_TRUE;		
	%end;  
	
	***********************************************************;
	*Execution Starts for Variables Present only in source.
	***********************************************************;
	%if %kupcase("&mode.") eq "EXECUTE" and  &m_only_in_src_valid_flag. eq &CHECK_FLAG_TRUE  and &syscc le 4  %then %do;	/*i18NOK:LINE*/
	
		*===================================================;
		* 1.Inserting record in &m_apdm_lib..variable_master;
		*===================================================;	

		%local 	m_tgt_master_tbl_sk
				m_src_var_level_sk 
				m_tgt_var_level_sk
				m_tgt_var_sk;

		*==================================================================================;
		* Finding SOURCE_TABLE_SK from mapping table src_tgt_tbl_sk_map_&src_project_sk. ;
		*==================================================================================;
		%let m_tgt_master_tbl_sk = .;
		
		%if %sysfunc(exist(&m_cprm_scr_lib..&m_master_table_sk_ds_nm.)) %then %do;

			proc sql noprint;
				select tgt_master_tbl_sk
					into:m_tgt_master_tbl_sk
				from &m_cprm_scr_lib..&m_master_table_sk_ds_nm. 
					where src_variable_sk=&m_only_src_var_sk.;
			quit;

		%end;

		%if &m_tgt_master_tbl_sk. ne . %then %do;
			%let m_tgt_master_tbl_sk = &m_tgt_master_tbl_sk.;
		%end;
	
		
		*==================================================================================;
		* Finding level_sk from mapping table _&src_project_sk._src_tgt_var_lvl_sk_map ;
		*==================================================================================;

		%let m_tgt_var_level_sk =.;
		%if %sysfunc(exist(&m_cprm_scr_lib..&m_var_lvl_sk_ds_nm.)) %then %do;
			proc sql noprint;
				select tgt_var_lvl_sk
					into:m_tgt_var_level_sk
				from &m_cprm_scr_lib..&m_var_lvl_sk_ds_nm. 
					where src_variable_sk=&m_only_src_var_sk.;
			quit;
		%end;
		%if &m_tgt_var_level_sk. ne . %then %do;
			%let m_tgt_var_level_sk = &m_tgt_var_level_sk.;
		%end;

		%let ins_cols_lst = ;

		%dabt_cprm_get_col_lst(	m_ds_nm=variable_master, 
								m_src_lib_nm=&m_cprm_src_apdm, 
								m_tgt_lib_nm=&m_apdm_lib, 
								m_exclued_col= variable_sk variable_definition_string source_table_sk level_sk,
								m_ins_cols_lst=ins_cols_lst
								);
		%let ins_cols_lst = &ins_cols_lst.;
		
		

		proc sql noprint  ;
	       insert into &m_apdm_lib..variable_master
			(&ins_cols_lst, variable_definition_string,source_table_sk,level_sk) 
		       select 
				 &ins_cols_lst, 
					"DUMMY_&m_only_src_var_sk." as variable_definition_string, /* i18nok:line */
					&m_tgt_master_tbl_sk. as source_table_sk,
					&m_tgt_var_level_sk. as level_sk
		       from 
					&m_cprm_src_apdm..variable_master src
		       where 
					src.variable_sk = &m_only_src_var_sk ; 
		quit;
		/*Finding last inserted variable_sk in  &m_apdm_lib..variable_master*/	

		proc sql noprint;
			select variable_sk
				into: m_tgt_var_sk
			from  &m_apdm_lib..variable_master
			where variable_definition_string="DUMMY_&m_only_src_var_sk.";		/*i18NOK:LINE*/
		quit;

		%let m_tgt_var_sk = &m_tgt_var_sk.;

		/******fetch the type of the variable need to be import******/ 
		
		proc sql noprint ; 
			select b.variable_type_cd  
				into: m_var_type_cd 
			from &m_cprm_src_apdm..variable_master a
			inner join &m_cprm_src_apdm..variable_type_master b
				on (a.variable_type_sk = b.variable_type_sk)	
			where a.variable_sk=&m_only_src_var_sk ;
		quit; 

		*=============================================================;
		* 2. Inserting record in &m_apdm_lib..VARIABLE_DIM_ATTRIBUTE_FILTER;
		*===============================================================;	
		%if (&m_var_type_cd = BEH) OR (&m_var_type_cd = RNT) %then %do ; 
			
			%if %sysfunc(exist(&m_cprm_scr_lib..&m_dim_attrib_sk_ds_nm.)) %then %do;

				* Finding DIM_ATTRIBUTE_VALUE_SK from mapping table src_tgt_dim_attrib_sk_map_&src_project_sk. ;
			
				%local m_tgt_dim_attrib_sk;

				proc sql noprint;
					select tgt_dim_attrib_sk 
						into:m_tgt_dim_attrib_sk separated by ',' /* I18NOK:LINE */
					from &m_cprm_scr_lib..&m_dim_attrib_sk_ds_nm.
						where src_variable_sk=&m_only_src_var_sk.;
				quit;
				
				%let m_tgt_dim_attrib_sk = &m_tgt_dim_attrib_sk.;

				%if &m_tgt_dim_attrib_sk. ne %then %do;

					%let m_tgt_dim_cnt = %eval(%sysfunc(countc(%quote(&m_tgt_dim_attrib_sk.), %str(,)))+1);	/*i18NOK:LINE*/

					%do m_dim_cnt = 1 %to &m_tgt_dim_cnt.;

						%let m_tgt_dim_attrib_sk_tkn = %scan(%quote(&m_tgt_dim_attrib_sk.),&m_dim_cnt,%str(,)); /* i18nOK:Line */
						proc sql noprint ;
							insert into &m_apdm_lib..variable_dim_attribute_filter
								(dim_attribute_value_sk,variable_sk)
									values(&m_tgt_dim_attrib_sk_tkn.,&m_tgt_var_sk.);
						quit;	
					%end;/*Loop end  for every m_tgt_dim_attrib_sk  */

				%end;/*m_tgt_dim_attrib_sk not blank check*/

			%end;/*Condition check end for &m_cprm_scr_lib..&m_dim_attrib_sk_ds_nm.*/

		%end;/*Condition check end for variable type */

		*=============================================================;
		* 3. Inserting record in &m_apdm_lib..SUPPLEMENTARY_VARIABLE;
		*===============================================================;	
		%if &m_var_type_cd = SPM %then %do ;
			
				* Finding SELECT_SOURCE_COLUMN_SK from mapping table src_tgt_tbl_col_sk_map_&src_project_sk. ;

			%local 	m_tgt_tbl_col_sk
					m_tgt_as_of_tm_sk ;

			%let m_tgt_tbl_col_sk =.;

			%if %sysfunc(exist(&m_cprm_scr_lib..&m_tbl_col_sk_ds_nm.)) %then %do;
				proc sql noprint;
					select tgt_tbl_col_sk
						into:m_tgt_tbl_col_sk
					from &m_cprm_scr_lib..&m_tbl_col_sk_ds_nm.
						where src_variable_sk=&m_only_src_var_sk.;
				quit;
			%end;

			%let m_tgt_tbl_col_sk = &m_tgt_tbl_col_sk.;

			* Finding AS_OF_TIME_SK from mapping table src_tgt_as_of_tm_sk_map ;

			%let m_tgt_as_of_tm_sk =.;

			%if %sysfunc(exist(&m_cprm_scr_lib..&m_as_of_tm_sk_ds_nm.)) %then %do;
				proc sql noprint;
					select tgt_as_of_tm_sk
						into:m_tgt_as_of_tm_sk
					from &m_cprm_scr_lib..&m_as_of_tm_sk_ds_nm.
						where src_variable_sk=&m_only_src_var_sk.;
				quit;
			%end;

			%let m_tgt_as_of_tm_sk = &m_tgt_as_of_tm_sk.;
			
			%if &m_tgt_as_of_tm_sk.  ne . and &m_tgt_tbl_col_sk. ne . %then %do;

				proc sql noprint ;
					insert into &m_apdm_lib..supplementary_variable
						(variable_sk
							,select_source_column_sk
							,as_of_time_sk)
						values(
							&m_tgt_var_sk. ,
							&m_tgt_tbl_col_sk. ,
							&m_tgt_as_of_tm_sk.);
				quit;

			%end;
		%end;
		%else %if &m_var_type_cd = RNT %then %do ; 

			*=============================================================;
			* 4. Inserting record in &m_apdm_lib..RECENT_VARIABLE;
			*===============================================================;	
		
			* Finding ORDER_BY_DATE_SOURCE_COLUMN_SK from mapping table src_tgt_tbl_col_sk_map_&src_project_sk. ;
			proc sql noprint ;
				insert into &m_apdm_lib..recent_variable
					(variable_sk,
						ORDER_BY_DATE_SOURCE_COLUMN_SK,
						SELECT_SOURCE_COLUMN_SK,
							TIME_PERIOD_SK)
					select 
						&m_tgt_var_sk. as variable_sk , 
						order_by_src_col.tgt_tbl_col_sk as order_by_date_source_column_sk, 
						select_src_col.tgt_tbl_col_sk as select_source_column_sk, 
						time_prd.tgt_tbl_tm_prd_sk as time_period_sk
					from &m_cprm_src_apdm..recent_variable 
					
					inner join &m_cprm_scr_lib..&m_tbl_col_sk_ds_nm. as select_src_col
						on (recent_variable.select_source_column_sk = select_src_col.src_tbl_col_sk
								and recent_variable.variable_sk=select_src_col.src_variable_sk)	
								
					inner join &m_cprm_scr_lib..&m_tbl_col_sk_ds_nm. as order_by_src_col
						on (recent_variable.order_by_date_source_column_sk = order_by_src_col.src_tbl_col_sk
								and recent_variable.variable_sk=order_by_src_col.src_variable_sk)	
								
					inner join &m_cprm_scr_lib..&m_tm_prd_sk_ds_nm. as time_prd
						on (recent_variable.time_period_sk = time_prd.src_tbl_tm_prd_sk
								and recent_variable.variable_sk=order_by_src_col.src_variable_sk)
								
					where recent_variable.variable_sk=&m_only_src_var_sk. ;
			quit;

		%end;
		%else %if (&m_var_type_cd = BEH) %then %do ; 
			*=============================================================;
			* 5. Inserting record in &m_apdm_lib..BEHAVIROL_VARIABLE;
			*===============================================================;
			proc sql noprint ; 
				insert into &m_apdm_lib..BEHAVIORAL_VARIABLE
				(
					variable_sk,
					measure_source_column_sk,
					aggregation_type_sk,
					time_period_sk
				)
					select 
						&m_tgt_var_sk. as variable_sk,
						measure_src_col.tgt_tbl_col_sk as measure_source_column_sk, 
						tgt_agg.aggregation_type_sk as aggregation_type_sk,
						time_prd.tgt_tbl_tm_prd_sk as time_period_sk
					from &m_cprm_src_apdm..behavioral_variable beh_var
					
					INNER JOIN &m_cprm_src_apdm..aggregation_type_master src_agg 
						ON (beh_var.aggregation_type_sk = src_agg.aggregation_type_sk) 
						
					INNER JOIN &m_apdm_lib..aggregation_type_master tgt_agg 
						ON (src_agg.aggregation_type_cd = tgt_agg.aggregation_type_cd) 
						
					INNER JOIN  &m_cprm_scr_lib..&m_tm_prd_sk_ds_nm. time_prd
						ON (beh_var.variable_sk = time_prd.src_variable_sk
							and beh_var.time_period_sk = time_prd.src_tbl_tm_prd_sk )
							
					LEFT JOIN &m_cprm_scr_lib..&m_tbl_col_sk_ds_nm. measure_src_col 
						ON (beh_var.variable_sk = measure_src_col.src_variable_sk 
							and beh_var.measure_source_column_sk = measure_src_col.src_tbl_col_sk)
							
					where beh_var.variable_sk =  &m_only_src_var_sk	;
			quit; 

		%end;
		%else %if &m_var_type_cd = EXT %then %do ;

			*=============================================================;
			* 6. Inserting record in &m_apdm_lib..EXTERNAL_VARIABLE;
			*===============================================================;
		
			proc sql noprint  ; 
				insert into &m_apdm_lib..EXTERNAL_VARIABLE 
				( 
					variable_sk,
					external_variable_sk
				) 
				select 
				   &m_tgt_var_sk. as variable_sk,
				   ext_var.tgt_ext_var_sk as external_variable_sk
				from &m_cprm_scr_lib..&m_ext_var_sk_ds_nm. ext_var 
				where ext_var.src_variable_sk = &m_only_src_var_sk ; 
			quit; 
		%end;
		%else %if &m_var_type_cd = DER %then %do ;
			*=============================================================;
			* 7. Inserting record in &m_apdm_lib..DERIVED_VARIABLE;
			*===============================================================;

			%let ins_cols_lst = ;

			%dabt_cprm_get_col_lst(	m_ds_nm=derived_variable, 
									m_src_lib_nm=&m_cprm_src_apdm, 
									m_tgt_lib_nm=&m_apdm_lib, 
									m_exclued_col= variable_sk,
									m_ins_cols_lst=ins_cols_lst
									);
			%let ins_cols_lst = &ins_cols_lst.;

			proc sql noprint  ; 
				insert into &m_apdm_lib..DERIVED_VARIABLE 
				(
				 variable_sk, 
				 &ins_cols_lst 
				)
				select &m_tgt_var_sk. as variable_sk,
					   &ins_cols_lst
				from &m_cprm_src_apdm..derived_variable a
				where a.variable_sk = &m_only_src_var_sk ;  
			quit ; 
		
		%end;
		*==================================================================;
		* 8. Inserting record in &m_apdm_lib..MODELLING_ABT_X_VARIABLE;
		*==================================================================;

			%local 	tgt_project_sk 
					tgt_abt_sk ;
					
			%let tgt_project_sk=;
			
			/* Finding tgt_project_sk from target machine.*/
			
			%dabt_cprm_get_entity_tgt_sk(entity_sk				= &src_project_sk.,
										 entity_type_cd			= &entity_type_cd,
										 src_apdm_lib			= &m_cprm_src_apdm.,
										 tgt_apdm_lib			= &m_apdm_lib.,
										 return_entity_tgt_sk	= tgt_project_sk
										);
										 
			%let tgt_project_sk = &tgt_project_sk.;
			
			/* Get modeling_abt_sk from target machine */
			
			proc sql noprint;
				select  abt_sk 
					into :tgt_abt_sk
				from &m_apdm_lib..Modeling_abt_master
				where project_sk = &tgt_project_sk.;
			quit;
			
			%let ins_cols_lst = ;

			%dabt_cprm_get_col_lst(	m_ds_nm			= modeling_abt_x_variable, 
									m_src_lib_nm	= &m_cprm_src_apdm, 
									m_tgt_lib_nm	= &m_apdm_lib, 
									m_exclued_col	= abt_sk variable_sk,
									m_ins_cols_lst	= ins_cols_lst
									);
									
			%let ins_cols_lst = &ins_cols_lst.;

			proc sql noprint  ;  
				insert into &m_apdm_lib..MODELING_ABT_X_VARIABLE 
				(
				 abt_sk, 
				 variable_sk,
				 &ins_cols_lst 
				)
				select 
					&tgt_abt_sk as abt_sk,
					&m_tgt_var_sk as variable_sk,
					&ins_cols_lst
				from &m_cprm_src_apdm..MODELING_ABT_X_VARIABLE 
				where variable_sk=&m_only_src_var_sk; 
			quit ; 



	%let m_rtrn_def_str_sk = ; 

	/*Need to update defintion string in variable_master */
		%dabt_cprm_create_var_defn_str (apdm_libname=&m_apdm_lib,
										variable_sk=&m_tgt_var_sk,
										return_def_string=m_return_def_string, 
										return_def_sk=m_rtrn_def_str_sk
										);

		%let m_rtrn_def_str_sk = &m_rtrn_def_str_sk.;

		proc sql noprint  ; 
			update &m_apdm_lib..variable_master 
			set variable_definition_string = "&m_rtrn_def_str_sk."
			Where variable_sk = &m_tgt_var_sk; 
		quit; 

		*==============================================;
		* To Drop temp tables created in scratch lib  ;
		*=============================================;

		%dabt_drop_table(m_table_nm=&m_cprm_scr_lib..&m_master_table_sk_ds_nm.);	
		%dabt_drop_table(m_table_nm=&m_cprm_scr_lib..&m_tbl_col_sk_ds_nm.);	
		%dabt_drop_table(m_table_nm=&m_cprm_scr_lib..&m_dim_attrib_sk_ds_nm.);	
		%dabt_drop_table(m_table_nm=&m_cprm_scr_lib..&m_tm_prd_sk_ds_nm.);	
		%dabt_drop_table(m_table_nm=&m_cprm_scr_lib..&m_as_of_tm_sk_ds_nm.);	
		%dabt_drop_table(m_table_nm=&m_cprm_scr_lib..&m_ext_var_sk_ds_nm.);	
		%dabt_drop_table(m_table_nm=&m_cprm_scr_lib..&m_var_lvl_sk_ds_nm.);	

	%end;


*============================;
* Set the output parameters  ;
*============================;

%let &return_var_validation_rslt_flg. =  &m_only_in_src_valid_flag.;

 
%mend dabt_cprm_imprt_var_only_in_src;
