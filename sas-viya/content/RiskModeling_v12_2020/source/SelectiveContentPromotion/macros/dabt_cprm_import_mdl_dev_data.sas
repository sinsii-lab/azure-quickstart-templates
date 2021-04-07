
/********************************************************************************************************
   Module:  dabt_cprm_import_mdl_dev_data

   Called by: dabt_cprm_import_mdl_dev_data.sas
   Function : This macro imports the model development data for the source model_sk (entity_sk) 
				from the SRC to TGT machine.

   Parameters: INPUT: 
			1. entity_sk                   : model_sk of the source machine, whose scorecard to be imported.
			2. import_analysis_report_path : Path where the impor analysis report is generated.
			3. scratch_ds_prefix		   : Prefix used to uniquely identify the scratch datasets.
            
*********************************************************************************************************/

%macro dabt_cprm_import_mdl_dev_data(
										entity_sk =, 
										import_analysis_report_path = , 
										scratch_ds_prefix =
									);
	
	%local m_cprm_src_mdl_dev_lib src_model_id m_src_dev_load_status m_src_level_sk 
			m_tgt_level_sk m_entity_type_cd m_tgt_model_sk ;

	%let import_analysis_report_path = &import_analysis_report_path.;
	
	/*m_cprm_src_apdm- Stores libref of source apdm. dabt_assign_lib macro will assign value to this*/
	/*m_cprm_scr_lib- Stores libref for scratch. dabt_assign_lib macro will assign value to this*/
	/*m_cprm_imp_ctl- Stores libref for control library. This lib will has CPRM_IMPORT_PARAMETER_LIST, . dabt_assign_lib macro will assign value to this*/

	%local m_cprm_src_apdm m_cprm_scr_lib m_cprm_imp_ctl m_apdm_lib m_cprm_src_mdl_dev_lib;
	%dabt_assign_libs(tmp_lib=m_cprm_scr_lib,m_workspace_type=CPRM_IMP,src_lib = m_apdm_lib,
	                                import_analysis_report_path = &import_analysis_report_path., 
									m_cprm_src_apdm_lib= m_cprm_src_apdm, 
	                                m_cprm_ctl_lib = m_cprm_imp_ctl);
	
	/*libname cprmdevm "&import_analysis_report_path/scratch/source_model_dev_data";*/		/*i18NOK:LINE*/

	/*%let m_cprm_src_mdl_dev_lib = cprmdevm;*/

	%let m_src_level_sk = ;
	proc sql noprint;
		select level_sk into :m_src_level_sk 
			from &m_cprm_src_apdm..model_master_extension 
			where model_key = &entity_sk.;
	quit;

	%let m_src_level_sk = &m_src_level_sk.;
	
	%let m_mdl_wrkspc_nm = ;
	proc sql noprint;
		select model_workspace_name into :m_mdl_wrkspc_nm
			from &m_cprm_src_apdm..model_master
			where model_sk= &entity_sk.
			and model_source_type_sk=4;
	quit;
	
	%let m_mdl_wrkspc_nm = &m_mdl_wrkspc_nm;
	
	%if "&m_mdl_wrkspc_nm" ="SWAT" %then %do;	/* i18nOK:Line */
		libname cprmastm "&import_analysis_report_path/scratch/source_model_ast_data";	/* i18nOK:Line */
	%end;
	
	%let m_tgt_level_sk = ;
	%let m_entity_type_cd = SOA;
	%dabt_cprm_get_entity_tgt_sk(	entity_sk = &m_src_level_sk.,
									entity_type_cd = &m_entity_type_cd. , 
									src_apdm_lib = &m_cprm_src_apdm., 
									tgt_apdm_lib = &lib_apdm., 
									return_entity_tgt_sk = m_tgt_level_sk);

	%if &m_tgt_level_sk. eq %then %do;
		%let syscc = 999;
		%return;
	%end;

	/*Get the target model sk*/
	%let m_tgt_model_sk = ;	
	%let m_entity_type_cd = MODEL;
	%dabt_cprm_get_entity_tgt_sk(	entity_sk = &entity_sk.,
									entity_type_cd = &m_entity_type_cd. , 
									src_apdm_lib = &m_cprm_src_apdm., 
									tgt_apdm_lib = &lib_apdm., 
									return_entity_tgt_sk = m_tgt_model_sk);

	/* CPRM CSB-24639 Begin: Extract report_specification_sk for deployed version to export Dev Data Tables */
		proc sql;
		select report_specification_sk into :m_src_rpt_spec_sk_dv  	
			from &m_cprm_src_apdm..mm_report_specification
				where deployed_flg = &CHECK_FLAG_TRUE. 			 		/* i18nOK:Line */	
				and model_sk = &entity_sk. ;
		quit;
	%if %symexist(m_src_rpt_spec_sk_dv) %then %do ;		
			%let m_src_rpt_spec_sk_dv = &m_src_rpt_spec_sk_dv;
			proc sql;
				select report_specification_sk into :m_tgt_rpt_spec_sk_dv
				from &lib_apdm..mm_report_specification
				where ready_for_deployment_flg = &CHECK_FLAG_TRUE. 			 		/* i18nOK:Line */	
				and model_sk = &m_tgt_model_sk. ;
			quit;
			
			%let m_tgt_rpt_spec_sk_dv = &m_tgt_rpt_spec_sk_dv. ;
	%end; /* if source has deployable version */	
		
		
	/* CPRM CSB-24639 End: Extract report_specification_sk for deployed version to export Dev Data Tables */
		
	/*Start: &RM_MODELPERF_DATA_LIBREF..<model>_<rpt_spec>_dev_fact*/ /* CPRM CSB-24639 : Table rename */
	%if %symexist(m_tgt_rpt_spec_sk_dv) %then %do ;	 /* if source has deployable version */		
		/*%csbmva_check_and_create_table( libref = &cs_fact_lib.,tablename = _&m_tgt_model_sk._&m_tgt_rpt_spec_sk_dv._dev_fact,filename = _model_dev_fact.sas ,replace_flag = Y );
		%let ins_cols_dev_fact = ;
		%dabt_cprm_get_col_lst(	m_ds_nm=_&m_tgt_model_sk._&m_tgt_rpt_spec_sk_dv._dev_fact,
								m_src_lib_nm=&m_cprm_src_mdl_dev_lib, 
								m_tgt_lib_nm=&cs_fact_lib., 
								m_exclued_col= model_rk,
								m_col_lst=, 
								m_prim_col_nm=, 
								m_prim_col_val=,
								m_ins_cols_lst= ins_cols_dev_fact
							  );
    
		proc sql noprint;
			insert into &cs_fact_lib.._&m_tgt_model_sk._&m_tgt_rpt_spec_sk_dv._dev_fact
				(&ins_cols_dev_fact., model_rk)
			select &ins_cols_dev_fact., &m_tgt_model_sk.
				from &m_cprm_src_mdl_dev_lib.._&entity_sk._&m_src_rpt_spec_sk_dv._dev_fact 
				where model_rk = &entity_sk.;
		quit;*/
	
	/*End: &RM_MODELPERF_DATA_LIBREF..<model>_<rpt_spec>_dev_fact*/ /* CPRM CSB-24639 : Table rename */


	/* START: Promoting Scorecard Model Specific Tables */
    %let m_mining_algorithm = ;      
	
	proc sql ;  
			select model_mining_algorithm into :m_mining_algorithm from &m_cprm_src_apdm..model_master
			where model_sk = &entity_sk.
			and kupcase(model_mining_algorithm) in (&DABT_VALID_SCRCRD_ALGRTHM_VAL.) ; /*i18NOK:LINE*/
	quit;
	%let m_mining_algorithm = &m_mining_algorithm.;
	
		%if &m_mining_algorithm. ne %then %do; /* For Scorecard Models Only */			
		
	/*Start: APDM.SCORE_CARD_DIM         */ /* CPRM CSB-24639 : Table CSBDIM.<MODEL_ID>_SCORE_CARD_DIM per entity replaced by single table APDM.SCORE_CARD_DIM */
	
				
				*-------Reserving sequence values for score_card_sk for inserting on target---------------;
				%local m_score_card_seq_to_reserve;
				%let m_score_card_seq_to_reserve = ;
				
				proc sql ;
					select score_card_sk, count(score_card_sk) into :score_card_sk_lst separated by ',', :m_score_card_seq_to_reserve from &m_cprm_src_apdm..score_card_dim		 /*i18NOK:LINE*/
						where report_specification_sk = &m_src_rpt_spec_sk_dv.;
						
				quit;
				

				%local m_sc_seq_start_val;
				%let m_sc_seq_start_val = ;
				%dabt_reserve_sequence_values(m_table_nm= SCORE_CARD_DIM, m_no_of_values_to_reserve = &m_score_card_seq_to_reserve., m_out_starting_sequence_value = m_sc_seq_start_val);
			
			
			
			*-----Creating a scratch table----------------; 
					proc sql noprint;
						create table &m_cprm_scr_lib.._&entity_sk._score_card_dim as
							select *, &m_sc_seq_start_val. + monotonic() as tgt_scorecard_sk
							from &m_cprm_src_apdm..score_card_dim 
							where report_specification_sk = 	&m_src_rpt_spec_sk_dv.		;
					quit;

			*---------------Populating on target-------------------------------------;
				%let ins_cols_score_card_dim = ;
				%dabt_cprm_get_col_lst(	m_ds_nm=score_card_dim , 
										m_src_lib_nm=&m_cprm_src_apdm , 
										m_tgt_lib_nm=&lib_apdm , 
										m_exclued_col= score_card_sk variable_sk  model_rk report_specification_sk score_card_bin_grp_sk ,
										m_col_lst=, 
										m_prim_col_nm=,  /* update does not happen */
										m_prim_col_val=,
										m_ins_cols_lst= ins_cols_score_card_dim
									  );


					
				proc sql ;
					insert into &lib_apdm..score_card_dim
						( &ins_cols_score_card_dim. , variable_sk, score_card_sk, score_card_bin_grp_sk, model_rk, report_specification_sk)
					select 
						 &ins_cols_score_card_dim. , b.tgt_var_sk, a.tgt_scorecard_sk , c.tgt_scorecard_bin_group_sk, &m_tgt_model_sk. , &m_tgt_rpt_spec_sk_dv.		 
					from 
						&m_cprm_scr_lib.._&entity_sk._score_card_dim a 
					inner join (select distinct src_var_sk, tgt_var_sk from &m_cprm_scr_lib.._&entity_sk._mdl_varamap ) b
						on a.variable_sk = b.src_var_sk
						inner join &m_cprm_scr_lib.._&entity_sk._scr_bin_grp_map c
						on c.scrcrd_bin_grp_variable_sk = a.variable_sk
							and c.scrcrd_bin_grp_id = a.variable_group_no
							;
				quit;			



	/*End: APDM.SCORE_CARD_DIM           */ /* CPRM CSB-24639 : Table CSBDIM.<MODEL_ID>_SCORE_CARD_DIM per entity replaced by single table APDM.SCORE_CARD_DIM */

	

			
	/* Start: APDM.MIP_MEASURE_STATS         */ /* CPRM CSB-24639 : Table &RM_MODELPERF_DATA_LIBREF.._<MODEL_ID>_SC_VAR_FACT per entity replaced by two single tables (1/2) APDM.MIP_MEASURE_STATS */
		
	
	*-----Creating a scratch table----------------; 
	
			proc sql noprint;
				create table &m_cprm_scr_lib.._&entity_sk._mip_msr_stats as
					select a.* , c.time_sk as tgt_scoring_as_of_time_sk 
						from &m_cprm_src_apdm..mip_measure_stats a
							inner join &m_cprm_src_apdm..time_dim b on b.time_sk = a.scoring_as_of_time_sk
							inner join &lib_apdm..time_dim c on b.period_first_dttm ge c.period_first_dttm and b.period_last_dttm le c.period_last_dttm
						where a.report_specification_sk = 	&m_src_rpt_spec_sk_dv.		
						and a.report_category_sk = (select report_category_sk from &m_cprm_src_apdm..mm_report_category_master where report_category_cd = 'DD' );	/*i18NOK:LINE*/
			quit;


	*---------------Populating on target-------------------------------------;
		%let ins_cols_mip_msr_stats = ;
		%dabt_cprm_get_col_lst(	m_ds_nm=mip_measure_stats , 
								m_src_lib_nm=&m_cprm_src_apdm , 
								m_tgt_lib_nm=&lib_apdm , 
								m_exclued_col= variable_sk report_specification_sk bin_analysis_scheme_sk scoring_as_of_time_sk,
								m_col_lst=, 
								m_prim_col_nm=,  /* update does not happen */
								m_prim_col_val=,
								m_ins_cols_lst= ins_cols_mip_msr_stats
							  );


			
		proc sql ;
			insert into &lib_apdm..mip_measure_stats
				( &ins_cols_mip_msr_stats. , variable_sk, report_specification_sk, bin_analysis_scheme_sk, scoring_as_of_time_sk)
			select 
				 &ins_cols_mip_msr_stats. , b.tgt_var_sk , &m_tgt_rpt_spec_sk_dv , c.tgt_bin_analys_scheme_sk, a.tgt_scoring_as_of_time_sk
			from 
				&m_cprm_scr_lib.._&entity_sk._mip_msr_stats a 
			inner join 
				&m_cprm_scr_lib.._&entity_sk._mdl_varamap b 
					on a.variable_sk = b.src_var_sk
			inner join 
				&m_cprm_scr_lib.._&entity_sk._mdl_bin_analys_map c
					on a.bin_analysis_scheme_sk = c.bin_analysis_scheme_sk
					;
		quit;
					
	/* End: APDM.MIP_MEASURE_STATS         */ /* CPRM CSB-24639 : Table &RM_MODELPERF_DATA_LIBREF.._<MODEL_ID>_SC_VAR_FACT per entity replaced by two single tables (1/2) APDM.MIP_MEASURE_STATS */
	%end;		/* For Scorecard Models Only */	
		
	/* END: Promoting Scorecard Model Specific Tables */	
	
	
	/* Start: APDM.MM_MEASURE_STATS         */ /* CPRM CSB-24639 : Table &RM_MODELPERF_DATA_LIBREF.._<MODEL_ID>_SC_VAR_FACT per entity replaced by single table APDM.MIP_MEASURE_STATS */
		
	
	*-----Creating a scratch table----------------; 
	
			proc sql noprint;
				create table &m_cprm_scr_lib.._&entity_sk._mm_msr_stats as
					select a.* , c.time_sk as tgt_scoring_as_of_time_sk 
						from &m_cprm_src_apdm..mm_measure_stats a
							inner join &m_cprm_src_apdm..time_dim b on b.time_sk = a.scoring_as_of_time_sk
							inner join &lib_apdm..time_dim c on b.period_first_dttm ge c.period_first_dttm and b.period_last_dttm le c.period_last_dttm
						where a.report_specification_sk = 	&m_src_rpt_spec_sk_dv.		
							and a.report_category_sk = (select report_category_sk from &m_cprm_src_apdm..mm_report_category_master where report_category_cd = 'DD' );	/*i18NOK:LINE*/
			quit;


	*---------------Populating on target-------------------------------------;
		%let ins_cols_mm_msr_stats = ;
		%dabt_cprm_get_col_lst(	m_ds_nm=mm_measure_stats , 
								m_src_lib_nm=&m_cprm_src_apdm , 
								m_tgt_lib_nm=&lib_apdm , 
								m_exclued_col= report_specification_sk bin_analysis_scheme_sk scoring_as_of_time_sk,
								m_col_lst=, 
								m_prim_col_nm=,  /* update does not happen */
								m_prim_col_val=,
								m_ins_cols_lst= ins_cols_mm_msr_stats
							  );


			
			proc sql ;
			insert into &lib_apdm..mm_measure_stats
				( &ins_cols_mm_msr_stats. , report_specification_sk, bin_analysis_scheme_sk, scoring_as_of_time_sk)
			select 
				 &ins_cols_mm_msr_stats., &m_tgt_rpt_spec_sk_dv , b.tgt_bin_analys_scheme_sk, a.tgt_scoring_as_of_time_sk
			from 
				&m_cprm_scr_lib.._&entity_sk._mm_msr_stats a 
			inner join 
				&m_cprm_scr_lib.._&entity_sk._mdl_bin_analys_map b
					on a.bin_analysis_scheme_sk = b.bin_analysis_scheme_sk
					;
		quit;
					
	/* End: APDM.MM_MEASURE_STATS         */ /* CPRM CSB-24639 : Table &RM_MODELPERF_DATA_LIBREF.._<MODEL_ID>_SC_VAR_FACT per entity replaced by single table APDM.MIP_MEASURE_STATS */
	
	/* Start: APDM.MM_MEASURE_CATEGORY_STATS         */ /* CPRM CSB-24639 : Table &RM_MODELPERF_DATA_LIBREF.._<MODEL_ID>_SC_VAR_FACT per entity replaced by two single tables (2/2) APDM.MM_MEASURE_CATEGORY_STATS */
		
	
	*-----Creating a scratch table----------------; 
	
			proc sql noprint;
				create table &m_cprm_scr_lib.._&entity_sk._mm_msr_cat_stats as
					select a.* , c.time_sk as tgt_scoring_as_of_time_sk 
						from &m_cprm_src_apdm..mm_measure_category_stats a
							inner join &m_cprm_src_apdm..time_dim b on b.time_sk = a.scoring_as_of_time_sk
							inner join &lib_apdm..time_dim c on b.period_first_dttm ge c.period_first_dttm and b.period_last_dttm le c.period_last_dttm
						where a.report_specification_sk = 	&m_src_rpt_spec_sk_dv.		
						and a.report_category_sk = (select report_category_sk from &m_cprm_src_apdm..mm_report_category_master where report_category_cd = 'DD' );	/*i18NOK:LINE*/
			quit;


	*---------------Populating on target-------------------------------------;
		%let ins_cols_mm_msr_cat_stats = ;
		%dabt_cprm_get_col_lst(	m_ds_nm=mm_measure_category_stats , 
								m_src_lib_nm=&m_cprm_src_apdm , 
								m_tgt_lib_nm=&lib_apdm , 
								m_exclued_col= report_specification_sk bin_analysis_scheme_sk scoring_as_of_time_sk,
								m_col_lst=, 
								m_prim_col_nm=,  /* update does not happen */
								m_prim_col_val=,
								m_ins_cols_lst= ins_cols_mm_msr_cat_stats
							  );


			
			proc sql ;
			insert into &lib_apdm..mm_measure_category_stats
				( &ins_cols_mm_msr_cat_stats. , report_specification_sk, bin_analysis_scheme_sk, scoring_as_of_time_sk)
			select 
				 &ins_cols_mm_msr_cat_stats., &m_tgt_rpt_spec_sk_dv , b.tgt_bin_analys_scheme_sk, a.tgt_scoring_as_of_time_sk
			from 
				&m_cprm_scr_lib.._&entity_sk._mm_msr_cat_stats a 
			inner join 
				&m_cprm_scr_lib.._&entity_sk._mdl_bin_analys_map b
					on a.bin_analysis_scheme_sk = b.bin_analysis_scheme_sk
					;
		quit;
					
	/* End: APDM.MM_MEASURE_CATEGORY_STATS         */ /* CPRM CSB-24639 : Table &RM_MODELPERF_DATA_LIBREF.._<MODEL_ID>_SC_VAR_FACT per entity replaced by two single tables (2/2) APDM.MM_MEASURE_CATEGORY_STATS */
	
	/*Start: APDM.MODEL_RANGE_SCHEME       */ /* CPRM CSB-24639 : Added column REPORT_SPECIFICAITON_SK */
	%let mdl_exist_range_scheme = ;
	proc sql noprint;
		select count(*) into :mdl_exist_range_scheme 			/*i18NOK:LINE*/
			from &lib_apdm..model_range_scheme 
			where report_specification_sk = &m_tgt_rpt_spec_sk_dv. ;
	quit;

	%if &mdl_exist_range_scheme. le 0 %then %do;
		%let m_range_scheme_sk_to_reserve = ;
		proc sql noprint;
			select count(*) into :m_range_scheme_sk_to_reserve 	/*i18NOK:LINE*/
				from &m_cprm_src_apdm..model_range_scheme  
				where report_specification_sk = &m_src_rpt_spec_sk_dv. ;
		quit;

		%let m_range_scheme_seq_start_val = ;
		%dabt_reserve_sequence_values(m_table_nm= model_range_scheme, m_no_of_values_to_reserve= &m_range_scheme_sk_to_reserve., m_out_starting_sequence_value = m_range_scheme_seq_start_val);

		data &m_cprm_scr_lib..&scratch_ds_prefix._range_sch_sk_map;
			set &m_cprm_src_apdm..model_range_scheme(where=(report_specification_sk = &m_src_rpt_spec_sk_dv.));
			if _n_ = 1 then do;
				tgt_range_scheme_sk  + &m_range_scheme_seq_start_val.;
			end;
			else do;
				tgt_range_scheme_sk  + 1;
			end;
		run;
		
		proc sql;
			create table &m_cprm_scr_lib..range_scheme_subset_&entity_sk. 
				as select source_bin_scheme_cd, model_rk, report_specification_sk, range_scheme_sk
			from &m_cprm_src_apdm..model_range_scheme a,
					&m_cprm_src_apdm..range_scheme_type_master b
			where a.range_scheme_type_sk = b.range_scheme_type_sk
				and report_specification_sk = &m_src_rpt_spec_sk_dv.;
		quit;
		
		proc sql;
			create table &m_cprm_scr_lib..range_scheme_map_&entity_sk. as
			select a.bin_analysis_scheme_sk as tgt_bin_analysis_scheme_sk, &m_tgt_model_sk. as tgt_model_sk, &m_tgt_rpt_spec_sk_dv. as tgt_report_specification_sk, c.range_scheme_sk
				from &lib_apdm..bin_analysis_scheme_defn a
					inner join &lib_apdm..report_spec_x_bin_scheme b
						on (a.bin_analysis_scheme_sk = b.bin_analysis_scheme_sk 
								and b.report_specification_sk = &m_tgt_rpt_spec_sk_dv.)
					inner join &m_cprm_scr_lib..range_scheme_subset_&entity_sk. c
						on (a.bin_analysis_scheme_cd = c.source_bin_scheme_cd
								and c.report_specification_sk = &m_src_rpt_spec_sk_dv.);
		quit;


		proc sql noprint;
			insert into &lib_apdm..model_range_scheme 
				(range_scheme_type_sk, active_flg, created_by_user, 
							created_dttm, processed_dttm, last_processed_by_user, 
							model_rk, report_specification_sk, range_scheme_sk, source_bin_scheme_sk)
			select a.range_scheme_type_sk, a.active_flg, a.created_by_user, 
							a.created_dttm, a.processed_dttm, a.last_processed_by_user, 
							b.tgt_model_sk, b.tgt_report_specification_sk, c.tgt_range_scheme_sk, b.tgt_bin_analysis_scheme_sk 
				from &m_cprm_src_apdm..model_range_scheme a, 
						&m_cprm_scr_lib..range_scheme_map_&entity_sk. b,
						&m_cprm_scr_lib..&scratch_ds_prefix._range_sch_sk_map c
				where a.range_scheme_sk = b.range_scheme_sk 
					and a.range_scheme_sk = c.range_scheme_sk;
		quit;

	%end;
	%else %do;
		proc sql noprint;
			update &lib_apdm..model_range_scheme a
				set ACTIVE_FLG = (select b.active_flg from
									&m_cprm_src_apdm..model_range_scheme b
									where a.range_scheme_type_sk = b.range_scheme_type_sk
									and b.model_rk = &entity_sk.)
				where model_rk = &m_tgt_model_sk.;
		quit;

		proc sql noprint;
			create table &m_cprm_scr_lib..&scratch_ds_prefix._range_sch_sk_map as
				select a.*, b.range_scheme_sk as tgt_range_scheme_sk
				from &m_cprm_src_apdm..model_range_scheme a,
						&lib_apdm..model_range_scheme b
				where a.range_scheme_type_sk = b.range_scheme_type_sk
						and a.model_rk = &entity_sk.
						and b.model_rk = &m_tgt_model_sk.;
		quit;

	%end;

		/*Start: &RM_MODELPERF_DATA_LIBREF.._<MODEL>_SC_RANGE_FACT*//*
	
			proc sql noprint;
				create table &m_cprm_scr_lib..sc_range_fact_src_&entity_sk._&m_range_scheme_sk_tkn. as 
					Select *
					from &m_cprm_src_mdl_dev_lib.._&entity_sk._sc_range_fact 
					where model_rk=&entity_sk 
							and range_scheme_sk =  &m_range_scheme_sk_tkn.;
			quit;

			proc sql noprint;
				create table &m_cprm_scr_lib..max_sc_range_fact_src_&entity_sk._&m_range_scheme_sk_tkn. as
					select * from &m_cprm_scr_lib..sc_range_fact_src_&entity_sk._&m_range_scheme_sk_tkn.
					where version_no = 
						(select max(version_no)
							 from  &m_cprm_scr_lib..sc_range_fact_src_&entity_sk._&m_range_scheme_sk_tkn.);
			quit;

			%csbmva_check_and_create_table(libref = &cs_fact_lib., tablename = _&m_tgt_model_sk._sc_range_fact,filename = _model_sc_range_fact.sas );

			%let m_tgt_sc_range_max_version = 0;
			proc sql noprint;
				select coalesce(max(version_no),0) into :m_tgt_sc_range_max_version 
					from &RM_MODELPERF_DATA_LIBREF.._&m_tgt_model_sk._sc_range_fact 
					where model_rk=&m_tgt_model_sk.
						and range_scheme_sk =  &m_tgt_range_scheme_sk.;
			quit;
		
			
			%let ins_cols_sc_range_fact = ;
			%dabt_cprm_get_col_lst(	m_ds_nm=_&m_tgt_model_sk._sc_range_fact, 
									m_src_lib_nm=&m_cprm_src_mdl_dev_lib, 
									m_tgt_lib_nm=&RM_MODELPERF_DATA_LIBREF., 
									m_exclued_col= model_rk range_scheme_sk version_no 
													score_card_sk range_sk no_of_records no_of_events 
													no_of_non_events range_name,
									m_col_lst=, 
									m_prim_col_nm=, 
									m_prim_col_val=,
									m_ins_cols_lst= ins_cols_sc_range_fact
								  );

			proc sql noprint;
				insert into &RM_MODELPERF_DATA_LIBREF.._&m_tgt_model_sk._sc_range_fact 
					(&ins_cols_sc_range_fact., model_rk, version_no, range_scheme_sk,
						score_card_sk, range_sk, no_of_records, no_of_events, 
							no_of_non_events, range_name)
				select &ins_cols_sc_range_fact., &m_tgt_model_sk., &m_tgt_sc_range_max_version. + 1, &m_tgt_range_scheme_sk.,
						b.tgt_scorecard_sk, c.tgt_range_sk, a.no_of_records, a.no_of_events, 
							a.no_of_non_events, a.range_name
					from &m_cprm_scr_lib..max_sc_range_fact_src_&entity_sk._&m_range_scheme_sk_tkn. a 
						inner join &m_cprm_scr_lib..dev_scorecard_sk_map_&entity_sk. b 
							on (a.score_card_sk = b.score_card_sk)
						inner join &m_cprm_scr_lib..&scratch_ds_prefix._range_sk_&m_range_scheme_sk_tkn. c
							on (a.range_sk = c.range_sk);
			quit;

	

		*//*End: &RM_MODELPERF_DATA_LIBREF.._<MODEL>_SC_RANGE_FACT  */

%end;  /* if target has deployed version */	
		
%dabt_initiate_cas_session(cas_session_ref=load_swat_astore);
/*Start-Load astore files for swat based python models*/
%if "&m_mdl_wrkspc_nm" ="SWAT" %then %do;/*Swat python models have astore files*/	/* i18nOK:Line */
	/*-----------Check table existence--------------*/
	filename ast_in filesrvc 
              folderpath="&import_package_path."      /* I18NOK:LINE */
              filename="MDL_ASTORE_&entity_sk..sashdat" debug=http 	/* i18nOK:Line */
              CD="attachment; filename=MDL_ASTORE_&entity_sk..sashdat" recfm=n;	/* i18nOK:Line */
	%let ast_tableExist = %sysfunc(fexist(ast_in));
	
	%if &ast_tableExist. eq 1 %then %do;

		 proc cas;
        table.caslibinfo result=ast / caslib="&DABT_MODELING_ABT_LIBREF." verbose="TRUE";	/* i18nOK:Line */
        exist_ast=findtable(ast);
        if exist_ast then
            saveresult ast dataout=work.ast_info;
        quit;

        %let ast_caslib_path= ;

        proc sql noprint;
            select path into :ast_caslib_path from work.ast_info ;
        quit;

		filename ast_out "&ast_caslib_path./MDL_ASTORE_&m_tgt_model_sk..sashdat"  lrecl=32767 recfm=n;	/* i18nOK:Line */
		
		data _null_;
              rc=fcopy('ast_in', 'ast_out');	/* i18nOK:Line */
           format msg $1000.;
               /* I18NOK:LINE */
               msg=sysmsg();
               put rc=msg=;
           run;
			
		
	%end;

%end;
/*End-Load astore files for python models*/

%dabt_terminate_cas_session(cas_session_ref=load_swat_astore);
	

%mend dabt_cprm_import_mdl_dev_data;
