/********************************************************************************************************
   	Module		:  dabt_cprm_import_all_subset

   	Function	:  This macro,for a given project, validates as well imports all the associated subset query.
						If it is run in ANALYSE mode, 
							it validates all the prerequisites are available for the subset query or not
						If it is run in EXECUTE mode, 
							it imports all the associated subset query

   	Parameters	:	NAME							TYPE		DESC
					entity_sk						INPUT		-> Key of the master entity from the source machine
					entity_type_cd					INPUT		-> Type of the master entity
					import_spec_ds_nm				INPUT		-> Name of the control dataset for import spec
					import_package_path				INPUT		-> Complete path where the import package is present
					import_analysis_report_path		INPUT		-> path where the output will be created
					import_analysis_report_ds_nm	INPUT		-> dataset name of pre import analysis table
					mode							INPUT		-> ANALYSE / EXECUTE
*********************************************************************************************************/
%macro dabt_cprm_import_all_subset( entity_sk						= , 
									entity_type_cd					= ,
									import_spec_ds_nm				= ,
									import_package_path				= , 
									import_analysis_report_path		= , 
									import_analysis_report_ds_nm 	= ,
									mode 							=
									);

*==================================================;
* Defining local macro variables ;
*==================================================;

%local m_cprm_src_apdm ; /*Stores libref of source apdm. dabt_assign_lib macro will assign value to this*/

%local m_cprm_scr_lib ;  /*Stores libref for scratch. dabt_assign_lib macro will assign value to this*/

%local m_cprm_imp_ctl  ; /*Stores libref for control library. This lib will have CPRM_IMPORT_PARAM_LIST_TMP . */

%local m_cprm_log_path  ; /*Stores the path where the logs will be created */

*==================================================;
* Create directories for staging tables and logs;
*==================================================;

/* dabt_assign_libs will declare library and send back librefs*/
%let m_apdm_lib = ;
%dabt_assign_libs(tmp_lib=m_cprm_scr_lib,m_workspace_type=CPRM_IMP,src_lib = m_apdm_lib,
                    import_analysis_report_path = &import_analysis_report_path., m_cprm_src_apdm_lib= m_cprm_src_apdm, 
                    m_cprm_ctl_lib = m_cprm_imp_ctl,log_path = m_cprm_log_path);
					
*==================================================;
* Get the master entity sk from the target machine;
*==================================================;

/* CPRM CSB-24614: Accommodating entity type: REPORT_SPECIFICATION */

%if &entity_type_cd = REPORT_SPECIFICATION %then %do;

	proc sql noprint;
			select model_sk into :src_mdl_sk from 
			&m_cprm_src_apdm..mm_report_specification 
			where report_specification_sk = &entity_sk.;
		quit;
	
		%local tgt_entity_mdl_sk;
		%local tgt_entity_sk;
		%dabt_cprm_get_entity_tgt_sk(entity_sk 		= &src_mdl_sk.,
								entity_type_cd 			= MODEL,
								src_apdm_lib 			= &m_cprm_src_apdm.,
								tgt_apdm_lib 			= &m_apdm_lib.,
								return_entity_tgt_sk 	= tgt_entity_mdl_sk);
		
		%let tgt_entity_mdl_sk = &tgt_entity_mdl_sk. ;		
				
		%if &tgt_entity_mdl_sk. ne %then %do;
		proc sql noprint;
			select report_specification_sk into :tgt_entity_sk 
			from &m_apdm_lib..mm_report_specification
			where ready_for_deployment_flg = &CHECK_FLAG_TRUE. 		 		/* i18nOK:Line */	
				and model_sk = &tgt_entity_mdl_sk. ;
		quit;						
		%end;
		
		
		%let entity_master_table_nm = MM_&entity_type_cd. ;		/* vp ??		*/
		%let entity_key_nm = &entity_type_cd._sk;			/* vp ??		*/
		
%end;
%else %do; /* This is for entity = model i.e. report_spec type as MS */
	
		%local tgt_entity_sk;
		%dabt_cprm_get_entity_tgt_sk(	entity_sk 		= &entity_sk.,
								entity_type_cd 			= &entity_type_cd,
								src_apdm_lib 			= &m_cprm_src_apdm.,
								tgt_apdm_lib 			= &m_apdm_lib.,
								return_entity_tgt_sk 	= tgt_entity_sk);
			
*==================================================;
* Target table name ;
*==================================================;

%let entity_master_table_nm = &entity_type_cd._master;		/* vp ??		*/
%let entity_key_nm = &entity_type_cd._sk;			/* vp ??		*/

%end;

*==================================================;
* Get all associated subset query from source machine;
*==================================================;

%local src_subset_query_sk src_implicit_subset_query_sk src_exclusion_subset_query_sk;

/*Finding subset_query_sk from source_apdm.Target_x_entity table */
%let src_subset_query_sk = .;
proc sql noprint;
	select 
		target_query_sk into:src_subset_query_sk
	from &m_cprm_src_apdm..target_x_entity
	where entity_sk=&entity_sk.
		and kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
		and kupcase(filter_type_cd)="INCLUSION";					/*i18NOK:LINE*/
quit;

/*Finding src_implicit_subset_query_sk from source_apdm.Target_x_entity table */
%let src_implicit_subset_query_sk = .;
proc sql noprint;
	select 
		target_query_sk into:src_implicit_subset_query_sk
	from &m_cprm_src_apdm..target_x_entity
	where entity_sk=&entity_sk.
		and kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
		and kupcase(filter_type_cd)="SYSTEM_INCLUSION";				/*i18NOK:LINE*/
quit;


/*Finding exclusion_subset_query_sk from source_apdm.Target_x_entity table */
%let src_exclusion_subset_query_sk = .;
proc sql noprint;
	select 
		target_query_sk into:src_exclusion_subset_query_sk
	from &m_cprm_src_apdm..target_x_entity
	where entity_sk=&entity_sk.
		and kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
		and kupcase(filter_type_cd)="EXCLUSION";					/*i18NOK:LINE*/
quit;

*==================================================;
* Get all associated subset query from target machine;
*==================================================;

%local tgt_subset_query_sk tgt_implicit_subset_query_sk tgt_exclusion_subset_query_sk;

%if &tgt_entity_sk. ne %then %do;

	/*Finding tgt_subset_query_sk from target_apdm.Target_x_entity table */
	proc sql noprint;
		select 
			target_query_sk into:tgt_subset_query_sk
		from &m_apdm_lib..target_x_entity
		where entity_sk=&tgt_entity_sk.
			and kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
			and kupcase(filter_type_cd)="INCLUSION";				/*i18NOK:LINE*/
	quit;

	/*Finding tgt_implicit_subset_query_sk from target_apdm.Target_x_entity table */
	proc sql noprint;
		select 
			target_query_sk into:tgt_implicit_subset_query_sk
		from &m_apdm_lib..target_x_entity
		where entity_sk=&tgt_entity_sk.
			and kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
			and kupcase(filter_type_cd)="SYSTEM_INCLUSION";			/*i18NOK:LINE*/
	quit;


	/*Finding tgt_exclusion_subset_query_sk from target_apdm.Target_x_entity table */
	proc sql noprint;
		select 
			target_query_sk into:tgt_exclusion_subset_query_sk
		from &m_apdm_lib..target_x_entity
		where entity_sk=&tgt_entity_sk.
			and kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
			and kupcase(filter_type_cd)="EXCLUSION";				/*i18NOK:LINE*/
	quit;
%end;

%let tgt_subset_query_sk = %sysfunc(tranwrd(&tgt_subset_query_sk,%str(.),%str()));							/*i18NOK:LINE*/
%let tgt_implicit_subset_query_sk = %sysfunc(tranwrd(&tgt_implicit_subset_query_sk,%str(.),%str()));		/*i18NOK:LINE*/
%let tgt_exclusion_subset_query_sk = %sysfunc(tranwrd(&tgt_exclusion_subset_query_sk,%str(.),%str()));		/*i18NOK:LINE*/

*==================================================;
* Call the macro to import subset query ;
*==================================================;

%let subset_query_list = subset_query_sk implicit_subset_query_sk exclusion_subset_query_sk;
%let subset_query_type_list = INCLUSION SYSTEM_INCLUSION EXCLUSION;

%do subset_query_type_loop = 1 %to 3;

	%let subset_query = %scan(&subset_query_list,&subset_query_type_loop,%str( ));							/*i18NOK:LINE*/
	%let subset_query_type = %scan(&subset_query_type_list,&subset_query_type_loop,%str( ));				/*i18NOK:LINE*/

	
	%if &&src_&subset_query. ne . %then %do;

		%dabt_cprm_import_subset_query ( 	master_entity_sk 			= &entity_sk.,
											master_entity_type_cd 		= &entity_type_cd.,
											subset_query_src_sk 		= &&src_&subset_query.,
											src_apdm_lib 				= &m_cprm_src_apdm.,
											tgt_apdm_lib 				= &m_apdm_lib.,
											cprm_scratch_lib			= &m_cprm_scr_lib.,
											mode 						= &MODE.,
											populate_pre_anlys_ds_flg 	= &CHECK_FLAG_TRUE.,
											subset_query_tgt_sk_var		= tgt_&subset_query.,
											return_validation_rslt_flg 	= 
												);

		%if &mode = EXECUTE %then %do;
		/*Deleting exisiting record from target_x_entity
				for selected subset query type(SYSTEM_INCLUSION/INCLUSION/EXCLUSION) */
				
			%if &tgt_entity_sk. ne %then %do;

				proc sql noprint;
					delete from &m_apdm_lib..target_x_entity
						where kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
							and filter_type_cd=kupcase("&subset_query_type.")
							and entity_sk = &tgt_entity_sk.;
				quit;

				proc sql noprint;
					insert into &m_apdm_lib..target_x_entity
						(target_query_sk,entity_type_cd,filter_type_cd,entity_sk)
					values(&&tgt_&subset_query.,"&entity_type_cd.","&subset_query_type.",&tgt_entity_sk.);
				quit;
				
			%end;
		%end;

	%end;
	%else %if &&tgt_&subset_query. ne and &mode = EXECUTE %then %do;
	
		proc sql noprint;
			delete from &m_apdm_lib..target_x_entity
				where kupcase(entity_type_cd)=kupcase("&entity_type_cd.")
					and filter_type_cd=kupcase("&subset_query_type.")
					and entity_sk = &tgt_entity_sk.;
		quit;
		
		proc sql noprint;

			delete from &m_apdm_lib..target_node_exprssion_x_value
				where target_node_sk IN (select target_node_sk from &m_apdm_lib..target_node
					where target_query_sk = &&tgt_&subset_query.);

			delete from &m_apdm_lib..target_node_expression 
				where target_node_sk IN (select target_node_sk from &m_apdm_lib..target_node
					where target_query_sk = &&tgt_&subset_query. );

			delete from &m_apdm_lib..target_node 
				where target_query_sk = &&tgt_&subset_query.;

			delete from &m_apdm_lib..target_query_master 
				where target_query_sk = &&tgt_&subset_query.;
		quit;

	%end;
		
%end;


%MEND dabt_cprm_import_all_subset;;


								
					
					

										
