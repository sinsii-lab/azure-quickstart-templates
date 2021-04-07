/*********************************************************************************************
* Macro Name : dabt_cprm_export_sbstmp_spec.sas
* Function   : Insert required Subset Map and dependent entities 
			   into CPRM_EXPORT_SPECIFICATION table which will 
			   be converted into excel later. 
* Input Parameters : 
*********************************************************************************************/

%macro dabt_cprm_export_sbstmp_spec(export_specs_ds_lib=, export_specs_ds_nm  =, m_sbstmp_sk_lst=*); 

	%local m_ent_type_nm m_ent_type_cd m_assoc_level_lst m_assoc_table_lst cnt_sbst_exist;

	%let m_ent_type_nm= ;
	%let m_ent_type_cd=SUBSET_MAP; /* I18NOK:LINE */
		
		proc sql noprint;
			select entity_type_nm 	length = 360 		/* sinvsp : Length modified : S1366235  */
				into :m_ent_type_nm
			from &lib_apdm..CPRM_ENTITY_MASTER
			where ktrim(kleft(kupcase(entity_type_cd)))="%upcase(&m_ent_type_cd)"; /* I18NOK:LINE */
		quit;
	%let m_ent_type_nm= &m_ent_type_nm; 


/********************************************************************************
  check if any subset map is already present in export specification dataset.
*********************************************************************************/
	proc sql noprint ; 					/* sinvsp : Column names, lengths made explicit : S1366235  */

		create table tmp_sbstmp_entity 
		as 
		select subset_from_path_sk,
			   from_path_short_nm length = 360,
			   from_path_desc length = 1800		
		from &lib_apdm..subset_from_path_master a  
		left join &export_specs_ds_lib..&export_specs_ds_nm b
			on a.subset_from_path_sk = b.entity_key
			and kcompress(kupcase(b.entity_type_cd)) = %upcase("&m_ent_type_cd") /* I18NOK:LINE */
		Where  b.entity_key is NULL 
		%IF "&m_sbstmp_sk_lst" ne "*" %THEN %DO; 
			  and a.subset_from_path_sk IN (&m_sbstmp_sk_lst)
		%END; 
		;
	quit; 

	 PROC SQL NOPRINT ; 
			 select count(*) into :cnt_sbst_exist	/* I18NOK:LINE */
			 from tmp_sbstmp_entity ; 
	 QUIT;

/********************************************************************************
  Export associted levels and tables for subset map.
*********************************************************************************/
/*
	proc sql noprint ; 
		create table tmp_assoc_table_lst 
		as 
		select sbst_table_sk 
		from 
		(
			select source_table_sk as sbst_table_sk
			from &lib_apdm..subset_from_path_x_level a,
				 &lib_apdm..source_column_master b
			where a.select_source_column_sk = b.source_column_sk
			%IF "&m_sbstmp_sk_lst" = "*" %THEN %DO; 
				and subset_from_path_sk IN (&m_sbstmp_sk_lst) 		
			%END; 
			UNION
			select left_table_sk as sbst_table_sk
			from &lib_apdm..subset_table_join_condition
			where 
			%IF "&m_sbstmp_sk_lst" = "*" %THEN %DO;  
					subset_from_path_sk IN (&m_sbstmp_sk_lst)      
			%END; 
			UNION 
			select right_table_sk as sbst_table_sk
			from &lib_apdm..subset_table_join_condition
			where 
			%IF "&m_sbstmp_sk_lst" = "*" %THEN %DO; 
					subset_from_path_sk IN (&m_sbstmp_sk_lst) 		
			%END;
		)
		;
	quit;

	proc sql noprint ; 
		select distinct level_sk into : m_assoc_level_lst separated by ','
		from &lib_apdm..subset_from_path_x_level
		where
			%IF "&m_sbstmp_sk_lst" = "*" %THEN %DO;
				subset_from_path_sk IN (&m_sbstmp_sk_lst)				
			%END;
		;
	quit; 

	%dabt_cprm_export_soa_spec(	export_specs_ds_lib=&export_specs_ds_lib, 
								export_specs_ds_nm  =&export_specs_ds_nm, 
								level_sk_lst= %str(&m_assoc_level_lst));

	proc sql noprint ; 
		select sbst_table_sk into:m_assoc_table_lst separated by ','
		from tmp_assoc_table_lst
		where sbst_table_sk IS NOT NULL
		;
	quit;

	%dabt_cprm_export_table_spec(
								 export_specs_ds_lib=&export_specs_ds_lib, 
								 export_specs_ds_nm  =&export_specs_ds_nm, 
								 table_sk_lst=%str(&m_assoc_table_lst) 			
								);
*/
/*****************************************************************
	Insert specification details in export specification dataset.
******************************************************************/
		%if &cnt_sbst_exist > 0 %then %do ; 
			PROC SQL NOPRINT; 
				INSERT INTO  &export_specs_ds_lib..&export_specs_ds_nm 
				(
					ENTITY_TYPE_CD, 
					ENTITY_TYPE_NM, 
					ENTITY_KEY, 
					ENTITY_NM, 
					ENTITY_DESC, 
					PROMOTE_FLG,OWNER
				) 
				SELECT 
					kcompress("&m_ent_type_cd") as ENTITY_TYPE_CD, 
					kcompress("&m_ent_type_nm") as ENTITY_TYPE_NM,  
					subset_from_path_sk as ENTITY_KEY, 
					from_path_short_nm as ENTITY_NM, 
					from_path_desc as ENTITY_DESC, 
					%IF "&m_sbstmp_sk_lst" = "*" %THEN %DO;  
						&CHECK_FLAG_FALSE as PROMOTE_FLG /* I18NOK:LINE */
					%END;  
					%ELSE %DO;  
						&CHECK_FLAG_TRUE as PROMOTE_FLG /* I18NOK:LINE */
					%END ; 
					,'NA'	/* i18nOK:Line */
				FROM tmp_sbstmp_entity    
				; 
			QUIT; 
		%end; 

%mend dabt_cprm_export_sbstmp_spec; 
