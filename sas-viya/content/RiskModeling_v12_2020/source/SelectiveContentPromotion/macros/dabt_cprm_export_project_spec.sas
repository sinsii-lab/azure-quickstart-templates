/*********************************************************************************************
* Macro Name : dabt_cprm_export_project_spec.sas
* Function   : Insert required Project entities 
			   into CPRM_EXPORT_SPECIFICATION table which will 
			   be converted into excel later. 
* Input Parameters : 
*********************************************************************************************/

%macro dabt_cprm_export_project_spec(export_specs_ds_lib=, export_specs_ds_nm  =, m_prj_sk_lst= ) ; 

	%local m_ent_type_nm m_ent_type_cd cnt_prj_exist; 

	%let m_ent_type_nm= ;
	%let m_ent_type_cd=PROJECT; /* I18NOK:LINE */
		
		proc sql noprint;
			select entity_type_nm length = 360  					/* sinvsp : Length modified : S1366235  */
				into :m_ent_type_nm
			from &lib_apdm..CPRM_ENTITY_MASTER
			where ktrim(kleft(kupcase(entity_type_cd)))="%upcase(&m_ent_type_cd)"; /* I18NOK:LINE */
		quit;
	%let m_ent_type_nm= &m_ent_type_nm; 

		/**************************************************************************************************
			Only Those Projects which are not present in specification will be exported in specification. 
		***************************************************************************************************/ 
			PROC SQL NOPRINT ; 
				CREATE TABLE tmp_prj_entity 
				AS 
				SELECT project_sk , 
					project_short_nm length = 360, 
					project_desc length = 1800	,		/* sinvsp : Column names, lengths made explicit : S1366235  */
					owned_by_user length = 32
				FROM  &lib_apdm..project_master a
				LEFT JOIN &export_specs_ds_lib..&export_specs_ds_nm b
				ON 
				 a.project_sk = b.entity_key
				 and ktrim(kleft(kupcase(b.entity_type_cd))) ="%upcase(&m_ent_type_cd)"  /* I18NOK:LINE */
				WHERE b.entity_key is NULL
				%IF "&m_prj_sk_lst" ne "*" %THEN %DO;
					AND (a.project_sk) IN (&m_prj_sk_lst)  	/* I18NOK:LINE */
				%END; 
				;	 
			QUIT;

			PROC SQL NOPRINT ; 
				select count(*) into :cnt_prj_exist	/* I18NOK:LINE */
				from tmp_prj_entity ; 
			QUIT; 
 
		%if &cnt_prj_exist > 0 %then %do ; 
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
					project_sk as ENTITY_KEY, 
					project_short_nm as ENTITY_NM, 
					project_desc as ENTITY_DESC, 
					%IF "&m_prj_sk_lst" = "*" %THEN %DO;  
						&CHECK_FLAG_FALSE as PROMOTE_FLG /* I18NOK:LINE */
					%END;  
					%ELSE %DO;  
						&CHECK_FLAG_TRUE as PROMOTE_FLG /* I18NOK:LINE */
					%END ; 
					,owned_by_user as OWNER
				FROM tmp_prj_entity    
				; 
			QUIT; 
		%end; 
	
%mend dabt_cprm_export_project_spec;
