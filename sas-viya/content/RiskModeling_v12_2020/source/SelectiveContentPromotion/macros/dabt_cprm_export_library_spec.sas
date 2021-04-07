%macro dabt_cprm_export_library_spec(export_specs_ds_lib=, export_specs_ds_nm  =, m_lib_sk_lst= ) ; 

	%local m_ent_type_nm m_ent_type_cd cnt_lib_exist; 

	%let m_ent_type_nm= ;
	%let m_ent_type_cd=LIBRARY; /* I18NOK:LINE */
		
		proc sql noprint;
			select entity_type_nm length = 360		/* sinvsp : Length modified : S1366235  */ 
				into :m_ent_type_nm
			from &lib_apdm..CPRM_ENTITY_MASTER
			where ktrim(kleft(kupcase(entity_type_cd)))="%upcase(&m_ent_type_cd)"; /* I18NOK:LINE */
		quit;
	%let m_ent_type_nm= &m_ent_type_nm; 

		/**************************************************************************************************
			Only Those libraries which are not present in specification will be exported in specification. 
		***************************************************************************************************/ 
			PROC SQL NOPRINT ; 
				CREATE TABLE tmp_lib_entity 
				AS 
				SELECT 				/* sinvsp : Column names, lengths made explicit : S1366235  */ 
							library_sk 
						,   library_short_nm length=360
						,   library_desc length=1800

				FROM  &lib_apdm..library_master a
				LEFT JOIN &export_specs_ds_lib..&export_specs_ds_nm b
				ON 
				 a.library_sk = b.entity_key
				 and ktrim(kleft(kupcase(b.entity_type_cd))) ="%upcase(&m_ent_type_cd)" 	/* I18NOK:LINE */		
				WHERE b.entity_key is NULL
				%IF "&m_lib_sk_lst" ne "*" %THEN %DO;
					AND (a.library_sk) IN (&m_lib_sk_lst)  	/* I18NOK:LINE */
				%END; 
				;	 
			QUIT;

			PROC SQL NOPRINT ; 
				select count(*) into :cnt_lib_exist			/* I18NOK:LINE */
				from tmp_lib_entity ; 	
			QUIT; 
 
		%if &cnt_lib_exist > 0 %then %do ; 
			PROC SQL NOPRINT; 
				INSERT INTO  &export_specs_ds_lib..&export_specs_ds_nm 
				(
					ENTITY_TYPE_CD, 
					ENTITY_TYPE_NM, 
					ENTITY_KEY, 
					ENTITY_NM, 
					ENTITY_DESC, 
					PROMOTE_FLG,
					OWNER	/**** owner added ****/
				) 
				SELECT 
					kcompress("&m_ent_type_cd") as ENTITY_TYPE_CD, 
					kcompress("&m_ent_type_nm") as ENTITY_TYPE_NM,  
					library_sk as ENTITY_KEY, 
					library_short_nm as ENTITY_NM, 
					library_desc as ENTITY_DESC, 
					%IF "&m_lib_sk_lst" = "*" %THEN %DO;  
						&CHECK_FLAG_FALSE as PROMOTE_FLG /* I18NOK:LINE */
					%END;  
					%ELSE %DO;  
						&CHECK_FLAG_TRUE as PROMOTE_FLG /* I18NOK:LINE */
					%END ; 
					,'NA'	/* i18nOK:Line */
				FROM tmp_lib_entity    
				; 
			QUIT; 
		%end; 
	
%mend dabt_cprm_export_library_spec;
