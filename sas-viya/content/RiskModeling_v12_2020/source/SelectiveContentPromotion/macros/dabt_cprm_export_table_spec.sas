/********************************************************************************************************
   Module:  dabt_cprm_export_table_spec

   Function:  This macro identifies the datasources on the source machine, to be promoted 
			  to the target.

   Parameters: INPUT: 
			1. export_specs_ds_lib : library where the data source containing the entity details
									 exists.
			2. export_specs_ds_nm  : dataset which contains the entity details to be exported.
			3. table_sk_lst 	   : list of data sources to be exported. If the value is '*'
									 that means all the datasources will be exported else only
									 the comma separated data sources mentioned, will be exported.

*********************************************************************************************************/

%macro dabt_cprm_export_table_spec(export_specs_ds_lib=, export_specs_ds_nm  =, table_sk_lst= );

	%local m_entity_type_cd m_entity_type_nm;

	%let export_specs_ds_lib = &export_specs_ds_lib.;
	%let export_specs_ds_nm = &export_specs_ds_nm.;
	%let table_sk_lst = &table_sk_lst.;

	%let m_entity_type_nm= ;
	%let m_entity_type_cd=DATASOURCE;	/* i18nOK:Line */
		
	proc sql noprint;
		select entity_type_nm length = 360 into :m_entity_type_nm  				/* sinvsp : Column names, length modified : S1366235  */
		from &lib_apdm..cprm_entity_master
		where ktrim(kleft(kupcase(entity_type_cd)))= "&m_entity_type_cd.";
	quit;

	/*Check if the data source is already present in the specification dataset.*/ 

	proc sql noprint ; 
		create table work.tables_to_import as 
				select 												/* sinvsp : Column names, lengths made explicit : S1366235  */
					source_table_sk 
					, source_table_short_nm length = 360
					, source_table_desc length = 1800				
				from  &lib_apdm..source_table_master src_tbl 
				where 
					%if "&table_sk_lst" ne "*" %then %do;	/* i18nOK:Line */
						(src_tbl.source_table_sk) in (&table_sk_lst) and  
					%end; 
					src_tbl.source_table_sk not in 
						(select param.entity_key from &export_specs_ds_lib..&export_specs_ds_nm param
							where strip(param.entity_type_cd) =%upcase("&m_entity_type_cd."))	/* i18nOK:Line */

		; 
	quit;


	proc sql noprint ; 
		select count(*) into :cnt_tables_to_import	/* i18nOK:Line */
			from work.tables_to_import; 
	quit; 

	/* Insert the data sources that are not present in the specification dataset.*/
	%if &cnt_tables_to_import. gt 0 %then %do;	/* i18nOK:Line */

		Proc sql noprint;
			insert into  &export_specs_ds_lib..&export_specs_ds_nm.
			(
				ENTITY_TYPE_CD, 
				ENTITY_TYPE_NM, 
				ENTITY_KEY, 
				ENTITY_NM, 
				ENTITY_DESC, 
				PROMOTE_FLG,OWNER
			) 
			select  "&m_entity_type_cd.", 
					"&m_entity_type_nm.", 
					source_table_sk,
					source_table_short_nm, 
					source_table_desc,  
					%if "&table_sk_lst." = "*" %then %do;	/* i18nOK:Line */
						&CHECK_FLAG_FALSE
					%end;   
					%else %do;
						&CHECK_FLAG_TRUE
					%end;
					,'NA'	/* i18nOK:Line */
			from work.tables_to_import
			%if  "&table_sk_lst." ne "*"  %then %do;	/* i18nOK:Line */
				where source_table_sk in (&table_sk_lst.)
			%end;
			;
		quit; 

	%end;
	
%mend dabt_cprm_export_table_spec;
