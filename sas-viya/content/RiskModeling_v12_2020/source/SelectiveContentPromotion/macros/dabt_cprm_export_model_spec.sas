/********************************************************************************************************
   Module:  dabt_cprm_export_model_spec

   Function:  This macro identifies the Models on the source machine, to be promoted 
			  to the target.

   Parameters: INPUT: 
			1. export_specs_ds_lib : library where the data source containing the entity details
									 exists.
			2. export_specs_ds_nm  : dataset which contains the entity details to be exported.
			3. model_sk_lst 	   : comma separated list of models to be exported. If the value is '*'
									 that means all the datasources will be exported else only
									 the models mentioned will be exported.

*********************************************************************************************************/

%macro dabt_cprm_export_model_spec(export_specs_ds_lib=, export_specs_ds_nm  =, m_model_sk_lst= );

	%local m_entity_type_cd m_entity_type_nm;

	%let export_specs_ds_lib = &export_specs_ds_lib.;
	%let export_specs_ds_nm = &export_specs_ds_nm.;
	%let m_model_sk_lst = &m_model_sk_lst.;
	%let m_entity_type_nm= ;
	%let m_entity_type_cd=MODEL;	/* i18nOK:Line */
		
	proc sql;
		select entity_type_nm length = 360  					/* sinvsp : Length modified : S1366235  */
		into :m_entity_type_nm
		from &lib_apdm..cprm_entity_master
		where ktrim(kleft(kupcase(entity_type_cd)))= "&m_entity_type_cd.";
	quit;

	/*Check if the data source is already present in the specification dataset.*/ 

	proc sql noprint ; /* sinvsp : Column names, lengths made explicit : S1366235  */
		create table work.model_to_import as 
				select model_sk,
					model_short_nm length = 360, 
					model_desc  length = 1800,
					owned_by_user length = 32
				from  &lib_apdm..model_master model_master
				where 
					%if "&m_model_sk_lst" ne "*" %then %do;	/*i18nOK:Line */
						(model_master.model_sk) in (&m_model_sk_lst) and  
					%end; 
					model_master.model_sk not in 
						(select param.entity_key from &export_specs_ds_lib..&export_specs_ds_nm param
							where upcase(strip(param.entity_type_cd)) ="&m_entity_type_cd.") /* i18nOK:LINE */
		; 
	quit;


	proc sql noprint ; 
		select count(*) into :cnt_model_to_import	/* i18nOK:LINE */
			from work.model_to_import; 
	quit; 

	/* Insert the data sources that are not present in the specification dataset.*/
	%if &cnt_model_to_import. gt 0 %then %do;	/* i18nOK:Line */

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
					model_sk,
					model_short_nm, 
					model_desc,  
					%if "&m_model_sk_lst." = "*" %then %do;	/* i18nOK:Line */
						&CHECK_FLAG_FALSE
					%end;   
					%else %do;
						&CHECK_FLAG_TRUE
					%end;
					,owned_by_user as OWNER
			from work.model_to_import
			%if  "&m_model_sk_lst." ne "*"  %then %do;	/* i18nOK:Line */
				where model_sk in (&m_model_sk_lst.)
			%end;
			;
		quit; 
	%end;
	
%mend dabt_cprm_export_model_spec;
