/********************************************************************************************************
   Module:  dabt_cprm_export_purpose_spec

   Function:  This macro identifies the purpose on the source machine, to be promoted 
			  to the target.

   Parameters: INPUT: 
			1. export_specs_ds_lib : library where the data source containing the entity details
									 exists.
			2. export_specs_ds_nm  : dataset which contains the entity details to be exported.
			3. purpose_sk_lst 	   : comma separated list of purpose to be exported. If the value is '*'
									 that means all the datasources will be exported else only
									 the purposes mentioned will be exported.

*********************************************************************************************************/

%macro dabt_cprm_export_purpose_spec(export_specs_ds_lib=, export_specs_ds_nm  =, purpose_sk_lst= );

	%local m_entity_type_cd m_entity_type_nm;

	%let export_specs_ds_lib = &export_specs_ds_lib.;
	%let export_specs_ds_nm = &export_specs_ds_nm.;
	%let purpose_sk_lst = &purpose_sk_lst.;

	%let m_entity_type_nm= ;
	%let m_entity_type_cd=PURPOSE;	/* i18nOK:Line */
		
	proc sql;
		select entity_type_nm length=360 into :m_entity_type_nm      /* sinvsp : length modified : S1366235  */
		from &lib_apdm..cprm_entity_master
		where ktrim(kleft(kupcase(entity_type_cd)))= "&m_entity_type_cd.";
	quit;

	/*Check if the data source is already present in the specification dataset.*/ 

	proc sql noprint ; 
		create table work.purpose_to_import as 
				select 												/* sinvsp : Column names, lengths made explicit : S1366235  */
						purpose_sk 
					,	purpose_short_nm length=360
					,	purpose_desc length=1800 
				from  &lib_apdm..purpose_master 
				where 
					%if "&purpose_sk_lst" ne "*" %then %do;	/*i18nOK:Line */
						(purpose_master.purpose_sk) in (&purpose_sk_lst) and  
					%end; 
					purpose_master.purpose_sk not in 
						(select param.entity_key from &export_specs_ds_lib..&export_specs_ds_nm param
							where %upcase(strip(param.entity_type_cd)) ="&m_entity_type_cd.") /* i18nOK:LINE */
		; 
	quit;


	proc sql noprint ; 
		select count(*) into :cnt_purpose_to_import		/* i18nOK:LINE */
			from work.purpose_to_import; 
	quit; 

	/* Insert the data sources that are not present in the specification dataset.*/
	%if &cnt_purpose_to_import. gt 0 %then %do;	/* i18nOK:Line */

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
					purpose_sk,
					purpose_short_nm, 
					purpose_desc,  
					%if "&purpose_sk_lst." = "*" %then %do;	/* i18nOK:Line */
						&CHECK_FLAG_FALSE
					%end;   
					%else %do;
						&CHECK_FLAG_TRUE
					%end;
					,'NA'	/* i18nOK:Line */
			from work.purpose_to_import
			%if  "&purpose_sk_lst." ne "*"  %then %do;	/* i18nOK:Line */
				where purpose_sk in (&purpose_sk_lst.)
			%end;
			;
		quit; 
	%end;
	
%mend dabt_cprm_export_purpose_spec;
