/********************************************************************************************************
   Module:  dabt_cprm_exprt_as_of_time_spec

   Function:  This macro identifies the time periods for direct variables on the source machine, to be promoted 
			  to the target.

   Parameters: INPUT: 
			1. export_specs_ds_lib : library where the data source containing the entity details
									 exists.
			2. export_specs_ds_nm  : dataset which contains the entity details to be exported.
			3. as_of_time_sk_lst 	   : comma separated list of time periods to be exported. If the value is '*'
									 that means all the time periods will be exported else only
									 the time periods mentioned will be exported.

*********************************************************************************************************/

%macro dabt_cprm_exprt_as_of_time_spec(export_specs_ds_lib=, export_specs_ds_nm  =, as_of_time_sk_lst= );

	%local m_entity_type_cd m_entity_type_nm;

	%let export_specs_ds_lib = &export_specs_ds_lib.;
	%let export_specs_ds_nm = &export_specs_ds_nm.;
	%let as_of_time_sk_lst = &as_of_time_sk_lst.;
	%let m_entity_type_nm= ;
	%let m_entity_type_cd=AS_OF_TIME;	/* i18nOK:Line */
		
	proc sql;			/* sinvsp : Length modified : S1366235  */
		select entity_type_nm length = 360 	into :m_entity_type_nm			
		from &lib_apdm..cprm_entity_master
		where ktrim(kleft(kupcase(entity_type_cd)))= "&m_entity_type_cd.";
	quit;

	/*Check if the data source is already present in the specification dataset.*/ 

	proc sql noprint ; 	/* sinvsp : Column names, lengths made explicit : S1366235  */
		create table work.time_to_import as 					
				select as_of_time_sk,							
					as_of_time_short_nm length = 360, 
					as_of_time_desc length = 1800 
				from  &lib_apdm..Source_as_of_time_master Source_as_of_time_master
				where 
					%if "&as_of_time_sk_lst" ne "*" %then %do;	/*i18nOK:Line */
						(Source_as_of_time_master.as_of_time_sk) in (&as_of_time_sk_lst) and  
					%end; 
					Source_as_of_time_master.as_of_time_sk not in 
						(select param.entity_key from &export_specs_ds_lib..&export_specs_ds_nm param
							where %upcase(strip(param.entity_type_cd)) ="&m_entity_type_cd.") /* i18nOK:LINE */
		; 
	quit;


	proc sql noprint ; 
		select count(*) into :cnt_time_to_import	/* i18nOK:LINE */
			from work.time_to_import; 
	quit; 

	/* Insert the data sources that are not present in the specification dataset.*/
	%if &cnt_time_to_import. gt 0 %then %do;	/* i18nOK:Line */

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
					as_of_time_sk,
					as_of_time_short_nm, 
					as_of_time_desc,  
					%if "&as_of_time_sk_lst." = "*" %then %do;	/* i18nOK:Line */
						&CHECK_FLAG_FALSE
					%end;   
					%else %do;
						&CHECK_FLAG_TRUE
					%end;
					,'NA'	/* i18nOK:Line */
			from work.time_to_import
			%if  "&as_of_time_sk_lst." ne "*"  %then %do;	/* i18nOK:Line */
				where as_of_time_sk in (&as_of_time_sk_lst.)
			%end;
			;
		quit; 
	%end;
	
%mend dabt_cprm_exprt_as_of_time_spec;
