/*****************************************************************/
/* NAME: dabt_cprm_export_ext_code_spec.sas                      */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to export all entities in to excel sheet   */
/*                                                               */
/* Parameters :  export_specs_ds_lib:user scratch library name   */
/* 			     export_specs_ds_nm:dataset to which entity
					data has to append.
				 m_ext_cd_sk_lst: list of external_code_sk
					which has to export.By default will be * 	 */
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Interanally Called by dabt_cprm_export_ext_code_spec */
/*          %dabt_cprm_export_spec_ext_code(export_specs_ds_lib=, 
			export_specs_ds_nm=,m_ext_cd_sk_lst=*);  			 */
/*                                                               */
/*****************************************************************/

/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*4May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/

%macro dabt_cprm_export_ext_code_spec(export_specs_ds_lib=, export_specs_ds_nm  =, m_ext_cd_sk_lst=* );
	
	%local m_entity_type_cd 
		  m_entity_type_nm ;

	%let m_entity_type_cd = EXT_CODE;  /* i18NOK:LINE */
	
	proc sql noprint;
		select entity_type_nm length = 360 into: m_entity_type_nm	/* sinvsp : Length modified : S1366235  */
			from &lib_apdm..cprm_entity_master
			where ktrim(kleft(kupcase(entity_type_cd))) = "&m_entity_type_cd."; /* i18nOK:EMS */
	quit;

	%let m_entity_type_nm = &m_entity_type_nm.;
	
	/**************************************************************************************************
			Only Those External code  which are not present in specification will be exported in specification. 
	***************************************************************************************************/ 
	proc sql noprint ; 
		create table work.tmp_ext_cd_entity as 
			select 							/* sinvsp : Column names, lengths made explicit : S1366235  */
				 external_code_sk 
				,external_code_short_nm length = 360
				,external_code_desc length = 1800
			from  &lib_apdm..external_code_master ext_cd_master
			where 
				%if "&m_ext_cd_sk_lst" ne "*" %then %do;
					(ext_cd_master.external_code_sk) in (&m_ext_cd_sk_lst) and  
				%end; 
				ext_cd_master.external_code_sk not in 
					(select param.entity_key from &export_specs_ds_lib..&export_specs_ds_nm param
						where kstrip(param.entity_type_cd) =%upcase("&m_entity_type_cd.")); 
	quit;

	%dabt_err_chk(type=SQL);

	proc sql noprint;
		insert into &export_specs_ds_lib..&export_specs_ds_nm.(
				entity_type_cd ,entity_type_nm ,entity_key ,
				entity_nm,entity_desc ,promote_flg,owner)    /**** owner added ****/
			select "&m_entity_type_cd.","&m_entity_type_nm.", external_code_sk,
				external_code_short_nm , external_code_desc ,
					%if "&m_ext_cd_sk_lst." ne "*" %then %do;
						&check_flag_true.
					%end;
					%else %do;
						&check_flag_false.
					%end;
				,'NA'	/* i18nOK:Line */
			from work.tmp_ext_cd_entity	;
	quit;

	%dabt_err_chk(type=SQL);

	%dabt_drop_table(m_table_nm=work.tmp_ext_cd_entity);

%mend dabt_cprm_export_ext_code_spec;
