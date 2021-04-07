/**************************************************************************
	Module		:  cprm_src_tgt_entity_mapping_info

   	Function	:  This macro will be used to create source_target_entity_mapping.csv file as per 
					APDM table named CPRM_SRC_TGT_ENTITY_MAPPING. 
					if type = CURRENT then only records which has latest_import_flg=Y gets extracted else all
					
	Author      :  CSB Team
	Parameter   :  type = CURRENT | ALL
	
**************************************************************************/
%macro cprm_src_tgt_entity_mapping_info(m_type=CURRENT);

	%let syscc = 0;

	%let m_type=&m_type.;  /** type can be CURRENT or ALL ***/
	%let export_spec_file_path= %str(/&m_file_srvr_job_folder_path/&m_job_sk);
	%let export_spec_file_nm=source_target_entity_mapping;
	%let export_specs_ds_lib=work;
	%let export_specs_ds_nm = &export_spec_file_nm.;

	%dabt_drop_table(m_table_nm=&export_specs_ds_lib..&export_specs_ds_nm.);

	%let cprm_entity_type_cd_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_ENTITY_TYPE_CD_LABEL, noquote)); 
	%let cprm_source_entity_nm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_SOURCE_ENTITY_NM_LABEL, noquote)); 
	%let cprm_source_entity_id_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_SOURCE_ENTITY_ID_LABEL, noquote)); 
	%let cprm_target_entity_id_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_TARGET_ENTITY_ID_LABEL, noquote)); 
	%let cprm_latest_import_flg_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_LATEST_IMPORT_FLG_LABEL, noquote)); 
	%let cprm_created_dttm_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_CREATED_DTTM_LABEL, noquote)); 
	%let cprm_created_by_user_label = %sysfunc(sasmsg(work.dabt_cprm_misc, CPRM_CREATED_BY_USER_LABEL, noquote)); 
	
	proc sql noprint;
		create table &export_specs_ds_lib..&export_specs_ds_nm. ( 
			ENTITY_TYPE_CD  character(10) not null 
					label="&cprm_entity_type_cd_label.", 
			SOURCE_ENTITY_NM  character(360) not null 
					label="&cprm_source_entity_nm_label.", 
			SOURCE_ENTITY_ID  numeric(10) not null 
					label="&cprm_source_entity_id_label.", 
			TARGET_ENTITY_ID  numeric(10)  not null 
					label="&cprm_target_entity_id_label.", 
		%if &m_type eq ALL %then %do;
			LATEST_IMPORT_FLG  character(1) not null 
					label="&cprm_latest_import_flg_label.", 
		%end;
			CREATED_DTTM  date not null FORMAT =DATETIME25.6 INFORMAT=DATETIME25.6	/*i18NOK:LINE*/
					label="&cprm_created_dttm_label.", 
			CREATED_BY_USER  character(360)
					label="&cprm_created_by_user_label."
	
		);
	quit;

	proc sql noprint;
		create table &export_specs_ds_lib..&export_specs_ds_nm._tmp
		as select * from &lib_apdm..CPRM_SRC_TGT_ENTITY_MAPPING 
		%if &m_type eq CURRENT %then %do;
			where LATEST_IMPORT_FLG = 'Y' 	 /* i18NOK:LINE */
		%end;
		;
	quit;
	
	proc sql noprint; 
		select count(*) into: m_exprt_rec_cnt /* i18nOK:Line */
			from &export_specs_ds_lib..&export_specs_ds_nm._tmp;
	quit;
	
	%dabt_err_chk(type=SQL);

	%let m_exprt_rec_cnt = &m_exprt_rec_cnt.;

	/* If &export_specs_ds_lib..&export_specs_ds_nm. contains observations only furthur steps will execute.*/

	%if &m_exprt_rec_cnt. gt 0 %then %do;
	
		PROC SQL NOPRINT; 
			INSERT INTO  &export_specs_ds_lib..&export_specs_ds_nm 
			%if &m_type eq ALL %then %do;
				(ENTITY_TYPE_CD,SOURCE_ENTITY_NM,SOURCE_ENTITY_ID,TARGET_ENTITY_ID,LATEST_IMPORT_FLG,CREATED_DTTM,CREATED_BY_USER)
			%end;
			%else %do;
				(ENTITY_TYPE_CD,SOURCE_ENTITY_NM,SOURCE_ENTITY_ID,TARGET_ENTITY_ID,CREATED_DTTM,CREATED_BY_USER)
			%end;
			SELECT 
				ENTITY_TYPE_CD as ENTITY_TYPE_CD,
				SOURCE_ENTITY_NM as SOURCE_ENTITY_NM,
				SOURCE_ENTITY_ID as SOURCE_ENTITY_ID,
				TARGET_ENTITY_ID as TARGET_ENTITY_ID,
			%if &m_type eq ALL %then %do;
				LATEST_IMPORT_FLG as LATEST_IMPORT_FLG,
			%end;
				CREATED_DTTM as CREATED_DTTM,
				CREATED_BY_USER as CREATED_BY_USER
				FROM &export_specs_ds_lib..&export_specs_ds_nm._tmp
			; 
		QUIT; 
		
		proc sort data=&export_specs_ds_lib..&export_specs_ds_nm. out=&export_specs_ds_lib..&export_specs_ds_nm.;
			by entity_type_cd source_entity_nm;
		run;
				
	
	filename exp_file filesrvc folderpath="&export_spec_file_path./" filename= "&export_spec_file_nm..csv" encoding='utf-8' debug=http; /* i18nOK:Line */
		
		/*Data present in cpspcscr.cprm_export_specification table will be exported to csv format*/
		proc export
			data=&export_specs_ds_lib..&export_specs_ds_nm.
			outfile = exp_file  /* i18NOK:LINE */
			DBMS= csv
			replace label;
		run;
		
		/*Export check : If export is not successful. Put message in log and return.*/
		%if &syserr. ne 0 %then %do;
			/* Some issue occured while creation of export mapping csv file */
			%put  %sysfunc(sasmsg(work.DABT_CPRM_MISC, ERR_EXPORT_SRC_TGT_MAP_EXCEL_NOT_CREATED noquote));	
			%let syscc=99;
			%return;
		%end;
	%end;
	%else %do;
		/* Noting found to export to entity mapping CSV file.  */
		%put  %sysfunc(sasmsg(work.DABT_CPRM_MISC, ERR_EXPORT_SRC_TGT_MAP_EMPTY_DS, noquote));	
		%let syscc=99;
		%return;
	%end;
	
%mend cprm_src_tgt_entity_mapping_info;