/**************************************************************
	Module		:  rmcr_config_cprm

   	Function	:  This macro will be used to create APDM table named CPRM_SRC_TGT_ENTITY_MAPPING, if exists already.
				   CPRM_SRC_TGT_ENTITY_MAPPING table used to track the CPRM activities of entity_type_cd project, model
	
	Called-by	:  Risk modeling Content Post Installation Wrapper macro
	Calls		:  None
	Author:   CSB Team

***************************************************************/
%macro rmcr_config_cprm;

	/**************************************************************************************************/
	/* Create a table APDM.CPRM_SRC_TGT_ENTITY_MAPPING for source target entity mapping of cprm		  */
	/**************************************************************************************************/
	/* I18NOK:BEGIN*/
	%if %sysfunc(exist(&lib_apdm..CPRM_SRC_TGT_ENTITY_MAPPING)) = 0 %then %do;
		proc sql noprint;
			create table &lib_apdm..CPRM_SRC_TGT_ENTITY_MAPPING (
				ENTITY_TYPE_CD CHAR(10)  FORMAT =$10. INFORMAT=$10. LABEL="ENTITY TYPE CODE", 
				SOURCE_ENTITY_NM CHAR(360)  FORMAT =$360. INFORMAT=$360. LABEL="SOURCE ENTITY NAME", 
				SOURCE_ENTITY_ID NUMERIC(10)  FORMAT =12. INFORMAT=12. LABEL="SOURCE ENTITY ID", 
				TARGET_ENTITY_ID NUMERIC(10)  FORMAT =12. INFORMAT=12. LABEL="TARGET ENTITY ID",
				LATEST_IMPORT_FLG CHAR(1)  FORMAT =$1. INFORMAT=$1. LABEL="LATEST IMPORT FLAG",
				CREATED_DTTM DATE  FORMAT =DATETIME25.6 INFORMAT=DATETIME25.6 LABEL="CREATED DATETIME",
				CREATED_BY_USER CHAR(360)  FORMAT =$360. INFORMAT=$360. LABEL="CREATED BY USER"
	   		);
		quit; 
	%end;
	/* I18NOK:END*/
	
	proc sql noprint;
	select count(*) into :cnt_scr_grp from &lib_apdm..MT_TABLE_RELATIONSHIP	/* I18NOK:LINE*/
	where parent_table_nm='VARIABLE_MASTER' and child_table_nm='SCORECARD_BIN_GROUP';/* I18NOK:LINE*/
	quit;

	%if &cnt_scr_grp eq 0 %then %do;

		proc sql noprint;
		insert into &lib_apdm..MT_TABLE_RELATIONSHIP(parent_table_nm,parent_column_nm,
		child_table_nm,child_column_nm,restrict_parent_delete_flg)
		values('VARIABLE_MASTER','VARIABLE_SK','SCORECARD_BIN_GROUP','SCRCRD_BIN_GRP_VARIABLE_SK','Y');/* I18NOK:LINE*/
		quit;

	%end;


%mend rmcr_config_cprm;
