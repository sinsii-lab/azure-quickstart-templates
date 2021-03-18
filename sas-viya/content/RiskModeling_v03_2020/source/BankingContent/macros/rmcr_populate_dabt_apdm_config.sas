%macro rmcr_populate_dabt_apdm_config;

	options LRECL=32766;
	%let rmcr_script_path = &m_cr_banking_solution_macro_path./;
	
	%rmcr_insert_library_master;
	
	%rmcr_insrt_src_table_master;

	%rmcr_insrt_src_column_master;
	
	
		proc sql noprint;
		update &lib_apdm..source_column_master
		set column_data_type_sk = 4 
		where 	column_data_type_sk = 3 
			and source_column_sk < 1000000;
		quit;

	
	

	%rmcr_insert_script_level_master;

	%rmcr_insert_level_key_column_dtl;

	proc sql;
	insert into &lib_apdm..SOURCE_TABLE_X_USAGE (
	SOURCE_TABLE_SK ,TABLE_USAGE_SK ,CREATED_DTTM ,CREATED_BY_USER
	)
	VALUES(1,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(3,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(6,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(7,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(13,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(13,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(17,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(18,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(18,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(19,2,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(41,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(45,2,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(47,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(64,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(62,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(63,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	;
	quit;
	
	proc sql;
	insert into &lib_apdm..SOURCE_TABLE_X_LEVEL (
	SOURCE_TABLE_SK ,LEVEL_SK ,HAS_RETAINED_KEY_FLG,CREATED_DTTM ,CREATED_BY_USER  
	)
	VALUES(1,1,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(1,2,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(2,1,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(2,2,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(3,1,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(3,2,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(6,2,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(7,1,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(7,2,"N","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(13,2,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(17,1,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(18,1,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(19,1,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(19,2,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(41,1,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(41,2,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(45,2,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(47,1,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(47,2,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(62,1,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(63,2,"Y","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	;
	quit;
	
	proc sql;
	insert into &lib_apdm..SOURCE_TABLE_X_TIME_FREQUENCY (
	SOURCE_TABLE_SK ,TIME_FREQUENCY_SK ,CREATED_DTTM ,CREATED_BY_USER
	)
	VALUES(1,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(3,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(6,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(7,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(41,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(47,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(19,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(45,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	
	VALUES(13,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(17,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(18,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	
	;
	quit;
	
	proc sql;
	insert into &lib_apdm..SOURCE_COLUMN_X_USAGE (
	SOURCE_COLUMN_SK ,COLUMN_USAGE_SK ,CREATED_DTTM ,CREATED_BY_USER
	)
	VALUES(61,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(119,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(170,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(299,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(319,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(833,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(834,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(1140,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(1141,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(1237,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(1238,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(1273,2,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(1958,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2235,2,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2236,2,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2267,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2878,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2879,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2884,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2885,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(3048,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(3049,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2969,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2970,4,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	;
	quit;
	
	%rmcr_insrt_src_dim_attrib;
	
	%rmcr_insrt_sbst_from_path_master;

	proc sql;
	insert into &lib_apdm..SUBSET_FROM_PATH_X_LEVEL (
	SUBSET_FROM_PATH_SK ,LEVEL_SK ,SELECT_SOURCE_COLUMN_SK ,CORRSP_LEVEL_KEY_COLUMN_DTL_SK , CREATED_DTTM ,CREATED_BY_USER
	)
	VALUES(1,1,1160,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(3,1,2978,1,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(2,2,820,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	VALUES(3,2,2888,3,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	;
	quit;
	
	proc sql;
	insert into &lib_apdm..SUBSET_TABLE_JOIN_CONDITION (
	SUBSET_TABLE_JOIN_CONDITION_SK ,SUBSET_FROM_PATH_SK ,JOIN_CONDITION_SEQUENCE_NUMBER ,JOIN_TYPE ,LEFT_TABLE_SK ,LEFT_COLUMN_SK ,RIGHT_TABLE_SK ,RIGHT_COLUMN_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 
	)
	VALUES(1,1,1,'NONE',18,.,.,.,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(2,2,1,'NONE',13,.,.,.,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	VALUES(3,3,1,'NONE',64,.,.,.,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid") /* i18NOK:LINE */
	;
	quit;
	
	proc sql;
	insert into &lib_apdm..LEVEL_ASSOC (
	LEVEL_SK ,ASSOC_LEVEL_SK ,MAPPING_SOURCE_TABLE_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 
	)
	VALUES(1,2,62,"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	;
	quit;
	
	proc sql;
	insert into &lib_apdm..TABLE_X_LEVEL_KEY_COLUMN (
	SOURCE_TABLE_SK ,LEVEL_KEY_COLUMN_DTL_SK ,CORRSP_SOURCE_COLUMN_SK
	)
	VALUES(62,3,2881)
	;
	quit;


	

%mend rmcr_populate_dabt_apdm_config;
