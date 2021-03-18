/************************************************************************

  Copyright (c) 2010 by SAS Institute Inc., Cary, NC, USA.             

                                                                       

  NAME:          rmcr_bank_insert_script_apdm_config                                  

                                                                       

  PURPOSE:       This script does the following :-

					-> Creates the ADM extensions in DABT.  

				  	-> Inserts CSB specific ADM configuration data in ADM.	

					-> Inserts data in Parameter_List_Input_Mart table for Input Mart extensions for CSB

                                                                       

  USAGE:         %rmcr_apply_banking_solution				                    

                                                                       

  PARAMETERS:    None												    

                                                                       

  HISTORY :                       										

  DATE             BY                    DETAILS                       

  12-Aug-2010      sindpc                Initial Version   

  12-Nov-2010	   sindpc				 Modifications as per deployment plan

  07-May-2012      sinmkr                Modifications as per new DABT structure

 ***********************************************************************/

/* DABT common tables insertion */



%Macro rmcr_bank_insert_apdm_config;

	%let rmcr_script_path = &m_cr_banking_solution_macro_path./;

	%rmcr_csb_insert_library_master;

	%rmcr_csb_source_table_master;

	%rmcr_csb_source_column_master;


	proc sql noprint;
	update &lib_apdm..source_column_master
	set column_data_type_sk = 4 
	where 	column_data_type_sk = 3 
		and source_column_sk < 1000000;
	quit;




	%rmcr_csb_insert_level_master;

	%rmcr_csb_level_key_column_dtl;



	%rmcr_csb_purpose_master;



	proc sql;

	insert into &lib_apdm..PURPOSE_X_LEVEL (

	LEVEL_SK ,PURPOSE_SK ,CREATED_DTTM ,CREATED_BY_USER

	)

	VALUES(1,101,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1,102,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1,103,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2,101,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3,101,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(4,102,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(5,101,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,101,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,102,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,103,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,101,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;


	proc sql;

	insert into &lib_apdm..SOURCE_TABLE_X_USAGE (

	SOURCE_TABLE_SK ,TABLE_USAGE_SK ,CREATED_DTTM ,CREATED_BY_USER

	)

	VALUES(4,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(9,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(11,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(11,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(12,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(14,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(15,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(16,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(20,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(20,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(21,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(23,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(23,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(26,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(27,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(28,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(28,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(31,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(31,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(32,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(32,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(33,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(33,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(34,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(35,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(36,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(37,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(37,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(38,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(39,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(40,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(43,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(44,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(46,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(48,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(49,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(50,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(51,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(52,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(53,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(54,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(55,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(56,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(57,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(58,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(65,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(66,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;

	proc sql;

	insert into &lib_apdm..SOURCE_TABLE_X_LEVEL (

	SOURCE_TABLE_SK ,LEVEL_SK ,HAS_RETAINED_KEY_FLG,CREATED_DTTM ,CREATED_BY_USER

	)
	 /* I18NOK:BEGIN */
	 
	VALUES(1,3,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1,4,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1,5,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1,7,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1,8,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(4,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(4,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,1,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,2,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,4,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,5,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,7,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(9,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(11,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(12,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(14,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(15,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(16,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(20,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(23,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(26,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(26,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(27,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(28,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(28,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(31,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(32,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(33,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(34,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(34,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(35,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(35,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(36,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(37,2,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(37,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(38,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(39,2,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(39,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(40,2,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(46,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(48,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(49,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(50,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(51,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(47,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(47,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(47,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(52,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(53,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(54,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(55,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(56,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(57,2,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(58,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(43,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(44,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(37,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(37,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2,5,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2,7,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(19,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(19,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(65,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(66,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(67,3,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(67,8,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(63,5,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(62,7,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2,4,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3,4,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3,5,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3,7,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(6,5,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,4,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,5,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,7,"N","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	 /* I18NOK:END */
	 
	quit;



	proc sql;

	insert into &lib_apdm..SOURCE_TABLE_X_TIME_FREQUENCY (

	SOURCE_TABLE_SK ,TIME_FREQUENCY_SK ,CREATED_DTTM ,CREATED_BY_USER

	)

	VALUES(4,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(9,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(37,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(40,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(52,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(53,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(54,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(55,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(56,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(57,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(58,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(65,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(66,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(11,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(12,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(14,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(15,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(16,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(20,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(21,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(23,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(26,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(27,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(28,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(31,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(32,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(33,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(34,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(35,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(36,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(38,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(39,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(43,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(44,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(46,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(48,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(49,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(50,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(51,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(67,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;





	proc sql;

	insert into &lib_apdm..SOURCE_COLUMN_X_USAGE (

	SOURCE_COLUMN_SK ,COLUMN_USAGE_SK ,CREATED_DTTM ,CREATED_BY_USER

	)

	VALUES(2083,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(369,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(626,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(627,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(729,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(730,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(936,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(937,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1035,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1036,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1134,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1135,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1282,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1283,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1330,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1331,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1426,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1427,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1554,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1555,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1559,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1560,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1565,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1566,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1587,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1588,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1984,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1985,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1981,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1975,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1970,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1969,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1944,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1908,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1710,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1711,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1676,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(1677,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2246,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2247,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2214,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2351,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2352,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2459,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2460,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2567,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2568,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2675,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2676,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2707,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2763,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2722,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2776,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2736,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2789,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2750,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2806,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2100,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2101,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2152,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2153,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3132,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3133,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3212,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3213,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3318,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3319,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3285,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3286,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3313,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3314,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")


	VALUES(3360,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3361,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3362,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3363,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;



	%rmcr_csb_source_dim_attrib;





	%rmcr_csb_sbst_from_path_master;





	proc sql;

	insert into &lib_apdm..SUBSET_FROM_PATH_X_LEVEL (

	SUBSET_FROM_PATH_SK ,LEVEL_SK ,SELECT_SOURCE_COLUMN_SK ,CORRSP_LEVEL_KEY_COLUMN_DTL_SK, CREATED_DTTM ,CREATED_BY_USER

	)

	VALUES(4,3,1281,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(5,4,1399,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(6,5,528,9,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,5,3094,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,5,3247,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,7,3136,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,4,3289,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(9,1,1561,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(10,4,1583,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(11,7,1596,11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(12,8,1709,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(9,7,1561,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;



	proc sql;

	insert into &lib_apdm..SUBSET_TABLE_JOIN_CONDITION (

	SUBSET_TABLE_JOIN_CONDITION_SK ,SUBSET_FROM_PATH_SK ,JOIN_CONDITION_SEQUENCE_NUMBER ,JOIN_TYPE ,LEFT_TABLE_SK ,LEFT_COLUMN_SK ,RIGHT_TABLE_SK ,RIGHT_COLUMN_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

	)

	VALUES(4,4,1,'NONE',20,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(5,5,1,'NONE',23,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(6,6,1,'NONE',11,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(7,7,1,'NONE',65,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(8,8,1,'NONE',66,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(9,9,1,'NONE',28,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(10,10,1,'NONE',31,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(11,11,1,'NONE',32,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	VALUES(12,12,1,'NONE',33,.,.,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")  /* I18NOK:LINE */

	;

	quit;





	%rmcr_csb_insert_event_master;



	proc sql;

	insert into &lib_apdm..EVENT_MAPPING_DTL (

	EVENT_MAPPING_DTL_SK, LEVEL_SK ,PURPOSE_SK ,EVENT_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

	)

	VALUES(1, 3,101,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(2, 1,102,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3, 4,102,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(4, 1,103,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(5, 8,101,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(6, 7,102,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7, 7,103,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;

	proc sql;

	insert into &lib_apdm..PURPOSE_TIME_GRAIN_CONFIG_DTL (

	PURPOSE_SK ,SOURCE_DATA_TIME_GRAIN_SK ,OUTCOME_PERIOD_DEFAULT_VALUE ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

	)

	VALUES(101,1,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,2,.,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,2,720,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,1,0,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,2,0,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;

	proc sql;

	insert into &lib_apdm..PURPOSE_LEVEL_CONFIG_DTL (
	PURPOSE_SK ,LEVEL_SK ,MODEL_CAPTURE_FORM_CD ,
	CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER
	)
	 /* I18NOK:BEGIN */
	 
	VALUES(101,1,'PD_A',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,2,'PD_I',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,3,'PD_P',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,5,'PD_I',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,7,'PD_A',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,8,'PD_P',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,'LGD_A',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,'LGD_F',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,'LGD_A',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,1,'CCF_A',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,7,'CCF_A',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	 /* I18NOK:END */
	 
	quit;

	proc sql;

	insert into &lib_apdm..LEVEL_ASSOC (

	LEVEL_SK ,ASSOC_LEVEL_SK ,MAPPING_SOURCE_TABLE_SK ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

	)

	VALUES(7,5,62,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(7,4,62,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(3,2,67,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(8,5,67,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(4,5,23,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;

	quit;

	%macro rmcr_ext_config_insert();
		%let external_code_path = %nrstr(/&m_file_srvr_root_folder_nm/Risk Modeling Content/v03.2020/Banking Solution/macros);

			proc sql;
			insert into &lib_apdm..EXTERNAL_CODE_MASTER(
			external_code_sk ,external_code_id,
			external_code_short_nm ,
			external_code_desc ,
			level_sk ,external_code_file_loc ,external_code_file_nm ,simultns_mltpl_var_calc_flg ,parallel_exec_flg ,
			created_dttm ,created_by_user ,last_processed_dttm ,last_processed_by_user 
			)

			VALUES(1,'1',    /* I18NOK:LINE */
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_SM1.1, noquote))",
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_DS1.1, noquote))",
			1,"&external_code_path.","csbmva_ext_calc_lgd_var.sas","Y","N", /* I18NOK:LINE */
			"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

			VALUES(2,'2',    /* I18NOK:LINE */
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_SM2.1, noquote))",
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_DS2.1, noquote))",
			7,"&external_code_path.","csbmva_ext_calc_lgd_var.sas","Y","N", /* I18NOK:LINE */
			"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
 
			VALUES(3,'3',   /* I18NOK:LINE */
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_SM3.1, noquote))",
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_DS3.1, noquote))",
			4,"&external_code_path.","csbmva_ext_calc_lgd_var.sas","Y","N", /* I18NOK:LINE */
			"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
			
			VALUES(4,'4',   /* I18NOK:LINE */
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_SM4.1, noquote))",
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_DS4.1, noquote))",
			3,"&external_code_path.","csbmva_get_subset_appl_list.sas","Y","N", /* I18NOK:LINE */
			"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
			
			
			VALUES(5,'5',  /* I18NOK:LINE */
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_SM5.1, noquote))",
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_DS5.1, noquote))",
			8,"&external_code_path.","csbmva_get_subset_appl_list.sas","Y","N", /* I18NOK:LINE */
			"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
			
			VALUES(6,'6',   /* I18NOK:LINE */
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_SM6.1, noquote))",
			"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,EXTERNAL_CODE_MASTER.EXTERNAL_CODE_DS6.1, noquote))",
			8,"&external_code_path.","bankfdn_create_event_data.sas","Y","N", /* I18NOK:LINE */
			"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
			;
			
			quit;
			

	%mend rmcr_ext_config_insert;
	%rmcr_ext_config_insert();

	proc sql;
	insert into &lib_apdm..EXTERNAL_VARIABLE_MASTER(
	external_variable_sk ,external_code_sk ,
	external_variable_desc ,external_variable_short_nm ,
	external_variable_column_nm, column_data_type_sk ,
	created_dttm ,created_by_user ,last_processed_dttm ,last_processed_by_user , 
	OUTCOME_VARIABLE_FLG
	)

	VALUES(1,1,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS135.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM135.1, noquote))",
	"EAD",1, /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(2,1,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS92.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM92.1, noquote))",
	"DEFAULT_DT",3,  /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(3,1,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS141.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM141.1, noquote))",
	"VALUE_AT_RECOVERY",1,  /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(4,1,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS138.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM138.1, noquote))",
	"RECOVERY_COST",1,  /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(5,1,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS102.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM102.1, noquote))",
	"LGD",1,  /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)

	VALUES(6,2,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS135.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM135.1, noquote))",
	"EAD", /* I18NOK:LINE */
	1,
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(7,2,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS92.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM92.1, noquote))",
	"DEFAULT_DT", /* I18NOK:LINE */
	3,
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(8,2,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS141.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM141.1, noquote))",
	"VALUE_AT_RECOVERY",1, /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(9,2,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS138.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM138.1, noquote))",
	"RECOVERY_COST",1, /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(10,2,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS103.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM103.1, noquote))",
	"LGD",1, /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)

	VALUES(11,3,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS135.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM135.1, noquote))",
	"EAD", /* I18NOK:LINE */
	1,
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(12,3,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS92.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM92.1, noquote))",
	"DEFAULT_DT", /* I18NOK:LINE */
	3,
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(13,3,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS141.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM141.1, noquote))",
	"VALUE_AT_RECOVERY",1, /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(14,3,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS138.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM138.1, noquote))",
	"RECOVERY_COST",1, /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	VALUES(15,3,
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_DS101.1, noquote))",
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,MODELING_ABT_X_VARIABLE.VARIABLE_SM101.1, noquote))",
	"LGD",1, /* I18NOK:LINE */
	"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","Y"/* I18NOK:LINE */
	)
	;
	quit;


	proc sql noprint;
	update &lib_apdm..External_variable_master
	set column_data_type_sk = 4 
	where 	column_data_type_sk = 3 
		and external_variable_sk < 1000;
	quit;


	%rmcr_csb_modeling_abt_master;


	proc sql;

	insert into &lib_apdm..VARIABLE_MASTER (

	VARIABLE_SK ,VARIABLE_TYPE_SK ,COLUMN_DATA_TYPE_SK ,VARIABLE_COLUMN_NM ,SOURCE_TABLE_SK ,VARIABLE_DEFINITION_STRING ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LEVEL_SK 

	)
	 /* I18NOK:BEGIN */
	 
	VALUES(1,3,2,'OUTCOME_CD',20,'spm_3_20_1317_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(12,3,2,'OUTCOME_CD',20,'spm_3_20_1317_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(13,1,1,'P_MAX_SNP_DAY_PAYP_1_12M',1,'beh_3_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(20,1,1,'P_MAX_SNP_DAY_PAYP_L1M',1,'beh_3_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(22,4,1,'PDA_OUT_VAR_EVER_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(23,4,1,'PDA_OUT_VAR_EVER_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(26,4,1,'PDA_OUT_VAR_EVER_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(27,4,1,'PDA_OUT_VAR_EVER_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(28,4,1,'PDA_OUT_VAR_EVER_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(29,4,1,'PDA_OUT_VAR_CURR_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(30,4,1,'PDA_OUT_VAR_CURR_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(31,4,1,'PDA_OUT_VAR_CURR_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(32,4,1,'PDA_OUT_VAR_CURR_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(33,4,1,'PDA_OUT_VAR_CURR_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(34,1,1,'P_MAX_SNP_DEF_CNT_1_12M',1,'beh_3_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(35,1,1,'P_MAX_SNP_DEF_CNT_L1M',1,'beh_3_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(36,4,1,'PDA_OUT_EVER_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(37,4,1,'PDA_OUT_CURR_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(38,1,1,'P_SUM_SNP_DPD30_1_12M',1,'beh_3_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(39,1,1,'P_SUM_SNP_DPD60_1_12M',1,'beh_3_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(40,1,1,'P_SUM_SNP_DPD90_1_12M',1,'beh_3_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(41,1,1,'P_SUM_SNP_DPD120_1_12M',1,'beh_3_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(42,1,1,'P_SUM_SNP_DPD180_1_12M',1,'beh_3_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(43,1,1,'P_SUM_SNP_CHRG_OFC_1_12M',1,'beh_3_1_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",3)

	VALUES(44,4,1,'PDA_OUT_VAR_COMPLEX_BAD_1',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(45,4,1,'PDA_OUT_VAR_COMPLEX_BAD_2',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(46,3,2,'OUTCOME_CD',33,'spm_8_33_1745_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(47,1,1,'A_MAX_SNP_DAY_PAYP_1_12M',1,'beh_1_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(48,4,1,'PDO_I_A_OUT_EVER_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(49,4,1,'PDO_I_A_OUT_EVER_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(50,4,1,'PDO_I_A_OUT_EVER_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(51,4,1,'PDO_I_A_OUT_EVER_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(52,4,1,'PDO_I_A_OUT_EVER_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(53,1,1,'A_MAX_SNP_DAY_PAYP_L1M',1,'beh_1_3_1_20_1',1610970315.3,"&sysuserid",1610970315.3,"&sysuserid",1)

	VALUES(54,4,1,'PDO_I_A_OUT_CURR_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(55,4,1,'PDO_I_A_OUT_CURR_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(56,4,1,'PDO_I_A_OUT_CURR_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(57,4,1,'PDO_I_A_OUT_CURR_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(58,4,1,'PDO_I_A_OUT_CURR_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(59,1,1,'A_MAX_SNP_DEF_CNT_1_12M',1,'beh_1_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(60,1,1,'A_MAX_SNP_DEF_CNT_L1M',1,'beh_1_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(61,4,1,'PDO_I_A_EVER_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(63,4,1,'PDO_I_A_CURR_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(64,1,1,'A_MAX_SNP_CHRG_OFC_1_12M',1,'beh_1_3_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(65,4,1,'PDO_I_A_EVER_CHARGED_OFF',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(66,1,1,'A_SUM_SNP_DPD30_1_12M',1,'beh_1_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(67,1,1,'A_SUM_SNP_DPD60_1_12M',1,'beh_1_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(68,1,1,'A_SUM_SNP_DPD90_1_12M',1,'beh_1_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(69,1,1,'A_SUM_SNP_DPD120_1_12M',1,'beh_1_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	;

	 /* I18NOK:END */
	 
	quit;

	proc sql;

	insert into &lib_apdm..VARIABLE_MASTER (

	VARIABLE_SK ,VARIABLE_TYPE_SK ,COLUMN_DATA_TYPE_SK ,VARIABLE_COLUMN_NM ,SOURCE_TABLE_SK ,VARIABLE_DEFINITION_STRING ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LEVEL_SK 

	)

	 /* I18NOK:BEGIN */
	 
	VALUES(70,1,1,'A_SUM_SNP_DPD180_1_12M',1,'beh_1_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(71,1,1,'A_SUM_SNP_CHRG_OFC_1_12M',1,'beh_1_1_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(72,4,1,'PDO_I_A_COMPLEX_BAD_1',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(73,4,1,'PDO_I_A_COMPLEX_BAD_2',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(74,1,1,'I_MAX_SNP_DAY_PAYP_1_12M',1,'beh_2_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(75,1,1,'I_MAX_SNP_DAY_PAYP_L1M',1,'beh_2_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(76,4,1,'PDO_I_CUS_EVER_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(77,4,1,'PDO_I_CUS_EVER_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(78,4,1,'PDO_I_CUS_EVER_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(79,4,1,'PDO_I_CUS_EVER_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(80,4,1,'PDO_I_CUS_EVER_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(81,4,1,'PDO_I_CUS_CURRENT_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(82,4,1,'PDO_I_CUS_CURRENT_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(83,4,1,'PDO_I_CUS_CURRENT_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(84,4,1,'PDO_I_CUS_CURRENT_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(85,4,1,'PDO_I_CUS_CURRENT_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(86,1,1,'I_MAX_SNP_DEF_CNT_1_12M',1,'beh_2_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(87,1,1,'I_MAX_SNP_DEF_CNT_L1M',1,'beh_2_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(88,1,1,'I_MAX_SNP_CHRG_OFC_1_12M',1,'beh_2_3_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(89,4,1,'PDO_I_CUS_EVER_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(91,4,1,'PDO_I_CUS_EVER_CHARGED_OFF',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(92,1,1,'I_SUM_SNP_DPD30_1_12M',1,'beh_2_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(93,1,1,'I_SUM_SNP_DPD60_1_12M',1,'beh_2_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(94,1,1,'I_SUM_SNP_DPD90_1_12M',1,'beh_2_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(95,1,1,'I_SUM_SNP_DPD180_1_12M',1,'beh_2_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(96,1,1,'I_SUM_SNP_DPD120_1_12M',1,'beh_2_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(97,4,1,'PDO_CUS_CURRENT_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(98,4,1,'PDO_I_CUS_COMPLEX_BAD_1',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(99,4,1,'PDO_I_CUS_COMPLEX_BAD_2',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(100,1,1,'C_MAX_SNP_DAY_PAYP_1_12M',1,'beh_5_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(101,1,1,'C_MAX_SNP_DAY_PAYP_L1M',1,'beh_5_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(102,1,1,'C_MAX_SNP_DEF_CNT_1_12M',1,'beh_5_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(103,1,1,'C_MAX_SNP_DEF_CNT_L1M',1,'beh_5_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(104,1,1,'C_MAX_SNP_CHRG_OFC_1_12M',1,'beh_5_3_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(105,1,1,'C_SUM_SNP_DPD30_1_12M',1,'beh_5_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(106,1,1,'C_SUM_SNP_DPD60_1_12M',1,'beh_5_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(107,1,1,'C_SUM_SNP_DPD90_1_12M',1,'beh_5_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(108,1,1,'C_SUM_SNP_DPD120_1_12M',1,'beh_5_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(109,1,1,'C_SUM_SNP_DPD180_1_12M',1,'beh_5_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(110,4,1,'PDO_C_CUS_EVER_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(111,4,1,'PDO_C_CUS_EVER_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(112,4,1,'PDO_C_CUS_EVER_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(113,4,1,'PDO_C_CUS_EVER_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(114,4,1,'PDO_C_CUS_EVER_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(115,4,1,'PDO_C_CUS_CURRENT_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(116,4,1,'PDO_C_CUS_CURRENT_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(117,4,1,'PDO_C_CUS_CURRENT_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(118,4,1,'PDO_C_CUS_CURRENT_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(119,4,1,'PDO_C_CUS_CURRENT_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(120,4,1,'PDO_CRP_CS_EVER_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	;

	quit;

	proc sql;

	insert into &lib_apdm..VARIABLE_MASTER (

	VARIABLE_SK ,VARIABLE_TYPE_SK ,COLUMN_DATA_TYPE_SK ,VARIABLE_COLUMN_NM ,SOURCE_TABLE_SK ,VARIABLE_DEFINITION_STRING ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LEVEL_SK 

	)

	VALUES(121,4,1,'PDO_CRP_CUS_CUR_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(122,4,1,'PDO_CRP_EVER_CHARGED_OFF',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(123,4,1,'PDO_CRP_CUS_COMPLEX_BAD_1',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(124,4,1,'PDO_CRP_CUS_COMPLEX_BAD_2',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(140,1,1,'SUM_TOT_BAL_L1M',1,'beh_1_1_1_80_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(141,1,1,'SUM_CUR_LIMIT_AMT_L1M',1,'beh_1_1_1_77_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(142,1,1,'SUM_CUR_LIMIT_AMT_12MB',1,'beh_1_1_1_77_13',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(143,1,1,'AVG_TOT_BAL_13_24M',1,'beh_1_2_1_80_3',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",1)

	VALUES(144,4,1,'CCF',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(145,1,1,'SUM_TOT_BAL_L1M',1,'beh_7_1_1_80_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(146,1,1,'SUM_CUR_LIMIT_AMT_L1M',1,'beh_7_1_1_77_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(147,1,1,'AVG_TOT_BAL_13_24M',1,'beh_7_2_1_80_3',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(148,1,1,'SUM_CUR_LIMIT_AMT_12MB',1,'beh_7_1_1_77_13',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(149,4,1,'CCF',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(150,3,2,'CUSTOMER_ID',13,'spm_2_13_736_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(151,3,2,'CUSTOMER_ID',11,'spm_5_11_529_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(152,3,2,'CUSTOMER_ID',13,'spm_2_13_736_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(153,3,2,'CUSTOMER_ID',11,'spm_5_11_529_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(154,3,2,'CREDIT_FACILITY_ID',23,'spm_4_23_1400_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",4)

	VALUES(158,3,2,'CUSTOMER_ID',13,'spm_2_13_736_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",2)

	VALUES(159,3,2,'CUSTOMER_ID',11,'spm_5_11_529_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",5)

	VALUES(160,3,2,'CREDIT_FACILITY_ID',23,'spm_4_23_1400_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",4)

	VALUES(164,3,2,'OUTCOME_CD',33,'spm_8_33_1745_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(165,1,1,'Q_MAX_SNP_DAY_PAYP_1_12M',1,'beh_8_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(166,1,1,'Q_MAX_SNP_DAY_PAYP_L1M',1,'beh_8_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(167,1,1,'Q_MAX_SNP_DEF_CNT_1_12M',1,'beh_8_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(168,1,1,'Q_MAX_SNP_DEF_CNT_L1M',1,'beh_8_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(169,1,1,'Q_SUM_SNP_DPD30_1_12M',1,'beh_8_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(170,1,1,'Q_SUM_SNP_DPD180_1_12M',1,'beh_8_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(171,1,1,'Q_SUM_SNP_DPD120_1_12M',1,'beh_8_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(172,1,1,'Q_SUM_SNP_DPD60_1_12M',1,'beh_8_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(173,1,1,'Q_SUM_SNP_DPD90_1_12M',1,'beh_8_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	;

	quit;

	proc sql;

	insert into &lib_apdm..VARIABLE_MASTER (

	VARIABLE_SK ,VARIABLE_TYPE_SK ,COLUMN_DATA_TYPE_SK ,VARIABLE_COLUMN_NM ,SOURCE_TABLE_SK ,VARIABLE_DEFINITION_STRING ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LEVEL_SK 

	)

	VALUES(174,1,1,'Q_SUM_SNP_CHRG_OFC_1_12M',1,'beh_8_1_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",8)

	VALUES(175,4,1,'PDA_OUT_VAR_EVER_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(176,4,1,'PDA_OUT_VAR_EVER_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(177,4,1,'PDA_OUT_VAR_EVER_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(178,4,1,'PDA_OUT_VAR_EVER_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(179,4,1,'PDA_OUT_VAR_EVER_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(180,4,1,'PDA_OUT_VAR_CURR_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(181,4,1,'PDA_OUT_VAR_CURR_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(182,4,1,'PDA_OUT_VAR_CURR_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(183,4,1,'PDA_OUT_VAR_CURR_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(184,4,1,'PDA_OUT_VAR_CURR_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(185,4,1,'PDA_OUT_EVER_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(186,4,1,'PDA_OUT_CURR_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(187,4,1,'PDA_OUT_VAR_COMPLEX_BAD_1',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(188,4,1,'PDA_OUT_VAR_COMPLEX_BAD_2',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(194,1,1,'B_MAX_SNP_DAY_PAYP_L1M',1,'beh_7_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(195,1,1,'B_MAX_SNP_DAY_PAYP_1_12M',1,'beh_7_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(196,1,1,'B_MAX_SNP_DEF_CNT_L1M',1,'beh_7_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(197,1,1,'B_MAX_SNP_DEF_CNT_1_12M',1,'beh_7_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(198,1,1,'B_MAX_SNP_CHRG_OFC_1_12M',1,'beh_7_3_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(199,1,1,'B_SUM_SNP_DPD120_1_12M',1,'beh_7_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(200,1,1,'B_SUM_SNP_DPD180_1_12M',1,'beh_7_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(201,1,1,'B_SUM_SNP_DPD30_1_12M',1,'beh_7_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(202,1,1,'B_SUM_SNP_DPD60_1_12M',1,'beh_7_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(203,1,1,'B_SUM_SNP_DPD90_1_12M',1,'beh_7_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(204,1,1,'B_SUM_SNP_CHRG_OFC_1_12M',1,'beh_7_1_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(205,1,1,'B_MAX_SNP_DAY_PAYP_L1M',1,'beh_7_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(206,1,1,'B_MAX_SNP_DAY_PAYP_1_12M',1,'beh_7_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(207,1,1,'B_MAX_SNP_DEF_CNT_L1M',1,'beh_7_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(208,1,1,'B_MAX_SNP_DEF_CNT_1_12M',1,'beh_7_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(209,1,1,'B_MAX_SNP_CHRG_OFC_1_12M',1,'beh_7_3_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(210,1,1,'B_SUM_SNP_DPD120_1_12M',1,'beh_7_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(211,1,1,'B_SUM_SNP_DPD180_1_12M',1,'beh_7_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(212,1,1,'B_SUM_SNP_DPD30_1_12M',1,'beh_7_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(213,1,1,'B_SUM_SNP_DPD60_1_12M',1,'beh_7_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(214,1,1,'B_SUM_SNP_DPD90_1_12M',1,'beh_7_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(215,1,1,'B_SUM_SNP_CHRG_OFC_1_12M',1,'beh_7_1_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(216,1,1,'B_MAX_SNP_DAY_PAYP_1_12M',1,'beh_7_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(217,1,1,'B_MAX_SNP_DAY_PAYP_L1M',1,'beh_7_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(218,1,1,'B_MAX_SNP_DEF_CNT_1_12M',1,'beh_7_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(219,1,1,'B_MAX_SNP_DEF_CNT_L1M',1,'beh_7_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(220,1,1,'B_MAX_SNP_DAY_PAYP_1_12M',1,'beh_7_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(221,1,1,'B_MAX_SNP_DAY_PAYP_L1M',1,'beh_7_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(222,1,1,'B_MAX_SNP_DEF_CNT_1_12M',1,'beh_7_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(223,1,1,'B_MAX_SNP_DEF_CNT_L1M',1,'beh_7_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	;

	quit;

	proc sql;

	insert into &lib_apdm..VARIABLE_MASTER (

	VARIABLE_SK ,VARIABLE_TYPE_SK ,COLUMN_DATA_TYPE_SK ,VARIABLE_COLUMN_NM ,SOURCE_TABLE_SK ,VARIABLE_DEFINITION_STRING ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER ,LEVEL_SK 

	)

	VALUES(224,1,1,'B_MAX_SNP_DAY_PAYP_L1M',1,'beh_7_3_1_20_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(225,1,1,'B_MAX_SNP_DAY_PAYP_1_12M',1,'beh_7_3_1_20_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(226,1,1,'B_MAX_SNP_DEF_CNT_L1M',1,'beh_7_3_1_27_1',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(227,1,1,'B_MAX_SNP_DEF_CNT_1_12M',1,'beh_7_3_1_27_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(228,1,1,'B_MAX_SNP_CHRG_OFC_1_12M',1,'beh_7_3_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(229,1,1,'B_SUM_SNP_CHRG_OFC_1_12M',1,'beh_7_1_1_1434_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(230,1,1,'B_SUM_SNP_DPD120_1_12M',1,'beh_7_1_1_1593_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(231,1,1,'B_SUM_SNP_DPD180_1_12M',1,'beh_7_1_1_2220_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(232,1,1,'B_SUM_SNP_DPD30_1_12M',1,'beh_7_1_1_1431_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(233,1,1,'B_SUM_SNP_DPD90_1_12M',1,'beh_7_1_1_1433_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(234,1,1,'B_SUM_SNP_DPD60_1_12M',1,'beh_7_1_1_1432_6',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",7)

	VALUES(235,4,1,'PDO_CRP_ACC_EVER_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(236,4,1,'PDO_CRP_ACC_EVER_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(237,4,1,'PDO_CRP_ACC_EVER_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(238,4,1,'PDO_CRP_ACC_EVER_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(239,4,1,'PDO_CRP_ACC_EVER_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(240,4,1,'PDO_CRP_ACC_CURR_BAD_30',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(241,4,1,'PDO_CRP_ACC_CURR_BAD_60',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(242,4,1,'PDO_CRP_ACC_CURR_BAD_90',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(243,4,1,'PDO_CRP_ACC_CURR_BAD_120',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(244,4,1,'PDO_CRP_ACC_CURR_BAD_180',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(245,4,1,'PDO_CRP_AC_EVER_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(246,4,1,'PDO_CRP_AC_CURR_CUSTOM_BAD',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(247,4,1,'PDO_CRP_AC_EVER_CHARGED_OFF',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(248,4,1,'PDO_CRP_ACC_COMPLEX_BAD_1',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	VALUES(249,4,1,'PDO_CRP_ACC_COMPLEX_BAD_2',.,'',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid",.)

	;

	quit;

	 /* I18NOK:END */
	 
	 /* I18NOK:BEGIN */
	 proc sql;
	 insert into &lib_apdm..VARIABLE_MASTER(
	 variable_sk ,variable_type_sk ,column_data_type_sk ,variable_column_nm ,source_table_sk ,variable_definition_string ,level_sk ,
	 created_dttm ,created_by_user ,last_processed_dttm ,last_processed_by_user 
	 )
	 
	 VALUES(322,5,1,"RECOVERY_COST",.,"ext_1_1_4",1,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(323,5,1,"LGD",.,"ext_1_1_5",1,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(324,5,3,"DEFAULT_DT",.,"ext_1_1_2",1,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(325,5,1,"VALUE_AT_RECOVERY",.,"ext_1_1_3",1,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(326,5,1,"EAD",.,"ext_1_1_1",1,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(329,5,1,"EAD",.,"ext_7_2_6",7,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(330,5,3,"DEFAULT_DT",.,"ext_7_2_7",7,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(331,5,1,"VALUE_AT_RECOVERY",.,"ext_7_2_8",7,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(332,5,1,"RECOVERY_COST",.,"ext_7_2_9",7,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(333,5,1,"LGD",.,"ext_7_2_10",7,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(346,5,3,"DEFAULT_DT",.,"ext_4_3_12",4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(347,5,1,"VALUE_AT_RECOVERY",.,"ext_4_3_13",4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(348,5,1,"EAD",.,"ext_4_3_11",4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(349,5,1,"LGD",.,"ext_4_3_15",4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(350,5,1,"RECOVERY_COST",.,"ext_4_3_14",4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 /* variable_sk ,variable_type_sk ,column_data_type_sk ,variable_column_nm ,source_table_sk ,variable_definition_string ,level_sk , */
	 VALUES(351,3,2,"CUSTOMER_ID",13,"spm_2_13_736_1",2,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(352,3,2,"CUSTOMER_ID",11,"spm_5_11_529_1",5,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(353,3,2,"CUSTOMER_ID",11,"spm_5_11_529_1",5,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(368,3,3,"INPUT_DEFAULT_DT",28,"spm_1_28_2224_1",1,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(369,3,2,"DEFAULT_STATUS_CD",28,"spm_1_28_1567_1",1,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(381,3,3,"INPUT_DEFAULT_DT",28,"spm_7_28_2224_1",7,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(382,3,2,"DEFAULT_STATUS_CD",28,"spm_7_28_1567_1",7,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(356,3,3,"INPUT_DEFAULT_DT",31,"spm_4_31_1586_1",4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(357,3,2,"DEFAULT_STATUS_CD",31,"spm_4_31_1589_1",4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 
	 VALUES(383,3,2,'CREDIT_FACILITY_ID',23,'spm_4_23_1400_1',4,
	 "%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	 ;
	quit;

	proc sql noprint;
	update &lib_apdm..VARIABLE_MASTER
	set column_data_type_sk = 4 
	where 	column_data_type_sk = 3 
		and variable_sk < 1000000;
	quit;



	/* I18NOK:END */

	%rmcr_csb_modeling_abt_x_variable;



	proc sql;
	insert into &lib_apdm..EXTERNAL_VARIABLE(
	variable_sk ,external_variable_sk 
	)
	VALUES(322,4)
	VALUES(323,5)
	VALUES(324,2)
	VALUES(325,3)
	VALUES(326,1)
	VALUES(329,6)
	VALUES(330,7)
	VALUES(331,8)
	VALUES(332,9)
	VALUES(333,10)
	VALUES(346,12)
	VALUES(347,13)
	VALUES(348,11)
	VALUES(349,15)
	VALUES(350,14)
	;
	quit;

	proc sql;

	insert into &lib_apdm..DERIVED_VARIABLE (

	VARIABLE_SK ,DERIVED_VAR_CALC_EXPRESSION ,VARIABLE_SUB_TYPE_CD ,VARIABLE_LEVEL_NO

	)
	 /* I18NOK:BEGIN */
	 
	VALUES(22,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_1_12M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(23,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_1_12M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(26,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_1_12M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(27,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_1_12M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(28,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_1_12M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(29,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_L1M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(30,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_L1M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(31,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_L1M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(32,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_L1M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(33,'( CASE WHEN ( <<P_MAX_SNP_DAY_PAYP_L1M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(36,'( CASE WHEN ( <<P_MAX_SNP_DEF_CNT_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(37,'( CASE WHEN ( <<P_MAX_SNP_DEF_CNT_L1M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(44,'( CASE WHEN ( <<P_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<P_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<P_SUM_SNP_DPD90_1_12M>> >1 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(45,'( CASE WHEN ( <<P_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<P_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<P_SUM_SNP_DPD90_1_12M>> >1 ) OR ( <<P_SUM_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(48,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_1_12M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(49,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_1_12M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(50,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_1_12M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(51,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_1_12M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(52,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_1_12M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(54,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_L1M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(55,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_L1M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(56,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_L1M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(57,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_L1M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(58,'( CASE WHEN ( <<A_MAX_SNP_DAY_PAYP_L1M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(61,'( CASE WHEN ( <<A_MAX_SNP_DEF_CNT_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(63,'( CASE WHEN ( <<A_MAX_SNP_DEF_CNT_L1M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(65,'( CASE WHEN ( <<A_MAX_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(72,'( CASE WHEN ( <<A_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<A_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<A_SUM_SNP_DPD90_1_12M>> >1 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(73,'( CASE WHEN ( <<A_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<A_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<A_SUM_SNP_DPD90_1_12M>> >1 ) OR ( <<A_SUM_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(76,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_1_12M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(77,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_1_12M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(78,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_1_12M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(79,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_1_12M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(80,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_1_12M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(81,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_L1M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(82,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_L1M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(83,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_L1M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(84,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_L1M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(85,'( CASE WHEN ( <<I_MAX_SNP_DAY_PAYP_L1M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(89,'( CASE WHEN ( <<I_MAX_SNP_DEF_CNT_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(91,'( CASE WHEN ( <<I_MAX_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(97,'( CASE WHEN ( <<I_MAX_SNP_DEF_CNT_L1M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(98,'( CASE WHEN ( <<I_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<I_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<I_SUM_SNP_DPD90_1_12M>> >1 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(99,'( CASE WHEN ( <<I_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<I_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<I_SUM_SNP_DPD90_1_12M>> >1 ) OR ( <<I_MAX_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(110,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_1_12M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(111,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_1_12M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(112,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_1_12M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(113,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_1_12M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(114,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_1_12M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	;

	quit;

	proc sql;

	insert into &lib_apdm..DERIVED_VARIABLE (

	VARIABLE_SK ,DERIVED_VAR_CALC_EXPRESSION ,VARIABLE_SUB_TYPE_CD,VARIABLE_LEVEL_NO

	)

	VALUES(115,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_L1M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(116,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_L1M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(117,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_L1M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(118,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_L1M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(119,'( CASE WHEN ( <<C_MAX_SNP_DAY_PAYP_L1M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(120,'( CASE WHEN ( <<C_MAX_SNP_DEF_CNT_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(121,'( CASE WHEN ( <<C_MAX_SNP_DEF_CNT_L1M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(122,'( CASE WHEN ( <<C_MAX_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(123,'( CASE WHEN ( <<C_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<C_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<C_SUM_SNP_DPD90_1_12M>> >1 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(124,'( CASE WHEN ( <<C_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<C_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<C_SUM_SNP_DPD90_1_12M>> >1 ) OR ( <<C_MAX_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(144,'(((<<SUM_TOT_BAL_L1M>>*<<SUM_CUR_LIMIT_AMT_12MB>>)/NULLIF(<<SUM_CUR_LIMIT_AMT_L1M>>,0))-<<AVG_TOT_BAL_13_24M>>)/NULLIF((<<SUM_CUR_LIMIT_AMT_L1M>>-<<AVG_TOT_BAL_13_24M>>),0)','SPL',1)

	VALUES(149,'(((<<SUM_TOT_BAL_L1M>>*<<SUM_CUR_LIMIT_AMT_12MB>>)/NULLIF(<<SUM_CUR_LIMIT_AMT_L1M>>,0))-<<AVG_TOT_BAL_13_24M>>)/NULLIF((<<SUM_CUR_LIMIT_AMT_L1M>>-<<AVG_TOT_BAL_13_24M>>),0)','SPL',1)

	VALUES(175,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_1_12M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(176,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_1_12M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(177,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_1_12M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(178,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_1_12M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(179,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_1_12M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(180,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_L1M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(181,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_L1M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(182,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_L1M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(183,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_L1M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(184,'( CASE WHEN ( <<Q_MAX_SNP_DAY_PAYP_L1M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(185,'( CASE WHEN ( <<Q_MAX_SNP_DEF_CNT_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(186,'( CASE WHEN ( <<Q_MAX_SNP_DEF_CNT_L1M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(187,'( CASE WHEN ( <<Q_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<Q_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<Q_SUM_SNP_DPD90_1_12M>> >1 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(188,'( CASE WHEN ( <<Q_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<Q_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<Q_SUM_SNP_DPD90_1_12M>> >1 ) OR ( <<Q_SUM_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(235,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_1_12M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(236,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_1_12M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(237,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_1_12M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(238,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_1_12M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(239,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_1_12M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(240,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_L1M>> >30 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(241,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_L1M>> >60 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(242,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_L1M>> >90 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(243,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_L1M>> >120 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(244,'( CASE WHEN ( <<B_MAX_SNP_DAY_PAYP_L1M>> >180 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(245,'( CASE WHEN ( <<B_MAX_SNP_DEF_CNT_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(246,'( CASE WHEN ( <<B_MAX_SNP_DEF_CNT_L1M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(247,'( CASE WHEN ( <<B_MAX_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(248,'( CASE WHEN ( <<B_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<B_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<B_SUM_SNP_DPD90_1_12M>> >1 ) THEN 1 ELSE 0 END )','CPX',1)

	VALUES(249,'( CASE WHEN ( <<B_SUM_SNP_DPD30_1_12M>> >3 ) OR ( <<B_SUM_SNP_DPD60_1_12M>> >2 ) OR ( <<B_SUM_SNP_DPD90_1_12M>> >1 ) OR ( <<B_SUM_SNP_CHRG_OFC_1_12M>> >0 ) THEN 1 ELSE 0 END )','CPX',1)

	;
	 /* I18NOK:END */
	quit;



	proc sql;

	insert into &lib_apdm..SUPPLEMENTARY_VARIABLE (

	VARIABLE_SK ,SELECT_SOURCE_COLUMN_SK, AS_OF_TIME_SK

	)

	VALUES(1,1317,1)

	VALUES(12,1317,1)

	VALUES(46,1745,1)

	VALUES(150,736,1)

	VALUES(151,529,1)

	VALUES(152,736,1)

	VALUES(153,529,1)

	VALUES(154,1400,1)

	VALUES(158,736,1)

	VALUES(159,529,1)

	VALUES(160,1400,1)

	VALUES(164,1745,1)
	;

	quit;


	proc sql;
	insert into &lib_apdm..SUPPLEMENTARY_VARIABLE(
	variable_sk ,select_source_column_sk ,as_of_time_sk 
	)

	VALUES(351,736,1)

	VALUES(352,529,1)

	VALUES(353,529,1)


	VALUES(356,1586,1)
	VALUES(357,1589,1)

	VALUES(368,2224,1)
	VALUES(369,1567,1)

	VALUES(381,2224,1)
	VALUES(382,1567,1)

	VALUES(383,1400,1)
	;
	quit;

	proc sql;

	insert into &lib_apdm..BEHAVIORAL_VARIABLE (

	VARIABLE_SK ,MEASURE_SOURCE_COLUMN_SK ,AGGREGATION_TYPE_SK ,TIME_PERIOD_SK 

	)

	VALUES(13,20,3,6)

	VALUES(20,20,3,1)

	VALUES(34,27,3,6)

	VALUES(35,27,3,1)

	VALUES(38,1431,1,6)

	VALUES(39,1432,1,6)

	VALUES(40,1433,1,6)

	VALUES(41,1593,1,6)

	VALUES(42,2220,1,6)

	VALUES(43,1434,1,6)

	VALUES(47,20,3,6)

	VALUES(53,20,3,1)

	VALUES(59,27,3,6)

	VALUES(60,27,3,1)

	VALUES(64,1434,3,6)

	VALUES(66,1431,1,6)

	VALUES(67,1432,1,6)

	VALUES(68,1433,1,6)

	VALUES(69,1593,1,6)

	VALUES(70,2220,1,6)

	VALUES(71,1434,1,6)

	VALUES(74,20,3,6)

	VALUES(75,20,3,1)

	VALUES(86,27,3,6)

	VALUES(87,27,3,1)

	VALUES(88,1434,3,6)

	VALUES(92,1431,1,6)

	VALUES(93,1432,1,6)

	VALUES(94,1433,1,6)

	VALUES(95,2220,1,6)

	VALUES(96,1593,1,6)

	VALUES(100,20,3,6)

	VALUES(101,20,3,1)

	VALUES(102,27,3,6)

	VALUES(103,27,3,1)

	VALUES(104,1434,3,6)

	VALUES(105,1431,1,6)

	VALUES(106,1432,1,6)

	VALUES(107,1433,1,6)

	VALUES(108,1593,1,6)

	VALUES(109,2220,1,6)

	;

	quit;

	proc sql;

	insert into &lib_apdm..BEHAVIORAL_VARIABLE (

	VARIABLE_SK ,MEASURE_SOURCE_COLUMN_SK ,AGGREGATION_TYPE_SK ,TIME_PERIOD_SK 

	)

	VALUES(140,80,1,1)

	VALUES(141,77,1,1)

	VALUES(142,77,1,13)

	VALUES(143,80,2,3)

	VALUES(145,80,1,1)

	VALUES(146,77,1,1)

	VALUES(147,80,2,3)

	VALUES(148,77,1,13)

	VALUES(165,20,3,6)

	VALUES(166,20,3,1)

	VALUES(167,27,3,6)

	VALUES(168,27,3,1)

	VALUES(169,1431,1,6)

	VALUES(170,2220,1,6)

	VALUES(171,1593,1,6)

	VALUES(172,1432,1,6)

	VALUES(173,1433,1,6)

	VALUES(174,1434,1,6)

	VALUES(224,20,3,1)

	VALUES(225,20,3,6)

	VALUES(226,27,3,1)

	VALUES(227,27,3,6)

	VALUES(228,1434,3,6)

	VALUES(229,1434,1,6)

	VALUES(230,1593,1,6)

	VALUES(231,2220,1,6)

	VALUES(232,1431,1,6)

	VALUES(233,1433,1,6)

	VALUES(234,1432,1,6)

	;

	quit;



	proc sql;

	insert into &lib_apdm..DERIVED_VAR_X_EXPRESSION_VAR (

	EXPRESSION_VARIABLE_SK ,DERIVED_VARIABLE_SK 

	)

	VALUES(13,22)

	VALUES(13,23)

	VALUES(13,26)

	VALUES(13,27)

	VALUES(13,28)

	VALUES(20,29)

	VALUES(20,30)

	VALUES(20,31)

	VALUES(20,32)

	VALUES(20,33)

	VALUES(34,36)

	VALUES(35,37)

	VALUES(38,44)

	VALUES(39,44)

	VALUES(40,44)

	VALUES(38,45)

	VALUES(39,45)

	VALUES(40,45)

	VALUES(43,45)

	VALUES(47,48)

	VALUES(47,49)

	VALUES(47,50)

	VALUES(47,51)

	VALUES(47,52)

	VALUES(53,54)

	VALUES(53,55)

	VALUES(53,56)

	VALUES(53,57)

	VALUES(53,58)

	VALUES(59,61)

	VALUES(60,63)

	VALUES(64,65)

	VALUES(66,72)

	VALUES(67,72)

	VALUES(68,72)

	VALUES(66,73)

	VALUES(67,73)

	VALUES(68,73)

	VALUES(71,73)

	VALUES(74,76)

	VALUES(74,77)

	VALUES(74,78)

	VALUES(74,79)

	VALUES(74,80)

	VALUES(75,81)

	VALUES(75,82)

	VALUES(75,83)

	VALUES(75,84)

	VALUES(75,85)

	;

	quit;

	proc sql;

	insert into &lib_apdm..DERIVED_VAR_X_EXPRESSION_VAR (

	EXPRESSION_VARIABLE_SK ,DERIVED_VARIABLE_SK 

	)

	VALUES(86,89)

	VALUES(88,91)

	VALUES(87,97)

	VALUES(92,98)

	VALUES(93,98)

	VALUES(94,98)

	VALUES(92,99)

	VALUES(93,99)

	VALUES(94,99)

	VALUES(88,99)

	VALUES(100,110)

	VALUES(100,111)

	VALUES(100,112)

	VALUES(100,113)

	VALUES(100,114)

	VALUES(101,115)

	VALUES(101,116)

	VALUES(101,117)

	VALUES(101,118)

	VALUES(101,119)

	VALUES(102,120)

	VALUES(103,121)

	VALUES(104,122)

	VALUES(105,123)

	VALUES(106,123)

	VALUES(107,123)

	VALUES(105,124)

	VALUES(106,124)

	VALUES(107,124)

	VALUES(104,124)

	VALUES(140,144)

	VALUES(142,144)

	VALUES(141,144)

	VALUES(143,144)

	VALUES(145,149)

	VALUES(148,149)

	VALUES(146,149)

	VALUES(147,149)

	VALUES(165,175)

	VALUES(165,176)

	VALUES(165,177)

	VALUES(165,178)

	VALUES(165,179)

	VALUES(166,180)

	;

	quit;

	proc sql;

	insert into &lib_apdm..DERIVED_VAR_X_EXPRESSION_VAR (

	EXPRESSION_VARIABLE_SK ,DERIVED_VARIABLE_SK 

	)

	VALUES(166,181)

	VALUES(166,182)

	VALUES(166,183)

	VALUES(166,184)

	VALUES(167,185)

	VALUES(168,186)

	VALUES(169,187)

	VALUES(172,187)

	VALUES(173,187)

	VALUES(169,188)

	VALUES(172,188)

	VALUES(173,188)

	VALUES(174,188)

	VALUES(225,235)

	VALUES(225,236)

	VALUES(225,237)

	VALUES(225,238)

	VALUES(225,239)

	VALUES(224,240)

	VALUES(224,241)

	VALUES(224,242)

	VALUES(224,243)

	VALUES(224,244)

	VALUES(227,245)

	VALUES(226,246)

	VALUES(228,247)

	VALUES(232,248)

	VALUES(234,248)

	VALUES(233,248)

	VALUES(232,249)

	VALUES(234,249)

	VALUES(233,249)

	VALUES(229,249)

	;

	quit;

	proc sql;

	insert into &lib_apdm..DERIVED_VAR_ALL_EXPRESSION_VAR (

	DERIVED_VARIABLE_SK, EXPRESSION_VARIABLE_SK, ABT_SK, EXPRESSION_VAR_LEVEL_SK 

	)
	select DERIVED_VARIABLE_SK, EXPRESSION_VARIABLE_SK, ABT_SK, VARIABLE_MASTER.LEVEL_SK
	from 	&lib_apdm..DERIVED_VAR_X_EXPRESSION_VAR INNER JOIN 
		&lib_apdm..VARIABLE_MASTER ON
		  (DERIVED_VAR_X_EXPRESSION_VAR.EXPRESSION_VARIABLE_SK = VARIABLE_MASTER.VARIABLE_SK ) INNER JOIN 
		&lib_apdm..MODELING_ABT_X_VARIABLE ON
		  (DERIVED_VAR_X_EXPRESSION_VAR.EXPRESSION_VARIABLE_SK = MODELING_ABT_X_VARIABLE.VARIABLE_SK );

	quit;


	proc sql;

	insert into &lib_apdm..IMPLICIT_VAR_SPECIFICATION_DTL (

	PURPOSE_SK ,LEVEL_SK ,ABT_TIME_GRAIN_SK ,VARIABLE_SK ,MDL_ABT_OUTCOME_VARIABLE_FLG ,APPLICABLE_FOR_SCR_PROCESS_FLG ,APPLICABLE_FOR_ACT_PROCESS_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

	)
	 /* I18NOK:BEGIN */
	VALUES(101,3,1,1,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,8,1,46,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,3,1,150,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,8,1,151,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,1,1,152,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,7,1,153,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,7,1,154,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	/* Retail Account */
		/*RECOVERY_COST*/
		/*VALUES(102,1,1,322,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/
		
		/*LGD*/
		/*VALUES(102,1,1,323,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*DEFAULT_DT*/
		/*VALUES(102,1,1,324,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*VALUE_AT_RECOVERY*/
		/*VALUES(102,1,1,325,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*EAD*/
		/*VALUES(102,1,1,326,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	*/ 

		/* CUSTOMER_ID*/
		VALUES(102,1,1,351,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 
		 
		/* INPUT_DEFAULT_DT */
		/*VALUES(102,1,1,368,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* DEFAULT_STATUS_CD */
		/*VALUES(102,1,1,369,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 


		/*RECOVERY_COST*/
		/*VALUES(102,1,2,322,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/
		
		/*LGD*/
		/*VALUES(102,1,2,323,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*DEFAULT_DT*/
		/*VALUES(102,1,2,324,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*VALUE_AT_RECOVERY*/
		/*VALUES(102,1,2,325,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*EAD*/
		/*VALUES(102,1,2,326,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* CUSTOMER_ID*/
		VALUES(102,1,2,351,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 
		 
		/* INPUT_DEFAULT_DT */
		/*VALUES(102,1,2,368,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* DEFAULT_STATUS_CD */
		/*VALUES(102,1,2,369,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 
		
		
	/* Corp Account */	
		
		/*RECOVERY_COST*/
		/*VALUES(102,7,1,332,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*LGD*/
		/*VALUES(102,7,1,333,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*DEFAULT_DT*/
		/*VALUES(102,7,1,330,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*VALUE_AT_RECOVERY*/
		/*VALUES(102,7,1,331,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*EAD*/
		/*VALUES(102,7,1,329,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* CUSTOMER_ID*/
		VALUES(102,7,1,352,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 

		/* INPUT_DEFAULT_DT */
		/*VALUES(102,7,1,381,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* DEFAULT_STATUS_CD */
		/*VALUES(102,7,1,382,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 
		
		/*Cred Facility ID*/
		VALUES(102,7,1,383,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 


		/*RECOVERY_COST*/
		/*VALUES(102,7,2,332,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*LGD*/
		/*VALUES(102,7,2,333,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*DEFAULT_DT*/
		/*VALUES(102,7,2,330,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*VALUE_AT_RECOVERY*/
		/*VALUES(102,7,2,331,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*EAD*/
		/*VALUES(102,7,2,329,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* CUSTOMER_ID*/
		VALUES(102,7,2,352,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 

		/* INPUT_DEFAULT_DT */
		/*VALUES(102,7,2,381,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* DEFAULT_STATUS_CD */
		/*VALUES(102,7,2,382,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 
		
		/*Cred Facility ID*/
		VALUES(102,7,2,383,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 
		
		
	/* Credit Facility */		
		
		/*RECOVERY_COST*/
		/*VALUES(102,4,1,350,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

		/*LGD*/
		/*VALUES(102,4,1,349,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

		/*DEFAULT_DT*/
		/*VALUES(102,4,1,346,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

		/*VALUE_AT_RECOVERY*/
		/*VALUES(102,4,1,347,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

		/*EAD*/
		/*VALUES(102,4,1,348,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 

		/* CUSTOMER_ID*/
		VALUES(102,4,1,352,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 

		/* INPUT_DEFAULT_DT */
		/*VALUES(102,4,1,356,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* DEFAULT_STATUS_CD */
		/*VALUES(102,4,1,357,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 


		/*RECOVERY_COST*/
		/*VALUES(102,4,2,350,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*LGD*/
		/*VALUES(102,4,2,349,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*DEFAULT_DT*/
		/*VALUES(102,4,2,346,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*VALUE_AT_RECOVERY*/
		/*VALUES(102,4,2,347,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/

		/*EAD*/
		/*VALUES(102,4,2,348,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* CUSTOMER_ID*/
		VALUES(102,4,2,352,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")	 

		/* INPUT_DEFAULT_DT */
		/*VALUES(102,4,2,356,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

		/* DEFAULT_STATUS_CD */
		/*VALUES(102,4,2,357,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/	 

	/* 
	VALUES(102,1,1,155,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,156,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,157,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	*/

	VALUES(103,1,1,158,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,7,1,159,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,7,1,160,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	/*

	VALUES(102,4,1,253,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,126,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,127,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,128,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,129,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,131,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,132,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,133,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,134,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,135,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,136,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,137,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,189,'Y','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,193,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,250,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,251,'Y','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,300,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,301,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,302,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,303,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,304,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,305,'N','N','N',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,306,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,307,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,308,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,309,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,310,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,311,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,312,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,313,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,314,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,1,315,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,1,316,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,1,317,'N','N','Y',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	*/

	;

	quit;



	proc sql;

	insert into &lib_apdm..IMPLICIT_VAR_SPECIFICATION_DTL (

	PURPOSE_SK ,LEVEL_SK ,ABT_TIME_GRAIN_SK ,VARIABLE_SK ,MDL_ABT_OUTCOME_VARIABLE_FLG ,APPLICABLE_FOR_SCR_PROCESS_FLG ,APPLICABLE_FOR_ACT_PROCESS_FLG ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

	)

	VALUES(101,3,2,1,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(101,8,2,46,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(101,3,2,150,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(101,8,2,151,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(101,1,2,152,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(101,7,2,153,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(101,7,2,154,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	/*

	VALUES(102,1,2,155,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,156,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,157,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	*/

	VALUES(103,1,2,158,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(103,7,2,159,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(103,7,2,160,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	/*

	VALUES(102,4,2,253,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,126,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,127,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,128,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,129,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,131,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,132,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,133,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,134,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,135,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,136,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,137,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,189,'Y','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,193,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,250,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,251,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,300,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,301,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,302,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,303,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,304,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,305,'N','N','N',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,306,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,307,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,308,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,309,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,310,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,311,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,312,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,313,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,314,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,1,2,315,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,7,2,316,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")

	VALUES(102,4,2,317,'Y','N','Y',"%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid","%sysfunc(datetime(),DATETIME.)"dt,"&sysuserid")
	*/
	;

	quit;
	 /* I18NOK:END */

	 /* I18NOK:BEGIN */

	proc sql;
	insert into &lib_apdm..TARGET_QUERY_MASTER(
	target_query_sk ,subset_from_path_sk ,publish_template_flg ,created_dttm ,created_by_user ,last_processed_dttm ,last_processed_by_user 
	)
	VALUES(85,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(43,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(34,2,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(81,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(32,2,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(25,11,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(11,12,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(138,11,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(18,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(134,12,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(126,10,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(78,10,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(39,6,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(54,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(132,12,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(61,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(13,12,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(63,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(87,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(70,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(45,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(27,11,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(83,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(130,10,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(128,10,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(140,11,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(4,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(95,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(93,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(2,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(97,4,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(65,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(76,10,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(52,9,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(20,1,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	VALUES(37,6,"Y","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")
	;
	quit;


	proc sql;
	insert into &lib_apdm..TARGET_NODE(
	target_node_sk ,target_query_sk ,node_type_cd ,parent_node_sk ,grouping_type_cd ,level_no ,sequence_no ,created_dttm ,created_by_user ,last_processed_dttm ,last_processed_by_user 
	)
	VALUES(72,132,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(75,61,"GROUPING_NODE",.,"ANY",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(84,61,"GROUPING_NODE",83,"ALL",.,10,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(90,13,"GROUPING_NODE",89,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(96,63,"GROUPING_NODE",95,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(98,63,"GROUPING_NODE",95,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(100,87,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(104,70,"GROUPING_NODE",103,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(106,70,"GROUPING_NODE",103,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(108,45,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(114,27,"GROUPING_NODE",113,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(116,27,"GROUPING_NODE",113,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(118,83,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(122,130,"GROUPING_NODE",121,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(124,128,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(170,76,"GROUPING_NODE",169,"ALL",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(174,76,"GROUPING_NODE",173,"ALL",.,10,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(176,76,"GROUPING_NODE",173,"ALL",.,12,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(180,52,"GROUPING_NODE",179,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(184,52,"GROUPING_NODE",180,"ALL",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(188,52,"GROUPING_NODE",187,"ALL",.,10,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(78,61,"CONDITION_NODE",77,"",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(81,61,"CONDITION_NODE",80,"",.,7,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(87,61,"CONDITION_NODE",86,"",.,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(128,140,"GROUPING_NODE",127,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(130,4,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(136,95,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(140,93,"GROUPING_NODE",139,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(142,2,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(148,97,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(152,65,"GROUPING_NODE",151,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(156,65,"GROUPING_NODE",152,"ALL",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(160,65,"GROUPING_NODE",159,"ALL",.,10,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(162,65,"GROUPING_NODE",159,"ALL",.,12,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(166,76,"GROUPING_NODE",165,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(131,4,"GROUPING_NODE",130,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(133,4,"GROUPING_NODE",130,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(137,95,"GROUPING_NODE",136,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(139,93,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(143,2,"GROUPING_NODE",142,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(145,2,"GROUPING_NODE",142,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(149,97,"GROUPING_NODE",148,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(151,65,"GROUPING_NODE",.,"ANY",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(153,65,"GROUPING_NODE",152,"ALL",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(159,65,"GROUPING_NODE",151,"ALL",.,9,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(165,76,"GROUPING_NODE",.,"ANY",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(167,76,"GROUPING_NODE",166,"ALL",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(169,76,"GROUPING_NODE",165,"ALL",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(173,76,"GROUPING_NODE",165,"ALL",.,9,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(179,52,"GROUPING_NODE",.,"ANY",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(181,52,"GROUPING_NODE",180,"ALL",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(187,52,"GROUPING_NODE",179,"ALL",.,9,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(193,20,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(199,37,"GROUPING_NODE",198,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(77,61,"GROUPING_NODE",76,"ALL",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(80,61,"GROUPING_NODE",76,"ALL",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(83,61,"GROUPING_NODE",75,"ALL",.,9,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(1,85,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(2,85,"GROUPING_NODE",1,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(4,43,"GROUPING_NODE",.,"ANY",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(5,43,"GROUPING_NODE",4,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(6,43,"GROUPING_NODE",5,"ALL",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(9,43,"GROUPING_NODE",5,"ALL",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(12,43,"GROUPING_NODE",4,"ALL",.,9,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(13,43,"GROUPING_NODE",12,"ALL",.,10,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(15,43,"GROUPING_NODE",12,"ALL",.,12,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(18,34,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(19,34,"GROUPING_NODE",18,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(21,34,"GROUPING_NODE",18,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(23,81,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(24,81,"GROUPING_NODE",23,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(26,32,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(27,32,"GROUPING_NODE",26,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(29,32,"GROUPING_NODE",26,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(32,25,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(33,25,"GROUPING_NODE",32,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(35,25,"GROUPING_NODE",32,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(38,11,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(39,11,"GROUPING_NODE",38,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(41,11,"GROUPING_NODE",38,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(44,138,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(45,138,"GROUPING_NODE",44,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(47,18,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(53,134,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(93,13,"CONDITION_NODE",92,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(102,87,"CONDITION_NODE",101,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(110,45,"CONDITION_NODE",109,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(112,45,"CONDITION_NODE",111,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(120,83,"CONDITION_NODE",119,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(126,128,"CONDITION_NODE",125,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(132,4,"CONDITION_NODE",131,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(134,4,"CONDITION_NODE",133,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(138,95,"CONDITION_NODE",137,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(144,2,"CONDITION_NODE",143,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(146,2,"CONDITION_NODE",145,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(150,97,"CONDITION_NODE",149,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(190,52,"GROUPING_NODE",187,"ALL",.,12,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(194,20,"GROUPING_NODE",193,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(196,20,"GROUPING_NODE",193,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(198,37,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(73,132,"GROUPING_NODE",72,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(76,61,"GROUPING_NODE",75,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(101,87,"GROUPING_NODE",100,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(103,70,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(109,45,"GROUPING_NODE",108,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(111,45,"GROUPING_NODE",108,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(113,27,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(119,83,"GROUPING_NODE",118,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(121,130,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(125,128,"GROUPING_NODE",124,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(127,140,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(154,65,"CONDITION_NODE",153,"",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(201,37,"GROUPING_NODE",198,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(203,37,"GROUPING_NODE",198,"ALL",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(158,65,"CONDITION_NODE",156,"",.,8,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(164,65,"CONDITION_NODE",162,"",.,14,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(168,76,"CONDITION_NODE",167,"",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(172,76,"CONDITION_NODE",170,"",.,8,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(178,76,"CONDITION_NODE",176,"",.,14,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(182,52,"CONDITION_NODE",181,"",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(186,52,"CONDITION_NODE",184,"",.,8,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(192,52,"CONDITION_NODE",190,"",.,14,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(200,37,"CONDITION_NODE",199,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(202,37,"CONDITION_NODE",201,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(204,37,"CONDITION_NODE",203,"",.,7,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(48,18,"GROUPING_NODE",47,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(50,18,"GROUPING_NODE",47,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(79,61,"CONDITION_NODE",77,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(82,61,"CONDITION_NODE",80,"",.,8,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(85,61,"CONDITION_NODE",84,"",.,11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(88,61,"CONDITION_NODE",86,"",.,14,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(91,13,"CONDITION_NODE",90,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(94,13,"CONDITION_NODE",92,"",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(97,63,"CONDITION_NODE",96,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(99,63,"CONDITION_NODE",98,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(105,70,"CONDITION_NODE",104,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(107,70,"CONDITION_NODE",106,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(115,27,"CONDITION_NODE",114,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(117,27,"CONDITION_NODE",116,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(123,130,"CONDITION_NODE",122,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(129,140,"CONDITION_NODE",128,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(135,4,"CONDITION_NODE",133,"",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(141,93,"CONDITION_NODE",140,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(147,2,"CONDITION_NODE",145,"",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(155,65,"CONDITION_NODE",153,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(157,65,"CONDITION_NODE",156,"",.,7,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(161,65,"CONDITION_NODE",160,"",.,11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(163,65,"CONDITION_NODE",162,"",.,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(171,76,"CONDITION_NODE",170,"",.,7,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(175,76,"CONDITION_NODE",174,"",.,11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(177,76,"CONDITION_NODE",176,"",.,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(183,52,"CONDITION_NODE",181,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(185,52,"CONDITION_NODE",184,"",.,7,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(189,52,"CONDITION_NODE",188,"",.,11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(191,52,"CONDITION_NODE",190,"",.,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(195,20,"CONDITION_NODE",194,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(197,20,"CONDITION_NODE",196,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(74,132,"CONDITION_NODE",73,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(3,85,"CONDITION_NODE",2,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(7,43,"CONDITION_NODE",6,"",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(8,43,"CONDITION_NODE",6,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(10,43,"CONDITION_NODE",9,"",.,7,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(11,43,"CONDITION_NODE",9,"",.,8,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(14,43,"CONDITION_NODE",13,"",.,11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(16,43,"CONDITION_NODE",15,"",.,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(17,43,"CONDITION_NODE",15,"",.,14,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(20,34,"CONDITION_NODE",19,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(22,34,"CONDITION_NODE",21,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(25,81,"CONDITION_NODE",24,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(28,32,"CONDITION_NODE",27,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(30,32,"CONDITION_NODE",29,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(31,32,"CONDITION_NODE",29,"",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(34,25,"CONDITION_NODE",33,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(36,25,"CONDITION_NODE",35,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(37,25,"CONDITION_NODE",35,"",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(40,11,"CONDITION_NODE",39,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(42,11,"CONDITION_NODE",41,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(43,11,"CONDITION_NODE",41,"",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(46,138,"CONDITION_NODE",45,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(49,18,"CONDITION_NODE",48,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(51,18,"CONDITION_NODE",50,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(52,18,"CONDITION_NODE",50,"",.,6,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(55,134,"CONDITION_NODE",54,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(58,126,"CONDITION_NODE",57,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(61,78,"CONDITION_NODE",60,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(64,39,"CONDITION_NODE",63,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(66,39,"CONDITION_NODE",65,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(69,54,"CONDITION_NODE",68,"",.,3,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(71,54,"CONDITION_NODE",70,"",.,5,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(54,134,"GROUPING_NODE",53,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(56,126,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(57,126,"GROUPING_NODE",56,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(59,78,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(60,78,"GROUPING_NODE",59,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(62,39,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(63,39,"GROUPING_NODE",62,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(65,39,"GROUPING_NODE",62,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(67,54,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(68,54,"GROUPING_NODE",67,"ALL",.,2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(70,54,"GROUPING_NODE",67,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(86,61,"GROUPING_NODE",83,"ALL",.,12,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(89,13,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(92,13,"GROUPING_NODE",89,"ALL",.,4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	VALUES(95,63,"GROUPING_NODE",.,"ALL",.,1,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,
	,"&sysuserid")
	;
	quit;

	proc sql;
	insert into &lib_apdm..TARGET_NODE_EXPRESSION(
	target_node_sk ,operand_source_column_sk ,operator_sk ,user_entered_value_flg ,user_entered_value_txt 
	)
	VALUES(71,1563,7,"N","")
	VALUES(74,1964,7,"N","")
	VALUES(78,2224,5,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, -&DEFAULT_CAPTURE_PERIOD+1,''begin'')')
	VALUES(79,2224,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, 0 ,''end'')')
	VALUES(81,1567,7,"N","")
	VALUES(82,2204,7,"N","")
	VALUES(85,1562,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,-&MOB,''end'')')
	VALUES(87,1563,7,"N","")
	VALUES(88,2204,7,"N","")
	VALUES(91,1717,7,"N","")
	VALUES(93,1716,5,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,0,''begin'')')
	VALUES(94,1716,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,0,''end'')')
	VALUES(97,2204,7,"N","")
	VALUES(99,1563,7,"N","")
	VALUES(102,1164,7,"N","")
	VALUES(105,2204,7,"N","")
	VALUES(107,1563,7,"N","")
	VALUES(110,2204,7,"N","")
	VALUES(112,1563,7,"N","")
	VALUES(115,2257,7,"N","")
	VALUES(117,1654,7,"N","")
	VALUES(120,1164,7,"N","")
	VALUES(123,2259,7,"N","")
	VALUES(126,2259,7,"N","")
	VALUES(129,1603,7,"N","")
	VALUES(132,1289,7,"N","")
	VALUES(134,1288,5,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,0,''begin'')')
	VALUES(135,1288,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,0,''end'')')
	VALUES(138,1549,7,"N","")
	VALUES(141,1549,7,"N","")
	VALUES(144,1289,7,"N","")
	VALUES(146,1288,5,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,-&APPL_CAPTURE_PERIOD+1,''begin'')')
	VALUES(147,1288,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,0,''end'')')
	VALUES(150,1549,7,"N","")
	VALUES(154,2224,5,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, -&DEFAULT_CAPTURE_PERIOD+1,''begin'')')
	VALUES(155,2224,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, 0 ,''end'')')
	VALUES(157,1567,7,"N","")
	VALUES(158,2204,7,"N","")
	VALUES(161,1562,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,-&MOB,''end'')')
	VALUES(163,1563,7,"N","")
	VALUES(164,2204,7,"N","")
	VALUES(168,1586,1,"Y",".")
	VALUES(171,1586,5,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM,-&DEFAULT_CAPTURE_PERIOD+1,''begin'')')
	VALUES(172,1586,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM,0,''end'')')
	VALUES(175,1589,7,"N","")
	VALUES(177,1586,5,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM,-&DEFAULT_CAPTURE_PERIOD+1,''begin'')')
	VALUES(178,1586,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM,0,''end'')')
	VALUES(182,2224,5,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, -&DEFAULT_CAPTURE_PERIOD+1,''begin'')')
	VALUES(183,2224,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, 0 ,''end'')')
	VALUES(185,1567,7,"N","")
	VALUES(186,2204,7,"N","")
	VALUES(189,1562,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,-&MOB,''end'')')
	VALUES(191,1563,7,"N","")
	VALUES(192,2204,7,"N","")
	VALUES(195,2256,7,"N","")
	VALUES(197,1215,7,"N","")
	VALUES(200,540,7,"N","")
	VALUES(202,538,7,"N","")
	VALUES(204,2223,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM , -&MOB_CUSTOMER ,''end'')')
	VALUES(3,1164,7,"N","")
	VALUES(7,1567,7,"N","")
	VALUES(8,2204,7,"N","")
	VALUES(10,2224,5,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, -&DEFAULT_CAPTURE_PERIOD+1,''begin'')')
	VALUES(11,2224,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, 0 ,''end'')')
	VALUES(14,1563,7,"N","")
	VALUES(16,1562,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,-&MOB,''end'')')
	VALUES(17,2204,7,"N","")
	VALUES(20,745,7,"N","")
	VALUES(22,747,7,"N","")
	VALUES(25,1164,7,"N","")
	VALUES(28,745,7,"N","")
	VALUES(30,2222,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM , -&MOB_CUSTOMER ,''end'')')
	VALUES(31,747,7,"N","")
	VALUES(34,2257,7,"N","")
	VALUES(36,1616,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, -&MOB ,''end'')')
	VALUES(37,1654,7,"N","")
	VALUES(40,1717,7,"N","")
	VALUES(42,1716,5,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,-&APPL_CAPTURE_PERIOD+1,''begin'')')
	VALUES(43,1716,6,"Y",'%dabt_intnx(''dtmonth'',&RUN_DTTM,0,''end'')')
	VALUES(46,1603,7,"N","")
	VALUES(49,2256,7,"N","")
	VALUES(51,2221,6,"Y",'%dabt_intnx(''dtmonth'', &RUN_DTTM, -&MOB ,''end'')')
	VALUES(52,1215,7,"N","")
	VALUES(55,1964,7,"N","")
	VALUES(58,2259,7,"N","")
	VALUES(61,1586,1,"Y",".")
	VALUES(64,538,7,"N","")
	VALUES(66,540,7,"N","")
	VALUES(69,2204,7,"N","")
	;
	quit;



	proc sql;
	insert into &lib_apdm..TARGET_NODE_EXPRSSION_X_VALUE(
	dim_attribute_value_sk ,target_node_sk 
	)
	VALUES(432,3)
	VALUES(779,7)
	VALUES(1229,8)
	VALUES(776,14)
	VALUES(1229,17)
	VALUES(1270,20)
	VALUES(1268,22)
	VALUES(433,25)
	VALUES(1270,28)
	VALUES(1268,31)
	VALUES(1314,34)
	VALUES(839,37)
	VALUES(1306,40)
	VALUES(788,46)
	VALUES(1315,49)
	VALUES(503,52)
	VALUES(915,55)
	VALUES(1357,58)
	VALUES(1307,64)
	VALUES(1269,66)
	VALUES(1230,69)
	VALUES(776,71)
	VALUES(914,74)
	VALUES(779,81)
	VALUES(1229,82)
	VALUES(776,87)
	VALUES(1229,88)
	VALUES(1306,91)
	VALUES(1229,97)
	VALUES(776,99)
	VALUES(434,102)
	VALUES(1230,105)
	VALUES(776,107)
	VALUES(1229,110)
	VALUES(776,112)
	VALUES(1314,115)
	VALUES(839,117)
	VALUES(431,120)
	VALUES(1358,123)
	VALUES(1359,126)
	VALUES(789,129)
	VALUES(583,132)
	VALUES(770,138)
	VALUES(773,141)
	VALUES(583,144)
	VALUES(771,150)
	VALUES(779,157)
	VALUES(1230,158)
	VALUES(776,163)
	VALUES(1230,164)
	VALUES(1360,175)
	VALUES(779,185)
	VALUES(1230,186)
	VALUES(776,191)
	VALUES(1230,192)
	VALUES(1315,195)
	VALUES(503,197)
	VALUES(1269,200)
	VALUES(1307,202)
	;
	quit;

	 /* I18NOK:END */


	%rmcr_csb_subject_group_master;

	proc sql;                                                                                                                               
																																			
	insert into &lib_apdm..SUBJECT_GROUP_SPCFCN_DTL (                                                                               
																																			
	LEVEL_SK ,SUBJECT_GROUP_SK ,DEPLOYED_CODE_FILE_NM, CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER           
																																			
	)                                                                                                                                       
																																			
																																			
																																			
	/*VALUES(1,11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/                        
																																			
	/*VALUES(1,12,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/                        
																																			
	/*VALUES(1,13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/                        
																																			
	/*VALUES(1,14,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/                        
																																			
	/*VALUES(7,15,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/                        
																																			
	/*VALUES(7,16,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")*/                        
	/* I18NOK:BEGIN */																																		
	VALUES(3,7,"p_ap_lon_p.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(3,8,"p_ap_mtg_p.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(3,9,"p_ap_ccd_p.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(4,18,"l_cf_loc_d.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")           
	VALUES(4,19,"l_cf_wc_d.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(8,21,"p_cp_lon_q.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")           
	VALUES(8,22,"p_cp_mtg_q.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")           
	VALUES(7,24,"p_ca_lon_b.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")           
	VALUES(7,25,"p_ca_mtg_b.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")           
	VALUES(1,1,"p_ic_lon_a.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(1,2,"p_ic_ccd_a.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(1,3,"p_ic_mtg_a.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(1,4,"p_ic_cor_a.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	VALUES(4,20,"l_cf_cc_d.sas","%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")            
	/* I18NOK:END */																																		
																																			
	;                                                                                                                                       
																																			
	quit;                                                                                                                                  
																																			
			
	proc sql;

	insert into &lib_apdm..IMPLICIT_SUBSET_SPCFCTN_DTL (

	PURPOSE_SK ,LEVEL_SK ,APPLICABLE_FOR_PROCESS_TYPE_CD ,TARGET_QUERY_SK, CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

	)
	 /* I18NOK:BEGIN */
	VALUES(101,1,'MDL',18,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,'MDL',43,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,1,'MDL',61,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,2,'MDL',32,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,3,'MDL',2,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,'MDL',76,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,5,'MDL',37,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,7,'MDL',25,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,'MDL',52,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,7,'MDL',65,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,8,'MDL',11,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")


	VALUES(101,1,'SCR',20,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,1,'SCR',45,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,1,'SCR',63,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,2,'SCR',34,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,3,'SCR',4,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,4,'SCR',78,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,5,'SCR',39,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,7,'SCR',27,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(102,7,'SCR',54,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(103,7,'SCR',70,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	VALUES(101,8,'SCR',13,"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid")

	;
	 /* I18NOK:END */
	quit;

	proc sql;
	 /* I18NOK:BEGIN */
	  insert into &lib_apdm..POST_ACTION_MACRO_CONFIG_DTL

	  (action_sk,POST_ACTION_MACRO_NM)
	 
	   values
	 
	   (3,'csbmva_post_new_abt') /*csbmva_setrecovery_lgd, csbmva_upd_impl_subset*/
	 
	   values
	 
	   (11,'csbmva_post_bld_mdl') /*csbmva_setdefaults*/
	 
	   values
	 
	  (12,'csbmva_post_scr_job') /*csbmva_setoutcome_period*/

	  ;

	quit;

	 /* I18NOK:END */

	proc sql;

	insert into &lib_apdm..TABLE_X_LEVEL_KEY_COLUMN (

	SOURCE_TABLE_SK ,LEVEL_KEY_COLUMN_DTL_SK ,CORRSP_SOURCE_COLUMN_SK

	)

	VALUES(62,9,2881)

	;

	quit;


	proc sql; 


	insert into &lib_apdm..PARAMETER_MASTER 


	( PARAMETER_NM, PARAMETER_DESC,PARAM_ADDED_BY_PRODUCT_CD ,VALUE_EDITABLE_FLG, VALUE_TO_BE_SET_BY_SOLUTN_FLG,  
	CREATED_DTTM, CREATED_BY_USER , LAST_PROCESSED_DTTM, LAST_PROCESSED_BY_USER 
	) 


	values  
	( 
	'DABT_AUTO_GEN_REPORT_POOLING_ABT',  /* I18NOK:LINE */
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PARAMETER_MASTER.PARAMETER_DS2.1, noquote))", 
	'csbmid','Y','Y',   /* I18NOK:LINE */
	"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid" 
	) 

	 
	values  
	( 
	'DEFAULT_CAPTURE_PERIOD',   /* I18NOK:LINE */
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PARAMETER_MASTER.PARAMETER_DS4.1, noquote))",  
	'csbmid','Y','Y',    /* I18NOK:LINE */
	"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid" 

	)

	values  
	( 
	'CSB_DEF_CPTR_PERIOD_POST_LGD_SCR',  /* I18NOK:LINE */
	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,PARAMETER_MASTER.PARAMETER_DS5.1, noquote))", 
	'csbmid','Y','Y',    /* I18NOK:LINE */
	"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid" 

	)


	; 


	quit; 

	proc sql;

	 insert into &lib_apdm..PARAMETER_VALUE_DTL

	(

		PARAMETER_NM, VALUE_APPLICABLE_TO_PRODUCT_CD, PARAMETER_VALUE,CREATED_DTTM, CREATED_BY_USER ,	LAST_PROCESSED_DTTM, LAST_PROCESSED_BY_USER

	)

	values 

	(

	'DEFAULT_CAPTURE_PERIOD', 'csbmid','6',   /* I18NOK:LINE */

	"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid"

	)

	values 

	(

	'CSB_DEF_CPTR_PERIOD_POST_LGD_SCR', 'csbmid','6',   /* I18NOK:LINE */

	"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid"

	)



	values 

	(

	"DABT_AUTO_GEN_REPORT_POOLING_ABT", "csbmid","Y",  /* I18NOK:LINE */

	"%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid"

	);

	quit;


	proc sql;
			insert into &lib_apdm..CPRM_ENTITY_MASTER
	(ENTITY_TYPE_SK, ENTITY_TYPE_CD, DIRECT_PRMTN_APPLICABLE_FLG,ENTITY_IMPORT_SEQ_NO,
	 ENTITY_TYPE_NM, ENTITY_TYPE_DESC, TABLE_NM, SK_COLUMN_NM, UID_COLUMN_NM, UNIQUE_CONSTRAINT_COLUMN_NM, DISPLAY_COLUMN_NM,	
	IMPORT_MACRO_NM )
			 
	values
	( 101, "POOL","Y",9 ,/* i18NOK:LINE*/

		/*

		"%sysfunc(sasmsg(SASHELP.dabt_cprm_misc,ENTITY_TYPE_NM_POOL_SCHEME, noquote))",  

		"%sysfunc(sasmsg(SASHELP.dabt_cprm_misc,ENTITY_TYPE_DS_POOL_SCHEME, noquote))"  ,

		*/

		

		"Pool scheme",   /* I18NOK:LINE */

		"Pool scheme",   /* I18NOK:LINE */

		"POOL_SCHEME", "POOL_SCHEME_SK","POOL_SCHEME_NAME"," ","POOL_SCHEME_NAME",     /* I18NOK:LINE */
/* I18NOK:BEGIN */
		'%csbmva_cprm_import_pool_scheme(  

		entity_sk=&entity_key., 

		import_spec_ds_nm=CPRM_IMPORT_PARAMETER_LIST,

		import_package_path=&import_package_path., 

		import_analysis_report_path=&import_analysis_report_path., 

		import_analysis_report_ds_nm = CPRM_PRE_IMPORT_ANALYSIS_DTL,

		mode = &mode);'   
/* I18NOK:END */
		)	
		
	values
	( 102, "MS","Y",8 ,/* i18NOK:LINE*/

		/*

		"%sysfunc(sasmsg(SASHELP.dabt_cprm_misc,ENTITY_TYPE_NM_POOL_SCHEME, noquote))",  

		"%sysfunc(sasmsg(SASHELP.dabt_cprm_misc,ENTITY_TYPE_DS_POOL_SCHEME, noquote))"  ,

		*/

		

		"Master scale",   /* I18NOK:LINE */

		"Master scale",   /* I18NOK:LINE */

		"MASTER_SCALE_DEFN", "MASTER_SCALE_NO","MASTER_SCALE_NAME"," ","MASTER_SCALE_NAME",    /* I18NOK:LINE */
   /* I18NOK:BEGIN */
		'%csbmva_cprm_import_master_scale(

		entity_sk=&entity_key., 

		import_spec_ds_nm=CPRM_IMPORT_PARAMETER_LIST,

		import_package_path=&import_package_path., 

		import_analysis_report_path=&import_analysis_report_path., 

		import_analysis_report_ds_nm = CPRM_PRE_IMPORT_ANALYSIS_DTL,

		mode = &mode);'   
/* I18NOK:END */
		)	;
	quit;	
		
%mend rmcr_bank_insert_apdm_config;	
			
		
				
				

