%macro dabt_cprm_ins_pre_analysis_dtl (
								m_promotion_entity_nm= ,
								m_promotion_entity_type_cd=,
								m_assoc_entity_nm= ,
								m_assoc_entity_type_cd=,
								m_unique_constr_violation_flg=,
								m_present_in_tgt_flg=,
								m_present_in_import_package_flg=,
								m_referred_in_other_entity_flg=,
								m_different_defn_flg=,
								m_addnl_info = 
								); 

		%let m_promotion_entity_nm= &m_promotion_entity_nm;
		%let m_promotion_entity_type_cd= &m_promotion_entity_type_cd;
		%let m_assoc_entity_nm= &m_assoc_entity_nm;
		%let m_assoc_entity_type_cd= &m_assoc_entity_type_cd;
		%let m_present_in_tgt_flg=&m_present_in_tgt_flg;
		%let m_present_in_import_package_flg= &m_present_in_import_package_flg;
		%let m_referred_in_other_entity_flg= &m_referred_in_other_entity_flg;
		%let m_unique_constr_violation_flg= &m_unique_constr_violation_flg;
		%let m_different_defn_flg= &m_different_defn_flg;
		%let m_ass_ent_import_action_cd= ;
		%let m_ass_ent_import_action_desc= ;


		/* Check that m_promotion_entity_nm  m_promotion_entity_type_cd m_assoc_entity_nm and m_assoc_entity_type_cd should not be null*/
			
			%if ("&m_promotion_entity_nm" eq "" ) and ("&m_promotion_entity_type_cd" eq "") and 
				("&m_assoc_entity_nm" eq "") and  ("&m_assoc_entity_type_cd" eq "") %then %do ; 
					%if (&syscc. le 4) %then %do;
						%let syscc = 9999 ;
					%end; 
					%return;
			%end; 
			
		/* Check that m_present_in_tgt_flg  m_present_in_import_package_flg should not be null*/
		
			%if (&m_present_in_tgt_flg eq )  or (&m_present_in_import_package_flg eq ) 
			%then %do;
					%if (&syscc. le 4) %then %do;
						%let syscc = 9999 ;
					%end;
				%return;
			%end; 
		
																				
		%if &m_unique_constr_violation_flg = &CHECK_FLAG_TRUE %then %do; 
			%let m_ass_ent_import_action_cd = ERROR_ASSOC_ENTITY_UNIQUE_CONSTRAINT_VIOLATION;
			%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC5, noquote));
		%end;

		%else %if (&m_present_in_tgt_flg  = &CHECK_FLAG_FALSE) and  (&m_present_in_import_package_flg) = &CHECK_FLAG_TRUE %then %do;

			/* Assoc entity is the main entity being promoted itself. Its not linkage to a parent entity */
			%if ("&m_promotion_entity_nm" = "&m_assoc_entity_nm") and ("&m_promotion_entity_type_cd" = "&m_assoc_entity_type_cd")  %then %do;
				%let m_ass_ent_import_action_cd = NO_ERROR_IMPORT; 
				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC2, noquote));
			%end;
			%else %do;
				%let m_ass_ent_import_action_cd = NO_ERROR_IMPORT; 
				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC2, noquote));
			%end;	
			
		%end;										
			
		%else %if (&m_present_in_tgt_flg  = &CHECK_FLAG_FALSE) and  (&m_present_in_import_package_flg) = &CHECK_FLAG_FALSE %then %do;

			%let m_ass_ent_import_action_cd = ERROR_ASSOC_ENTITY_NOT_AVAILBLE_FOR_LINKAGE;
			%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC3, noquote));
		%end;
			
			
	%else %if (&m_present_in_tgt_flg  = &CHECK_FLAG_TRUE) and  (&m_present_in_import_package_flg = &CHECK_FLAG_TRUE or &m_present_in_import_package_flg eq ) %then %do;

		%if (&m_different_defn_flg = ) %then %do;
			/**** Modified for Subset Map ****/ 
			%if (&m_referred_in_other_entity_flg = &CHECK_FLAG_TRUE AND "&m_promotion_entity_type_cd" = "SUBSET_MAP" ) %THEN %DO ; 	/* i18NOK:LINE */
				%let m_ass_ent_import_action_cd = ERROR_ASSOC_ENTITY_REPRMTN_NOT_SUPPORTED_OF_USED_ENTITY;
				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC7, noquote));
			%end; 
			%else %do ; 
			/* No action assumption that check of whether there is definiation change is not done programmatically. It is assumed that user will take care to not change it in such way that it becomes unusable*/		
				%let m_ass_ent_import_action_cd = NO_ERROR_IMPORT;
				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC2, noquote));
			%end; 
		%end;
		
		%else %if (&m_different_defn_flg = &CHECK_FLAG_TRUE) %then %do;
			 %if (&m_referred_in_other_entity_flg = &CHECK_FLAG_TRUE) %then %do;
				%let m_ass_ent_import_action_cd = ERROR_ASSOC_ENTITY_DIFF_DEFN_OF_USED_ENTITY;
				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC6, noquote));
			 %end;			 
			 %else %if (&m_referred_in_other_entity_flg = &CHECK_FLAG_FALSE) %then %do;
			 	%let m_ass_ent_import_action_cd = NO_ERROR_IMPORT;
				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC2, noquote));;
			 %end;
			 %else %do;
				%let m_ass_ent_import_action_cd = ERROR_ASSOC_ENTITY_DIFF_DEFN;
				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC8, noquote));			 
			 %end;
		%end;
		
		%else  %if (&m_different_defn_flg = &CHECK_FLAG_FALSE) %then %do;
			%let m_ass_ent_import_action_cd = NO_ERROR_IMPORT;
			%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC2, noquote));;
		%end;
	%end;

	%else %if (&m_present_in_tgt_flg  = &CHECK_FLAG_TRUE) and  (&m_present_in_import_package_flg) = &CHECK_FLAG_FALSE  %then %do;
		%if (&m_different_defn_flg = &CHECK_FLAG_TRUE) %then %do;
		/* Specifically added while checkibg project existice while import of model*/
			    %if (&m_different_defn_flg = &CHECK_FLAG_TRUE) %then %do;
					%let m_ass_ent_import_action_cd = ERROR_ASSOC_ENTITY_DIFF_DEFN;
					%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC8, noquote));			 
				%end;
		%end;
		%else %do;
			%if (&m_referred_in_other_entity_flg=&CHECK_FLAG_TRUE) %then %do;
				%let m_ass_ent_import_action_cd = ERROR_ASSOC_ENTITY_CANNOT_BE_DELETED;

				%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC4, noquote));
			%end;
			
			%else %if (&m_referred_in_other_entity_flg=&CHECK_FLAG_FALSE) %then %do;
					%let m_ass_ent_import_action_cd = NO_ERROR_DELETE_FROM_TGT; 
					%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC1, noquote));
			%end;
			
			%else %do;
					%let m_ass_ent_import_action_cd = NO_ERROR_IMPORT; 
					%let m_ass_ent_import_action_desc = %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, CPRM_PRE_IMPORT_ANALYSIS_DTL.DESC2, noquote));
			%end;
		%end;
	%end;
		%if &m_present_in_tgt_flg ne %then %do;
				%let m_present_in_tgt_flg=&m_present_in_tgt_flg;
		%end;
		%else %do;
				%let m_present_in_tgt_flg="";
		%end;

		%if &m_present_in_import_package_flg ne %then %do;
				%let m_present_in_import_package_flg= &m_present_in_import_package_flg;
		%end;
		%else %do;
				%let m_present_in_import_package_flg="";
		%end;

		%if &m_referred_in_other_entity_flg ne %then %do;
				%let m_referred_in_other_entity_flg= &m_referred_in_other_entity_flg;
		%end;
		%else %do;
				%let m_referred_in_other_entity_flg= "";
		%end;

		%if &m_unique_constr_violation_flg ne %then %do;
				%let m_unique_constr_violation_flg= &m_unique_constr_violation_flg;
		%end;
		%else %do;
				%let m_unique_constr_violation_flg="";
		%end;
		
		%if &m_different_defn_flg ne %then %do;
				%let m_different_defn_flg= &m_different_defn_flg;
		%end;
		%else %do;
				%let m_different_defn_flg="";
		%end;

		PROC SQL nowarn ; 
			INSERT INTO  &m_cprm_scr_lib..CPRM_PRE_IMPORT_ANALYSIS_DTL
				( 
					PROMOTION_ENTITY_NM, 
					PROMOTION_ENTITY_TYPE_CD,
					ASSOC_ENTITY_NM, 
					ASSOC_ENTITY_TYPE_CD,
					PRESENT_IN_TGT_FLG,
					PRESENT_IN_IMPORT_PACKAGE_FLG,
					REFERRED_IN_OTHER_ENTITY_FLG,
					UNIQUE_CONSTRAINT_VIOLATION_FLG,
					DIFFERENT_DEFN_FLG,
					ASS_ENT_IMPORT_ACTION_CD,
					ASS_ENT_IMPORT_ACTION_DESC,
					ADDNL_INFO
				)
			 VALUES 
				( 
					"&m_promotion_entity_nm", 
					"&m_promotion_entity_type_cd",
					"&m_assoc_entity_nm", 
					"&m_assoc_entity_type_cd",
					&m_present_in_tgt_flg,
					&m_present_in_import_package_flg,
					&m_referred_in_other_entity_flg,
					&m_unique_constr_violation_flg,
					&m_different_defn_flg,
					"&m_ass_ent_import_action_cd",
					"&m_ass_ent_import_action_desc",
					"&m_addnl_info."

				) ; 
		QUIT;
	 
%mend dabt_cprm_ins_pre_analysis_dtl;
