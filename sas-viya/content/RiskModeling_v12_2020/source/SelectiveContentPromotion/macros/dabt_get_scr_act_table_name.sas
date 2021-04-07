%macro dabt_get_scr_act_table_name(m_scoring_model_sk = , m_scr_arm_table_nm =  , m_act_arm_table_nm =  );

	%local m_model_analysis_type_cd;
	%local m_scoring_table_nm;
	%local m_actual_table_nm;
	%local m_model_sk;
	
	proc sql noprint;
		select mat.model_analysis_type_cd, mat.ara_scoring_table_nm, mat.ara_actual_table_nm, sm.model_sk 
		  into :m_model_analysis_type_cd , :m_scoring_table_nm, :m_actual_table_nm, :m_model_sk
			from &lib_apdm..scoring_model sm
				inner join &lib_apdm..model_master mm
					on (sm.model_sk = mm.model_sk)
				inner join &lib_apdm..project_master pm
					on (pm.project_sk = mm.project_sk)
				inner join &lib_apdm..purpose_master purp
					on (pm.purpose_sk = purp.purpose_sk)
				inner join &lib_apdm..model_analysis_type_master mat
					on (purp.model_analysis_type_sk = mat.model_analysis_type_sk)
			where sm.scoring_model_sk = &m_scoring_model_sk.;
	quit;
	
	%let m_model_analysis_type_cd = &m_model_analysis_type_cd.;
	%let m_scoring_table_nm = &m_scoring_table_nm.;
	%let m_actual_table_nm = &m_actual_table_nm.;
	%let m_model_sk = &m_model_sk.;
	
	%if &m_model_sk eq %then %do;
		%let m_model_sk=DUMMY;
	%end;
	
	%if %kupcase(%sysfunc(kcompress(&arm_database_engine.))) = CAS %then %do;
	%let m_arm_tbl_prefix = %sysfunc(cat(_,&m_model_sk.)); 			/* I18NOK:LINE */
	%end;
	%else %if %kupcase(%sysfunc(kcompress(&arm_database_engine.))) = HADOOP %then %do;
		%let m_arm_tbl_prefix = %sysfunc(cat(h_,&m_model_sk.)); 			/* I18NOK:LINE */
	%end;
	%else %if %kupcase(%sysfunc(kcompress(&arm_database_engine.))) = TERADATA %then %do;
		%let m_arm_tbl_prefix = COMMON;
	%end;

	%if "&m_scoring_table_nm." ne "" %then 	%let &m_scr_arm_table_nm. = %sysfunc(cat(&m_arm_tbl_prefix.,_,&m_scoring_table_nm.)); /* I18NOK:LINE */
	
	%if "&m_actual_table_nm." ne "" %then %let &m_act_arm_table_nm. = %sysfunc(cat(&m_arm_tbl_prefix.,_,&m_actual_table_nm.));	/* I18NOK:LINE */

%mend dabt_get_scr_act_table_name;
