%macro dabt_cprm_import_vdmml;
	
	%let m_entity_list=;
        
	proc sql noprint;
	select entity_key into :m_entity_list separated by ',' from &m_cprm_imp_ctl..CPRM_IMPORT_PARAMETER_LIST  /* i18nOK:Line */
	where PROMOTE_FLG='Y' and ENTITY_TYPE_CD = 'MODEL';  /* i18nOK:Line */
	quit;
        
    %let m_entity_list=&m_entity_list;
	
	%if "&m_entity_list." ne "" %then %do;
	
		%let m_all_pub_nm=;
		proc sql noprint;
		select "'"||kstrip(last_registered_model_nm)||"'" into :m_all_pub_nm separated by ','  /* i18nOK:Line */
		from &m_cprm_src_apdm..model_master where model_source_type_sk=3 and model_sk in (&m_entity_list.);
		quit;
	
		%if "&m_all_pub_nm." ne "" %then %do;
			%dabt_initiate_cas_session(cas_session_ref=load_vdmml_mdl);
			
			/*Check if publishing destination table exist in memory. If not load it*/
			%let table_exist=%eval(%sysfunc(exist(&RM_PUBLISHED_DEST_CAS_LIB..&RM_PUBLISHED_DEST_CAS_TABLE_NM., DATA)));
			%if &table_exist eq 0 %then %do;
				%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_IMPORT_VDMML_MODEL2.1, noquote,&RM_PUBLISHED_DEST_CAS_TABLE_NM.,&RM_PUBLISHED_DEST_CAS_LIB.));
				%dabt_load_table_to_cas(m_in_cas_lib_ref=&RM_PUBLISHED_DEST_CAS_LIB, m_in_table_nm=&RM_PUBLISHED_DEST_CAS_TABLE_NM., m_out_cas_lib_ref=&RM_PUBLISHED_DEST_CAS_LIB, m_out_table_nm=&RM_PUBLISHED_DEST_CAS_TABLE_NM., m_replace_if_exists=N, m_promote_flg=Y);
			%end;
			%else %if &table_exist eq 1 %then %do;
				%let m_hdt_exists=Y;
			%end;
			
			%if &m_hdt_exists. ne N %then %do;
				proc cas;
						table.fetch result=vdml / fetchVars={{name="ModelName"}}   /* i18nOK:Line */
							sortby={{name="ModelName"}} table={caslib="&RM_PUBLISHED_DEST_CAS_LIB.",   /* i18nOK:Line */
							name="&RM_PUBLISHED_DEST_CAS_TABLE_NM.",
							where="ModelName in (&m_all_pub_nm.)"};   /* i18nOK:Line */
						exist_Files=findtable(vdml);
				   
						if exist_Files then
							saveresult vdml dataout=work.vdml;
						run;
				quit;
				
				proc sql noprint;
				select count(*) into :cnt_vdml from work.vdml;  /* i18nOK:Line */
				quit;
				
				
				%if &cnt_vdml gt 0 %then %do;
					%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_IMPORT_VDMML_MODEL1.1, noquote,&m_all_pub_nm.,&RM_PUBLISHED_DEST_CAS_LIB.,&RM_PUBLISHED_DEST_CAS_TABLE_NM.));
				%end;
			%end;
			%else %do;
				%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM_IMPORT_VDMML_MODEL3.1, noquote,&RM_PUBLISHED_DEST_CAS_TABLE_NM.,&RM_PUBLISHED_DEST_CAS_LIB.));
			%end;
			%if &user_provided_mode = EXECUTE %then %do;
		
				%let work_lib_path = %sysfunc(pathname(work));
				
				
				proc cas;
				table.caslibinfo result=ast / caslib="&DABT_MODELING_ABT_LIBREF." verbose="TRUE"; /* i18nOK:Line */
				exist_ast=findtable(ast);
				if exist_ast then
					saveresult ast dataout=work.ast_info;
				quit;

				%let ast_caslib_path= ;

				proc sql noprint;
					select path into :ast_caslib_path from work.ast_info ;
				quit;	

				/*Delete old sashadt file change as part of defect*/
				
				proc cas;
				table.deleteSource source="SRC_RM_PUB_CAS_TBL.sashdat"  CASLIB="&DABT_MODELING_ABT_LIBREF." quiet=true removeAccessControls=TRUE; run;  /* i18nOK:Line */
				quit;
				
				filename vd_in filesrvc 
					folderpath="&import_package_path"      /* I18NOK:LINE */
					filename="SRC_RM_PUB_CAS_TBL.sashdat" debug=http CD="attachment; filename=SRC_RM_PUB_CAS_TBL.sashdat"; /* i18nOK:Line */
				filename vd_out "&ast_caslib_path./SRC_RM_PUB_CAS_TBL.sashdat";         /*i18NOK:LINE*/
		 
		
				data _null_;
					rc=fcopy('vd_in', 'vd_out');      /* I18NOK:LINE */
					msg=sysmsg();
					put rc=msg=;
				run;
				
				proc cas; 
						table.loadTable / casout={caslib="&DABT_MODELING_ABT_LIBREF.", name="SRC_RM_PUB_CAS_TBL",    /* I18NOK:LINE */
							promote="FALSE" replace="TRUE"} /* i18nOK:Line */
							caslib="&DABT_MODELING_ABT_LIBREF.", path="SRC_RM_PUB_CAS_TBL.sashdat";     /* I18NOK:LINE */
						run;
				quit;
				
				%if &m_hdt_exists. eq Y %then %do;
					proc cas;
						simple.numRows result=r /
						table={
						caslib="&RM_PUBLISHED_DEST_CAS_LIB",
						name="&RM_PUBLISHED_DEST_CAS_TABLE_NM",
						where="ModelName in (&m_all_pub_nm)"   /* i18NOK:LINE */
						};
						run;
						print "rows :" r["numrows"];   /* i18NOK:LINE */
						run;
						if (r["numrows"]) then do;   /* i18NOK:LINE */
						table.deleteRows /
						table={
						caslib="&RM_PUBLISHED_DEST_CAS_LIB",
						name="&RM_PUBLISHED_DEST_CAS_TABLE_NM",
						where="ModelName in (&m_all_pub_nm)"   /* i18NOK:LINE */
						};
						end;
						run;
					quit;
			
					proc fedsql sessref=load_vdmml_mdl;
					create table &RM_PUBLISHED_DEST_CAS_LIB..&RM_PUBLISHED_DEST_CAS_TABLE_NM._TEMP  {options replace=true} as 
					select * from &RM_PUBLISHED_DEST_CAS_LIB..&RM_PUBLISHED_DEST_CAS_TABLE_NM.
					union 
					select * from &DABT_MODELING_ABT_LIBREF..SRC_RM_PUB_CAS_TBL
					;
					quit;
					
					%dabt_drop_table(m_table_nm=&RM_PUBLISHED_DEST_CAS_LIB..&RM_PUBLISHED_DEST_CAS_TABLE_NM., m_cas_flg=Y);
					
					proc fedsql sessref=load_vdmml_mdl;
					create table &RM_PUBLISHED_DEST_CAS_LIB..&RM_PUBLISHED_DEST_CAS_TABLE_NM.  {options replace=true}
					as select * from &RM_PUBLISHED_DEST_CAS_LIB..&RM_PUBLISHED_DEST_CAS_TABLE_NM._TEMP;
					quit;
					
					%dabt_promote_table_to_cas(input_caslib_nm =&RM_PUBLISHED_DEST_CAS_LIB.,input_table_nm =&RM_PUBLISHED_DEST_CAS_TABLE_NM.);
									
					proc cas;
						table.save / caslib="&RM_PUBLISHED_DEST_CAS_LIB." name="&RM_PUBLISHED_DEST_CAS_TABLE_NM." replace=True 
							table={caslib="&RM_PUBLISHED_DEST_CAS_LIB." name="&RM_PUBLISHED_DEST_CAS_TABLE_NM."};
					quit;
				%end;
				%else %if &m_hdt_exists. eq N %then %do;
					proc fedsql sessref=load_vdmml_mdl;
					create table &RM_PUBLISHED_DEST_CAS_LIB..&RM_PUBLISHED_DEST_CAS_TABLE_NM.  {options replace=true}
					as select * from &DABT_MODELING_ABT_LIBREF..SRC_RM_PUB_CAS_TBL;
					quit;
					
					%dabt_promote_table_to_cas(input_caslib_nm =&RM_PUBLISHED_DEST_CAS_LIB.,input_table_nm =&RM_PUBLISHED_DEST_CAS_TABLE_NM.);
									
					proc cas;
						table.save / caslib="&RM_PUBLISHED_DEST_CAS_LIB." name="&RM_PUBLISHED_DEST_CAS_TABLE_NM." replace=True 
							table={caslib="&RM_PUBLISHED_DEST_CAS_LIB." name="&RM_PUBLISHED_DEST_CAS_TABLE_NM."};
					quit;
						
				%end;
			%end;  
			
			%dabt_terminate_cas_session(cas_session_ref=load_vdmml_mdl);
		%end;

	%end;

%mend dabt_cprm_import_vdmml;