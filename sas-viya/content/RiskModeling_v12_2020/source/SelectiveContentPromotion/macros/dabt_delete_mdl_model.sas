/**********************************************************************************************

Module         : dabt_delete_mdl_model.sas
Function       : This macro cleans up model attributes 

Parameters     : 	m_project_sk -> project sk under which model has to be deleted
					m_model_sk -> model sk for which clean up needs to be executed
					m_lst_prc_usr -> last processed user
                 	m_called_from -> can take values APPLICATION or CPRM or MDL_DEL
**********************************************************************************************/

%macro dabt_delete_mdl_model(m_project_sk=, m_model_sk=, m_lst_prc_usr=,m_called_from=APPLICATION) ;
	
	%let m_called_from = &m_called_from ;
	%let m_project_sk = &m_project_sk;
	%let m_model_sk = &m_model_sk;

	%local m_tmp_lib m_src_lib m_log_path m_tgt_rpt_spec_sk ;
	
	%dabt_assign_libs(tmp_lib=m_tmp_lib, src_lib=m_src_lib, 
						m_workspace_type = ADM);

	/* If invoked from APPLICATION or MDL_DEL/PRJ_DEL processes, deletion is done for the model. If invoked from CPRM, deletion is done only for data associated with MS */
	%if &m_called_from. eq APPLICATION %then %do;
		%if &m_model_sk eq %then %do;
			proc sql noprint;
				select put(model_sk,12.) into :m_model_sk separated by ',' /* i18nOK:Line */
				from &m_src_lib..model_master
				where project_sk = &m_project_sk.;
			quit;
			%let m_model_sk = &m_model_sk;
		%end;
	%end;
	
	%if &m_model_sk ne %then %do;	/* model is valid */
	
			/* CPRM CSB-23670: Extract TGT report specification sk */
			proc sql noprint;
				select MMRS.report_specification_sk into :m_tgt_rpt_spec_sk separated by ','  /* i18nOK:Line */
				from &m_src_lib..mm_report_specification MMRS
				
				%if &m_called_from = CPRM %then %do;	/* CPRM only deletes and re-inserts rows for MS, not for versions */
					inner join &m_src_lib..mm_rpt_spec_type_master  MRST
						on MMRS.report_specification_type_sk = MRST.report_specification_type_sk
						and MRST.report_specification_type_cd = 'MS'            /* i18nOK:Line */
				%end;		
					where model_sk = &m_model_sk. ;
			quit;
			%let m_tgt_rpt_spec_sk = &m_tgt_rpt_spec_sk.;
		
			%if &m_tgt_rpt_spec_sk eq %then %do;	/* version not created in MMRS properly, cannot proceed with further clean-up */
				%put NOTE: Error in getting REPORT_SPECIFICATION_SK from MM_REPORT_SPECIFICATION table. Proceeding to delete Model level details ;
			%end;
			
			%else %do; /* at least one version including MS exists, continue with deleting */
					/* Get sk lists for child tables */
					
					%let m_scrcrd_grp_sk_lst = ;
					%let m_bin_scheme_sk_lst = ;
					%let m_scrcrd_chrstc_lst = ;
					
					proc sql noprint;
						select put(scrcrd_bin_grp_sk,12.) into :m_scrcrd_grp_sk_lst separated by ',' /* i18nOK:Line */
							from &m_src_lib..scorecard_bin_group
						where model_sk in (&m_model_sk.);
						
						select put(bin_analysis_scheme_sk,12.) into :m_bin_scheme_sk_lst separated by ',' /* i18nOK:Line */
						from &m_src_lib..report_spec_x_bin_scheme
						where report_specification_sk in (&m_tgt_rpt_spec_sk.); 
									
						select put(bin_chrstc_sk,12.) into :m_scrcrd_chrstc_lst separated by ',' /* i18nOK:Line */
						from &m_src_lib..model_x_scorecard_chrstc
						where model_sk in (&m_model_sk.);
					quit;
					
					%let m_scrcrd_grp_sk_lst = &m_scrcrd_grp_sk_lst;
					%let m_bin_scheme_sk_lst = &m_bin_scheme_sk_lst;
					%let m_scrcrd_chrstc_lst = &m_scrcrd_chrstc_lst;
		
					/* Delete bin schemes and characteristic value */
		
									%let m_bin_scheme_chrstc_sk_lst = ;
									%if "&m_bin_scheme_sk_lst" ne "" %then %do;  								/* bin schemes exist *//* i18nOK:Line */
										%let m_bin_attrb_sk_lst = ;
										%let m_bin_spec_sk_lst = ;
										proc sql noprint;
											select put(bin_chrstc_sk,12.) into :m_bin_scheme_chrstc_sk_lst separated by ',' /* i18nOK:Line */
											from &m_src_lib..bin_scheme_bin_chrstc_defn
											where bin_analysis_scheme_sk in (&m_bin_scheme_sk_lst.);
											
											select put(bin_scheme_bnng_attrb_sk,12.) into :m_bin_attrb_sk_lst separated by ',' /* i18nOK:Line */
											from &m_src_lib..bin_scheme_bnng_attrb_defn
											where bin_analysis_scheme_sk in (&m_bin_scheme_sk_lst.);
											
											select put(bin_specification_sk,12.) into :m_bin_spec_sk_lst separated by ',' /* i18nOK:Line */
											from &m_src_lib..bin_specification
											where bin_analysis_scheme_sk in (&m_bin_scheme_sk_lst.);
										quit;
										%let m_bin_scheme_chrstc_sk_lst = &m_bin_scheme_chrstc_sk_lst;
										%let m_bin_attrb_sk_lst = &m_bin_attrb_sk_lst;
										%let m_bin_spec_sk_lst = &m_bin_spec_sk_lst;
									
									
												%if "&m_bin_spec_sk_lst" ne "" %then %do; /* i18nOK:Line */
													proc sql noprint;
														delete from &m_src_lib..bin_spec_attrb_dstnct_value
														where bin_specification_sk in (&m_bin_spec_sk_lst);
																			
														delete from &m_src_lib..bin_chrstc_value
														where bin_specification_sk in (&m_bin_spec_sk_lst);
														
														delete from &m_src_lib..bin_specification
														where bin_specification_sk in (&m_bin_spec_sk_lst);
													quit;
												%end;
								
												%if "&m_bin_attrb_sk_lst" ne "" %then %do; /* i18nOK:Line */
													proc sql noprint;
														delete from &m_src_lib..bnng_attrb_distinct_value
														where bin_scheme_bnng_attrb_sk in (&m_bin_attrb_sk_lst);
														
														delete from &m_src_lib..bin_scheme_bnng_attrb_defn
														where bin_scheme_bnng_attrb_sk in (&m_bin_attrb_sk_lst);
													quit;
												%end;
								
										proc sql noprint;
											delete from &m_src_lib..bin_scheme_bin_chrstc_defn
											where bin_analysis_scheme_sk in (&m_bin_scheme_sk_lst.);
											
											delete from &m_src_lib..report_spec_model_param						/* entries exist only for versions */
											where report_specification_sk in (&m_tgt_rpt_spec_sk.);
											
											delete from &m_src_lib..report_spec_x_bin_scheme
											where report_specification_sk in (&m_tgt_rpt_spec_sk.);
											
											delete from &m_src_lib..last_bin_chrstc_specify_dtl
											where report_specification_sk in (&m_tgt_rpt_spec_sk.);
													
											delete from &m_src_lib..bin_analysis_scheme_defn
											where bin_analysis_scheme_sk in (&m_bin_scheme_sk_lst.);
										quit;
									%end;	/* bin schemes exist */

				
									/* Delete scorecard group and characteristic value */
									%if "&m_scrcrd_grp_sk_lst" ne "" %then %do; 	/* i18nOK:Line */
										proc sql noprint;
											delete from &m_src_lib..scorecard_bin_chrstc_value
											where scrcrd_bin_grp_sk in (&m_scrcrd_grp_sk_lst);

											delete from &m_src_lib..scorecard_bin
											where scrcrd_bin_grp_sk in (&m_scrcrd_grp_sk_lst);

											delete from &m_src_lib..scorecard_bin_group
											where model_sk in (&m_model_sk);
											
											delete from &m_src_lib..MODEL_BIN_INFO_STAGING
											where model_sk in (&m_model_sk);
											
											delete from &m_src_lib..app_scr_scorecard_info
											where model_sk in (&m_model_sk);
										quit;
									%end; 
		
									/* Delete characteristics created for bins and scorecard */
									%let m_chrstc_sk_lst = ;
									%if "&m_bin_scheme_chrstc_sk_lst" ne "" %then %do; /* i18nOK:Line */
										%let m_chrstc_sk_lst = &m_bin_scheme_chrstc_sk_lst;
										%if "&m_scrcrd_chrstc_lst" ne "" %then %do; /* i18nOK:Line */
											%let m_chrstc_sk_lst = &m_bin_scheme_chrstc_sk_lst, &m_scrcrd_chrstc_lst;
										%end;
									%end;
		
									%else %if "&m_scrcrd_chrstc_lst" ne "" %then %do; /* i18nOK:Line */
										%let m_chrstc_sk_lst = &m_scrcrd_chrstc_lst;
									%end;
		
									%if "&m_chrstc_sk_lst" ne "" %then %do; /* i18nOK:Line */
										proc sql noprint;
											delete from &m_src_lib..model_x_scorecard_chrstc
											where model_sk in (&m_model_sk);
								
											delete from &m_src_lib..last_scrcrd_chrstc_specify_dtl
											where report_specification_sk in (&m_tgt_rpt_spec_sk.);

											delete from &m_src_lib..bin_chrstc_aggregation_dtl
											where bin_chrstc_sk in (&m_chrstc_sk_lst);
											
											delete from &m_src_lib..bin_chrstc_filter_dtl
											where bin_chrstc_sk in (&m_chrstc_sk_lst);
											
											delete from &m_src_lib..bin_characteristic
											where bin_chrstc_sk in (&m_chrstc_sk_lst);
										quit;
									%end;
									
					%end; /* at least one version including MS exists */ 
		
				%if &m_called_from. eq APPLICATION or &m_called_from. eq MDL_DEL %then %do;
					/* Delete subset query, if any, associated with the model. */
					%dabt_delete_subset_query(m_project_sk=&m_project_sk, m_model_sk=%quote(&m_model_sk), m_called_for_process= &m_called_from , m_sub_qry_lst_prc_by_usr=&m_lst_prc_usr)  ;
				%end;
				
			*==================================================;
			* Deletion from  apdm extension tables ;
			*==================================================;
			
			proc sql noprint ;
				%if &m_called_from. eq APPLICATION or &m_called_from. eq MDL_DEL %then %do;
					delete * from &m_src_lib..model_x_modeling_abt
					where model_sk in (&m_model_sk.);
					
					delete * from &m_src_lib..model_segment_master
					where model_sk in (&m_model_sk.);
				%end;
			
					delete * from &m_src_lib..MODEL_OUTPUT_COLUMN
					where model_sk in (&m_model_sk.);
					
					delete * from &m_src_lib..MODEL_X_SCR_INPUT_VARIABLE
					where model_sk in (&m_model_sk.);
			
				%if &m_called_from. eq APPLICATION or &m_called_from. eq MDL_DEL %then %do;
					delete * from &m_src_lib..MODEL_X_ACT_OUTCOME_VAR
					where model_sk in (&m_model_sk.);
					
					delete * from &m_src_lib..MDL_EVL_LAST_RUN_AS_OF_DT_DTL
					where model_sk in (&m_model_sk.);
								
					/* Code added for CAC53 - Start */
					/* Deleting records for the MBA model */
					delete * from &m_src_lib..MODEL_RULE_MASTER
					where model_sk in (&m_model_sk.);
					
					delete * from &m_src_lib..MODEL_RULE_DTLS
					where model_sk in (&m_model_sk.);
					/* Code added for CAC53 - End */
				%end;
			quit;
			
			*==================================================;
			* Deletion from  apdm core tables ;
			*==================================================;
			%if &m_called_from. eq APPLICATION or &m_called_from. eq MDL_DEL %then %do;/*Start Don't delete scoring actual when called from CPRM*/
			
				%let md_cl_model_rk = &m_model_sk. ;
				%let m_scoring_template_sk = .;
				%let m_scoring_model_sk = .;
				proc sql noprint;
					select scoring_template_sk, scoring_model_sk into :m_scoring_template_sk, :m_scoring_model_sk
					from &m_src_lib..scoring_model
					where model_sk = &md_cl_model_rk.;
				quit;

				%let m_scoring_template_sk = &m_scoring_template_sk;
				%let m_scoring_model_sk = &m_scoring_model_sk;

				%if &m_scoring_template_sk ne . %then %do;
						proc sql noprint;
							update &m_src_lib..scoring_model
							set scoring_template_sk = .
							where model_sk = &md_cl_model_rk.;
						quit;

						proc sql noprint;
							delete from &m_src_lib..actual_result_control_detail
							where scoring_control_detail_sk in (select scoring_control_detail_sk from &m_src_lib..scoring_control_detail t1
																where t1.scoring_template_sk = &m_scoring_template_sk);

							delete from &m_src_lib..scoring_control_detail
							where scoring_template_sk = &m_scoring_template_sk;

							delete from &m_src_lib..scoring_template_detail
							where scoring_template_sk = &m_scoring_template_sk;
						quit;
				%end;

				%if &m_called_from ne MDL_DEL and &m_scoring_model_sk ne . %then %do;	/* for delete model, csbmva_scoring_load_cleanup.sas will take care of this part */
					%dabt_delete_scoring_model(m_scoring_model_sk=&m_scoring_model_sk);
				%end;
				
			%end;/*End for Don't delete scoring actual when called from CPRM*/
			
			/* Clear files and folders associated with model */ 
		
			%if &m_called_from. eq APPLICATION or &m_called_from. eq MDL_DEL %then %do;
		
					%let mdl_tot = %eval(%sysfunc(countc(%quote(&m_model_sk),","))+1); /* i18nOK:Line */
					%do mdl_cnt = 1 %to &mdl_tot;
						%let mdl_tkn = %scan(%quote(&m_model_sk),&mdl_cnt,%str(,)); /* i18nOK:Line */
						%let m_project_id = ;
						%let m_model_id = ;
						proc sql noprint;
							select project_id, model_id into :m_project_id, :m_model_id
							from &m_src_lib..model_master(where=(model_sk in (&mdl_tkn.))) as mm
									inner join &m_src_lib..project_master as pm
									on (mm.project_sk = pm.project_sk);
						quit;
						%let m_project_id = &m_project_id.;
						%let m_model_id = &m_model_id.;
						
						/* Delete the physical folder for the given model */
						/*SS: Changes Required- Need to remove this code...
						AM:Changes done*/
						/*%dabt_dir_delete(dirname=&project_path/&m_project_id./model/&m_model_id.);*/
						
						proc sql noprint;
							delete * from &m_src_lib..model_master
							where model_sk in (&mdl_tkn.);
						quit;
						
					%end;
	
					%let m_purpose_sk = .;
					proc sql noprint;
						select put(purpose_sk,12.) into :m_purpose_sk
						from &m_src_lib..project_master
						where project_sk = &m_project_sk.;
					quit;
					%let m_purpose_sk = &m_purpose_sk;

					%do mdl_cnt = 1 %to &mdl_tot;
						%let mdl_tkn = %scan(%quote(&m_model_sk),&mdl_cnt,%str(,)); /* i18nOK:Line */
						/* This macro variable will be assigned the value N, if the post action macro executes without error,
						 else will have value Y. */
						%let post_action_err_flg = ;
						%dabt_call_post_action_macro(m_action_cd=DEL_MDL, m_purpose_sk=&m_purpose_sk, m_model_sk=&mdl_tkn, m_post_action_flg=post_action_err_flg);
					%end;
			%end;
	%end;	/* model is valid */


%mend dabt_delete_mdl_model;
