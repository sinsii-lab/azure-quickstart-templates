%macro rmcr_csb_cr_alt_seq_apdm(m_lib_apdm=);

/*
%let apdm_connect_string = connect to POSTGRES( database=SharedServices user=dbmsowner password="Go4thsas" PORT=5432 server="localhost" );
%let apdm_disconnect_string = disconnect from POSTGRES;
%let apdm_pass_through_start = execute%str(%();
%let apdm_pass_through_end = %str(%))by Postgres;
%let access_apdm = riskmodelingcore.;
*/
%let access_apdm=&apdm_schema..; 

%let rec_cnt = 0;

proc sql noprint;
select max(actual_result_control_dtl_sk) into :rec_cnt from &m_lib_apdm..actual_result_control_detail;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.actual_result_control_detail_actual_result_control_dtl_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(event_sk) into :rec_cnt from &m_lib_apdm..event_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.event_master_event_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(level_sk) into :rec_cnt from &m_lib_apdm..level_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.level_master_level_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(library_sk) into :rec_cnt from &m_lib_apdm..library_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.library_master_library_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(model_sk) into :rec_cnt from &m_lib_apdm..model_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.model_master_model_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(model_output_column_sk) into :rec_cnt from &m_lib_apdm..model_output_column;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.model_output_column_model_output_column_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(model_segment_sk) into :rec_cnt from &m_lib_apdm..model_segment_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.model_segment_master_model_segment_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(abt_sk) into :rec_cnt from &m_lib_apdm..modeling_abt_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.modeling_abt_master_abt_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(project_sk) into :rec_cnt from &m_lib_apdm..project_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.project_master_project_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(purpose_sk) into :rec_cnt from &m_lib_apdm..purpose_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.purpose_master_purpose_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(scoring_control_detail_sk) into :rec_cnt from &m_lib_apdm..scoring_control_detail;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.scoring_control_detail_scoring_control_detail_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(scoring_model_sk) into :rec_cnt from &m_lib_apdm..scoring_model;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.scoring_model_scoring_model_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(scoring_template_sk) into :rec_cnt from &m_lib_apdm..scoring_template_detail;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.scoring_template_detail_scoring_template_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(as_of_time_sk) into :rec_cnt from &m_lib_apdm..source_as_of_time_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.source_as_of_time_master_as_of_time_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(source_column_sk) into :rec_cnt from &m_lib_apdm..source_column_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.source_column_master_source_column_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(dim_attribute_value_sk) into :rec_cnt from &m_lib_apdm..source_dim_attrib_value_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.source_dim_attrib_value_master_dim_attribute_value_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(source_table_sk) into :rec_cnt from &m_lib_apdm..source_table_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.source_table_master_source_table_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(time_period_sk) into :rec_cnt from &m_lib_apdm..source_time_period;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.source_time_period_time_period_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(subject_group_sk) into :rec_cnt from &m_lib_apdm..subject_group_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.subject_group_master_subject_group_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(subset_from_path_sk) into :rec_cnt from &m_lib_apdm..subset_from_path_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.subset_from_path_master_subset_from_path_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(target_query_sk) into :rec_cnt from &m_lib_apdm..target_query_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.target_query_master_target_query_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;




%let rec_cnt = 0;

proc sql noprint;
select max(target_node_sk) into :rec_cnt from &m_lib_apdm..TARGET_NODE;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.TARGET_NODE_TARGET_NODE_SK_SEQ RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;

%let rec_cnt = 0;

proc sql noprint;
select max(subset_table_join_condition_sk) into :rec_cnt from &m_lib_apdm..subset_table_join_condition;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.subset_table_join_condition_subset_table_join_condition_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(variable_sk) into :rec_cnt from &m_lib_apdm..variable_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.variable_master_variable_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(external_code_sk) into :rec_cnt from &m_lib_apdm..external_code_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.external_code_master_external_code_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(external_variable_sk) into :rec_cnt from &m_lib_apdm..external_variable_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.external_variable_master_external_variable_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(bin_chrstc_sk) into :rec_cnt from &m_lib_apdm..bin_characteristic;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.bin_characteristic_bin_chrstc_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(scorecard_bin_sk) into :rec_cnt from &m_lib_apdm..scorecard_bin;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.scorecard_bin_scorecard_bin_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(bin_analysis_scheme_sk) into :rec_cnt from &m_lib_apdm..bin_analysis_scheme_defn;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),10000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.bin_analysis_scheme_defn_bin_analysis_scheme_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(bin_scheme_bnng_attrb_sk) into :rec_cnt from &m_lib_apdm..bin_scheme_bnng_attrb_defn;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),10000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.bin_scheme_bnng_attrb_defn_bin_scheme_bnng_attrb_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(bin_scheme_bin_chrstc_sk) into :rec_cnt from &m_lib_apdm..bin_scheme_bin_chrstc_defn;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),10000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.bin_scheme_bin_chrstc_defn_bin_scheme_bin_chrstc_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(bin_specification_sk) into :rec_cnt from &m_lib_apdm..bin_specification;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.bin_specification_bin_specification_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(bnng_attrb_distinct_value_sk) into :rec_cnt from &m_lib_apdm..bnng_attrb_distinct_value;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.bnng_attrb_distinct_value_bnng_attrb_distinct_value_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(scrcrd_bin_grp_sk) into :rec_cnt from &m_lib_apdm..scorecard_bin_group;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.scorecard_bin_group_scrcrd_bin_grp_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(level_key_column_dtl_sk) into :rec_cnt from &m_lib_apdm..level_key_column_dtl;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.level_key_column_dtl_level_key_column_dtl_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(filter_query_sk) into :rec_cnt from &m_lib_apdm..filter_query_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.filter_query_master_filter_query_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(abt_no) into :rec_cnt from &m_lib_apdm..build_backtest_abt_status;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.build_backtest_abt_status_abt_no_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;



%let rec_cnt = 0;

proc sql noprint;
select max(export_format_sk) into :rec_cnt from &m_lib_apdm..export_format_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.export_format_master_export_format_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(export_template_sk) into :rec_cnt from &m_lib_apdm..export_template_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),3 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.export_template_master_export_template_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(export_profile_sk) into :rec_cnt from &m_lib_apdm..export_profile_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.export_profile_master_export_profile_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


/*
%let rec_cnt = 0;

proc sql noprint;
select max(range_scheme_type_sk) into :rec_cnt from &m_lib_apdm..range_scheme_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.range_scheme_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;



%let rec_cnt = 0;

proc sql noprint;
select max(range_scheme_sk) into :rec_cnt from &m_lib_apdm..model_range_scheme;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.range_scheme_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(model_type_sk) into :rec_cnt from &m_lib_apdm..model_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.model_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(report_category_sk) into :rec_cnt from &m_lib_apdm..mm_report_category_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.report_category_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(report_type_sk) into :rec_cnt from &m_lib_apdm..mm_report_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.report_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(data_grouping_type_sk) into :rec_cnt from &m_lib_apdm..mm_data_grouping_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.data_grouping_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(report_subtype_sk) into :rec_cnt from &m_lib_apdm..mm_report_subtype_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.report_subtype_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(measure_sk) into :rec_cnt from &m_lib_apdm..measure_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.measure_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(model_mining_type_sk) into :rec_cnt from &m_lib_apdm..model_mining_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.mining_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(model_target_type_sk) into :rec_cnt from &m_lib_apdm..model_target_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.model_target_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(report_category_map_sk) into :rec_cnt from &m_lib_apdm..mm_report_category_map;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.report_category_map_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(report_type_map_sk) into :rec_cnt from &m_lib_apdm..mm_report_type_map;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.report_type_map_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(data_grouping_type_map_sk) into :rec_cnt from &m_lib_apdm..mm_data_grouping_type_map;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.data_grouping_type_map_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(report_specification_map_sk) into :rec_cnt from &m_lib_apdm..report_specification_map;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.report_specification_map_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(measure_map_sk) into :rec_cnt from &m_lib_apdm..measure_map;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.measure_map_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(measure_report_spec_map_sk) into :rec_cnt from &m_lib_apdm..measure_report_spec_map;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.measure_report_spec_map_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;

*/

%let rec_cnt = 0;

proc sql noprint;
select max(alignment_type_sk) into :rec_cnt from &m_lib_apdm..axis_alignment_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.alignment_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;

/*

%let rec_cnt = 0;

proc sql noprint;
select max(display_usage_type_sk) into :rec_cnt from &m_lib_apdm..column_display_usage_type;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.display_usage_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(measure_config_sk) into :rec_cnt from &m_lib_apdm..measure_config_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.measure_config_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(data_config_sk) into :rec_cnt from &m_lib_apdm..measure_data_config;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.data_config_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(data_config_column_sk) into :rec_cnt from &m_lib_apdm..measure_data_config_col_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.data_config_column_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(data_config_type_sk) into :rec_cnt from &m_lib_apdm..measure_data_config_type;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.data_config_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(graph_sk) into :rec_cnt from &m_lib_apdm..measure_graph_config;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.graph_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(graph_group_sk) into :rec_cnt from &m_lib_apdm..measure_graph_group_config;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.graph_group_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(graph_layout_sk) into :rec_cnt from &m_lib_apdm..measure_graph_layout;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.graph_layout_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(graph_layout_type_sk) into :rec_cnt from &m_lib_apdm..measure_graph_layout_type;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.graph_layout_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(layout_axis_sk) into :rec_cnt from &m_lib_apdm..measure_layout_axis;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.layout_axis_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(renderer_type_sk) into :rec_cnt from &m_lib_apdm..renderer_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.renderer_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(graph_axis_sk) into :rec_cnt from &m_lib_apdm..measure_graph_axis;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.graph_axis_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;
*/

%let rec_cnt = 0;

proc sql noprint;
select max(axis_type_sk) into :rec_cnt from &m_lib_apdm..axis_type_master;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.axis_type_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(abt_no) into :rec_cnt from &m_lib_apdm..build_pool_abt_status;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.build_pool_abt_status_abt_no_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(master_scale_no) into :rec_cnt from &m_lib_apdm..master_scale_defn;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.master_scale_defn_master_scale_no_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(ms_detail_no) into :rec_cnt from &m_lib_apdm..master_scale_detail;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.master_scale_detail_ms_detail_no_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(pool_sk) into :rec_cnt from &m_lib_apdm..pool_dim;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.pool_dim_pool_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(pool_scheme_sk) into :rec_cnt from &m_lib_apdm..pool_scheme;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.pool_scheme_pool_scheme_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(pool_scheme_no) into :rec_cnt from &m_lib_apdm..pool_scheme_defn;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),100 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.pool_scheme_defn_pool_scheme_no_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(pool_scheme_chrstc_sk) into :rec_cnt from &m_lib_apdm..pool_scheme_chrstc;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.pool_scheme_chrstc_pool_scheme_chrstc_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


/*
%let rec_cnt = 0;

proc sql noprint;
select max(pool_attrib_sk) into :rec_cnt from &m_lib_apdm..pool_scheme_bin_chrstc_attrib;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.pool_scheme_bin_chrstc_attrib_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;
*/

%let rec_cnt = 0;

proc sql noprint;
select max(pool_node_sk) into :rec_cnt from &m_lib_apdm..pool_scheme_node_plot;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.pool_scheme_node_plot_pool_node_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;


%let rec_cnt = 0;

proc sql noprint;
select max(pool_sk) into :rec_cnt from &m_lib_apdm..pool_detail;
quit;

%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),1000 ));

proc sql noprint;
   &apdm_connect_string.;
&apdm_pass_through_start. ALTER SEQUENCE &access_apdm.pool_detail_pool_sk_seq RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;





%let rec_cnt = 0;
proc sql noprint;
   select max(MODEL_STAGING_SK) into :rec_cnt from &m_lib_apdm..MODEL_MASTER_STAGING;
quit;
%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),0));

proc sql noprint;
   &apdm_connect_string.;
       &apdm_pass_through_start. ALTER SEQUENCE &access_apdm.MODEL_MASTER_STAGING_MODEL_STAGING_SK_SEQ RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;

%let rec_cnt = 0;
proc sql noprint;
   select max(JOB_SK) into :rec_cnt from &m_lib_apdm..JOB_MASTER;
quit;
%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),0));

proc sql noprint;
   &apdm_connect_string.;
       &apdm_pass_through_start. ALTER SEQUENCE &access_apdm.JOB_MASTER_JOB_SK_SEQ RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;

%let rec_cnt = 0;
proc sql noprint;
   select max(RUN_SK) into :rec_cnt from &m_lib_apdm..MM_RUN;
quit;
%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),0));

proc sql noprint;
   &apdm_connect_string.;
       &apdm_pass_through_start. ALTER SEQUENCE &access_apdm.MM_RUN_RUN_SK_SEQ RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;

/*
%let rec_cnt = 0;
proc sql noprint;
   select max(REPORT_SPECIFICATION_SK) into :rec_cnt from &m_lib_apdm..MM_REPORT_SPECIFICATION;
quit;
%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),0));

proc sql noprint;
   &apdm_connect_string.;
       &apdm_pass_through_start. ALTER SEQUENCE &access_apdm.MM_REPORT_SPECIFICATION_REPORT_SPECIFICATION_SK RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;
*/

%let rec_cnt = 0;
proc sql noprint;
   select max(SCORE_CARD_SK) into :rec_cnt from &m_lib_apdm..SCORE_CARD_DIM;
quit;
%let rec_cnt = %sysfunc(coalesce(&rec_cnt,0));
%let seq_cnt = %sysfunc(max(%eval(&rec_cnt+1),0));

proc sql noprint;
   &apdm_connect_string.;
       &apdm_pass_through_start. ALTER SEQUENCE &access_apdm.SCORE_CARD_DIM_SCORE_CARD_SK_SEQ RESTART WITH &seq_cnt &apdm_pass_through_end. ;
   &apdm_disconnect_string.;
quit;



%mend rmcr_csb_cr_alt_seq_apdm;


