%macro csbmva_get_subset_appl_list(m_level_cd = , m_out_lib = , m_out_ds = , m_key_clmn = , m_as_of_dttm = );

	%let m_level_cd = &m_level_cd.;
	%let m_out_lib = &m_out_lib.;
	%let m_out_ds = &m_out_ds.;
	%let m_key_clmn = &m_key_clmn.;
	%let m_valid_to_dttm = &m_as_of_dttm.;
	
	%if &m_level_cd = P %then %do; /*If the model is of type retail application*/
	
		proc fedsql sessref=create_binary_feed_from_fact;
			create table &m_out_lib..&m_out_ds.{options replace=true} as
				select application_rk as  &m_key_clmn.
				from bankcrfm.application_dim 
						where kupcase(kcompress(outcome_cd)) = &OUTCOME_ACCEPT. and 
							valid_end_dttm = &m_valid_to_dttm.;
		quit;
	%end;
	%else %if &m_level_cd = Q %then %do; /*If the model is of type corporate application*/
	
		proc fedsql sessref=create_binary_feed_from_fact;
			create table &m_out_lib..&m_out_ds.{options replace=true} as
				select application_rk as  &m_key_clmn.
				from bankcrfm.application_dim 
						where kupcase(kcompress(outcome_cd)) = &OUTCOME_ACCEPT. and 
							valid_end_dttm = &m_valid_to_dttm.;
		quit;
	%end;
	
%mend csbmva_get_subset_appl_list; 


