/********************************************************************************************************
   Module:  dabt_reserve_sequence_values

   Function : This macro reserves sequences of an auto-increment table column of apdm.
			  It returns the starting sequence value of the reserved sequence block.

   Parameters: INPUT: 
				m_table_nm                    : table for which sequence needs to be reserved.
				m_no_of_values_to_reserve     : Number of sequences to reserve.

			   OUTPUT:
				m_out_starting_sequence_value : Starting sequence of the sequence block.

	Usage Example:
		%let m_begin_sequence = ;
		%let m_no_of_values_to_reserve = 5;
		%dabt_reserve_sequence_values(m_table_nm= library_master, 
									m_no_of_values_to_reserve= &m_no_of_values_to_reserve., 
									m_out_starting_sequence_value= m_begin_sequence);

		data work.library_master;
			set cprmapdm.library_master;
			if _n_ = 1 then do;
				target_sk + &m_begin_sequence. + &m_no_of_values_to_reserve.;
			end;
			else do;
				target_sk + 1;
			end;
		run;
*********************************************************************************************************/

%macro dabt_reserve_sequence_values(m_table_nm= , 
									m_no_of_values_to_reserve= , 
									m_out_starting_sequence_value= );


	%local apdm_schema_nm m_sequence_name m_next_prefix_val; 

	%let m_table_nm = &m_table_nm.;

	/*Explici pass-through requires apdm schema in single quotes. Below data step will do that.*/
	data _null_;
		apdm_schema_lower = klowcase("&apdm_schema");
		apdm_schema_quoted = "'"||apdm_schema_lower||"'";  /* i18nOK:Line */	
		call symput('apdm_schema_nm', apdm_schema_quoted);  /* i18nOK:Line */	
	run;

	/*This proc sql will create a table in work and contains table_name, sequence_name for all the 
	  tables of the apdm schema.*/
	proc sql noprint;

		&apdm_connect_string.; 
	 
		create table work.table_sequence_detail as select *       
		   from connection to postgres          
		     (select s.relname as sequence_name, n.nspname as schema_name, 
						t.relname as table_name, a.attname as column_name
				from pg_class s
				  join pg_depend d on d.objid=s.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass    /* i18nOK:Line */	
				  join pg_class t on t.oid=d.refobjid
				  join pg_namespace n on n.oid=t.relnamespace
				  join pg_attribute a on a.attrelid=t.oid and a.attnum=d.refobjsubid
				where s.relkind='S' and d.deptype='a'   /* i18nOK:Line */	
				and n.nspname=&apdm_schema_nm.);

		&apdm_disconnect_string.; 

	quit;

	/*Get the sequence name for the table name supplied as parameter.*/
	proc sql noprint;
		select sequence_name into :m_sequence_name 
			from work.table_sequence_detail 
			where table_name = klowcase("&m_table_nm.");
	quit;
	
	/*Harcoded sequence because sequence names of below tables were not found in above query*/
	%if "&m_sequence_name" = "" %then %do;
		%if &m_table_nm. eq bin_characteristic %then %do;
			%let m_sequence_name=bin_characteristic_bin_chrstc_sk_seq;
		%end;
		%else %if &m_table_nm. eq BIN_SCHEME_BIN_CHRSTC_DEFN %then %do;
			%let m_sequence_name=bin_scheme_bin_chrstc_defn_bin_scheme_bin_chrstc_sk_seq;
		%end;
		%else %if &m_table_nm. eq BIN_SCHEME_BNNG_ATTRB_DEFN %then %do;
			%let m_sequence_name=bin_scheme_bnng_attrb_defn_bin_scheme_bnng_attrb_sk_seq;
		%end;
		%else %if &m_table_nm. eq BNNG_ATTRB_DISTINCT_VALUE %then %do;
			%let m_sequence_name=bnng_attrb_distinct_value_bnng_attrb_distinct_value_sk_seq;
		%end;
		%else %if &m_table_nm. eq bin_analysis_scheme_defn %then %do;
			%let m_sequence_name=bin_analysis_scheme_defn_bin_analysis_scheme_sk_seq;
		%end;
	%end;

	%let m_next_prefix_val = .;
	proc sql;
		&apdm_connect_string.; 
			select 
				temp into :m_next_prefix_val
			from 
				connection to postgres 
				( 
					select nextval( %nrbquote('&apdm_schema..generic_seq_tbl_generic_seq_tbl_sk_seq') ) as temp
				);
		&apdm_disconnect_string.; 
	quit;

	%let m_next_prefix_val = &m_next_prefix_val.;

	/*Get the current sequence value. Add the m_no_of_values_to_reserve to the current value and set the 
	  sequence current value with this new derived value.*/
	  
	%if %sysfunc(exist(&lib_apdm.._&m_next_prefix_val._temp_seq)) %then %do;
		proc sql noprint;

			&apdm_connect_string.; 

				execute ( drop table if exists &apdm_schema.._&m_next_prefix_val._temp_seq) by postgres;

			&apdm_disconnect_string.; 
		quit;
	%end;
	
	proc sql noprint;

		&apdm_connect_string.; 

			execute (
						Begin ISOLATION LEVEL SERIALIZABLE;

							create table &apdm_schema.._&m_next_prefix_val._temp_seq as
								select last_value as sequence_start_val,
										( last_value + &m_no_of_values_to_reserve. + 1 ) as sequence_new_val
								from &apdm_schema..&m_sequence_name.;
					
							select setval( %nrbquote('&apdm_schema..&m_sequence_name.'),(select sequence_new_val from &apdm_schema.._&m_next_prefix_val._temp_seq) );

						commit

					) by postgres ;

			select sequence_start_val into :sequence_start_val
				from connection to postgres ( select sequence_start_val from &apdm_schema.._&m_next_prefix_val._temp_seq );

			execute ( drop table &apdm_schema.._&m_next_prefix_val._temp_seq ) by postgres;

		&apdm_disconnect_string.; 
	quit;

	/*Return the starting sequence block to the calling macro*/
	%if &m_out_starting_sequence_value. ne %then %do;
		%let &m_out_starting_sequence_value = %eval(&sequence_start_val. + 1) ;
	%end;

%mend dabt_reserve_sequence_values; 
