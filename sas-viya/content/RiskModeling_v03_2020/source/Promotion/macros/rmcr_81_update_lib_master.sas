%macro rmcr_81_update_lib_master()/minoperator;


data library_mapping;
infile "&m_staging_area_path./cs63_export_pkg/source_data/library_mapping.csv" dlm=',' dsd firstobs=2;   /* i18nOK:LINE */
input FROM_LIB $ TO_LIB $;
run;

%let bnk_cnt_appl=;
proc sql;
		select count(*) into :bnk_cnt_appl from &lib_apdm..CS_CR_INFO   /* i18nOK:LINE */
		where cr_unique_cd='BANKING_BASEL' and status_sk=1 ;   /* i18nOK:LINE */
quit;
%let bnk_cnt_appl=&bnk_cnt_appl;

%if &bnk_cnt_appl ge 1 %then %do;
	proc sql; 
	update library_mapping set TO_LIB="BANKCRFM"    /* i18nOK:LINE */
	where kupcase(FROM_LIB) in ('CSB_IM','BNKF_IM','DETAIL','BRIDGE','DIM','INPBASE');    /* i18nOK:LINE */
	quit;
%end;

proc sql;
select * from library_mapping;
quit;

%let lib_cnt=&sqlobs;

%if &lib_cnt ge 1 %then %do;

	%do i=1 %to &lib_cnt;
		data _null_;
		obs=&i;
		set library_mapping point=obs;
		call symput('m_from_lib',FROM_LIB);    /* i18nOK:LINE */
		call symput('m_to_lib',TO_LIB);    /* i18nOK:LINE */
		stop;
		run;
		
		
		%let m_from_lib=%kupcase(&m_from_lib);
		%let m_to_lib=%kupcase(&m_to_lib);
		
		%let m_lib_ref_list=;
		proc sql;
		select distinct kupcase(library_reference) into :m_lib_ref_list separated by ' '     /* i18nOK:LINE */
		from &lib_apdm..library_master;
		quit;


		%if not(&m_to_lib in &m_lib_ref_list) %then %do;
			 /************************************************
			 insert in library master
			 ************************************************/
			 proc sql;
				insert into &lib_apdm..library_master(library_reference,library_type_cd ,library_short_nm,created_dttm,created_by_user,last_processed_dttm,last_processed_by_user)
				values("&m_to_lib",'SRC_TBL',"&m_to_lib","%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid", "%sysfunc(datetime(),DATETIME.)"dt, "&sysuserid");   /* i18nOK:LINE */
			 quit;

		%end;	
 
			 proc sql;
				update &lib_apdm..source_table_master src_tbl_mstr set library_sk = (select library_sk from &lib_apdm..library_master lib_mstr where kupcase(library_reference)="&m_to_lib")
				where src_tbl_mstr.library_sk=(select library_sk from &lib_apdm..library_master lib_mstr where kupcase(library_reference)="&m_from_lib");
			 quit;
			 
			 proc sql;
				delete from &lib_apdm..library_master where kupcase(library_reference)="&m_from_lib";
			 quit;
		
	%end;
%end;
%mend rmcr_81_update_lib_master;