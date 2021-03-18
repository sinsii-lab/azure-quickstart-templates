/********************************************************************************************************
   	Module		:  rmcr_solution_data_prmtn

   	Function	:  This macro will be used to import RM solution data to cas libraries.

	Called-by	:  None
	Calls		:  None
					
   	Parameters	:  None
	
	Author		:  CSB Team 
				   
*********************************************************************************************************/

%macro rmcr_solution_data_prmtn(inlib=,outlib=);

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;

%let solution_xpt_path=&m_staging_area_path/cs63_export_pkg/solution_data;

%let path_to_unzip_solution_data_xpt=;
%dabt_make_work_area(dir=&solution_xpt_path, create_dir=unzip, path=path_to_unzip_solution_data_xpt);

%let path_to_unzip_solution_data_xpt=&path_to_unzip_solution_data_xpt;

libname unzip "&path_to_unzip_solution_data_xpt";
proc cimport infile="&solution_xpt_path/&inlib..xpt" library=unzip;     /* i18nOK:LINE */
run;

%dabt_initiate_cas_session(cas_session_ref=load_solution_data_cas);

caslib sol_data datasource=(srctype="path") path="&path_to_unzip_solution_data_xpt";    /* i18nOK:LINE */

proc sql noprint;
			create table files as 
				select memname as name from dictionary.tables
				where kupcase(libname) eq "UNZIP" ;      /* i18nOK:LINE */
		quit;
		
%local m_cnt m_name ;
	%let m_cnt = 0;
	proc sql noprint;
		select count(*) , name  /* i18nOK:Line */
			into :m_cnt, 
				:m_name separated by '~'  /* i18nOK:Line */
		from files;
	quit;
		
	%let m_name = &m_name.;
	%let m_cnt = &m_cnt.;
	
%if &m_cnt. gt 0 %then %do;
	%do i=1 %to &m_cnt;
		%let current_nm = %scan(&m_name,&i.,~);      /* i18nOK:LINE */
		%let op_nm = %scan(&current_nm,1);     /* i18nOK:LINE */
		
		%let etls_tableExist = %eval(%sysfunc(exist(&outlib..&current_nm., DATA)));
		
		%if &etls_tableExist. eq 0 %then %do;
		
			proc casutil ; 
				load casdata="&current_nm..sas7bdat" incaslib="sol_data"     /* i18nOK:LINE */
				casout="&current_nm." outcaslib="&outlib."  
				importoptions=(filetype="basesas" dataTransferMode="parallel") promote  ;     /* i18nOK:LINE */
			quit ; 
	
			%dabt_save_cas_table(m_in_cas_lib_ref=&outlib., m_in_cas_table_nm=&current_nm.);
			
		%end;
		
		
	%end;
%end;

proc datasets lib=unzip
		nolist kill;
		quit;
		run;

caslib sol_data drop;
libname unzip clear;

%dabt_terminate_cas_session(cas_session_ref=load_solution_data_cas);

%mend rmcr_solution_data_prmtn;


