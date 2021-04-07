/********************************************************************************************************
   	Module		:  rmcr_63_solution_data_export

   	Function	:  This macro will be used to create export package for solution data from 63 to 81.

	Called-by	:  NONE
	Calls		:  	NONE
					
   	Parameters	:  None
	Author		:   RM Team
	
	Sample call :
					%let m_staging_area_path=/tmp;
					%let m_source_code_path=/tmp/source_code;
					%include "&m_source_code_path/rmcr_63_solution_data_export.sas";
					%CSBINIT;
					%rmcr_63_solution_data_export();
				   
*********************************************************************************************************/


	%macro rmcr_63_solution_data_export();
	
		%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
		%end;
		%if %symexist(sqlrc)=0 %then %do;
			%global sqlrc;
		%end;
		
		/****** Creating macro variable m_staging_area which holds the staging path ******/
		
		%let m_staging_area=&m_staging_area_path;
		
		/****** Creating a timestamp macro variable that will be used for logs and txt files ******/	
		data _null_;   
		   call symputx('timestamp', put(datetime(),datetime20.));    /* i18nOK:Line */
		run;
		%let timestamp=%sysfunc(ktranslate(&timestamp,'.',':'));   /* i18nOK:Line */
		
		/****** path_to_migration is a macro variable that holds the path of folder cs63_export_pkg 
			under which all folders will be made ******/	
		%let path_to_migration=;
		%dabt_make_work_area(dir=&m_staging_area, create_dir=cs63_export_pkg, path=path_to_migration);
		%let path_to_migration=&path_to_migration;
		
		/****** path_to_log is a macro variable that holds the path of log folder under cs63_export_pkg log ******/	
		%let path_to_log=;
		%dabt_make_work_area(dir=&path_to_migration, create_dir=log, path=path_to_log);
		%let path_to_log=&path_to_log;

		/****** Divert log to log folder ******/
		proc printto log="&path_to_log/rmcr_63_solution_data_export_&timestamp..log";     /* i18nOK:Line */
		run;
		
		/****** Create directory for solution data xpt -starts ******/		
		%let path_to_solution_data_xpt=;
		%dabt_make_work_area(dir=&path_to_migration, create_dir=solution_data, path=path_to_solution_data_xpt);
		%let path_to_solution_data_xpt=&path_to_solution_data_xpt;
		/****** Create directory for solution data xpt -ends ******/

		/****** Create xpts at the solution data path -starts ******/
		proc cport lib=cs_mdl file="&path_to_solution_data_xpt./cs_mdl.xpt"; run;    /* i18nOK:Line */
		%let job_rc=&syscc;

		proc cport lib=cs_scrng file="&path_to_solution_data_xpt./cs_scrng.xpt"; run;    /* i18nOK:Line */
		%let job_rc=&syscc;

		proc cport lib=cs_act file="&path_to_solution_data_xpt./cs_act.xpt"; run;    /* i18nOK:Line */
		%let job_rc=&syscc;

		proc cport lib=cs_bck file="&path_to_solution_data_xpt./cs_bck.xpt"; run;    /* i18nOK:Line */
		%let job_rc=&syscc;

		proc cport lib=arm file="&path_to_solution_data_xpt./arm.xpt"; run;     /* i18nOK:Line */
		%let job_rc=&syscc;

		proc cport lib=csbfact file="&path_to_solution_data_xpt./csbfact.xpt"; run;     /* i18nOK:Line */
		%let job_rc=&syscc;

		proc cport lib=csbdim file="&path_to_solution_data_xpt./csbdim.xpt"; run;    /* i18nOK:Line */
		%let job_rc=&syscc;
		/****** Create xpts at the solution data path -ends ******/

	%mend rmcr_63_solution_data_export;
	
