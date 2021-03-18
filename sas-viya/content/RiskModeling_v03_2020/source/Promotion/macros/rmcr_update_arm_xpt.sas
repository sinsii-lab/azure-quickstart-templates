%macro rmcr_update_arm_xpt();

%if %symexist(job_rc)=0 %then %do;
	%global job_rc;
%end;
%if %symexist(sqlrc)=0 %then %do;
	%global sqlrc;
%end;

%let solution_xpt_path=&m_staging_area_path/cs63_export_pkg/solution_data;

%let arm_chng_exist = %sysfunc(fileexist(&solution_xpt_path/arm_new.xpt));

%if &arm_chng_exist ne 1 %then %do;/*Make changes for rm_arm -start*/
	proc datasets lib=work
			nolist kill;
			quit;
			run;

	proc cimport infile="&solution_xpt_path/arm.xpt" library=work;   /* i18nOK:Line */
	run;
	/*create a folder named arm*/
	%let path_to_create_new_arm_xpt=;
	%dabt_make_work_area(dir=&solution_xpt_path, create_dir=arm, path=path_to_create_new_arm_xpt);

	%let path_to_create_new_arm_xpt=&path_to_create_new_arm_xpt;
	
	/*count the number of tables of arm lib*/
	proc sql;
	select count(*),memname into :cnt, :table_list separated by '~' from dictionary.tables where upcase(libname)="WORK";/* i18nOK:Line */
	quit;

	%if &cnt gt 0 %then %do;
		%do tbl=1 %to &cnt;
			%let current_tbl = %scan(&table_list,&tbl,~);     /* i18nOK:Line */ 
				%let scr_mdl_sk=%scan(&current_tbl,1,_);    /* i18nOK:Line */  /*get scoring_model_sk from table name*/
				
				/*fetch model_sk corresponding to scoring_model_sk*/
				%let model_sk=;
				proc sql;
				select model_sk into :model_sk from &lib_apdm..scoring_model where scoring_model_sk=&scr_mdl_sk;
				quit;
				%let model_sk=&model_sk;
				
				/*get the new name for table to be loaded in rm_arm(change from scoring_model_sk to model_sk)*/
				%let tbl_nm=%sysfunc(tranwrd(&current_tbl,&scr_mdl_sk,&model_sk));     /* i18nOK:Line */
				
				/*library reference for folder unzip_arm*/
                libname arm "&path_to_create_new_arm_xpt";/* i18nOK:Line */
				
				data arm.&tbl_nm.;
				set work.&current_tbl(rename=(SCORING_MODEL_SK=MODEL_SK));
				MODEL_SK=&model_sk;
				run;
				
		%end;
		
		proc cport file="&solution_xpt_path/arm_new.xpt" lib=arm;     /* i18nOK:Line */
				run;
		
		proc datasets lib=arm
		nolist kill;
		quit;
		run;
		
		proc datasets lib=work
		nolist kill;
		quit;
		run;
		
		libname arm clear;
	%end;
%end;/*Make changes for rm_arm -end*/


%mend rmcr_update_arm_xpt;