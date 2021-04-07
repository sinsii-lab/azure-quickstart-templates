%macro rmcr_init_cprm;

	/******   smd message extarction ******/
	proc sql;
	create table work.dabt_cprm_misc  (
	
	locale char(5) ,
	   key char(60) ,
	lineno num 3,
	text char(1200) 
	  );

	insert into work.dabt_cprm_misc 
	select locale, kstrip(key), lineno, text from &lib_apdm..RMCR_MESSAGE_DETAIL where kupcase(kstrip(cr_type_cd))='CPRM'; /* I18NOK:LINE */
	quit;
	proc sort data=work.dabt_cprm_misc;
	by locale key descending lineno;
	run;
	
	proc datasets lib=work
	memtype=data nolist;
	modify dabt_cprm_misc;
	index create indx=(LOCALE KEY);
	run;
	quit;  
	
%macro cprm_exec(m_file_nm=);
		%let m_file_path =&m_cr_cprm_macro_path;
		%let m_file_nm = &m_file_nm.;
		
			%let m_macr_nm = %sysfunc(kscan(&m_file_nm., 1,"."));     /* I18NOK:LINE */
		
			filename cr_cd filesrvc folderpath="&m_file_path./" filename= "&m_file_nm" debug=http; /* i18nOK:Line */
			
			%if "&_FILESRVC_cr_cd_URI" eq "" %then %do;
				%let job_rc = 1012; 
				%put %sysfunc(sasmsg(work.dabt_cprm_misc, RMCR_CPRM.INIT_CODE1.1, noquote,&m_file_nm.,&m_file_path.));
			%end;
			
			%else %do;		
				%include cr_cd / lrecl=64000; /* i18nOK:Line */
			%end;
			
			filename cr_cd clear;
			
%mend cprm_exec;

filename myfldr filesrvc folderPath="&m_cr_cprm_macro_path" debug=http;

data test(keep=memname);
   did = dopen('myfldr');	/* i18nOK:Line */
   mcount = dnum(did);
   do i=1 to mcount;
      memname = dread(did, i);  
		output;
   end;  
   rc = dclose(did);
run;

data _null_;
	set test;
	where kstrip(memname) like "%.sas" and kstrip(memname) not in ("rmcr_init_cprm.sas","rmcr_config_cprm.sas");    	/* i18nOK:Line */
	call execute('%nrstr(%cprm_exec(m_file_nm='||strip(memname)||'));');    /* I18NOK:LINE */
run;

%mend rmcr_init_cprm;