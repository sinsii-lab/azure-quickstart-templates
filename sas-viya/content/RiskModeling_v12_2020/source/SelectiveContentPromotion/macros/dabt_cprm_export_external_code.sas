/*****************************************************************/
/* NAME: dabt_cprm_export_external_code.sas               		 */
/* VERSION: 6.2                                                  */
/* DESCRIPTION: Macro to move external code sas files to 
				specified location								 */
/*                                                               */
/* Parameters :  export_spec_ds_lib						         */
/* 			     export_spec_ds_nm:								 */
/*				 export_ouput_folder_path: Folder location 
					to which export package to create		 	 */
/*                                                               */
/*                                                               */
/* PRODUCT: CS                                                   */
/* USAGE: 	Called Internally by dabt_cprm_export_wrapper macro  */
/*          dabt_cprm_export_external_code(export_spec_ds_lib =,
				export_spec_ds_nm= ,export_ouput_folder_path=)	 */
/*                                                               */
/*****************************************************************/
 
/*****************************************************************************************/
/*History:                                                                               */
/*ddmonyyyy                                                                              */
/*5May2016 - First Version of the Code.                                                 */
/*****************************************************************************************/

%macro dabt_cprm_export_external_code(export_spec_ds_lib =,export_spec_ds_nm= ,export_ouput_folder_path=); 

	%if %symexist(job_rc)=0 %then %do;
		%global job_rc;
	%end;

	%if %symexist(sqlrc)=0 %then %do;
		%global sqlrc;
	%end;

	%local  export_folder_nm
			m_rel_path  
			m_entity_type_cd 
			m_entity_key
			m_ext_cd_cnt  
			m_external_code_file_loc
			m_external_code_file_nm
			m_external_code_sk
		;

	/*Folder name where all external code files will be copied.*/

	%let export_folder_nm = external_code; /* i18NOK:LINE */

	/*Macro variable to store full path of export package folder */

	%let m_rel_path =;
	
	/* Deleeting all contents of &export_ouput_folder_path./&export_folder_nm. */

	*%dabt_dir_delete(dirname=&export_ouput_folder_path./&export_folder_nm.);

	/*This will create cprm_export_package folder next to export_ouput_folder_path  */
	%let m_work_lib_path = %sysfunc(pathname(work));
	%dabt_make_work_area(dir=&m_work_lib_path., create_dir=&export_folder_nm., path=m_rel_path); 

	%let m_rel_path = &m_rel_path.;/*Accurate path upto &export_folder_nm */

	%let m_entity_type_cd = EXT_CODE;  /* i18NOK:LINE */
	
	/*Counting number of external codes and extracting external_code_sk from 
					cpexpscr.cprm_export_specification*/

	proc sql noprint;
		select
			cp_exp_spec.entity_key , count(*) 			/* i18NOK:LINE */
			into
				:m_entity_key separated by '#', /* i18NOK:LINE */
				:m_ext_cd_cnt 
		from 
				&export_spec_ds_lib..&export_spec_ds_nm. as cp_exp_spec 
			inner join &lib_apdm..cprm_entity_master
				on(cp_exp_spec.entity_type_cd = cprm_entity_master.entity_type_cd)
		where ktrim(kleft(kupcase(cprm_entity_master.entity_type_cd)))="&m_entity_type_cd." /* i18NOK:LINE */
				and kupcase(promote_flg) = kupcase(&check_flag_true.)
		order by cp_exp_spec.entity_key ; /* i18NOK:LINE */
	quit;

	%dabt_err_chk(type=SQL);

	%let m_entity_key = &m_entity_key.;
	%let m_ext_cd_cnt = &m_ext_cd_cnt.;

	%if &m_ext_cd_cnt. gt 0 %then %do;
		%do i=1 %to &m_ext_cd_cnt.;
			%let m_external_code_sk = %scan(&m_entity_key.,&i,%str(#));					/* i18NOK:LINE */

			/*Finding external code file name and external code file path from external code master.*/

			proc sql noprint;
				select external_code_file_loc,external_code_file_nm
					into:m_external_code_file_loc,
						:m_external_code_file_nm
				from
					&lib_apdm..external_code_master
				where 
					external_code_sk eq &m_external_code_sk.;
			quit;

			%let m_external_code_file_loc = &m_external_code_file_loc.;
			%let m_external_code_file_nm = &m_external_code_file_nm.;
		
			/*Defining source file and desination external file name and it's path.*/

			*filename src "&m_external_code_file_loc./&m_external_code_file_nm." recfm=n; /* i18NOK:LINE */
filename src filesrvc folderpath="&m_external_code_file_loc./" filename= "&m_external_code_file_nm." recfm=n debug=http; /* i18nOK:Line */						
			*filename dest "&m_rel_path./&m_external_code_file_nm." recfm=n; /* i18NOK:LINE */
filename des filesrvc folderpath="&export_ouput_folder_path./" filename= "&m_external_code_file_nm." recfm=n debug=http; /* i18nOK:Line */			
	
			/*Copying external code file from source to destination.*/

			data _null_;
				rc=fcopy('src', 'des'); /* i18NOK:LINE */
				if rc ne 0 then do;
					call symput ('syscc',99); /* i18NOK:LINE */
				end;
			run;
		%end;/*loop end for all external codes*/

		%if &syscc. gt 4 %then %do;
				%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_CODE_FILE_MOVE_ERROR, noquote));
				%let syscc=99;
				%return;
			%end;
		

	%end;/*External code count condition check end*/

%mend dabt_cprm_export_external_code;
