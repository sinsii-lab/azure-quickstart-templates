/********************************************************************************************************
   	Module		:  dabt_cprm_mdl_export_wrapper

   	Function	:  This macro for a given list of model ids creates export specification file and export package as per export file         
					specification.					

   	Parameters	:	NAME							TYPE		DESC
					model_id_lst					INPUT		-> 	comma separated model id list.
					export_ouput_folder_path		INPUT		-> 	Variable Sk.Its key on the target machine.
					
*********************************************************************************************************/

%macro dabt_cprm_mdl_export_wrapper(model_id_lst=,export_ouput_folder_path=,log_divert_flg=N);

	%let syscc = 0;
	/**** Assigning job folder path ***/
	%if ("&export_ouput_folder_path." eq "") %then %do;
		%let export_ouput_folder_path=%str(/&m_file_srvr_job_folder_path/&m_job_sk);
	%end;
	%if ("&export_ouput_folder_path." eq "" ) %then %do;
		/* Exported package folder path cannot be blank. */
		%put  %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_EXPORT_PACKAGE_FOLDER_PATH_NOT_SPECIFIED, noquote));	
		%let syscc=99;
		%return;
	%end;

	%if ("&model_id_lst." eq "") %then %do;
		/* Model id list cannot be blank */
		%put %sysfunc(sasmsg(SASHELP.DABT_CPRM_MISC, ERR_MODEL_ID_NOT_SPECIFIED, noquote));
		%let syscc=99;
		%return;
	%end;
	
	%let export_spec_file_nm = model_export_spec_file;

	***************************************************************;
	*Creating export specification file to export specified model.
	***************************************************************;
	%dabt_cprm_create_mdl_exp_spec(	export_spec_file_path	= 	&export_ouput_folder_path., 
									export_spec_file_nm		=	&export_spec_file_nm.,
									model_id_lst	=	&model_id_lst.,
									log_divert_flg=&log_divert_flg.
								  );
	***********************************************************************************;
	*To abort furthur exedcution if it fails while creating export specification file
	***********************************************************************************;
	
	%if &syscc. > 4 %then %do;
		%let syscc=99;
		%return;
	%end;

	***************************************************************;
		*Creating export package to export specified model.
	***************************************************************;
	
	%dabt_cprm_export_wrapper(	export_spec_file_path	 =	&export_ouput_folder_path.,
								export_spec_file_nm		 =	&export_spec_file_nm., 
								export_ouput_folder_path =	&export_ouput_folder_path.,
								log_divert_flg=&log_divert_flg.
							 );
	

	**************************************************************;
	*To delete specifcation file created earlier in the process
	**************************************************************;
	

	*filename myfile "&export_ouput_folder_path.\&export_spec_file_nm..csv";		/* i18NOK:LINE */
	filename myfile filesrvc folderpath="&export_ouput_folder_path./" filename= "&export_spec_file_nm..csv" encoding='utf-8' debug=http; /* i18nOK:Line */			

	data _null_;
		rc=fdelete("myfile");													/* i18NOK:LINE */
	run;

%mend dabt_cprm_mdl_export_wrapper;
