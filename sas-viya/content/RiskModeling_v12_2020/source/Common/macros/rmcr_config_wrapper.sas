/*************************************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * Name			: rmcr_config_wrapper  					                       
 *             
 * Assumption	: This macro is run in Code window of ADB
 *
 * Logic		: Invokes the macro %rmcr_config to perform RM CR configuration
 *							   
 * Authors		: BIS Team
 *************************************************************************************************************/
 
%let m_cr_version=v12.2020;
filename cfg_cd filesrvc folderpath="/&M_FILE_SRVR_ROOT_FOLDER_NM./Risk Modeling Content/&m_cr_version./Common/Macros/" filename= "rmcr_config.sas" debug=http; /* i18NOK:LINE */
%include cfg_cd;
%rmcr_config(cr_version=&m_cr_version.);