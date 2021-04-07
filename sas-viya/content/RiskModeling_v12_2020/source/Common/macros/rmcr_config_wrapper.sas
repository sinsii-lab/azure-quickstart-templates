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

	filename cfg_cd filesrvc folderpath="/Products/SAS Risk Modeling/Risk Modeling Content/v03.2020/Common/Macros/" filename= "rmcr_config.sas" debug=http;    /* I18NOK:LINE */
	
	%include cfg_cd;
	
	%rmcr_config;
 
 
 