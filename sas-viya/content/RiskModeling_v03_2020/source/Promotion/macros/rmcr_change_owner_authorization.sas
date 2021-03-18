/*****************************************************************************************
 * Copyright (c) 2019 by SAS Institute Inc., Cary, NC, USA.            
 *                                                                     
 * NAME			: rmcr_change_owner_authorization  					                       
 *                                                                 
 * LOGIC		: It transfers the ownership of authorization rule to correct owners 
 *						of projects and models
 *
 *
 * Called By	: %rmcr_solution_config_prmtn
 *                                                                 
 * PARAMETERS	:  
 *														   
 * Authors		: BIS Team
 *****************************************************************************************/

%macro rmcr_change_owner_authorization();
/*********************************
Base URI for the service call
**********************************/

%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

proc sql noprint;
create table authorization_user as
select authorization_rule_id,
owned_by_user
from &lib_apdm..project_master
union
select authorization_rule_id,
owned_by_user
from &lib_apdm..model_master
;
quit;

proc sql;
select * from authorization_user;
quit;

%let cnt=&sqlobs;

%if &cnt gt 0 %then %do;
	%do rule=1 %to &cnt;
		data _null_;
		obs=&rule;
		set authorization_user point=obs;
		call symputx('authorization_rule_id',authorization_rule_id);	/* I18NOK:LINE */
		call symputx('owned_by_user',owned_by_user);	/* I18NOK:LINE */
		stop;
		run;
		
		filename resp     temp;
			filename resp_hdr temp;

			/******************************
			Getting inputs from restAPI 
			*******************************/
			proc http url="&BASE_URI./authorization/rules/&authorization_rule_id."/* i18nOK:Line */
						 method='get'/* i18nOK:Line */
			 oauth_bearer=sas_services
			 out=resp
			 headerout=resp_hdr
			 headerout_overwrite;
			 debug level=3;
			run; quit;

			data _null_;
					infile resp_hdr scanover truncover;
					input @'ETag:' etag $40.;	/* I18NOK:LINE */
					call symputx('etag',etag,'g');/* i18nOK:Line */
				run;
			%put &etag;

			libname resp json fileref=resp;

			data _null_;
					set resp.root;
					call symputx("type",type);   /* i18nOK:LINE */
					call symputx("principal",principal);   /* i18nOK:LINE */
					call symputx("objectUri",objectUri);	/* i18nOK:LINE */
					call symputx("id",id);	/* i18nOK:LINE */
					call symputx("principalType",principalType);/* i18nOK:LINE */
					run;

			data _null_;
					set resp.permissions;
					call symputx("permissions1",permissions1);   /* i18nOK:LINE */
					call symputx("permissions2",permissions2);   /* i18nOK:LINE */
					call symputx("permissions3",permissions3);	/* i18nOK:LINE */
					call symputx("permissions4",permissions4);	/* i18nOK:LINE */
					run;

			filename genjson temp;
			
			proc json out=genjson pretty;
				write open object;
				write values "id" "&id";    /* i18nOK:LINE */
				write values "type" "&type"; 	/* I18NOK:LINE */
				write values "permissions";	/* I18NOK:LINE */
				write open array ; /* i18nOK:LINE */
				write values "&permissions1";	/* I18NOK:LINE */
				write values "&permissions2";	/* I18NOK:LINE */
                write values "&permissions3";	/* I18NOK:LINE */
                write values "&permissions4";	/* I18NOK:LINE */
				write close;
				write values "objectUri" "&objectUri";   /* i18nOK:LINE */
				write values "principal" "&owned_by_user";    /* i18nOK:LINE */
				write values "description" "Allows user &owned_by_user CRUD capabilities.";    /* i18nOK:LINE */
				write values "principalType" "&principalType"; /* i18nOK:LINE */
				write close;
			run;
			
	
			filename resp     temp;
			filename resp_hdr temp;
			
			/****************************************************
			Updating the restAPI for changing the resource uri field
			*****************************************************/
			proc http url="&BASE_URI./authorization/rules/&authorization_rule_id."/* i18nOK:Line */
			 method='PUT'/* i18nOK:Line */
			 oauth_bearer=sas_services
			 in=genjson
			 headerout=resp_hdr
			 headerout_overwrite
			 ct="application/vnd.sas.authorization.rule+json";/* i18nOK:Line */
			 headers "IF-MATCH"=%sysfunc(quote(&etag));/* i18nOK:Line */
			 DEBUG LEVEL=3;
			run; quit;
	
	%end;
%end;

%mend rmcr_change_owner_authorization;



