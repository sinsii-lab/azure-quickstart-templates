/************************************************************************************
sample code to execute this:
%let m_staging_location=physical location of staging area;
Ex:
%let m_staging_location=\tmp;
%rmcr_81_cmmnts_import();

************************************************************************************/

%macro rmcr_81_cmmnts_import();

%let m_lib_apdm=apdm;

/*Check if wip_comments table exists in apdm library*/

%let is_table_exists = %sysfunc(exist(&m_lib_apdm..wip_comments));

/*Create wip_comments table if it does not exist*/

%if &is_table_exists. eq 0 %then %do;

	/*Create file reference for comment.xpt*/

	filename comments "&path_of_internal_folder./comment.xpt";    /* i18nOK:LINE */

	/*Import comment.xpt in work library*/

	proc cimport lib=work infile=comments;
	run;

	proc sql;
	create table &m_lib_apdm..wip_comments as
	select cmnt.* ,'PENDING' AS  migration_status length=10 ,      /* i18nOK:LINE */
	'' AS viya_comment_id length=36
	from sas_comment cmnt
	WHERE object_type_id IN (594001,594002,594003)
	;
	update &m_lib_apdm..wip_comments set object_id=tranwrd(object_id,(scan(object_id,1,'_')),(catx('_',scan(object_id,1,'_'),0))) /* i18nOK:LINE */
	where count(object_id,'_')=1;   /* i18nOK:LINE */
	QUIT;
	
%end;

%let BASE_URI=%sysfunc(getoption(servicesbaseurl));

filename resp temp;

/*Call end point of restAPI for comments migration*/

proc http url="&BASE_URI/riskModelingCore/migration/comments"/* i18nOK:Line */
	 method='POST'/* i18nOK:Line */
	 oauth_bearer=sas_services
	 out=resp
	 ;
	run; quit;
	
%put &SYS_PROCHTTP_STATUS_CODE. ;

/*Check response code and put message in log*/

%if &SYS_PROCHTTP_STATUS_CODE = 201 %then 
%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.COMMENTS_SM1.1, noquote)); 

%else %if &SYS_PROCHTTP_STATUS_CODE = 400 %then 
%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.COMMENTS_SM2.1, noquote));

%else %if &SYS_PROCHTTP_STATUS_CODE = 503 %then
%put %sysfunc(sasmsg(work.rmcr_message_dtl_promotion,RMCR_PROMOTION_MSG.COMMENTS_SM3.1, noquote)); 


proc sql;
create table check_comment
as select viya_comment_id from &m_lib_apdm..wip_comments
where object_type_id = 594003 or object_id contains "_0_";  /* i18nOK:LINE */
select * from check_comment;
quit;
 
%let cmnt_id_count=&sqlobs;
%if &cmnt_id_count gt 0 %then %do;
	%do i=1 %to &cmnt_id_count;
		data _null_;
		obs=&i;
		set check_comment point=obs;
		call symputx('viya_comment_id',viya_comment_id);  /* i18nOK:LINE */
		stop;
		run;
		
		filename resp     temp;
		filename resp_hdr temp;
		
		proc http url="&BASE_URI/comments/comments/&viya_comment_id"/* i18nOK:Line */
		 method='GET'/* i18nOK:Line */
		 oauth_bearer=sas_services
		 out=resp
		 headerout=resp_hdr
		headerout_overwrite;
		debug level=3;
		run; quit;
		
		/*********************************************************
		Retrieve the last-modified timestamp from response headers
		**********************************************************/
		data _null_;
			infile resp_hdr scanover truncover;
			input @'Last-Modified:' lastmod $40.;  /* i18nOK:LINE */
			call symputx('last_modified',lastmod,'g'); /* i18nOK:Line */
		run;
		
		%put INFO: Last-Modified timestamp is &last_modified.;
		
		/*****************************************
		Retrieve the resource uri from body
		****************************************/
		libname resp json fileref=resp;

		data _null_;
		set resp.root;
		call symputx("resource_uri",resourceUri);   /* i18nOK:LINE */
		call symputx("parentId",parentId);   /* i18nOK:LINE */
		call symputx("subject",subject);	/* i18nOK:LINE */
		call symputx("text",text);	/* i18nOK:LINE */
		run;
		
		%if %sysfunc(find(&resource_uri.,_UkVQT1JUX1NQRUNJRklDQVRJT05fMA)) gt 0 or %sysfunc(find(&resource_uri.,modelInputMonitoring)) gt 0 %then %do; /* i18nOK:LINE */
			%if %sysfunc(find(&resource_uri.,_UkVQT1JUX1NQRUNJRklDQVRJT05fMA)) gt 0 %then %do;   /* i18nOK:LINE */
						%let replaceuri=_;
						%let resource_uri=%sysfunc(tranwrd(&resource_uri,_UkVQT1JUX1NQRUNJRklDQVRJT05fMA_,&replaceuri));   /* i18nOK:Line */
			%end; 
			%if %sysfunc(find(&resource_uri.,modelInputMonitoring)) gt 0 %then %do;   /* i18nOK:LINE */
						%let replaceuri=inputMonitoring;
						%let resource_uri=%sysfunc(tranwrd(&resource_uri,modelInputMonitoring,&replaceuri));   /* i18nOK:Line */
			%end; 
			
			/*******************************************************************************************************************
			This macro would create json file that would replace the original json for resource uri
			********************************************************************************************************************/
			filename genjson temp;
			
			proc json out=genjson pretty;
				write open object;
				write values "id" "&viya_comment_id";    /* i18nOK:LINE */
				write values "resourceUri" "&resource_uri";   /* i18nOK:LINE */
				%if "&parentId" ne "." %then %do;
				write values "parentId" "&parentId";    /* i18nOK:LINE */
				%end;
				write values "subject" "&subject";   /* i18nOK:LINE */
				write values "text" "&text";    /* i18nOK:LINE */
				write close;
			run;
			
	
			filename resp     temp;
			filename resp_hdr temp;
			
			/****************************************************
			Updating the restAPI for changing the resource uri field
			*****************************************************/
			proc http url="&BASE_URI/comments/comments/&viya_comment_id"/* i18nOK:Line */
			 method='PUT'/* i18nOK:Line */
			 oauth_bearer=sas_services
			 in=genjson
			 headerout=resp_hdr
			 headerout_overwrite
			 ct="application/vnd.sas.comment+json";/* i18nOK:Line */
			 headers "If-Unmodified-Since"="&last_modified.";/* i18nOK:Line */
			 DEBUG LEVEL=3;
			run; quit;

		%end;
	%end;
%end;

%mend rmcr_81_cmmnts_import;