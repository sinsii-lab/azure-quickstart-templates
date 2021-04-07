%macro rmcr_enable_rerun_sync_dtl_data(model_id=,version_id=,scoringdate=/*MONYYYY*/,report_category=/*ONG or BCK*/);

/*------------------------------------------------------------
Fetch model_sk from model_id
--------------------------------------------------------------*/
%let m_model_sk=;

proc sql noprint;
select model_sk into :m_model_sk
from &lib_apdm..model_master
where model_id ="&model_id";
quit;

%let m_model_sk=&m_model_sk;
/*------------------------------------------------------------
Fetch report_specification_sk from model_sk and version_id
--------------------------------------------------------------*/
%let m_report_specification_sk=;

proc sql noprint;
select report_specification_sk into :m_report_specification_sk
from &lib_apdm..mm_report_specification
where model_sk=&m_model_sk and version_no=&version_id
and active_flg='Y';		/* i18NOK:LINE */
quit;

%let m_report_specification_sk=&m_report_specification_sk;
/*------------------------------------------------------------
Fetch report_specification_sk from model_sk and version_id
--------------------------------------------------------------*/
%let start=01;
%let m_scoring_date=%sysfunc(cats(&start,&scoringdate));	/* i18NOK:LINE */

%let m_time_sk=;

proc sql noprint;
select time_sk into :m_time_sk
from &lib_apdm..time_dim
where datepart(period_first_dttm)="&m_scoring_date"d;
quit;

%let m_time_sk=&m_time_sk;

/*------------------------------------------------------------
Delete from table based on report category
--------------------------------------------------------------*/
%if %kupcase(&report_category)=ONG %then %do;
	%let m_apdm_table=DETAIL_REPORT_ONG;
	%let category=Latest Reports;
%end;
%else %if %kupcase(&report_category)=BCK %then %do;
	%let m_apdm_table=DETAIL_REPORT_BACKTEST;
	%let category=Backtesting;
%end;
%else %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM6.1, noquote));
	%goto exit;
%end;

%if &m_report_specification_sk ne and &m_time_sk ne %then %do;

	proc sql;
		delete from &lib_apdm..&m_apdm_table
		where report_specification_sk=&m_report_specification_sk
		and scoring_as_of_time_sk=&m_time_sk;
	quit;
	
	%let cnt=&sqlobs;
	
	%if &sqlobs>0 %then %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM13.1, noquote,&model_id,&version_id,&scoringdate,&category));
	%end;
	%else %do;
		%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM12.1, noquote,&model_id,&version_id,&scoringdate,&category));
		%goto exit;
	%end;
	
%end;
%else %do;
	%put %sysfunc(sasmsg(work.rmcr_message_dtl_rpm_reports, RMCR_RPM_REPORTS.MM_SM14.1, noquote));
	%goto exit;
%end;

%exit:

%mend rmcr_enable_rerun_sync_dtl_data;