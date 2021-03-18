/*-- Copyright (c) 2012 by SAS Institute Inc., Cary, NC, USA. All Rights Reserved. --*/ 
/*
Model         Model_1
Subject Area  FM Foundation Mart Entire1
Model Version 
Macro Name    rmcr_ddlgen.sas
DBMS          SAS
Created       5/22/2012 4:06:17 AM
* Copyright (c) 2012 by SAS Institute Inc., Cary, NC 27513
*/
 /* assign the macro variable to location of the DDL                  */ 
%macro rmcr_ddlgen(LIBREF=WORK, DTTMFMT=NLDATM21., TMFMT=NLTIMAP11., DTFMT=NLDATE9., FMTRK=12.);   /* I18NOK:LINE */

%dabt_initiate_cas_session(cas_session_ref=load_table);
	  /* I18NOK:BEGIN */
filename a1 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "account_country_detail.sas" debug=http;
filename a2 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "account_default_dim.sas" debug=http;
filename a3 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "account_other_info_base.sas" debug=http; 
filename a4 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "country_snapshot_base.sas" debug=http;  
filename a5 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "credit_facility_default_dim.sas" debug=http;  
filename a6 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "account_detail.sas" debug=http;  
filename a7 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "account_dim.sas" debug=http;  
filename a8 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "account_transaction_detail.sas" debug=http;  
filename a9 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "applicant_dim.sas" debug=http;  
filename a10 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "application_dim.sas" debug=http; 
filename a11 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "bank_card_detail.sas" debug=http;  
filename a12 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "corporate_customer_qual_detail.sas" debug=http;  
filename a13 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "corporate_financial_detail.sas" debug=http;  
filename a14 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "corporate_owner_detail.sas" debug=http;  
filename a15 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "credit_facility_country_detail.sas" debug=http; 
filename a16 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "credit_facility_dim.sas" debug=http;  
filename a17 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "customer_dim.sas" debug=http;  
filename a18 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "default_event_dim.sas" debug=http;  
filename a19 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "recovery_fact.sas" debug=http;  
filename a20 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "time_dim.sas" debug=http;  
filename a21 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CORP_CRD_FACILITY_X_CUSTOMER.sas" debug=http;
filename a22 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CORP_ACCOUNT_X_CUSTOMER.sas" debug=http;
filename a23 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CONTACTS_FACT_BASE.sas" debug=http; 
filename a24 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "ALL_APPLICATION_DIM.sas" debug=http;  
filename a25 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "ACCOUNT_TRANSACTION_BASE.sas" debug=http;  
filename a26 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "ACCOUNT_SNAPSHOT_BASE.sas" debug=http;  
filename a27 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "RECOVERY_FACT_BASE.sas" debug=http;  
filename a28 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "OUTBOUND_COMM_FACT_BASE.sas" debug=http;  
filename a29 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "INQUIRY_FACT_BASE.sas" debug=http;  
filename a30 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "IND_CUSTOMER_X_ACCOUNT.sas" debug=http;
filename a31 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CORP_ACCT_COUNTRY_SNAPSHOT.sas" debug=http;
filename a32 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CORP_APPL_COUNTRY_SNAPSHOT.sas" debug=http;
filename a33 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CORP_CUST_COUNTRY_SNAPSHOT.sas" debug=http; 
filename a34 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CREDIT_BUREAU_DTL_BASE.sas" debug=http;  
filename a35 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "CREDIT_FACILITY_CNTRY_SNAPSHOT.sas" debug=http;  
filename a36 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "IND_ACCT_COUNTRY_SNAPSHOT.sas" debug=http;  
filename a37 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "IND_APPL_COUNTRY_SNAPSHOT.sas" debug=http;  
filename a38 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "IND_CUST_COUNTRY_SNAPSHOT.sas" debug=http;
filename a39 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "country_detail.sas" debug=http;  
filename a40 filesrvc folderpath="&m_cr_banking_solution_ddl_path./" filename= "financial_product_dim.sas" debug=http;      
/* I18NOK:END */


PROC SQL;
%include a1;
%include a2;
%include a3;
%include a4;
%include a5;
%include a6;
%include a7;
%include a8;
%include a9;
%include a10;
%include a11;
%include a12;
%include a13;
%include a14;
%include a15;
%include a16;
%include a17;
%include a18;
%include a19;
%include a20;
%include a21;
%include a22;
%include a23;
%include a24;
%include a25;
%include a26;
%include a27;
%include a28;
%include a29;
%include a30;
%include a31;
%include a32;
%include a33;
%include a34;
%include a35;
%include a36;
%include a37;
%include a38;
%include a39;
%include a40;

QUIT;

%let table_list = ACCOUNT_COUNTRY_DETAIL~ACCOUNT_DEFAULT_DIM~ACCOUNT_DETAIL~ACCOUNT_DIM~ACCOUNT_OTHER_INFO_BASE~
ACCOUNT_TRANSACTION_DETAIL~APPLICANT_DIM~APPLICATION_DIM~BANK_CARD_DETAIL~
CORPORATE_CUSTOMER_QUAL_DETAIL~CORPORATE_FINANCIAL_DETAIL~CORPORATE_OWNER_DETAIL~COUNTRY_SNAPSHOT_BASE~CREDIT_FACILITY_COUNTRY_DETAIL~
CREDIT_FACILITY_DEFAULT_DIM~CREDIT_FACILITY_DIM~CUSTOMER_DIM~DEFAULT_EVENT_DIM~RECOVERY_FACT~TIME_DIM~
CORP_CRD_FACILITY_X_CUSTOMER~CORP_ACCOUNT_X_CUSTOMER~CONTACTS_FACT_BASE~ALL_APPLICATION_DIM~ACCOUNT_TRANSACTION_BASE~ACCOUNT_SNAPSHOT_BASE~RECOVERY_FACT_BASE~
OUTBOUND_COMM_FACT_BASE~INQUIRY_FACT_BASE~IND_CUSTOMER_X_ACCOUNT~CORP_ACCT_COUNTRY_SNAPSHOT~CORP_APPL_COUNTRY_SNAPSHOT~CORP_CUST_COUNTRY_SNAPSHOT~
CREDIT_BUREAU_DTL_BASE~CREDIT_FACILITY_CNTRY_SNAPSHOT~IND_ACCT_COUNTRY_SNAPSHOT~IND_APPL_COUNTRY_SNAPSHOT~IND_CUST_COUNTRY_SNAPSHOT~COUNTRY_DETAIL~FINANCIAL_PRODUCT_DIM;

%let cnt = %eval(%sysfunc(countc(%quote(&table_list),"~"))+1);     /* I18NOK:LINE */

%if &cnt gt 0 %then %do;
		%do tbl=1 %to &cnt;
			%let current_tbl = %scan(&table_list,&tbl,~);      /* I18NOK:LINE */
			%let etls_viewExist = %eval(%sysfunc(exist(bankcrfm.&current_tbl,DATA)); 
		
			%if &etls_viewExist. eq 0 %then %do;
				data bankcrfm.&current_tbl;
				set &LIBREF..&current_tbl;
				run;
			%dabt_save_cas_table(m_in_cas_lib_ref=bankcrfm, m_in_cas_table_nm=&current_tbl.);
			%dabt_promote_table_to_cas(input_caslib_nm =bankcrfm,input_table_nm =&current_tbl.); 
			%end;
		%end;
%end; 
%dabt_terminate_cas_session(cas_session_ref=load_table);

%MEND;

