/************************************************************************
 * Copyright (c) 2010 by SAS Institute Inc., Cary, NC, USA.             *
 *                                                                      *
 * NAME:          rmcr_bank_parameter		                        	*
 *                                                                      *
 * PURPOSE:       Macro for inserting CS Banking  specific parameters  	*
 *					in Parameter table									*
 *                                                                      *
 * USAGE:         %rmcr_bank_parameter		                    	*
 *                                                                      *
 * PARAMETERS:    None			  			        					*
 *                                                                      *
 * HISTORY :                       										*
 * DATE             BY                    DETAILS                       *
 * 06-July-2015     sindpc                Initial Version               *
 ***********************************************************************/

%macro rmcr_bank_parameter;

/* 
The following code inserts data in the Parameter table for the parameters residing in csbmva branch
that are used in Credit Scoring For Banking code.
*/

proc sql;
delete from work.PARAMETER;
quit;


/* i18NOK:BEGIN */

proc sql;
insert into work.PARAMETER (
PARAMETER_NO ,PARAMETER_NM ,PARAMETER_VALUE ,PARAMETER_DESC 
)
VALUES(11,'OUTCOME_ACCEPT','''ACC''','Accepted/Approved Application')
VALUES(12,'OUTCOME_REJECT','''REJ''','Rejected Application')
VALUES(37,'LONG_RUN','N','Provide Y or N depending on whether LONG RUN method should or should not be used. In case LONG RUN method is not used, a simple average of estimated account PD/LGD/CCF is used to calculate the pool PD/LGD/CCF')
VALUES(39,'LONG_RUN_YRS','3','In case of LONG RUN method, no of years of historical data to be considered')
VALUES(179,'GUR1_APP_TYPE_CD','''GR1''','Parameter to specify that the applicant type is that of guarantor one.')
VALUES(219,'MAX_EST_TYPE','''MAX''','Parameter to specify that the method used to assign a rate to the pool is the maximum of estimated values method')
VALUES(327,'NOM1_APP_TYPE_CD','''NOM''','Parameter to specify that the applicant type is that of the first nominee.')
VALUES(360,'APPL_CAPTURE_PERIOD','6','This parameter denotes the number of days to be considered in the past from the current load for capturing the processed application.')
VALUES(508,'CUST_TYPES_CORP','&CORP_CUST_TYPE, &SME_CUST_TYPE','Customer types to be considered for Corporates')
VALUES(634,'APPLC_TC_JH1','''JH1''','Parameter for DABT to specify value for code JH1 for column APPLC_TC')
VALUES(635,'APPLC_TC_JH2','''JH2''','Parameter for DABT to specify value for code JH2 for column APPLC_TC')
VALUES(636,'APPLC_TC_NOM','''NOM''','Parameter for DABT to specify value for code NOM for column APPLC_TC')
VALUES(638,'APPLC_TC_SEC','''SEC''','Parameter for DABT to specify value for code SEC for column APPLC_TC')
VALUES(639,'BNK_STS_ACT','''ACT''','Parameter for DABT to specify value for code ACT for column BNK_STS')
VALUES(700,'DEC_CD_REJ','''REJ''','Parameter for DABT to specify value for code REJ for column DEC_CD')
VALUES(701,'DEC_OFLG_N','''N''','Parameter for DABT to specify value for code N for column DEC_OFLG')
VALUES(702,'DEC_OFLG_Y','''Y''','Parameter for DABT to specify value for code Y for column DEC_OFLG')
VALUES(713,'DELINQUENCY_FLG_N','''N''','Parameter for DABT to specify value for code N for column DELINQUENCY_FLG')
VALUES(714,'DELINQUENCY_FLG_Y','''Y''','Parameter for DABT to specify value for code Y for column DELINQUENCY_FLG')
VALUES(872,'REC_FTYC_GUR','''GUR''','Parameter for DABT to specify value for code GUR for column REC_FTYC')
VALUES(874,'REC_FTYC_PCL','''PCL''','Parameter for DABT to specify value for code PCL for column REC_FTYC')
VALUES(875,'REC_FTYC_PCU','''PCU''','Parameter for DABT to specify value for code PCU for column REC_FTYC')
VALUES(876,'REC_FTYC_SRD','''SRD''','Parameter for DABT to specify value for code SRD for column REC_FTYC')
VALUES(980,'MOB','6','Parameter for DABT to specify value for 6 for MOB Code')
VALUES(981,'MASTER_SCALE_DIST_METHOD','DIS','Distance Method')
VALUES(982,'MASTER_SCALE_AVG_METHOD','AVG','Average Method')
VALUES(1009,'MOB_CUSTOMER','6','Parameter for DABT to specify value for code 6 for column MOB_CUSTOMER')
VALUES(1012,'CREDIT_FACILITY_LOC','''LOC''','Parameter for DABT to specify value for code LOC for column  CREDIT_FACILITY_LOC')
VALUES(1013,'CREDIT_FACILITY_WC','''WC''','Parameter for DABT to specify value for code WC for column  CREDIT_FACILITY_WC')
VALUES(1014,'CREDIT_FACILITY_CC','''CC''','Parameter for DABT to specify value for code CC for column  CREDIT_FACILITY_CC')
VALUES(1027,'RECOVERY_PERIOD_LGD','13','Parameter to specify the default value IN MONTHS for recovery period of LGD models')
VALUES(1028,'RECOVERY_PERIOD_CCF_MONTH','13','Parameter to specify the default value for recovery period of CCF models in months')
VALUES(1031,'RECOVERY_PERIOD_CCF_DAY','395','Parameter to specify the default value for recovery period of CCF models in days')
VALUES(1034,'STACKED_ABT_MIN_MAX','MIN','Parameter to specify whether to consider MIN or MAX of estimated values in case of Stacked ABT')
VALUES(1035,'LGD_CCF_UPPER_BOUND','99','Parameter to specify the default upper limit for LGD value')
VALUES(10002,'DLQ_RANGE_PERIOD','4','Parameter to specify the default value for comparing previous delinquency ranges')
VALUES(10003,'SCORE_TIME_PERIOD','4','Parameter to specify the default value for comparing previous pools for PD and LGD')
VALUES(10004,'APPL_LOWSIDE_OVERRIDE','''L''','Parameter to specify whether the application was low side and overridden')
VALUES(10005,'APPL_HIGHSIDE_OVERRIDE','''H''','Parameter to specify whether the application was high side and overridden')
VALUES(10006,'APPL_DECISION_AUTO','''AUT''','Parameter to specify whether the application decision was automatic')
VALUES(10007,'APPL_DECISION_MANUAL','''MAN''','Parameter to specify whether the application decision was manual')
VALUES(10008,'OUTCOME_NTU','''NTU''','Parameter to specify whether the application was approved but not taken up')
VALUES(10009,'APPL_ANALYSIS_TIME_PERIOD','6','Parameter to specify the default value for comparing previous applications')
VALUES(10010,'RECOVERY_PERIOD_LGD_DAYS','395','Parameter to specify default value IN DAYS for recovery period for LGD models')
VALUES(1024,'PRI_OWNER_TYPE_CD','''OWN''','Parameter for DABT to specify value of OWN for parameter PRI_OWNER_TYPE_CD')
VALUES(1025,'SEC_OWNER_TYPE_CD','''EMP''','Parameter for DABT to specify value of EMP for parameter SEC_OWNER_TYPE_CD')
;
quit;

/* i18NOK:END */

	proc sql;
	INSERT INTO APDM.PARAMETER_MASTER (PARAMETER_NM, PARAMETER_DESC,PARAM_ADDED_BY_PRODUCT_CD ,VALUE_EDITABLE_FLG, VALUE_TO_BE_SET_BY_SOLUTN_FLG,CREATED_DTTM, CREATED_BY_USER , LAST_PROCESSED_DTTM, LAST_PROCESSED_BY_USER) 
		SELECT distinct PARAMETER_NM,PARAMETER_DESC,'csbmid','N','Y',"%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID", "%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID"   /* I18NOK:LINE */
		FROM WORK.PARAMETER;
		quit;
	
	proc sql;
	INSERT INTO APDM.PARAMETER_VALUE_DTL(PARAMETER_NM, VALUE_APPLICABLE_TO_PRODUCT_CD, PARAMETER_VALUE,CREATED_DTTM, CREATED_BY_USER ,LAST_PROCESSED_DTTM, LAST_PROCESSED_BY_USER)
		SELECT distinct PARAMETER_NM,'csbmid',PARAMETER_VALUE,"%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID", "%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID"   /* I18NOK:LINE */
		FROM WORK.PARAMETER;
	quit;
	
%mend rmcr_bank_parameter;
