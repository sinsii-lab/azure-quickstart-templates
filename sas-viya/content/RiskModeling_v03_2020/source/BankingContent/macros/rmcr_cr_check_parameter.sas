/******************************************************************************************************
       Module:  PARAMETER

     Function:  This code checks the existence PARAMETER data set in the custpara library. 
				If the data set is already present in the library it only insert the parameter
				required . If data set is not present in the custpara library 
				this code creates the data set and insert the parameter	required.

      Authors:  BIS Team
         Date:  17 May 2007
          SAS:  9.1
    
Datasets used:  None
 
        	
   Parameters:  
			INPUT 	: None 
				
		    OUTPUT  : Table PARAMETER  with all parameter.
******************************************************************************************************/
%macro rmcr_cr_check_parameter;

proc sql;

CREATE TABLE work.parameter (
		       PARAMETER_NO         NUMERIC(8) NOT NULL LABEL='Parameter No', /* i18NOK:LINE */
		       PARAMETER_NM         CHARACTER(200) NOT NULL FORMAT=$200. INFORMAT=$200. LABEL='Parameter Name', /* i18NOK:LINE */
		       PARAMETER_VALUE      CHARACTER(255) FORMAT=$255. INFORMAT=$255. LABEL='Parameter Value', /* i18NOK:LINE */
		       PARAMETER_DESC       CHARACTER(255) FORMAT=$255. INFORMAT=$255. LABEL='Parameter Description' /* i18NOK:LINE */
		);

		CREATE UNIQUE INDEX PARAMETER_NO ON work.parameter
		(
		       PARAMETER_NO
		);

quit;
/* i18NOK:BEGIN */
		proc sql;
			insert into work.PARAMETER (
			PARAMETER_NO ,PARAMETER_NM ,PARAMETER_VALUE ,PARAMETER_DESC 
			)
			VALUES(1,'MTG_PRD_TYPE','''MTG''','Mortage Product')
			VALUES(2,'CCD_PRD_TYPE','''CCD''','Credit Card Product')
			VALUES(3,'LON_PRD_TYPE','''LON''','Loan Product')
			VALUES(5,'INV_PRD_TYPE','''INV''','Investment Product')
			VALUES(24,'PRI_RELATIONSHIP_ACCNT','''PRI''','Relationship of the customer/applicant to the account is primary')
			VALUES(25,'SEC_RELATIONSHIP_ACCNT','''SEC''','Relationship of the customer/applicant to the account is secondary')
			VALUES(26,'IND_CUST_TYPE','''IND''','Individual Customer')
			VALUES(27,'SME_CUST_TYPE','''SME''','Small or Medium Enterprise Customer')
			VALUES(82,'PRI_APP_TYPE_CD','''PRI''','Primary Applicant')
			VALUES(88,'DEFAULT_STATUS_CD','''DFT''','Account delinquency status is default')
			VALUES(92,'PR_ASSOC_TYPE_REP','''REP''','Parameter to specify which association to be selected for Reporting while creating Financial Product Dimension')
			VALUES(94,'IO_ASSOC_TYPE','''FHI''','Parameter to specify which association to be selected while creating Internal Organization Dimension')
			VALUES(96,'CUS_INCOME','''INC''','Parameter to specify that the cash flow type is income. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(97,'CUS_EXPENSE','''EXP''','Parameter to specify that the cash flow type is expense. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(98,'CUS_SALARY_INCOME_TYPE','''SAL''','Parameter to specify that the income is monthly salary income of customer. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(99,'CUS_BUSINESS_INCOME_TYPE','''BUS''','Parameter to specify that the income is monthly business income of customer. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(100,'CUS_RENTAL_INCOME_TYPE','''REN''','Parameter to specify that the income is monthly rental income of customer. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(101,'CUS_REPLACEMENT_INCOME_TYPE','''REP''','Parameter to specify that the income is monthly replacement income of customer for example, unemployment income. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(102,'CUS_CHILD_ALLOWANCE_TYPE','''CAW''','Parameter to specify that the income is monthly child allowance income of customer. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(103,'CUS_RENTAL_TYPE','''REN''','Parameter to specify that the expense is for monthly rental charges of customer. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(104,'CUS_ALIMONY_TYPE','''ALM''','Parameter to specify that the expense is for monthly alimony charges of customer. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(105,'CUS_CHILD_SUPPORT_TYPE','''SUP''','Parameter to specify that the expense is for monthly child support charges of customer. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(106,'CUS_REPAYMENT_TYPE','''RPY''','Parameter to specify that the expense is for monthly repayment on existing loans with this bank for example mortgage, auto loan, consumer durable loan etc. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(107,'CUS_REPAYMENT_OTHERS_TYPE','''RPO''','Parameter to specify that the expense is for monthly repayment on existing loans with other bank for example mortgage, auto loan, consumer durable loan etc. The assumption is that same set of codes are used for applicant level and customer level cash flow')
			VALUES(108,'CUS_EMPLOY_STATUS_SELF','''OWN''','Parameter to specify whether the customer/applicant is self employed or not.For example if the value of Employment_status_cd is ''OWN'' then the self_employment_flag is set to &FLAG_TRUE else it is set to &FLAG_FALSE in the Customer/Applicant Dimension')
			VALUES(109,'CUS_RESIDENCE_STATUS_SELF','''OWN''','Parameter to specify whether the customer owns a residence property or not.For example if the value of Residence_status_cd is ''OWN'' then the Own_Residence_property_flg is set to &FLAG_TRUE else it is setto &FLAG_FALSE in the Customer Dimension')
			VALUES(110,'FLAG_FALSE','''N''','Parameter to assign the flag to false')
			VALUES(111,'FLAG_TRUE','''Y''','Parameter to assign the flag to true')
			VALUES(112,'SIGNATORY','''SIG''','Parameter to specify that the financial account role is that of a signatory. The assumption is that same set of codes are used for applicant level role and account level role.')
			VALUES(113,'GUARANTOR','''GRN''','Parameter to specify that the financial account role is that of a guarantor. The assumption is that same set of codes are used for applicant level role and account level role.')
			VALUES(114,'NOMINEE','''NOM''','Parameter to specify that the financial account role is that of a nominee. The assumption is that same set of codes are used for applicant level role and account level role.')
			VALUES(116,'COP_CUS_ADDRESS_TYPE','''MLG''','Parameter to specify the Address type for a corporate customer, for example mailing address, postal address.')
			VALUES(119,'CUS_STATUS_ACT','''ACT''','Parameter to specify that the customer is an active customer')
			VALUES(120,'ACCT_STATUS_ACT','''ACT''','Parameter to specify that the account is active')
			VALUES(134,'BEH_SEGMENT_SCHEME','''SCH1''','Parameter to specify that the segment scheme is of behavior type')
			VALUES(139,'ACCT_STATUS_INA','''INA''','Parameter to specify that the account is inactive')
			VALUES(140,'ACCT_STATUS_CLO','''CLO''','Parameter to specify that the account is closed')
			VALUES(141,'CROSS_SELL_CAMPAIGN_TYPE','''CRS''','Parameter to specify that the campaign is for cross sell')
			VALUES(142,'DIFFER_PAYMENT_STATUS','''DFR''','Parameter to specify that the last payment made is a different payment i.e. the payment made on or before the bill due date is greater than the minimum due amount and less than the bill amount. Applicable to line of credit products.')
			VALUES(144,'MTHS_TO_TRACK_UP_SELL','1','Parameter to specify the number of months to be tracked in history for up-sell event occurrence. For e.g. for current month this parameter should have value 1.')
			VALUES(147,'CUS_RET_MODEL','''CRM''','Parameter to specify the retention model at customer level')
			VALUES(148,'CROSS_SELL_MODEL','''CSM''','Parameter to specify the cross sell model')
			VALUES(150,'UP_SELL_MODEL','''UPS''','Parameter to specify the up sell model')
			VALUES(153,'SEC_APP_TYPE_CD','''SEC''','Secondary Applicant')
			VALUES(154,'CUS_INVESTMENT_TYPE','''INV''','Parameter to specify that the expense is for monthly investment. The assumption is that same set of codes are used for applicant level and customer level cash flow.')
			VALUES(155,'COR_PRD_TYPE','''COR''','Core Product')
			;
		quit;

		proc sql;
			insert into work.PARAMETER (
			PARAMETER_NO ,PARAMETER_NM ,PARAMETER_VALUE ,PARAMETER_DESC 
			)
			VALUES(158,'ACCT_STATUS_OPN','''OPN''','Parameter to specify that the account is open')
			VALUES(163,'PRODUCT_CLASS_ASSET','''AST''','Parameter to specify the value for asset product class code.')
			VALUES(164,'PRODUCT_CLASS_LIABILITY','''LBL''','Parameter to specify the value for liability product class code.')
			VALUES(170,'SUS_RESTRICTION_TYPE','''SUS''','Parameter to specify the suspended restriction type.')
			VALUES(184,'MTG_ACCOUNT_TYPE','''MTG''','Mortgage Account')
			VALUES(185,'LON_ACCOUNT_TYPE','''LON''','Loan Account')
			VALUES(188,'CCD_ACCOUNT_TYPE','''CCD''','Credit Card Account')
			VALUES(189,'COR_ACCOUNT_TYPE','''COR''','Core Account')
			VALUES(190,'SSL_ACCOUNT_SUB_TYPE','''SSL''','Salary Savings Account (sub type of core account)')
			VALUES(191,'CHK_ACCOUNT_SUB_TYPE','''CHK''','Checkings Account (sub type of core account)')
			VALUES(193,'TDP_ACCOUNT_SUB_TYPE','''TDP''','Term Deposit Account (sub type of core account)')
			VALUES(194,'RDP_ACCOUNT_SUB_TYPE','''RDP''','Recurring Deposit Account (sub type of core account)')
			VALUES(203,'SCORE_RANGE_TYPE','''SCR''','Parameter to specify that the pool scheme type is a score range type scheme')
			VALUES(204,'PD_RANGE_TYPE','''PDR''','Parameter to specify that the pool scheme type is a PD range type scheme')
			VALUES(205,'PD_LGD_RANGE_TYPE','''PLR''','Parameter to specify that the pool scheme type is a PD and LGD range type scheme')
			VALUES(206,'PD_LGD_CCF_RANGE_TYPE','''PLC''','Parameter to specify that the pool scheme type is a PD, LGD and CCF range type scheme')
			VALUES(207,'MAP_PD_MASTER_TYPE','''PMS''','Parameter to specify that the pool scheme type is a map PD to master scale type scheme')
			VALUES(208,'SEGMENT_TYPE','''SEG''','Parameter to specify that the pool scheme type is a segmentation type scheme')
			VALUES(209,'LONG_RUN_MAX_TYPE','''LRM''','Parameter to specify that the method used to assign a rate to the pool is the maximum of long run values method')
			VALUES(210,'LONG_RUN_AVG_TYPE','''LRA''','Parameter to specify that the method used to assign a rate to the pool is the average of long run values method')
			VALUES(211,'AVG_EST_TYPE','''AVG''','Parameter to specify that the method used to assign a rate to the pool is the average of estimated values method')
			VALUES(218,'SAV_ACCOUNT_SUB_TYPE','''SAV'', &SSL_ACCOUNT_SUB_TYPE','Savings Account (sub type of core account)')
			VALUES(228,'DIS_TRANS_TYPE','''DIS''','Parameter to specify that the transaction type denotes the dispute in transactions. This parameter is same as the parameter DIS_TRANS_TYPE, hence assign the same value for both parameters.')
			VALUES(304,'CREDIT_FLAG','''C''','Parameter to specify that the credit debit flag is set to credit card.')
			VALUES(305,'DEBIT_FLAG','''D''','Parameter to specify that the credit debit flag is set to debit card.')
			VALUES(332,'SEG_MODEL','''SEG''','Segmentation Model')
			VALUES(337,'DIR_EMP_ASSOC_TYPE','''DIR''','Parameter to specify the association type of employee for direct reporting purpose.')
			VALUES(339,'REV_ASSET_PRD_CAT_TYPE','''REV''','Parameter specify that the product category code is of revolving asset.')
			VALUES(340,'NRV_ASSET_PRD_CAT_TYPE','''NRV''','Parameter to specify that the product category code is of non revolving asset.')
			VALUES(341,'TDL_LIABILITY_PRD_CAT_TYPE','''TDL''','Parameter to specify that the product category code is of term liability.')
			VALUES(342,'DDL_LIABILITY_PRD_CAT_TYPE','''DDL''','Parameter to specify that the product category code is of demand liability.')
			VALUES(343,'STY_SEGMENT_TYPE','''STY'',''TCT''','Parameter to specify that the segment type is strategy.')
			VALUES(428,'MULTI_LOAD','''Y''','Parameter to specify whether the data loading is to be executed for multiple months')
			VALUES(443,'COL_TYPE_PRP','''PRP''','Parameter to specify collateral type to property')
			VALUES(507,'CORP_CUST_TYPE','''COR''','Corporate Customer')
			;
		quit;

		proc sql;
			insert into work.PARAMETER (
			PARAMETER_NO ,PARAMETER_NM ,PARAMETER_VALUE ,PARAMETER_DESC 
			)
			VALUES(649,'CHANNEL_CD_INN','''INN''','Parameter for DABT to specify value for code INN for column CHANNEL_CD')
			VALUES(651,'CHANNEL_CD_TLR','''TLR''','Parameter for DABT to specify value for code TLR for column CHANNEL_CD')
			VALUES(675,'CLOSE_REASON_CD_PPY','''PPY''','Parameter for DABT to specify value for code PPY for column CLOSE_REASON_CD')
			VALUES(691,'CUS_ACT_ACT','''ACT''','Parameter for DABT to specify value for code ACT for column CUS')
			VALUES(796,'MED_CD_CHQ','''CHQ''','Parameter for DABT to specify value for code CHQ for column MED_CD')
			VALUES(797,'MED_CD_CSH','''CSH''','Parameter for DABT to specify value for code CSH for column MED_CD')
			VALUES(937,'TRAN_TYP_DEP','''DEP''','Parameter for DABT to specify value for code DEP for column TRAN_TYP')
			VALUES(951,'TRAN_TYP_PUR','''PUR''','Parameter for DABT to specify value for code PUR for column TRAN_TYP')
			VALUES(958,'TRAN_TYP_WDR','''WDR''','Parameter for DABT to specify value for code WDR for column TRAN_TYP')
			VALUES(979,'IND_ACCT_LEVEL_CD','A','Parameter for DABT to specify value for code A for Account Level Code')
			VALUES(984,'ACC_HIER_RD','''RD''','Parameter for DABT to specify value for code RD for column ACC_HIER')
			VALUES(985,'ACC_HIER_SAV','''SAV''','Parameter for DABT to specify value for code SAV for column ACC_HIER')
			VALUES(986,'ACC_HIER_SLN','''SLN''','Parameter for DABT to specify value for code SLN for column ACC_HIER')
			;
		quit;

		proc sql;
			insert into work.PARAMETER (
			PARAMETER_NO ,PARAMETER_NM ,PARAMETER_VALUE ,PARAMETER_DESC 
			)
			VALUES(987,'ACC_HIER_TD','''TD''','Parameter for DABT to specify value for code TD for column ACC_HIER')
			VALUES(988,'ACC_HIER_ULN','''ULN''','Parameter for DABT to specify value for code ULN for column ACC_HIER')
			VALUES(989,'ACC_HIER_CHK','''CHK''','Parameter for DABT to specify value for code CHK for column ACC_HIER')
			VALUES(990,'ACC_HIER_CCD','''CCD''','Parameter for DABT to specify value for code CCD for column ACC_HIER')
			VALUES(991,'ACC_HIER_OD','''OD''','Parameter for DABT to specify value for code OD for column ACC_HIER')
			VALUES(1015,'TERM_TYPE_LNG','''LNG''','Parameter for DABT to specify value for code LNG for column  TERM_TYPE_LNG')
			VALUES(1016,'TERM_TYPE_MID','''MID''','Parameter for DABT to specify value for code MID for column TERM_TYPE_MID')
			VALUES(1017,'TERM_TYPE_SHT','''SHT''','Parameter for DABT to specify value for code SHT for column TERM_TYPE_SHT')
			VALUES(1032,'FM_DEBUG','''N''','Parameter to specify whether scrarch table for foundation mart will be deleted or not. N - tables will be deleted')
			VALUES(682,'COL_STS_AGY','''AGY''','Parameter for DABT to specify value for code AGY for column COL_STS')
			VALUES(1022,'LONG_TERM_DEP_MTH_CNT','6','Parameter for DABT to specify value of 6 for parameter LONG_TERM_DEP_MTH_CNT')
			VALUES(1020,'PRD_HIER_SLN','''SLN''','Parameter for DABT to specify value for code SLN for column PRODUCT_HIERARCHY_CD')
			VALUES(1021,'PRD_HIER_ULN','''ULN''','Parameter for DABT to specify value for code ULN for column PRODUCT_HIERARCHY_CD')
			VALUES(1023,'SHORT_TERM_DEP_MTH_CNT','3','Parameter for DABT to specify value of 3 for parameter SHORT_TERM_DEP_MTH_CNT')
			VALUES(10011,'ADD_COMMUNICATION_FACT_JOBS','Y','This parameter is set to decide,whether to include communication fact jobs such as inquiry_fact_outbound_comm_fact,contacts_fact etc.to parameter_list table')
			;
		quit;
		
		
	




/* i18NOK:END */

	
	proc sql;
	INSERT INTO APDM.PARAMETER_MASTER (PARAMETER_NM, PARAMETER_DESC,PARAM_ADDED_BY_PRODUCT_CD ,VALUE_EDITABLE_FLG, VALUE_TO_BE_SET_BY_SOLUTN_FLG,CREATED_DTTM, CREATED_BY_USER , LAST_PROCESSED_DTTM, LAST_PROCESSED_BY_USER) 
		SELECT distinct PARAMETER_NM,PARAMETER_DESC,'csbmid','N','Y',"%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID", "%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID"  /* i18NOK:LINE */
		FROM WORK.PARAMETER;
		quit;
	

	proc sql;
	INSERT INTO APDM.PARAMETER_VALUE_DTL(PARAMETER_NM, VALUE_APPLICABLE_TO_PRODUCT_CD, PARAMETER_VALUE,CREATED_DTTM, CREATED_BY_USER ,LAST_PROCESSED_DTTM, LAST_PROCESSED_BY_USER)
		SELECT distinct PARAMETER_NM,'csbmid',PARAMETER_VALUE,"%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID", "%SYSFUNC(DATETIME(),DATETIME.)"DT, "&SYSUSERID"  /* i18NOK:LINE */
		FROM WORK.PARAMETER;
		quit;

	
%mend rmcr_cr_check_parameter;
