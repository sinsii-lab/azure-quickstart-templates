*==========================================================;

* This file contains all the configuration related to Banking

*==========================================================;



*==========================================================;

* Pooling Specific Tables ;

*==========================================================;

%Macro rmcr_bank_insert_script_adm_ext;

proc sql;

insert into &lib_apdm..POOL_SCHEME_TYPE_MASTER (

POOL_SCHEME_TYPE_NO ,POOL_SCHEME_TYPE_CD_PARAM ,POOL_SCHEME_TYPE_DESC 

)

VALUES( 1, 'SCORE_RANGE_TYPE', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHEME_TYPE_MASTER.POOL_SCHEME_TYPE_DS1.1, noquote))")

VALUES( 2, 'PD_RANGE_TYPE', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHEME_TYPE_MASTER.POOL_SCHEME_TYPE_DS2.1, noquote))")

VALUES( 3, 'PD_LGD_RANGE_TYPE', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHEME_TYPE_MASTER.POOL_SCHEME_TYPE_DS3.1, noquote))")

VALUES( 4, 'PD_LGD_CCF_RANGE_TYPE', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHEME_TYPE_MASTER.POOL_SCHEME_TYPE_DS4.1, noquote))")

VALUES( 5, 'MAP_PD_MASTER_TYPE', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHEME_TYPE_MASTER.POOL_SCHEME_TYPE_DS5.1, noquote))")

;

quit;



/* i18nOK:BEGIN */



proc sql;

insert into &lib_apdm..POOL_SCHEME_COL_MASTER (

pool_scheme_type_cd_param ,attribute_name ,display_pane_level ,display_name ,attribute_type ,level ,display_flag 

)

VALUES('MAP_PD_MASTER_TYPE','ASSIGNED_PD',2,'Assigned-PD','Double',7,1)

VALUES('MAP_PD_MASTER_TYPE','createdBy',1,'createdBy','Character',17,1)

VALUES('MAP_PD_MASTER_TYPE','createdDate',1,'createdDate','Double',10,1)

VALUES('MAP_PD_MASTER_TYPE','description',1,'description','Character',3,1)

VALUES('MAP_PD_MASTER_TYPE','FROM_PD_NO',2,'PD Start','Double',2,1)

VALUES('MAP_PD_MASTER_TYPE','ksValue',1,'ksValue','Double',5,1)

VALUES('MAP_PD_MASTER_TYPE','lockedBy',1,'lockedBy','Character',18,1)

VALUES('MAP_PD_MASTER_TYPE','masterScaleMapingMethod',1,'masterScaleMapingMethod','Character',.,0)

VALUES('MAP_PD_MASTER_TYPE','masterScaleMappingMethodTypeDesc',1,'masterScaleMappingMethodTypeDesc','Character',13,1)

VALUES('MAP_PD_MASTER_TYPE','masterScaleNumber',1,'masterScaleNumber','Integer',12,1)

VALUES('MAP_PD_MASTER_TYPE','NO_OF_RECORDS',2,'No Of Records','Integer',5,1)

VALUES('MAP_PD_MASTER_TYPE','obsDate',1,'obsDate','Double',.,0)

VALUES('MAP_PD_MASTER_TYPE','pdMethodType',1,'pdMethodType','Character',.,0)

VALUES('MAP_PD_MASTER_TYPE','pdMethodTypeDesc',1,'pdMethodTypeDesc','Character',8,1)

VALUES('MAP_PD_MASTER_TYPE','pdModelName',1,'pdModelName','Character',7,1)

VALUES('MAP_PD_MASTER_TYPE','poolSchemeNo',1,'poolSchemeNo','Double',1,1)

VALUES('MAP_PD_MASTER_TYPE','poolSchemeTypeCodeParam',1,'poolSchemeTypeCodeParam','Character',14,1)

VALUES('MAP_PD_MASTER_TYPE','poolSchemeTypeCodeValue',1,'poolSchemeTypeCodeValue','Character',4,0)

VALUES('MAP_PD_MASTER_TYPE','POOL_NAME',2,'Description','Character',4,1)

VALUES('MAP_PD_MASTER_TYPE','POOL_SEQ_NO',2,'Pool Sequence No','Integer',1,1)

VALUES('MAP_PD_MASTER_TYPE','RECORDS_PERCENTAGE',2,'% Proportion','Double',6,1)

VALUES('MAP_PD_MASTER_TYPE','stabilityValue',1,'stabilityValue','Double',6,1)

VALUES('MAP_PD_MASTER_TYPE','statusCode',1,'statusCode','Character',.,0)

VALUES('MAP_PD_MASTER_TYPE','statusDesc',1,'statusDesc','Character',9,1)

VALUES('MAP_PD_MASTER_TYPE','TO_PD_NO',2,'PD End','Double',3,1)

VALUES('MASTER_SCALE','Created',1,'Created','Datetime',3,1)

VALUES('MASTER_SCALE','description',1,'Description','Character',2,1)

VALUES('MASTER_SCALE','lastModifiedDate',1,'Last Modified','Datetime',4,1)

VALUES('MASTER_SCALE','linkToModelFlag',1,'LinkToModelFlag','Integer',5,1)

VALUES('MASTER_SCALE','masterScaleNumber',1,'Master Scale No','Integer',.,0)

VALUES('MASTER_SCALE','MASTER_SCALE_NAME',1,'Name','Character',1,1)

VALUES('MASTER_SCALE','MS_GRADE_NAME',2,'Grade Name','Character',2,1)

VALUES('MASTER_SCALE','MS_GRADE_PD_NO',2,'Grade PD','Double',4,1)

VALUES('MASTER_SCALE','MS_RATING_GRADE_CD',2,'Rating Grade Code','Character',3,1)

VALUES('MASTER_SCALE','MS_SEQ_NO',2,'MasterScaleSequenceNumber','Integer',1,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','ASSIGNED_CCF',2,'Assigned-CCF','Double',13,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','ASSIGNED_LGD',2,'Assigned-LGD','Double',12,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','ASSIGNED_PD',2,'Assigned-PD','Double',11,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','ccfMethodType',1,'ccfMethodType','Character',.,0)

VALUES('PD_LGD_CCF_RANGE_TYPE','ccfMethodTypeDesc',1,'ccfMethodTypeDesc','Character',12,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','ccfModelName',1,'ccfModelName','Character',9,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','createdBy',1,'createdBy','Character',15,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','createdDate',1,'createdDate','Double',14,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','description',1,'description','Character',3,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','FROM_CCF_NO',2,'CCF Start','Double',6,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','FROM_LGD_NO',2,'LGD Start','Double',4,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','FROM_PD_NO',2,'PD Start','Double',2,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','ksValue',1,'ksValue','Double',5,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','lgdMethodType',1,'lgdMethodType','Character',.,0)

;

quit;

proc sql;

insert into &lib_apdm..POOL_SCHEME_COL_MASTER (

pool_scheme_type_cd_param ,attribute_name ,display_pane_level ,display_name ,attribute_type ,level ,display_flag 

)

VALUES('PD_LGD_CCF_RANGE_TYPE','lgdMethodTypeDesc',1,'lgdMethodTypeDesc','Character',11,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','lgdModelName',1,'lgdModelName','Character',8,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','lockedBy',1,'lockedBy','Character',17,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','NO_OF_RECORDS',2,'No Of Records','Integer',9,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','obsDate',1,'obsDate','Double',.,0)

VALUES('PD_LGD_CCF_RANGE_TYPE','pdMethodType',1,'pdMethodType','Character',.,0)

VALUES('PD_LGD_CCF_RANGE_TYPE','pdMethodTypeDesc',1,'pdMethodTypeDesc','Character',10,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','pdModelName',1,'pdModelName','Character',7,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','poolSchemeNo',1,'poolSchemeNo','Double',1,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','poolSchemeTypeCodeParam',1,'poolSchemeTypeCodeParam','Character',16,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','poolSchemeTypeCodeValue',1,'poolSchemeTypeCodeValue','Character',4,0)

VALUES('PD_LGD_CCF_RANGE_TYPE','POOL_NAME',2,'Description','Character',8,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','POOL_SEQ_NO',2,'Pool Sequence No','Integer',1,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','RECORDS_PERCENTAGE',2,'% Proportion','Double',10,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','stabilityValue',1,'stabilityValue','Double',6,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','statusCode',1,'statusCode','Character',.,0)

VALUES('PD_LGD_CCF_RANGE_TYPE','statusDesc',1,'statusDesc','Character',13,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','TO_CCF_NO',2,'CCF End','Double',7,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','TO_LGD_NO',2,'LGD End','Double',5,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','TO_PD_NO',2,'PD End','Double',3,1)

VALUES('PD_LGD_RANGE_TYPE','ASSIGNED_LGD',2,'Assigned-LGD','Double',10,1)

VALUES('PD_LGD_RANGE_TYPE','ASSIGNED_PD',2,'Assigned-PD','Double',9,1)

VALUES('PD_LGD_RANGE_TYPE','createdBy',1,'createdBy','Character',13,1)

VALUES('PD_LGD_RANGE_TYPE','createdDate',1,'createdDate','Double',12,1)

VALUES('PD_LGD_RANGE_TYPE','description',1,'description','Character',3,1)

VALUES('PD_LGD_RANGE_TYPE','FROM_LGD_NO',2,'LGD Start','Double',4,1)

VALUES('PD_LGD_RANGE_TYPE','FROM_PD_NO',2,'PD Start','Double',2,1)

VALUES('PD_LGD_RANGE_TYPE','ksValue',1,'ksValue','Double',5,1)

VALUES('PD_LGD_RANGE_TYPE','lgdMethodType',1,'lgdMethodType','Character',.,0)

VALUES('PD_LGD_RANGE_TYPE','lgdMethodTypeDesc',1,'lgdMethodTypeDesc','Character',10,1)

VALUES('PD_LGD_RANGE_TYPE','lgdModelName',1,'lgdModelName','Character',8,1)

VALUES('PD_LGD_RANGE_TYPE','lockedBy',1,'lockedBy','Character',17,1)

VALUES('PD_LGD_RANGE_TYPE','NO_OF_RECORDS',2,'No Of Records','Integer',7,1)

VALUES('PD_LGD_RANGE_TYPE','obsDate',1,'obsDate','Double',.,0)

VALUES('PD_LGD_RANGE_TYPE','pdMethodType',1,'pdMethodType','Character',.,0)

VALUES('PD_LGD_RANGE_TYPE','pdMethodTypeDesc',1,'pdMethodTypeDesc','Character',9,1)

VALUES('PD_LGD_RANGE_TYPE','pdModelName',1,'pdModelName','Character',7,1)

VALUES('PD_LGD_RANGE_TYPE','poolSchemeNo',1,'poolSchemeNo','Double',1,1)

VALUES('PD_LGD_RANGE_TYPE','poolSchemeTypeCodeParam',1,'poolSchemeTypeCodeParam','Character',14,1)

VALUES('PD_LGD_RANGE_TYPE','poolSchemeTypeCodeValue',1,'poolSchemeTypeCodeValue','Character',4,0)

VALUES('PD_LGD_RANGE_TYPE','POOL_NAME',2,'Description','Character',6,1)

VALUES('PD_LGD_RANGE_TYPE','POOL_SEQ_NO',2,'Pool Sequence No','Integer',1,1)

VALUES('PD_LGD_RANGE_TYPE','RECORDS_PERCENTAGE',2,'% Proportion','Double',8,1)

VALUES('PD_LGD_RANGE_TYPE','stabilityValue',1,'stabilityValue','Double',6,1)

VALUES('PD_LGD_RANGE_TYPE','statusCode',1,'statusCode','Character',.,0)

VALUES('PD_LGD_RANGE_TYPE','statusDesc',1,'statusDesc','Character',11,1)

VALUES('PD_LGD_RANGE_TYPE','TO_LGD_NO',2,'LGD End','Double',5,1)

VALUES('PD_LGD_RANGE_TYPE','TO_PD_NO',2,'PD End','Double',3,1)

VALUES('PD_RANGE_TYPE','ASSIGNED_PD',2,'Assigned-PD','Double',7,1)

VALUES('PD_RANGE_TYPE','createdBy',1,'createdBy','Character',11,1)

;

quit;

proc sql;

insert into &lib_apdm..POOL_SCHEME_COL_MASTER (

pool_scheme_type_cd_param ,attribute_name ,display_pane_level ,display_name ,attribute_type ,level ,display_flag 

)

VALUES('PD_RANGE_TYPE','createdDate',1,'createdDate','Double',10,1)

VALUES('PD_RANGE_TYPE','description',1,'description','Character',3,1)

VALUES('PD_RANGE_TYPE','FROM_PD_NO',2,'PD Start','Double',2,1)

VALUES('PD_RANGE_TYPE','ksValue',1,'ksValue','Double',5,1)

VALUES('PD_RANGE_TYPE','lockedBy',1,'lockedBy','Character',17,1)

VALUES('PD_RANGE_TYPE','NO_OF_RECORDS',2,'No Of Records','Integer',5,1)

VALUES('PD_RANGE_TYPE','obsDate',1,'obsDate','Double',.,0)

VALUES('PD_RANGE_TYPE','pdMethodType',1,'pdMethodType','Character',.,0)

VALUES('PD_RANGE_TYPE','pdMethodTypeDesc',1,'pdMethodTypeDesc','Character',8,1)

VALUES('PD_RANGE_TYPE','pdModelName',1,'pdModelName','Character',7,1)

VALUES('PD_RANGE_TYPE','poolSchemeNo',1,'poolSchemeNo','Double',1,1)

VALUES('PD_RANGE_TYPE','poolSchemeTypeCodeParam',1,'poolSchemeTypeCodeParam','Character',12,1)

VALUES('PD_RANGE_TYPE','poolSchemeTypeCodeValue',1,'poolSchemeTypeCodeValue','Character',4,0)

VALUES('PD_RANGE_TYPE','POOL_NAME',2,'Description','Character',4,1)

VALUES('PD_RANGE_TYPE','POOL_SEQ_NO',2,'Pool Sequence No','Integer',1,1)

VALUES('PD_RANGE_TYPE','RECORDS_PERCENTAGE',2,'% Proportion','Double',6,1)

VALUES('PD_RANGE_TYPE','stabilityValue',1,'stabilityValue','Double',6,1)

VALUES('PD_RANGE_TYPE','statusCode',1,'statusCode','Character',.,0)

VALUES('PD_RANGE_TYPE','statusDesc',1,'statusDesc','Character',9,1)

VALUES('PD_RANGE_TYPE','TO_PD_NO',2,'PD End','Double',3,1)

VALUES('SCORE_RANGE_TYPE','ASSIGNED_PD',2,'Assigned PD','Double',7,1)

VALUES('SCORE_RANGE_TYPE','createdBy',1,'createdBy','Character',11,1)

VALUES('SCORE_RANGE_TYPE','createdDate',1,'createdDate','Double',10,1)

VALUES('SCORE_RANGE_TYPE','description',1,'description','Character',3,1)

VALUES('SCORE_RANGE_TYPE','FROM_SCORE_NO',2,'Score Start','Double',2,1)

VALUES('SCORE_RANGE_TYPE','ksValue',1,'ksValue','Double',5,1)

VALUES('SCORE_RANGE_TYPE','lockedBy',1,'lockedBy','Character',17,1)

VALUES('SCORE_RANGE_TYPE','NO_OF_RECORDS',2,'No Of Records','Integer',5,1)

VALUES('SCORE_RANGE_TYPE','obsDate',1,'obsDate','Double',.,0)

VALUES('SCORE_RANGE_TYPE','pdMethodType',1,'pdMethodType','Character',.,0)

VALUES('SCORE_RANGE_TYPE','pdMethodTypeDesc',1,'pdMethodTypeDesc','Character',8,1)

VALUES('SCORE_RANGE_TYPE','pdModelName',1,'pdModelName','Character',7,1)

VALUES('SCORE_RANGE_TYPE','poolSchemeNo',1,'poolSchemeNo','Double',1,1)

VALUES('SCORE_RANGE_TYPE','poolSchemeTypeCodeParam',1,'poolSchemeTypeCodeParam','Character',12,1)

VALUES('SCORE_RANGE_TYPE','poolSchemeTypeCodeValue',1,'poolSchemeTypeCodeValue','Character',4,0)

VALUES('SCORE_RANGE_TYPE','POOL_NAME',2,'Description','Character',4,1)

VALUES('SCORE_RANGE_TYPE','POOL_SEQ_NO',2,'Pool Sequence No','Integer',1,1)

VALUES('SCORE_RANGE_TYPE','RECORDS_PERCENTAGE',2,'% Proportion','Double',6,1)

VALUES('SCORE_RANGE_TYPE','stabilityValue',1,'stabilityValue','Double',6,1)

VALUES('SCORE_RANGE_TYPE','statusCode',1,'statusCode','Character',.,0)

VALUES('SCORE_RANGE_TYPE','statusDesc',1,'statusDesc','Character',9,1)

VALUES('SCORE_RANGE_TYPE','TO_SCORE_NO',2,'Score End','Double',3,1)

VALUES('MAP_PD_MASTER_TYPE','name',1,'poolSchemeName','Character',2,1)

VALUES('PD_LGD_CCF_RANGE_TYPE','name',1,'poolSchemeName','Character',2,1)

VALUES('PD_LGD_RANGE_TYPE','name',1,'poolSchemeName','Character',2,1)

VALUES('PD_RANGE_TYPE','name',1,'poolSchemeName','Character',2,1)

VALUES('SCORE_RANGE_TYPE','name',1,'poolSchemeName','Character',2,1)

;

quit;

/* i18nOK:END */



proc sql;

insert into &lib_apdm..POOL_SCHEME_TYPE_X_METHOD (

POOL_SCHEME_TYPE_NO ,MODEL_TYPE_CD ,POOL_SCHEME_METHOD_TYPE_CD ,POOL_SCHEME_METHOD_TYPE_DESC,POOL_SCHEME_TYPE_CD_PARAM 

)

VALUES( 1, 'PD_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS1.1, noquote))",'SCORE_RANGE_TYPE') /* i18nOK:Line */

VALUES( 1, 'PD_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS2.1, noquote))",'SCORE_RANGE_TYPE') /* i18nOK:Line */

VALUES( 1, 'PD_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS3.1, noquote))",'SCORE_RANGE_TYPE') /* i18nOK:Line */

VALUES( 1, 'PD_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS4.1, noquote))",'SCORE_RANGE_TYPE') /* i18nOK:Line */

VALUES( 2, 'PD_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS5.1, noquote))",'PD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 2, 'PD_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS6.1, noquote))",'PD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 2, 'PD_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS7.1, noquote))",'PD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 2, 'PD_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS8.1, noquote))",'PD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'LGD_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS9.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'LGD_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS10.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'LGD_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS11.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'LGD_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS12.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'PD_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS13.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'PD_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS14.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'PD_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS15.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 3, 'PD_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS16.1, noquote))",'PD_LGD_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'CCF_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS17.1, noquote))",'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'CCF_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS18.1, noquote))",'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'CCF_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS19.1, noquote))",'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

;

quit;



proc sql;

insert into &lib_apdm..POOL_SCHEME_TYPE_X_METHOD (

POOL_SCHEME_TYPE_NO ,MODEL_TYPE_CD ,POOL_SCHEME_METHOD_TYPE_CD ,POOL_SCHEME_METHOD_TYPE_DESC, POOL_SCHEME_TYPE_CD_PARAM

)

VALUES( 4, 'CCF_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS20.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'LGD_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS21.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'LGD_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS22.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'LGD_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS23.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'LGD_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS24.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'PD_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS25.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'PD_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS26.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'PD_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS27.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 4, 'PD_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS28.1, noquote))", 'PD_LGD_CCF_RANGE_TYPE') /* i18nOK:Line */

VALUES( 5, 'PD_MODEL', 'AVG', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS29.1, noquote))",'MAP_PD_MASTER_TYPE') /* i18nOK:Line */

VALUES( 5, 'PD_MODEL', 'LRA', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS30.1, noquote))",'MAP_PD_MASTER_TYPE') /* i18nOK:Line */

VALUES( 5, 'PD_MODEL', 'LRM', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS31.1, noquote))",'MAP_PD_MASTER_TYPE') /* i18nOK:Line */

VALUES( 5, 'PD_MODEL', 'MAX', /* i18nOK:Line */

"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOL_SCHM_TYPE_X_METHOD.POOL_SCHEME_METHOD_TYPE_DS32.1, noquote))",'MAP_PD_MASTER_TYPE') /* i18nOK:Line */

;

quit;



proc sql;

insert into &lib_apdm..MS_MAPPING_TYPE_MASTER (

	MS_MAPPING_TYPE_NO,	MS_MAPPING_TYPE_CD,	MS_MAPPING_TYPE_SHORT_NM,	MS_MAPPING_TYPE_DESC

)

values (1,	'DIS',	'Distance',	"%sysfunc(sasmsg(smd_ds.bismsg,MASTER_SCALE_MAPPING_METHOD.DIS, noquote))") /* i18nOK:Line */

values (2,	'AVG',	'Average',	"%sysfunc(sasmsg(smd_ds.bismsg,MASTER_SCALE_MAPPING_METHOD.AVG, noquote))") /* i18nOK:Line */

;



quit;



/* START : New Pooling Tables release CSB53 onwards */



proc sql;



insert into &lib_apdm..POOLING_METHOD_MASTER (

	POOLING_METHOD_SK,	POOLING_METHOD_CD,	POOLING_METHOD_SHORT_NM,	POOLING_METHOD_DESC

)

values (1,	'MATRIX',	'MATRIX',	'Matrix Based Pooling') /* i18nOK:Line */

values (2,	'TREE',	'TREE',	'Tree Based Pooling') /* i18nOK:Line */

;



quit;



proc sql;

insert into &lib_apdm..POOLING_CHRSTC_SRC_TYPE_MASTER

(

	CHRSTC_SOURCE_TYPE_SK,	CHRSTC_SOURCE_TYPE_CD,	CHRSTC_SOURCE_TYPE_SHORT_NM,	CHRSTC_SOURCE_TYPE_DESC

)

values (1,	'ABT',	'ABT from application',	'ABT from application') /* i18nOK:Line */

values (2,	'EXTSRC',	'External Data Source',	'External Data Source') /* i18nOK:Line */

;



quit;



proc sql;



insert into &lib_apdm..POOLING_CHRSTC_USAGE_TYPE_MASTER

(

	CHRSTC_USAGE_TYPE_SK,	CHRSTC_USAGE_TYPE_CD,	CHRSTC_USAGE_TYPE_SHORT_NM,	CHRSTC_USAGE_TYPE_DESC

)

values (1,	'BINNING',	'BINNING',	'Variables Used for creation for Pool Ranges') /* i18nOK:Line */

values (2,	'OUTCOME',	'OUTCOME',	'Variables Used for calculating output measures of Pool') /* i18nOK:Line */

;



quit;



proc sql;



insert into &lib_apdm..POOLING_CHRSTC_SCALE_TYPE_MASTER

(

	CHRSTC_MSR_SCALE_TYPE_SK,	CHRSTC_MSR_SCALE_TYPE_CD,	CHRSTC_MSR_SCALE_TYPE_SHORT_NM,	CHRSTC_MSR_SCALE_TYPE_DESC

)

values (1,	'NOMINAL',	'Nominal',	'Discrete data values') /* i18nOK:Line */

values (2,	'INTERVAL',	'Interval',	'Continuous data values') /* i18nOK:Line */

;



quit;



/* i18nOK:BEGIN */

proc sql;

insert into &lib_apdm..POOLING_CHRSTC_CALC_TYPE_MASTER

(

	CHRSTC_CALC_TYPE_SK,	CHRSTC_CALC_TYPE_CD,	CHRSTC_CALC_TYPE_SHORT_NM,	CHRSTC_CALC_TYPE_DESC

)

values (1,	'AVG',	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_SM1.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_DS1.1, noquote))" )

values (2,	'MAX',	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_SM2.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_DS2.1, noquote))" )

values (3,	'LRA',	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_SM3.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_DS3.1, noquote))" )

values (4,	'LRM',	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_SM4.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_CHRSTC_CALC_TYPE_MASTER.CHRSTC_CALC_TYPE_DS4.1, noquote))" )

;

quit;



proc sql;

insert into &lib_apdm..POOLING_IMPLICIT_CHRSTC_MASTER

(

	IMPLICIT_CHRSTC_SK,	IMPLICIT_CHRSTC_CD,	IMPLICIT_CHRSTC_MSR_SCL_TYPE_SK, IMPLICIT_CHRSTC_USAGE_TYPE_SK,	IMPLICIT_CHRSTC_COLUMN_NM,	IMPLICIT_CHRSTC_SHORT_NM,	IMPLICIT_CHRSTC_DESC,	IMPLICIT_CHRSTC_DATA_TYPE_SK,	IS_LOWER_BOUNDED,	LOWER_LIMIT,	IS_UPPER_BOUNDED,	UPPER_LIMIT

)

values (1,	'PD',	2,	1,	'ESTIMATED_PD',		"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_SM1.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_DS1.1, noquote))",	1,	1,	0,	1,	1)

values (2,	'LGD',	2,	1,	'ESTIMATED_LGD',	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_SM2.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_DS2.1, noquote))",	1,	0,	.,	0,	.)

values (3,	'CCF',	2,	1,	'ESTIMATED_CCF',	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_SM3.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_DS3.1, noquote))",	1,	0,	.,	0,	.)

values (4,	'SCR',	2,	1,	'SCORE_POINTS',		"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_SM4.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_DS4.1, noquote))",	3,	0,	.,	0,	.)

values (5,	'PD',	2,	2,	'EXPECTED_PD',		"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_SM5.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_DS5.1, noquote))",	1,	1,	0,	1,	1)

values (6,	'LGD',	2,	2,	'EXPECTED_LGD',		"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_SM6.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_DS6.1, noquote))",	1,	0,	.,	0,	.)

values (7,	'CCF',	2,	2,	'EXPECTED_CCF',		"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_SM7.1, noquote))",	"%sysfunc(sasmsg(work.rmcr_message_dtl_banking,POOLING_IMPLICIT_CHRSTC_MASTER.IMPLICIT_CHRSTC_DS7.1, noquote))",	1,	0,	.,	0,	.)

;

quit;



/* i18nOK:END */



proc sql;

insert into &lib_apdm..POOLING_IMPLCT_CHRSTC_X_SRC_TYPE

(

	IMPLICIT_CHRSTC_SK,	IMPLICIT_CHRSTC_SOURCE_TYPE_SK

)

values (1,	1)

values (2,	1)

values (3,	1)

values (4,	1)

values (5,	1)

values (6,	1)

values (7,	1)

values (5,	2)

values (6,	2)

values (7,	2)

;



quit;



/* END : New Pooling Tables release CSB53 onwards */





*==========================================================;

* CSMART Specific Tables ;

*==========================================================;



proc sql;

insert into &lib_apdm..PRODUCT_RECOVERY_CONFIG (

PRODUCT_TYPE_CD_PARAM ,RECOVERY_PERIOD ,RECOVERY_PERIOD_DAYS ,RECOVERY_PERIOD_WEEKS ,ADJUSTMENT_PERIOD ,ADJUSTMENT_PERIOD_DAYS ,

ADJUSTMENT_PERIOD_WEEKS ,CCF_OUTCOME_PERIOD ,CCF_OUTCOME_PERIOD_DAYS ,CCF_OUTCOME_PERIOD_WEEKS 

)

VALUES('MTG_PRD_TYPE',12,365,52,1,30,4,.,.,.) /* i18nOK:Line */

VALUES('CCD_PRD_TYPE',12,365,52,1,30,4,12,365,52) /* i18nOK:Line */

VALUES('LON_PRD_TYPE',12,365,52,1,30,4,.,.,.) /* i18nOK:Line */

VALUES('CREDIT_FACILITY_LOC',12,365,52,1,30,4,.,.,.) /* i18nOK:Line */

VALUES('CREDIT_FACILITY_WC',12,365,52,1,30,4,.,.,.) /* i18nOK:Line */

VALUES('CREDIT_FACILITY_CC',12,365,52,1,30,4,12,365,52) /* i18nOK:Line */

VALUES('COR_PRD_TYPE',12,365,52,1,30,4,.,.,.) /* i18nOK:Line */

;

quit;



proc sql;

insert into &lib_apdm..SUBJECT_GROUP_X_PRODUCT_TYPE (

SUBJECT_GROUP_SK ,PARAM_NM_MODEL_PROD_TYPE_CD ,CREATED_DTTM ,CREATED_BY_USER ,LAST_PROCESSED_DTTM ,LAST_PROCESSED_BY_USER 

)

VALUES(1,'LON_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(2,'CCD_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(3,'MTG_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(4,'COR_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(7,'LON_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(8,'MTG_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(9,'CCD_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

/*

VALUES(11,'LON_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") 

VALUES(12,'MTG_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") 

VALUES(13,'CCD_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") 

VALUES(14,'CCD_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") 

VALUES(15,'LON_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") 

VALUES(16,'MTG_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") 
*/

VALUES(18,'CREDIT_FACILITY_LOC',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(19,'CREDIT_FACILITY_WC',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(20,'CREDIT_FACILITY_CC',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(21,'LON_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(22,'MTG_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(24,'LON_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

VALUES(25,'MTG_PRD_TYPE',"%sysfunc(datetime(),datetime.)"dt,"&sysuserid","%sysfunc(datetime(),datetime.)"dt,"&sysuserid") /* i18nOK:Line */

;

quit;

%mend rmcr_bank_insert_script_adm_ext;