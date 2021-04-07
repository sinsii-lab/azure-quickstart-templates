CREATE TABLE &LIBREF..APPLICATION_DIM (
/* I18NOK:BEGIN */
     APPLICATION_SK       NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     APPLICATION_RK       NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     EXTERNAL_ORG_RK      NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     PRODUCT_RK           NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     CUSTOMER_RK          NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     APPLICATION_ID       VARCHAR(32) label='_RKorSK',
     APPLICATION_DT       DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CUSTOMER_TYPE_CD     VARCHAR(3) label='_CD',
     PURPOSE_CD           VARCHAR(3) label='_CD',
     APPLICANTS_CNT       NUMERIC(6) label='APPLICANTS_CNT',
     GUARANTORS_CNT       NUMERIC(6) label='GUARANTORS_CNT',
     NOMINEES_CNT         NUMERIC(6) label='NOMINEES_CNT',
     SIGNATORIES_CNT      NUMERIC(6) label='SIGNATORIES_CNT',
     LINKED_ACCOUNT_FLG   CHARACTER(1) label='LINKED_ACCOUNT_FLG',
     LINKED_DEPOSIT_ACCOUNT_FLG CHARACTER(1) label='LINKED_DEPOSIT_ACCOUNT_FLG',
     PRIOR_MORTGAGE_CNT   NUMERIC(6) label='PRIOR_MORTGAGE_CNT',
     LOAN_TERM_MTHS_CNT   NUMERIC(6) label='LOAN_TERM_MTHS_CNT',
     APPLIED_AMT          NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     LOAN_SECURED_FLG     CHARACTER(1) label='LOAN_SECURED_FLG',
     CREDIT_CARD_CONSOLIDATION_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MORTGAGE_CONSOLIDATION_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     LOAN_CONSOLIDATION_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     CREDIT_CARD_CONSOLIDATION_CNT NUMERIC(6) label='CREDIT_CARD_CONSOLIDATION_CNT',
     MORTGAGE_CONSOLIDATION_CNT NUMERIC(6) label='MORTGAGE_CONSOLIDATION_CNT',
     LOAN_CONSOLIDATION_CNT NUMERIC(6) label='LOAN_CONSOLIDATION_CNT',
     SAVINGS_IN_CONSOLIDATION_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     COLLATERAL_CD        VARCHAR(3) label='_CD',
     COLLATERAL_AMT       NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     BORROWED_DOWNPAYMENT_FLG CHARACTER(1) label='BORROWED_DOWNPAYMENT_FLG',
     BORROWED_DOWNPAYMENT_PCT NUMERIC(9,4) FORMAT=NLNUM9.4 INFORMAT=NLNUM9.4 label='BORROWED_DOWNPAYMENT_PCT',
     FORECLOSED_FLG       CHARACTER(1) label='FORECLOSED_FLG',
     CLLTRL_MTHLY_INS_PREM_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     CLLTRL_ONE_TIME_INS_PREM_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     CLLTRL_INS_START_DT  DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CLLTRL_INS_END_DT    DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     OUTCOME_CD           VARCHAR(3) label='_CD',
     OUTCOME_DT           DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     DECISION_CD          VARCHAR(3) label='_CD',
     DECISION_OVERRIDE_FLG CHARACTER(1) label='DECISION_OVERRIDE_FLG',
     OVERRIDE_REASON_CD   VARCHAR(3) label='_CD',
     CHANNEL_CD           VARCHAR(3) label='_CD',
     PROCESSING_CHARGES_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     SOURCE_SYSTEM_CD     VARCHAR(3) label='_CD',
     VALID_START_DTTM     DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     VALID_END_DTTM       DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     PROCESSED_DTTM       DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     CREATED_BY           VARCHAR(20) label='CREATED_BY',
     CREATED_DTTM         DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     SOURCE_CD            VARCHAR(5) label='SOURCE_CD',
     COUNTRY_CD           VARCHAR(3) label='COUNTRY_CD',
     INTERNAL_ORG_RK      NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='INTERNAL_ORG_RK',	/* I18NOK:END */
     CONSTRAINT PRIM_KEY PRIMARY KEY (APPLICATION_SK)
);

