CREATE TABLE &LIBREF..CREDIT_FACILITY_DIM (
     CREDIT_FACILITY_SK   NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Credit Facility Key',
     CREDIT_FACILITY_RK   NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Credit Facility Key',
     CREDIT_FACILITY_ID   VARCHAR(32) label='Credit Facility Identifier',
     CURRENCY_CD          VARCHAR(3) label='Currency Code',
     LIMIT_AMT            NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='Limit Amount',
     IN_DEFAULT_FLG       CHARACTER(1) label='In Default Flag',
     SPECIALIZED_LENDING_CD VARCHAR(3) label='Specialized Lending Code',
     SENIORITY_CD         VARCHAR(3) label='Seniority Code',
     CREDIT_FACILITY_TYPE_CD VARCHAR(3) label='Credit Facility Type Code',
     COMMITMENT_TYPE_CD   VARCHAR(3) label='Commitment Type Code',
     COUNTERPARTY_RK      NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Counterparty Key',
     OWNED_BY_INTERNAL_ORG_RK NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Owned By Internal Organization Key',
     SOURCE_SYSTEM_CD     VARCHAR(3) label='Source System Code',
     EFFECTIVE_MATURITY_YEAR_NO NUMERIC(8) label='Effective Maturity Year Number',
     EXPOSURE_AT_DEFAULT_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='Exposure At Default Amount',
     RESETS_TYPE_CD       VARCHAR(3) label='Resets Type Code',
     INTERNAL_REPORTING_CATEGORY_CD VARCHAR(3) label='Internal Reporting Category
Code',
     CUSTOMER_RK          NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Customer Key',
     NON_CUSTOMER_EXTERNAL_ORG_RK NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Non Customer External Organization
Key',
     NON_CUSTOMER_EXTERNAL_IND_RK NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Non Customer External Individual
Key',
     COUNTERPARTY_ID      VARCHAR(32) label='Counterparty Identifier',
     COUNTERPARTY_TYPE_CD VARCHAR(3) label='Counterparty Type Code',
     ECONOMIC_SECTOR_CD   VARCHAR(3) label='Economic Sector Code',
     HIGH_RISK_CATEGORY_FLG CHARACTER(1) label='High Risk Category Flag',
     COUNTERPARTY_LEGAL_TYPE_CD VARCHAR(3) label='Counterparty Legal Type Code',
     CORE_MKT_PARTICIPANT_FLG CHARACTER(1) label='Core Market Participant Flag',
     COUNTERPARTY_RLN_TYPE_CD VARCHAR(3) label='Counterparty Relationship Type
Code',
     CPTY_INTERNAL_RPT_CATEGORY_CD VARCHAR(3) label='Internal Reporting Category
Code',
     VALID_START_DTTM     DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     VALID_END_DTTM       DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     PROCESSED_DTTM       DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     CREATED_BY           VARCHAR(20) label='CREATED_BY',
     CREATED_DTTM         DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     CONSTRAINT PRIM_KEY PRIMARY KEY (CREDIT_FACILITY_SK)
);

