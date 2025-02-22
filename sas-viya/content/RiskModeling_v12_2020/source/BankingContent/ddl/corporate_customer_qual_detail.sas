CREATE TABLE &LIBREF..CORPORATE_CUSTOMER_QUAL_DETAIL (
/* I18NOK:BEGIN */
     CUSTOMER_RK          NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='Customer Key',
     VALID_START_DTTM     DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='Valid From Datetime',
     VALID_END_DTTM       DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='Valid To Datetime',
     EXTERNAL_ORG_RK      NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='External Org Key',
     SECTOR_MATURITY_MEASURE_CD VARCHAR(3) label='Sector Maturity Measure Code',
     COMPANY_MATURITY_MEASURE_CD VARCHAR(3) label='Company Maturity Measure Code',
     MANAGEMENT_QUALITY_MEASURE_CD VARCHAR(3) label='Management Quality Measure Code',
     BAL_SHEET_QUALITY_MEASURE_CD VARCHAR(3) label='Balance Sheet Quality Measure Code',
     COMPETITIVE_ENVRNMT_MEASURE_CD VARCHAR(3) label='Competitive Environment Measure Code',
     COMPETITIVE_POSN_MEASURE_CD VARCHAR(3) label='Competitive Position Measure Code',
     DEPENDENCE_MEASURE_CD VARCHAR(3) label='Dependence Measure Code',
     DIVERSIFICATION_MEASURE_CD VARCHAR(3) label='Diversification Measure Code',
     SUPPLIER_DEPENDENCE_MEASURE_CD VARCHAR(3) label='Supplier Dependence Measure Code',
     FINANCIAL_STRENGTH_MEASURE_CD VARCHAR(3) label='Financial Strength Measure Code',
     PROJECT_PHASE_MEASURE_CD VARCHAR(3) label='Project Phase Measure Code',
     PROJECT_STRENGTH_MEASURE_CD VARCHAR(3) label='Project Strength Measure Code',
     MARKET_CONDITIONS_MEASURE_CD VARCHAR(3) label='Market Conditions Measure Code',
     MARKET_FUTURE_MEASURE_CD VARCHAR(3) label='Market Future Measure Code',
     MANGEMENT_STRENGTH_MEASURE_CD VARCHAR(3) label='Mangement Strength Measure Code',
     PROPERTY_QUALITY_MEASURE_CD VARCHAR(3) label='Property Quality Measure Code',
     MARKETABILTY_MEASURE_CD VARCHAR(3) label='Marketabilty Measure Code',
     REPAYMENT_CAPACITY_MEASURE_CD VARCHAR(3) label='Repayment Capacity Measure Code',
     PAYMENT_COVERAGE_MEASURE_CD VARCHAR(3) label='Payment Coverage Measure Code',
     REPAYMENT_RECORD_MEASURE_CD VARCHAR(3) label='Repayment Record Measure Code',
     REPUTATION_MEASURE_CD VARCHAR(3) label='Reputation Measure Code',
     TRACK_RECORD_MEASURE_CD VARCHAR(3) label='Track Record Measure Code',
     EVIDENT_CAPACITY_MEASURE_CD VARCHAR(3) label='Evident Capacity Measure Code',
     PROCESSED_DTTM       DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='PROCESSED_DTTM',	/* I18NOK:END */
     CONSTRAINT PRIM_KEY PRIMARY KEY (CUSTOMER_RK, VALID_START_DTTM, VALID_END_DTTM)
);

