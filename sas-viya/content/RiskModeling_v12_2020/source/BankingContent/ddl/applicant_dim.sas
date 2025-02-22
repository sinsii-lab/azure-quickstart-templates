CREATE TABLE &LIBREF..APPLICANT_DIM (	/* I18NOK:BEGIN */
     APPLICANT_SK         NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     APPLICANT_ID         VARCHAR(32) label='_RKorSK',
     APPLICANT_RK         NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     APPLICATION_RK       NUMERIC(10) NOT NULL FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     CUSTOMER_RK          NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     EXTERNAL_ORG_RK      NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     APPLICANT_NM         VARCHAR(81) label='APPLICANT_NM',
     APPLICANT_TYPE_CD    VARCHAR(3) label='_CD',
     APPLICANT_BIRTH_DT   DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     GENDER_CD            VARCHAR(3) label='_CD',
     MARITAL_STATUS_CD    VARCHAR(3) label='_CD',
     AGE                  NUMERIC(8) label='No or Amt',
     RELATIONSHIP_CD      VARCHAR(3) label='_CD',
     APPLICANT_CITY_NM    VARCHAR(40) label='APPLICANT_CITY_NM',
     APPLICANT_POSTAL_CD  VARCHAR(20) label='APPLICANT_POSTAL_CD',
     APPLICANT_STATE_REGION_CD VARCHAR(4) label='APPLICANT_STATE_REGION_CD',
     APPLICANT_COUNTRY_CD VARCHAR(3) label='_CD',
     EDUCATION_LEVEL_CD   VARCHAR(10) label='EDUCATION_LEVEL_CD',
     STD_OCCUPATION_CD    VARCHAR(3) label='_CD',
     PRIMARY_CITIZENSHIP_COUNTRY_CD VARCHAR(3) label='_CD',
     CURRENT_COUNTRY_START_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CHILDREN_CNT         NUMERIC(6) label='CHILDREN_CNT',
     DEPENDENTS_CNT       NUMERIC(6) label='DEPENDENTS_CNT',
     ELDEST_CHILD_BIRTH_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     YOUNGEST_CHILD_BIRTH_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     PENDING_LAWSUIT_FLG  CHARACTER(1) label='PENDING_LAWSUIT_FLG',
     LEGAL_JUDGEMENT_FLG  CHARACTER(1) label='LEGAL_JUDGEMENT_FLG',
     BANKRUPTCY_STATUS_CD VARCHAR(3) label='_CD',
     BANKRUPTCY_FILED_DT  DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     FAMILY_MEMBER_ACCOUNT_CNT NUMERIC(6) label='FAMILY_MEMBER_ACCOUNT_CNT',
     HHOLD_INDIVIDUALS_CNT NUMERIC(6) label='HHOLD_INDIVIDUALS_CNT',
     INTERNAL_CREDIT_RATING_CD VARCHAR(20) label='INTERNAL_CREDIT_RATING_CD',
     OTHER_CREDIT_CARDS_CNT NUMERIC(6) label='OTHER_CREDIT_CARDS_CNT',
     OWN_RESIDENCE_PROPERTY_FLG CHARACTER(1) label='OWN_RESIDENCE_PROPERTY_FLG',
     RESIDENCE_STATUS_CD  VARCHAR(3) label='_CD',
     OWN_AUTOMOBILE_FLG   CHARACTER(1) label='OWN_AUTOMOBILE_FLG',
     OWN_MOTORCYCLE_FLG   CHARACTER(1) label='OWN_MOTORCYCLE_FLG',
     TIME_RESIDENCE_YEAR_CNT NUMERIC(6,2) FORMAT=NLNUM6.2 INFORMAT=NLNUM6.2 label='TIME_RESIDENCE_YEAR_CNT',
     CURRENT_ADDRESS_START_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     MONTHS_AT_PREVIOUS_ADDRESS_CNT NUMERIC(6) label='MONTHS_AT_PREVIOUS_ADDRESS_CNT',
     EMPLOYMENT_STATUS_CD VARCHAR(3) label='_CD',
     EMPLOYMENT_POSITION_STATUS_CD VARCHAR(3) label='_CD',
     NO_OF_EMPLOYERS_CNT  NUMERIC(6) label='NO_OF_EMPLOYERS_CNT',
     TEMPORARY_EMPLOYMENT_END_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     EMPLOYMENT_YEARS_CNT NUMERIC(6,2) FORMAT=NLNUM6.2 INFORMAT=NLNUM6.2 label='EMPLOYMENT_YEARS_CNT',
     TOTAL_EMPLOYMENT_YEARS_CNT NUMERIC(6,2) FORMAT=NLNUM6.2 INFORMAT=NLNUM6.2 label='TOTAL_EMPLOYMENT_YEARS_CNT',
     CURRENT_EMP_START_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     EMPLOYMENT_START_DT  DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CURRENT_EMP_TYPE_START_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     INDUSTRY_CD          VARCHAR(10) label='INDUSTRY_CD',
     BUSINESS_NATURE_CD   VARCHAR(3) label='_CD',
     TAX_BRACKET_CD       VARCHAR(3) label='_CD',
     TAX_ID               VARCHAR(32) label='_Id',
     TAX_ID_TYPE_CD       VARCHAR(3) label='_CD',
     MONTHLY_SALARY_INCOME_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_BUSINESS_INCOME_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_PRI_INCOME_SOURCE_CD VARCHAR(10) label='MONTHLY_PRI_INCOME_SOURCE_CD',
     MONTHLY_RENTAL_INCOME_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_REPLACEMENT_INCOME_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_OTHER_INCOME_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_CHILD_ALLOW_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     ANNUAL_INCOME_AMT    NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     ANNUAL_SALARY_BUSINESS_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     HHOLD_INCOME_AMT     NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_HOUSING_AMT  NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_ALIMONY_AMT  NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_CHILD_SUPPORT_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_REPAYMENT_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_REPAYMENT_OTHERS_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_OTHER_CHARGE_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MONTHLY_RENTAL_AMT   NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     MAINTENANCE_FLG      CHARACTER(1) label='MAINTENANCE_FLG',
     MONTHLY_INVESTMENT_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     LIQUID_ASSETS_AMT    NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     ASSET_OTHER_AMT      NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     REAL_ESTATE_AMT      NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     TOTAL_ASSET_AMT      NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     LIABILITY_REAL_ESTATE_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     LIABILITY_OTHER_AMT  NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     TOTAL_LIABILITY_AMT  NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     NET_WORTH_AMT        NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     LIQUID_NET_WORTH_AMT NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     LAST_APPLICATION_REFUSED_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     BUSINESS_COMMENCED_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     BUSINESS_ESTABLISHED_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     NUM_EMPLOYEES_CNT    NUMERIC(6) label='NUM_EMPLOYEES_CNT',
     NUM_OFFICES_CNT      NUMERIC(6) label='NUM_OFFICES_CNT',
     ORGANIZATION_NM      VARCHAR(40) label='ORGANIZATION_NM',
     OWNERSHIP_CD         VARCHAR(3) label='_CD',
     OWNERSHIP_CNT        NUMERIC(6) label='OWNERSHIP_CNT',
     SELF_EMPLOYMENT_FLG  CHARACTER(1) label='SELF_EMPLOYMENT_FLG',
     OWNERSHIP_AMT        NUMERIC(18,5) FORMAT=NLNUM18.5 INFORMAT=NLNUM18.5 label='_AMT',
     PERCENT_OWNED        NUMERIC(9,4) FORMAT=NLNUM9.4 INFORMAT=NLNUM9.4 label='PERCENT_OWNED',
     PASSPORT_ISSUE_COUNTRY_CD VARCHAR(3) label='_CD',
     VALID_START_DTTM     DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     VALID_END_DTTM       DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     CREATED_BY           VARCHAR(20) label='CREATED_BY',
     CREATED_DTTM         DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     PROCESSED_DTTM       DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     APPLICATION_DT       DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',	/* I18NOK:END */
     CONSTRAINT PRIM_KEY PRIMARY KEY (APPLICANT_SK)
);