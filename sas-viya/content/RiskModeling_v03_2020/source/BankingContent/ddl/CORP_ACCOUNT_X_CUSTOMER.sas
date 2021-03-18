create table  &LIBREF..CORP_ACCOUNT_X_CUSTOMER 
(CUSTOMER_SK num (8) "CUSTOMER_SK ",
CUSTOMER_RK num (8) "CUSTOMER_RK ",
CUSTOMER_ID char (32) "CUSTOMER_ID ",
ORGANIZATION_TYPE_CD char (3) "ORGANIZATION_TYPE_CD ",
CUSTOMER_CITY_NM char (100) "CUSTOMER_CITY_NM ",
CUSTOMER_POSTAL_CD char (20) "CUSTOMER_POSTAL_CD ",
CUSTOMER_STATE_REGION_CD char (4) "CUSTOMER_STATE_REGION_CD ",
CUSTOMER_COUNTRY_CD char (3) "CUSTOMER_COUNTRY_CD ",
CUSTOMER_TYPE_CD char (3) "CUSTOMER_TYPE_CD ",
CUSTOMER_ACTIVE_CD char (3) "CUSTOMER_ACTIVE_CD ",
CUSTOMER_LIFECYCLE_CD char (3) "CUSTOMER_LIFECYCLE_CD ",
BANKRUPTCY_STATUS_CD char (3) "BANKRUPTCY_STATUS_CD ",
PENDING_LAWSUIT_FLG char (1) "PENDING_LAWSUIT_FLG ",
LEGAL_JUDGEMENT_FLG char (1) "LEGAL_JUDGEMENT_FLG ",
TAX_BRACKET_CD char (3) "TAX_BRACKET_CD ",
TAX_ID char (32) "TAX_ID ",
TAX_ID_TYPE_CD char (3) "TAX_ID_TYPE_CD ",
FIRST_ACCOUNT_TYPE_CD char (3) "FIRST_ACCOUNT_TYPE_CD ",
EVER_IN_COLLECTION_FLG char (1) "EVER_IN_COLLECTION_FLG ",
PREFERRED_CHANNEL_CD char (3) "PREFERRED_CHANNEL_CD ",
OTHER_BANK_CLIENT_FLG char (1) "OTHER_BANK_CLIENT_FLG ",
OTHER_CREDIT_CARDS_CNT num (8) "OTHER_CREDIT_CARDS_CNT ",
ANNUAL_INCOME_AMT num (8) "ANNUAL_INCOME_AMT ",
INTERNAL_CREDIT_RATING_CD char (20) "INTERNAL_CREDIT_RATING_CD ",
PRIMARY_INTERNAL_ORG_RK num (8) "PRIMARY_INTERNAL_ORG_RK ",
INDUSTRY_CD char (10) "INDUSTRY_CD ",
NUM_EMPLOYEES_CNT num (8) "NUM_EMPLOYEES_CNT ",
EXTERNAL_CREDIT_RATING1_CD char (4) "EXTERNAL_CREDIT_RATING1_CD ",
NUM_OFFICES_CNT num (8) "NUM_OFFICES_CNT ",
EXTERNAL_CREDIT_RATING2_CD char (4) "EXTERNAL_CREDIT_RATING2_CD ",
OWNERSHIP_CNT num (8) "OWNERSHIP_CNT ",
CUSTOMER_CLASS_CD char (3) "CUSTOMER_CLASS_CD ",
OWNERSHIP_CD char (3) "OWNERSHIP_CD ",
BUSINESS_NATURE_CD char (3) "BUSINESS_NATURE_CD ",
VALID_START_DTTM num (8) "VALID_START_DTTM ",
VALID_END_DTTM num (8) "VALID_END_DTTM ",
CURRENT_COUNTRY_START_DT num (8) "CURRENT_COUNTRY_START_DT ",
BANKRUPTCY_FILED_DT num (8) "BANKRUPTCY_FILED_DT ",
INCORPORATION_DT num (8) "INCORPORATION_DT ",
BUSINESS_ESTABLISHED_DT num (8) "BUSINESS_ESTABLISHED_DT ",
BUSINESS_COMMENCED_DT num (8) "BUSINESS_COMMENCED_DT ",
FIRST_ACCOUNT_OPEN_DT num (8) "FIRST_ACCOUNT_OPEN_DT ",
CURRENT_ADDRESS_START_DT num (8) "CURRENT_ADDRESS_START_DT ",
ACCOUNT_SK num (8) "ACCOUNT_SK ",
ACCOUNT_RK num (8) "ACCOUNT_RK ",
OWNED_BY_INTERNAL_ORG_RK num (8) "OWNED_BY_INTERNAL_ORG_RK ",
PRIMARY_PRODUCT_RK num (8) "PRIMARY_PRODUCT_RK ",
CREDIT_FACILITY_RK num (8) "CREDIT_FACILITY_RK ",
ACCOUNT_ID char (32) "ACCOUNT_ID ",
ACCOUNT_TYPE_CD char (3) "ACCOUNT_TYPE_CD ",
ACCOUNT_TYPE_DESC char (100) "ACCOUNT_TYPE_DESC ",
ACCOUNT_SUB_TYPE_CD char (3) "ACCOUNT_SUB_TYPE_CD ",
ACCOUNT_SUB_TYPE_DESC char (100) "ACCOUNT_SUB_TYPE_DESC ",
PURPOSE_CD char (3) "PURPOSE_CD ",
ACCT_HOLDERS_CNT num (8) "ACCT_HOLDERS_CNT ",
SIGNATORIES_CNT num (8) "SIGNATORIES_CNT ",
ADDITIONAL_CARD_CNT num (8) "ADDITIONAL_CARD_CNT ",
SECURITY_CD char (3) "SECURITY_CD ",
COLLATERAL_CD char (3) "COLLATERAL_CD ",
MORTGAGE_INSURED_FLG char (1) "MORTGAGE_INSURED_FLG ",
PAYMENT_INSURED_FLG char (1) "PAYMENT_INSURED_FLG ",
CURRENCY_CD char (3) "CURRENCY_CD ",
OPENING_CHANNEL_CD char (3) "OPENING_CHANNEL_CD ",
BROKER_FLG char (1) "BROKER_FLG ",
OPENING_CAMPAIGN_CD char (30) "OPENING_CAMPAIGN_CD ",
DOCUMENTATION_TYPE_CD char (3) "DOCUMENTATION_TYPE_CD ",
CHECK_BOOK_FLG char (1) "CHECK_BOOK_FLG ",
MIN_ACCOUNT_OPENING_AMT num (8) "MIN_ACCOUNT_OPENING_AMT ",
REQUIRED_MIN_BALANCE_AMT num (8) "REQUIRED_MIN_BALANCE_AMT ",
ALLOW_OVERDRAFT_FLG char (1) "ALLOW_OVERDRAFT_FLG ",
SAFE_DEPOSIT_FLG char (1) "SAFE_DEPOSIT_FLG ",
MIN_REDRAW_AMT num (8) "MIN_REDRAW_AMT ",
MIN_PAYMENT_FLG char (1) "MIN_PAYMENT_FLG ",
DISBURSEMENT_TYPE_CD char (3) "DISBURSEMENT_TYPE_CD ",
GRACE_PERIOD_DAYS_CNT num (8) "GRACE_PERIOD_DAYS_CNT ",
CASH_BACK_FLG char (1) "CASH_BACK_FLG ",
CASH_BACK_PCT num (8) "CASH_BACK_PCT ",
FINANCE_CHARGE_FLG char (1) "FINANCE_CHARGE_FLG ",
SPLIT_LOAN_FLG char (1) "SPLIT_LOAN_FLG ",
ANNUAL_FEES_AMT num (8) "ANNUAL_FEES_AMT ",
PORTABILITY_FLG char (1) "PORTABILITY_FLG ",
ACTUAL_ACCOUNT_OPENING_AMT num (8) "ACTUAL_ACCOUNT_OPENING_AMT ",
CURRENT_LIMIT_AMT num (8) "CURRENT_LIMIT_AMT ",
CASH_LIMIT_AMT num (8) "CASH_LIMIT_AMT ",
SECURITY_DEPOSIT_AMT num (8) "SECURITY_DEPOSIT_AMT ",
APPROVED_AMT num (8) "APPROVED_AMT ",
ACTUAL_ADVANCE_AMT num (8) "ACTUAL_ADVANCE_AMT ",
COLLECTIONS_STATUS_CD char (3) "COLLECTIONS_STATUS_CD ",
DOWNPAYMENT_AMT num (8) "DOWNPAYMENT_AMT ",
IMPROVEMENTS_ALLOCATION_AMT num (8) "IMPROVEMENTS_ALLOCATION_AMT ",
INTEREST_PAYOUT_FLG char (1) "INTEREST_PAYOUT_FLG ",
MULTI_RATE_FLG char (1) "MULTI_RATE_FLG ",
CLOSE_REASON_CD char (3) "CLOSE_REASON_CD ",
ACCOUNT_STATUS_CD char (3) "ACCOUNT_STATUS_CD ",
ACCOUNT_LIFECYCLE_STG_CD char (3) "ACCOUNT_LIFECYCLE_STG_CD ",
OFFSET_ACCOUNT_FLG char (1) "OFFSET_ACCOUNT_FLG ",
DO_NOT_CONTACT_FLG char (1) "DO_NOT_CONTACT_FLG ",
HONEYMOON_PERIOD_FLG char (1) "HONEYMOON_PERIOD_FLG ",
LINKED_ACCOUNT_FLG char (1) "LINKED_ACCOUNT_FLG ",
LINKED_DEP_ACCT_FLG char (1) "LINKED_DEP_ACCT_FLG ",
STATEMENT_FREQUENCY_CD char (3) "STATEMENT_FREQUENCY_CD ",
REPAYMENT_HOLIDAY_FLG char (1) "REPAYMENT_HOLIDAY_FLG ",
PAYMENT_FREQUENCY_CD char (3) "PAYMENT_FREQUENCY_CD ",
REGULAR_PERIODIC_PAYMENT_AMT num (8) "REGULAR_PERIODIC_PAYMENT_AMT ",
PAYMENT_DAY_OF_MONTH num (8) "PAYMENT_DAY_OF_MONTH ",
PAYMENT_MODE_CD char (3) "PAYMENT_MODE_CD ",
SPECIAL_RATE_TYPE_CD char (3) "SPECIAL_RATE_TYPE_CD ",
SPECIAL_INTEREST_RT num (8) "SPECIAL_INTEREST_RT ",
VALID_START_DTTM_2 num (8) "VALID_START_DTTM_2 ",
VALID_END_DTTM_2 num (8) "VALID_END_DTTM_2 ",
HOME_STATUS_CD char (3) "HOME_STATUS_CD ",
STEP_UP_FACILITY_USED_FLG char (1) "STEP_UP_FACILITY_USED_FLG ",
CALLABLE_FACILITY_USED_FLG char (1) "CALLABLE_FACILITY_USED_FLG ",
ACCOUNT_RENEWAL_TYPE_CD char (3) "ACCOUNT_RENEWAL_TYPE_CD ",
COLLATERAL_AMT num (8) "COLLATERAL_AMT ",
GUARANTORS_CNT num (8) "GUARANTORS_CNT ",
NOMINEES_CNT num (8) "NOMINEES_CNT ",
REDRAW_FLG char (1) "REDRAW_FLG ",
CLOSING_COST_AMT num (8) "CLOSING_COST_AMT ",
SENIORITY_CD char (3) "SENIORITY_CD ",
LIEN_INDICATOR_FLG char (1) "LIEN_INDICATOR_FLG ",
OVERDRAFT_LIMIT_AMT num (8) "OVERDRAFT_LIMIT_AMT ",
SECURITY_DEPOSIT_REFUND_AMT num (8) "SECURITY_DEPOSIT_REFUND_AMT ",
STANDING_ORDERS_CNT num (8) "STANDING_ORDERS_CNT ",
CHECKING_MAIN_FLG char (1) "CHECKING_MAIN_FLG ",
INITIAL_LIMIT_AMT num (8) "INITIAL_LIMIT_AMT ",
DIRECT_DEBITS_CNT num (8) "DIRECT_DEBITS_CNT ",
GUARANTEE_AMT num (8) "GUARANTEE_AMT ",
PROVISIONAL_CREDIT_LIMIT_AMT num (8) "PROVISIONAL_CREDIT_LIMIT_AMT ",
MORTGAGE_OPEN_TO_BUY_AMT num (8) "MORTGAGE_OPEN_TO_BUY_AMT ",
CHARGE_OFF_AMT num (8) "CHARGE_OFF_AMT ",
CHARGE_OFF_FLG char (1) "CHARGE_OFF_FLG ",
ACCOUNT_RENEWAL_CNT num (8) "ACCOUNT_RENEWAL_CNT ",
INDIVIDUAL_ORGANIZATION_CD char (3) "INDIVIDUAL_ORGANIZATION_CD ",
REGISTER_ONLINE_DT num (8) "REGISTER_ONLINE_DT ",
OPEN_DT num (8) "OPEN_DT ",
CURRENT_LIMIT_DT num (8) "CURRENT_LIMIT_DT ",
CARD_EXPIRATION_DT num (8) "CARD_EXPIRATION_DT ",
CARD_ISSUE_DT num (8) "CARD_ISSUE_DT ",
CARD_REISSUE_DT num (8) "CARD_REISSUE_DT ",
HONEYMOON_END_DT num (8) "HONEYMOON_END_DT ",
REPAYMENT_HOLIDAY_END_DT num (8) "REPAYMENT_HOLIDAY_END_DT ",
REPAYMENT_HOLIDAY_START_DT num (8) "REPAYMENT_HOLIDAY_START_DT ",
SPECIAL_RATE_END_DT num (8) "SPECIAL_RATE_END_DT ",
SPECIAL_RATE_START_DT num (8) "SPECIAL_RATE_START_DT ",
OVERDRAFT_LIMIT_DT num (8) "OVERDRAFT_LIMIT_DT ",
PROV_CREDIT_LIMIT_CLOSE_DT num (8) "PROV_CREDIT_LIMIT_CLOSE_DT ",
PROV_CREDIT_LIMIT_OPEN_DT num (8) "PROV_CREDIT_LIMIT_OPEN_DT ",
CHARGE_OFF_DT num (8) "CHARGE_OFF_DT ",
CLOSE_DT num (8) "CLOSE_DT ",
ACCOUNT_HIERARCHY_CD char (3) "ACCOUNT_HIERARCHY_CD ")
;
