create table  &LIBREF..ACCOUNT_TRANSACTION_BASE 
/* I18NOK:BEGIN */
(ACCOUNT_RK num (8) "ACCOUNT_RK ",
CUSTOMER_RK num (8) "CUSTOMER_RK ",
CREDIT_FACILITY_RK num (8) "CREDIT_FACILITY_RK ",
PERIOD_LAST_DTTM num (8) "PERIOD_LAST_DTTM ",
LATE_PAYMENT_CNT num (8) "Late Payment Count ",
MAX_TRANSACTION_AMT num (8) "Max Transaction Amount ",
MIN_TRANSACTION_AMT num (8) "Min Transaction Amount ",
OVER_LIMIT_CNT num (8) "Over Limit Count ",
TRANSACTION_AMT num (8) "Transaction Amount ",
TRANSACTION_CNT num (8) "Transaction Count ",
ACCOUNT_SUB_TYPE_CD char (3) "Account Sub Type Code ",
ACCOUNT_TYPE_CD char (3) "Account Type Code ",
FINANCIAL_PRODUCT_TYPE_CD char (3) "Financial Product Type Code ",
PRODUCT_SUB_TYPE_CD char (3) "Product Sub Type Code ",
CREDIT_FACILITY_TYPE_CD char (3) "Credit Facility Type Code ",
CHANNEL_CD char (3) "Channel Code ",
COUNTRY_CD char (3) "Country Code ",
FEE_REASON_TYPE_CD char (3) "Fee Reason Type Code ",
INTERNAL_ORG_TYPE_CD char (3) "Internal Org Type Code ",
MEDIUM_CD char (3) "Medium Code ",
TRANSACTION_STATUS_CD char (3) "Transaction Status Code ",
TRANSACTION_TYPE_CD char (3) "Transaction Type Code ",
CREDIT_DEBIT_FLG char (1) "Credit Debit Flag ",
MERCHANT_CATEGORY_CD char (3) "Merchant Category Code ",
TRANSACTION_STATUS_REASON_CD char (3) "Transaction Status Reason Code ",
ACCOUNT_HIERARCHY_CD char (3) "ACCOUNT_HIERARCHY_CD ",
INTERNATIONAL_CNT num (8) "INTERNATIONAL_CNT ") /* I18NOK:END */
;
