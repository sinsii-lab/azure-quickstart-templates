create table  &LIBREF..RECOVERY_FACT_BASE 
(ACCOUNT_RK num (8) "_RKorSK ",
CREDIT_FACILITY_RK num (8) "Credit Facility Key ",
CUSTOMER_RK num (8) "_RKorSK ",
RECOVERY_COST_AMT num (8) "_AMT ",
VALUE_AT_RECOVERY_AMT num (8) "_AMT ",
VALUE_REALIZED_AMT num (8) "_AMT ",
PERIOD_LAST_DTTM num (8) "period_last_dttm ",
ACCOUNT_TYPE_CD char (3) "_CD ",
ACCOUNT_SUB_TYPE_CD char (3) "_CD ",
FINANCIAL_PRODUCT_TYPE_CD char (3) "_CD ",
PRODUCT_SUB_TYPE_CD char (3) "_CD ",
RECOVERY_FROM_TYPE_CD char (3) "_Id ",
CREDIT_FACILITY_TYPE_CD char (3) "Credit Facility Type Code ",
ACCOUNT_HIERARCHY_CD char (3) "  ")
;