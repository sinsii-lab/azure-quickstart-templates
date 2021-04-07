%macro rmcr_insert_level_key_column_dtl;

proc sql;
insert into &lib_apdm..LEVEL_KEY_COLUMN_DTL (
LEVEL_KEY_COLUMN_DTL_SK, LEVEL_SK ,KEY_COLUMN_SK, KEY_TYPE_CD
)
VALUES( 1, 1,2877,"RETAIN_KEY") /* i18NOK:LINE */
VALUES( 2, 1,2876,"SURROGATE_KEY") /* i18NOK:LINE */
VALUES( 3, 2,2883,"RETAIN_KEY") /* i18NOK:LINE */
VALUES( 4, 2,2882,"SURROGATE_KEY") /* i18NOK:LINE */
;
quit;

%mend rmcr_insert_level_key_column_dtl;
