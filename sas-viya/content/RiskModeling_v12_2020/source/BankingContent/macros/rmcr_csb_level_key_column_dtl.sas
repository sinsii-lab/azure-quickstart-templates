%macro rmcr_csb_level_key_column_dtl;

proc sql;
insert into &lib_apdm..LEVEL_KEY_COLUMN_DTL (
LEVEL_KEY_COLUMN_DTL_SK, LEVEL_SK ,KEY_COLUMN_SK, KEY_TYPE_CD
)
VALUES( 5, 3,3317,"RETAIN_KEY")/* i18nOK:Line */
VALUES( 6, 3,3316,"SURROGATE_KEY")/* i18nOK:Line */

VALUES( 7, 4,1399,"RETAIN_KEY")/* i18nOK:Line */
VALUES( 8, 4,1398,"SURROGATE_KEY")/* i18nOK:Line */

VALUES( 9, 5,2883,"RETAIN_KEY")/* i18nOK:Line */
VALUES( 10, 5,2882,"SURROGATE_KEY")/* i18nOK:Line */

VALUES( 11 7,2877,"RETAIN_KEY")/* i18nOK:Line */
VALUES( 12, 7,2876,"SURROGATE_KEY")/* i18nOK:Line */

VALUES( 13, 8,3317,"RETAIN_KEY")/* i18nOK:Line */
VALUES( 14, 8,3316,"SURROGATE_KEY")/* i18nOK:Line */
;
quit;

%mend rmcr_csb_level_key_column_dtl;