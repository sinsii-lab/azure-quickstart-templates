CREATE TABLE &LIBREF..TIME_DIM (
     TIME_SK              NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     CAL_DAY_NO           NUMERIC(8) label='No or Amt',
     CAL_QUARTER_NO       NUMERIC(8) label='No or Amt',
     CAL_YEAR_NO          NUMERIC(8) label='No or Amt',
     CAL_MONTH_NO         NUMERIC(8) label='No or Amt',
     CAL_MONTH_NM         VARCHAR(9) label='CAL_MONTH_NM',
     CAL_MONTH_FIRST_DT   DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CAL_MONTH_LAST_DT    DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     DAYS_IN_CAL_MONTH    NUMERIC(8) label='No or Amt',
     CAL_QUARTER_NM       VARCHAR(13) label='CAL_QUARTER_NM',
     CAL_QUARTER_FIRST_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CAL_QUARTER_LAST_DT  DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     DAYS_IN_CAL_QUARTER  NUMERIC(8) label='No or Amt',
     CAL_YYYYMM           VARCHAR(6) label='CAL_YYYYMM',
     CAL_YEAR_FIRST_DT    DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CAL_YEAR_LAST_DT     DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     DAYS_IN_CAL_YEAR     NUMERIC(8) label='No or Amt',
     FISCAL_MONTH_NO      NUMERIC(8) label='No or Amt',
     FISCAL_QUARTER_NO    NUMERIC(8) label='No or Amt',
     FISCAL_YEAR_NO       NUMERIC(8) label='No or Amt',
     FISCAL_QUARTER_NM    VARCHAR(40) label='FISCAL_QUARTER_NM',
     FISCAL_QUARTER_FIRST_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     FISCAL_QUARTER_LAST_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     DAYS_IN_FISCAL_QUARTER NUMERIC(8) label='No or Amt',
     FISCAL_YEAR_FIRST_DT DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     FISCAL_YEAR_LAST_DT  DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     FISCAL_YYYYMM        VARCHAR(6) label='FISCAL_YYYYMM',
     DAYS_IN_FISCAL_YEAR  NUMERIC(8) label='No or Amt',
     LAST_PROCESSED_DTTM  DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     CAL_MONTH_END_FLG    CHARACTER(1) label='CAL_MONTH_END_FLG',
     CAL_WEEK_END_FLG     CHARACTER(1) label='CAL_WEEK_END_FLG',
     CAL_WEEK_NO          INTEGER label='CAL_WEEK_NO',
     DAY_NM               CHARACTER(1) label='DAY_NM',
     FISCAL_DAY_NO        INTEGER label='FISCAL_DAY_NO',
     FISCAL_WEEK_NO       INTEGER label='FISCAL_WEEK_NO',
     PERIOD_FIRST_DTTM    DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='PERIOD_FIRST_DTTM',
     PERIOD_LAST_DTTM     DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='PERIOD_LAST_DTTM',
     WEEK_FIRST_DT        DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     WEEK_LAST_DT         DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     CONSTRAINT PRIM_KEY PRIMARY KEY (TIME_SK)
);

