CREATE TABLE &LIBREF..CREDIT_FACILITY_DEFAULT_DIM (
/* I18NOK:BEGIN */
     CREDIT_FACILITY_RK   NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     VALID_START_DTTM     DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     VALID_END_DTTM       DATE NOT NULL FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     CUSTOMER_RK          NUMERIC(10) NOT NULL FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',
     DEFAULT_STATUS_CD    VARCHAR(3) label='_CD',
     CREDIT_FACILITY_TYPE_CD VARCHAR(3) label='_CD',
     DEFAULT_DT           DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DT',
     PROCESSED_DTTM       DATE FORMAT=&DTTMFMT INFORMAT=&DTTMFMT label='_DTTM',
     DEFAULT_EVENT_RK     NUMERIC(10) FORMAT=&FMTRK INFORMAT=&FMTRK label='_RKorSK',	/* I18NOK:END */
     CONSTRAINT PRIM_KEY PRIMARY KEY (CREDIT_FACILITY_RK, VALID_START_DTTM)
);

