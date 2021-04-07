data &rm_reporting_mart_libref..&m_msr_table;	/* I18NOK:BEGIN */
attrib RANGE_SEQ_NO length=8 label='Range Sequence';
attrib RANGE_NAME length=$3000 label='Range Name';
attrib PER_OF_RECORDS_ACTUAL length=8 label='Actual Records (A)[%]';
attrib PER_OF_RECORDS_DEV length=8 label='Development Records (D)[%]';
attrib CUM_PER_OF_RECORDS_ACTUAL length=8 label='Cumulative Actual Records (CA)[%]' ;
attrib CUM_PER_OF_RECORDS_DEV length=8 label='Cumulative Development Records (CD)[%]';
attrib DIFF_AD length=8 label='Difference (A-D)[%]';
attrib RATIO_AD length=8 label='Ratio (A/D)';
attrib LN_RATIO_AD length=8 label='Log Ratio Ln(A/D)' ;
attrib SYSSTBINDX length=8 label='SSI[Difference (A-D) * Log Ratio Ln(A/D)]';
attrib MODEL length=$480 label='Model Name';
attrib VERSION length=$500 label='Version Name';
attrib SCORING_DATE length=8 label='Scoring Date' format=DATETIME25.6;
attrib REPORT_CATEGORY_CD length=$2400 label='Report Category Code';
attrib PURPOSE length=$500 label='Purpose';
attrib SUBJECT_OF_ANALYSIS length=$500 label='Subject of Analysis';
attrib BIN_TYPE length=$480 label='Bin Type';
attrib RANGE_SCHEME_TYPE_SK length=8 label='Range Scheme Type SK';
attrib MODEL_SK length=8 label='Model SK';
attrib REPORT_SPECIFICATION_SK length=8 label='Version SK';
attrib SCORING_AS_OF_TIME_SK length=8 label='Scoring Time SK';
attrib REPORT_CATEGORY_SK length=8 label='Report Category SK';	/* I18NOK:END */
stop;
run;