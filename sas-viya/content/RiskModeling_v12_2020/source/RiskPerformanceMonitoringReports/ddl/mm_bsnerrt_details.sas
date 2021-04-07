data &rm_reporting_mart_libref..&m_msr_table;	/* I18NOK:BEGIN */
attrib RANGE_SEQ_NO length=8 label='Range Sequence';
attrib MAX_RANGE_VALUE length=8 label='Maximum Probability of Event/Maximum Score';
attrib BSNERRT length=8 label='Error Rate';
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