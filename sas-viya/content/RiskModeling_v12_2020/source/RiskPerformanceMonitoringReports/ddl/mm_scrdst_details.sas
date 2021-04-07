data &rm_reporting_mart_libref..&m_msr_table;	/* I18NOK:BEGIN */
attrib RANGE_SCHEME_TYPE_SK length=8 label='Range Scheme Type SK';
attrib RANGE_SEQ_NO length=8 label='Range Sequence SK';
attrib RANGE_NAME length=$3000 label='Range Name';
attrib NO_OF_RECORDS_DEV length=8 label='Number of Development Records';
attrib NO_OF_RECORDS_ACTUAL length=8 label='Number of Actual Records';
attrib NO_OF_EVENTS_ACTUAL length=8 label='Number of Actual Events';
attrib NO_OF_NON_EVENTS_ACTUAL length=8 label='Number of Actual Non-Events';
attrib EXPECTED_PROBABILITY_OF_EVENT length=8 label='Expected Probability of Event';
attrib MAX_RANGE_VALUE length=8 label='Maximum Probability of Event/Maximum Score';
attrib MODEL_RK length=8 label='Model Retained Key';
attrib SCORE_TIME_SK length=8 label='Score Time SK';
attrib ACTUAL_PROPORTION length=8 label='Actual Proportion';
attrib DEV_PROPORTION length=8 label='Development Proportion';
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