data &rm_reporting_mart_libref..&m_feed_table.;	/* I18NOK:BEGIN */
attrib RANGE_SCHEME_TYPE_SK length=8 label='Range Sequence SK';
attrib RANGE_SEQ_NO length=8 label='Range Sequence';
attrib RANGE_NAME length=$3000 label='Range Name';
attrib NO_OF_RECORDS_DEV length=8 label='Number of Development Records';
attrib NO_OF_RECORDS_ACTUAL length=8 label='Number of Actual Records';
attrib EXPECTED_OUTCOME_VALUE length=8 label='Expected Outcome Value';
attrib ACTUAL_OUTCOME_VALUE length=8 label='Actual Outcome Value';
attrib MAX_RANGE_VALUE length=8 label='Maximum Probability of Event/Maximum Score' ;
attrib MODEL_RK length=8 label='Model Number RK';
attrib SCORE_TIME_SK length=8 label='Scoring Time SK';
attrib MODEL length=$480 label='Model Name';
attrib VERSION length=$500 label='Version Name';
attrib SCORING_DATE length=8 label='Scoring Date' format=DATETIME25.6;
attrib REPORT_CATEGORY_CD length=$2400 label='Report Category Code';
attrib BIN_TYPE length=$480 label='Bin Type';
attrib REPORT_SPECIFICATION_SK length=8 label='Version SK';
attrib REPORT_CATEGORY_SK length=8 label='Report Category SK';	/* I18NOK:END */
stop;
run;