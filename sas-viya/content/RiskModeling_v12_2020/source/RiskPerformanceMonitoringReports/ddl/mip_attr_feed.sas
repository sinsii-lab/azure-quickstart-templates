data &rm_reporting_mart_libref..&m_feed_table; 	/* I18NOK:BEGIN */
attrib VARIABLE length=$3000 label='Variabe Name';
attrib VARIABLE_SK length=8 label='Variable SK';
attrib ATTRIBUTE_SEQ_NO length=8 label='Attribute Sequence';
attrib ATTRIBUTE_NAME length=$3000 label='Attribute Name';
attrib SCORECARD_POINTS length=8 label='Scorecard Points';
attrib NO_OF_RECORDS_DEV length=8 label='Number of Development Records';
attrib NO_OF_EVENTS_DEV length=8 label='Number of Development Events';
attrib NO_OF_NON_EVENTS_DEV length=8 label='Number of Development Non-Events';
attrib NO_OF_RECORDS_ACTUAL length=8 label='Number of Actual Records';
attrib NO_OF_EVENTS_ACTUAL length=8 label='Number of Actual Events';
attrib NO_OF_NON_EVENTS_ACTUAL length=8 label='Number of Actual Non-Events';
attrib MODEL_RK length=8 label='Model Number RK';
attrib SCORE_TIME_SK length=8 label='Scoring Time SK';
attrib MODEL length=$480 label='Model Name';
attrib VERSION length=$500 label='Version Name';
attrib SCORING_DATE length=8 label='Scoring Date' format=DATETIME25.6;
attrib REPORT_CATEGORY_CD length=$2400 label='Report Category Code';
attrib REPORT_SPECIFICATION_SK length=8 label='Version SK';
attrib REPORT_CATEGORY_SK length=8 label='Report Category SK';
stop;	/* I18NOK:END */
run;