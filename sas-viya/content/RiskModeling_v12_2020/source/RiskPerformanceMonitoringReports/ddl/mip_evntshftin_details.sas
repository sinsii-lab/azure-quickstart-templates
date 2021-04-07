data &rm_reporting_mart_libref..&m_msr_table;	/* I18NOK:BEGIN */
attrib VARIABLE length=$3000 label='Variabe Name';
attrib VARIABLE_SK length=8 label='Variable SK';
attrib ATTRIBUTE_SEQ_NO length=8 label='Sequence Number';
attrib ATTRIBUTE_NAME length=$3000 label='Attribute Name';
attrib SCORECARD_POINTS length=8 label='Scorecard Points (SP)';
attrib PROP_ACTUAL length=8 label='Attribute Proportion-Actual';
attrib PROP_DEV length=8 label='Attribute Proportion-Development';
attrib PROP_EVT_ACT length=8 label='Event Proportion-Actual';
attrib PROP_EVT_DEV length=8 label='Event Proportion-Development';
attrib SCORE_SHIFT_INDEX length=8 label='Score Shift Index';
attrib EVENT_SHIFT_INDEX length=8 label='Event Shift Index';
attrib MODEL length=$480 label='Model Name';
attrib VERSION length=$500 label='Version Name';
attrib SCORING_DATE length=8 label='Scoring Date' format=DATETIME25.6;
attrib REPORT_CATEGORY_CD length=$2400 label='Report Category Code';
attrib PURPOSE length=$500 label='Purpose';
attrib SUBJECT_OF_ANALYSIS length=$500 label='Subject of Analysis';
attrib MODEL_SK length=8 label='Model SK';
attrib REPORT_SPECIFICATION_SK length=8 label='Version SK';
attrib SCORING_AS_OF_TIME_SK length=8 label='Scoring Time SK';
attrib REPORT_CATEGORY_SK length=8 label='Report Category SK';	/* I18NOK:END */
stop;
run;