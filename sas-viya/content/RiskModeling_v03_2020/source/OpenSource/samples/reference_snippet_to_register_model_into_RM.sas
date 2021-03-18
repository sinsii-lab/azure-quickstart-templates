/********************************************************************************************************
   Module:  reference_snippet_to_register_model_into_RM

   Function:  This is a sample snippet macro to import SWAT based Python model in to Risk Modeling

   Parameters: INPUT: 
			1. m_model_type				            : Type of Model to be imported into RM
													  Allowed values are {PYTHON,R,SAS_CODE,PMML,EMN}
			2. m_model_name 			            : Name with which Model is to be registered in Risk Modeling
													  In case of Python Models name should be same as 
													  the name registered/Published in ModelRepository using SASCTL
															 
			4. m_target_variable_name 	            : Modeling Target variable Column name. 
													  It should match with physical varible name specified in RM Analytical Data Builder.
													
			5. m_input_variable_names               : Comma seperated Significant Input Variable Column name.
													  It should match with physical varible name specified in RM Analytical Data Builder.
			
			6. m_abt_table_name                     : Physical Name of Modeling Dataset used to develop Model. 
													  It should macth with Dataset sepecified in RM Analytical Data Builder
 													

			7. m_output_variable_names	            : Output Variables of Model
														
			8. m_model_scoring_code_location		: Scoring code location in the content folder
			
	Mandatory inputs : m_model_type, m_model_name, m_abt_table_name, m_target_variable_name
	
*********************************************************************************************************/

******************************************************************************************************************;
* Start - Python Model devloped, Register, Published into RM PublishingDestination using SWAT/SKLEARN            *;
******************************************************************************************************************;

%csbmva_initialize_cr(m_cr_unique_cd=RM_CONTENT); 
%rmcr_init_open_source;
%rmcr_register_model(m_model_type=PYTHON,								/*** do not change ***/
				  m_model_name=python_glm_model,    					/*** name under which model is created/published in to CAS same name will be used to register model in to SAS Risk Modeling ***/
				  m_target_variable_name=PDO_I_CUS_CURRENT_BAD_30,				
				  m_abt_table_name=PDO_C_ACCOUNT					/*** Physical name of Modeling Dataset used to develop Model  ***/
				   );

/* End - Python Model devloped, Register, Published into RM PublishingDestination using SWAT/SKLEARN */