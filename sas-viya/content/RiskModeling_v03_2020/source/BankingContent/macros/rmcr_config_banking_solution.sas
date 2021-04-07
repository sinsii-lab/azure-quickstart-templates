/********************************************************************************************************
   	Module		:  rmcr_config_banking_solution

   	Function	:  This macro will be used to update the EXTERNAL_CODE_MASTER table with external_code_file_loc to the latest versoin of CR
	Called-by	:  Risk modeling Content Post Installation Wrapper macro
	Calls		:  None

						
	Author:   CSB Team
	
	Input :	m_cr_banking_solution_macro_path
	
	Processing:
		-> This macro updates the external_code_master table with external_code_file_loc to updated CR version folder
			if that column already points to older CR version banking content macro path.
		-> This update undergo only on our prepackage external codes say external_code_sk in (1,2,3,4,5,6)	

*********************************************************************************************************/
%macro rmcr_config_banking_solution;

******************************************************************************;
* Start - Update EXTERNAL_CODE_MASTER table with external code path;
******************************************************************************;
%let external_code_path = &m_cr_banking_solution_macro_path.;

proc sql noprint;
	update &lib_apdm..EXTERNAL_CODE_MASTER 
		set external_code_file_loc = "&external_code_path." 
	 where external_code_sk in (1,2,3,4,5,6) and find(external_code_file_loc,'banking','i') gt 1; /* i18NOK:LINE */
quit;

	 	
%mend rmcr_config_banking_solution;