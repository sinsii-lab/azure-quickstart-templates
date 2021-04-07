/************************************************************************************************
       Module:  dabt_load_table_to_cas

     Function:  This macro loads all source data that is prerequisite for build ABT.
								
      Authors:  BIS Team
         Date:  28 February 2019
          SAS:  Viya

    Called-by:	macro dabt_load_src_data

        Calls:  1. dabt_drop_table macro 

Datasets used:   

        Logic:  1. Determine the library type, e.g. PATH
				2. For PATH based caslib, determine the file format type, i.e. sas7bdat or sashdat
				3. If the table in memory already exists, 
						if the parameter m_replace_if_exists is set to Y, then drop the existing table.
						if the parameter m_replace_if_exists is set to N, then put a note in log. 
				4. If table in memory doesn't exist or to be replaced, then
					load the table in memory with value of option promote set as parameter m_promote_flg.
				
   Parameters: 	INPUT:
				1. m_in_cas_lib_ref		: caslib reference from where data is to be loaded in CAS memory.
				2. m_in_table_nm		: Name of the table to be loaded in memory.
				3. m_out_cas_lib_ref	: cas library name where the tables are being pushed.
				4. m_out_table_nm		: Name of the table when loaded in memory.
				5. m_replace_if_exists	: Flag indicating whether table in memory should be replaced if already present.
				6. m_promote_flg		: Flag indicating whether table being loaded into memory should be promoted.
										  For a table to be promoted, the parameter m_out_cas_lib_ref must have a global scope.

************************************************************************************************/

%macro dabt_load_table_to_cas(m_in_cas_lib_ref=, m_in_table_nm=, m_out_cas_lib_ref=, m_out_table_nm=, m_replace_if_exists=Y, m_promote_flg=N);
	%global m_hdt_exists;
	
	%let m_hdt_exists=N;
    /* Start of get lib type */
    %if %sysfunc(exist(libinfo)) %then %do;
        proc sql noprint;
            drop table libinfo; 
        quit;
    %end;
                
    proc cas;
        table.caslibInfo result = libinfo /
        active=TRUE 
        caslib="&m_in_cas_lib_ref."
        verbose=TRUE              ;
        
        exist_libinfo= findtable(libinfo);         
        if exist_libinfo then saveresult libinfo dataout= work.libinfo;
    
    run;
                
    %let m_lib_type = ;     
    proc sql noprint;    
        select type into :m_lib_type
			from work.libinfo where type is not null;    
    quit;
                
    /* If lib type is path then we determine the file format i.e. sashdat or sas7bdat */
    %if %kupcase(&m_lib_type)=PATH %then %do;

		/* Create a work table to store the information (inclusing file format type) of the source table */
		proc cas;
			table.fileInfo result=Files / caslib="&m_in_cas_lib_ref.";
			exist_Files = findtable(Files);
			
			if exist_Files then saveresult Files dataout= work.in_file_info;
		quit; 

		%let m_in_table_format_var = ;
		/* derive the file format type of required source file name */
		proc sql noprint;
			select scan(name,2,'.') into :m_in_table_format_var separated by "." /* i18nOK:Line */
				from work.in_file_info
					where kupcase(scan(name,1,'.'))= %kupcase("&m_in_table_nm."); /* i18nOK:Line */
		quit;
		
		%let m_in_table_format="&m_in_table_format_var";

		/* If both types are present for a file, sashdat file is loaded into memory */                           
		%if %sysfunc(find(&m_in_table_format.,sashdat)) ge 1 %then %do;  /* i18nOK:Line */
			%let m_hdt_exists=Y;
			%let m_in_table_nm=&m_in_table_nm..sashdat;
		%end;
		%else %if %sysfunc(find(&m_in_table_format.,sas7bdat)) ge 1 %then %do; /* i18nOK:Line */
			%let m_in_table_nm=&m_in_table_nm..sas7bdat;
		%end;      
    %end;    
	/* End of file format type derivation */
	
	/* Proceed if sashdat / sas7bdat file exists */
	%if %sysfunc(find(&m_in_table_format.,sashdat)) ge 1 OR %sysfunc(find(&m_in_table_format.,sas7bdat)) ge 1 %then %do;	/* i18nOK:Line */
		
		/* Set option values for CAS action table.loadTable */ 
		%if "&m_promote_flg"="N" %then %do; /* i18nOK:Line */
			%let m_promote_option=FALSE; 
		%end;
		%else %do;
			 %let m_promote_option=TRUE;
		%end;

		%if "&m_out_table_nm"="" %then %do;
			%let m_out_table_nm=&m_in_table_nm.;
		%end;   
		

	   %if "&m_out_cas_lib_ref"="" %then %do;
			%let m_out_cas_lib_ref=&m_in_cas_lib_ref.;
		%end;   
					
		/* Check if the table already exists in memory */
		proc cas;
			table.tableexists result=r / 
			caslib="&m_out_cas_lib_ref."
			name="&m_out_table_nm."    ;        
		run;
					
			if (r.exists) then do; 									
				/* 1. Check if the table in-memory is to be replaced. */
				%if %kupcase("&m_replace_if_exists") = "Y" %then %do;              /* i18nOK:Line */                 
					%dabt_drop_table(m_table_nm=&m_out_cas_lib_ref..&m_out_table_nm., m_cas_flg=Y);                                    
				%end;			
				/* If the table is not supposed to be replaced, put a note and exit */
				%else %do; 
					%Put "Table &m_out_table_nm. already exists as in-memory table of &m_in_cas_lib_ref. library. It is not being replaced."; /* i18nOK:Line */
				%end; 													
			end; /* end of r.exists block */                      
							  
			if (r.exists=0) OR (r.exists AND  kupcase("&m_replace_if_exists") = "Y") then  /* i18nOK:Line */
			do;
				table.loadTable /
				casout={caslib="&m_out_cas_lib_ref.",name="&m_out_table_nm.",promote="&m_promote_option." replace="FALSE"} /* i18nOK:Line */
				caslib = "&m_in_cas_lib_ref.",
				path = "&m_in_table_nm."   
				;
				run;
			end;
			
		quit;	
	
	%end; /* end of %sysfunc(find(&m_in_table_format.,sashdat)) ge 1 OR %sysfunc(find(&m_in_table_format.,sas7bdat)) ge 1 */
	
	%else %do;
		%PUT " The file doesn't exist in .sashdat or .sas7bdat format. "; /* i18nOK:Line */ 
	%end;
	
  
%mend dabt_load_table_to_cas;
