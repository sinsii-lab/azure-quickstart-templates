#This is a sample Python code to get started with the SAS SWAT and sasctl packages to create and register a model.
#This code contains examples for binary and continuous models using the 'Regression' and 'decisiontree' action sets that use the 'glm' and 'gbtreeTrain' actions respectively.

#Section a: Creating a connection with the CAS server 

"""
Import the SAS SWAT and sasctl packages 
SAS SWAT is used to work with CAS objects such as data sets and models from Python.
sasctl is used to register a model into the model repository.
For more information about the SAS SWAT and sasctl packages, see the Overview section of <Developing Models Using Python>.
"""

import swat
from sasctl import Session
from sasctl.tasks import register_model

"""
Use the CAS constructor provided by SAS SWAT to establish a connection with the CAS server. 
Note: The default port for the connection between Python and SAS SWAT is 5570.
"""

conn=swat.CAS(host,port,authinfo='/path/to/.authinfo')
"""
Provide the complete path with the file name of your authinfo or netrc file.

For Example,
conn=swat.CAS('my-cas-host.com',5570,authinfo='~/.authinfo') 
Only the owner of the authinfo file must have read and write accesses to the file.
"""

#Section b: Importing the analytical data set created in SAS Analytical Data Builder

#Provide the name of the CAS modeling data set in the following code snippet:
tbl = conn.CASTable(name  = 'ModelingDataSet', caslib='rm_mdl') 
#Note: Do not change the name of the CAS library.

""" For Example
tbl = conn.CASTable(name  = 'PDO_C_ACCOUNT', caslib='rm_mdl') 
"""

#Section c: Creating a model in Python using the CAS analytical data set

"""
Use the LoadActionSet command to load an ActionSet supported by CAS in the current session. 
For more information about the ActionSets, go to SAS SWAT doumentation from the Overview section of <Developing Models Using Python>.
You must load the ActionSet before you use any Action in it.
Load the ActionSet such as 'regression', 'decisiontree', 'neuralnet', and so on.
"""

#Use the appropriate example from the following code snippets as per the model outcome type (binary or continuous).
conn.loadactionset('regression')    
#This is an example for continuous outcome models with the 'glm' action in the 'regression' actionSet. The action trains a linear regression model using the method of least squares.

conn.loadactionset('decisionTree')
#This is an example for binary outcome models with the 'gbtreeTrain' action in the 'decisionTree' actionSet. The action trains a gradient boosting tree model.

"""
Provide the values to the arguments such as target, inputs, nominals. 
Provide the name of an Object to the savestate argument. Should be unique for each model. The model will be saved in this object.
Provide a model name that is unique in model repository.
"""
tbl.glm(target='target_variable', inputs=['input_variables seperated by comma'],savestate='model_CAS_table')                         


""" For example,
tbl.glm(target='CCF', inputs=['A_AVG_SNP_TOT_BAL_13_24M','A_SUM_SNP_TOT_BAL_L1M'],savestate='glmmodel') 
#This is an example of a continuous outcome model.

tbl.decisiontree.gbtreetrain(target='EVER_CHARGE_OFF', inputs=['C_AVC_WDR_TR_AMT_1_12M','B_MAX_SNP_OVR_LMT_A_L3M'], savestate='gbtreemodel')
#This is an example of a binary outcome model.
"""

#Use the object created in the preceeding code snippet in the savestate argument.
model_object = conn.CASTable(name  = 'model_CAS_table')

""" For example,
glm = conn.CASTable(name  = 'glmmodel')      
Use the name specified in the tbl.glm statement in the preceeding example.

gbm = conn.CASTable(name  = 'gbtreemodel')   
Use the name specified in the tbl.decisiontree.gbtreetrain statement in the preceeding example.
"""

#Section d: Registering the model in the model repository

#For more information, see the Authentication section of the sasctl User Guide.
Session(host,authinfo='~/.authinfo',protocol='http')

register_model(model = model_object,             # Provide the model object that stores the developed model.    					
               name = 'model_name',        # Provide a model name that is unique in model repository.					
               project = 'project_name',   # Provide the name of the project in which your model will be stored in model repository.
               force=True)           	   # To create a project with project_name if it doesn't exist.

""" For Example
register_model(model = glm,						
               name = 'glmmodel',			   
               project = 'continuous',                
               force=True)

register_model(model = gbm,						
               name = 'gbtreemodel',			   
               project = 'binary',                
               force=True)
"""
