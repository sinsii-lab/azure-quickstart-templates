#This is a sample Python code to get started with the SAS SWAT and sasctl packages to create and register a model.

#Section a: Creating a connection with the CAS server 

"""
Import the SAS SWAT and sasctl packages 
SAS SWAT is used to work with CAS objects such as data sets and models from Python.
sasctl is used to register a model into the model repository.
For more information about the SAS SWAT and sasctl packages, see the Overview section of <Developing Models Using Python>.
<Note:Currently, only the models developed in scikit-learn are supported>.
"""
import swat
from sasctl import Session
from sasctl.tasks import register_model
from sklearn.linear_model import LogisticRegression

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

""" For example,
tbl = conn.CASTable(name  = 'PDO_C_ACCOUNT', caslib='rm_mdl') 
"""

#Section c: Creating a model in Python using the CAS analytical data set 

#The following code snippet is an example of using Python for model fitting:
tbl1=tbl[tbl.PDO_CRP_CUS_COMPLEX_BAD_1.notnull()]
X=tbl1['C_SUM_SNP_DPD60_1_12M','C_SUM_SNP_DPD30_1_12M']
y=tbl1.PDO_CRP_CUS_COMPLEX_BAD_1

logreg = LogisticRegression(random_state=0,max_iter=1000).fit(X,y) 

#Section d: Registering the model in the model repository

#For more information, see the Authentication section of the sasctl User Guide.
Session(host,authinfo='~/.authinfo',protocol='http')


register_model(model = logreg,             # Provide the model object that stores the developed model.						
                name = 'model_name',       # Provide a model name that is unique in model repository.
                input = X,                               
                project = 'project_name',  # Provide the name of the project in which your model will be stored in model repository.
                force = True)         	   # To create a project with project_name if it doesn't exist.

    """ For example,
    register_model(model = logreg,						
                   name = 'glmmodel',
                   input = X,
                   project = 'continous',                
                   force = True)
    """
