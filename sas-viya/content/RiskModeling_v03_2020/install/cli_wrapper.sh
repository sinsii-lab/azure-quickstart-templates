#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -e apiEndPoint -u adminUser -p adminPassword -P packagePath"
   echo -e "\t-e apiEndPoint of viya application ex: http://demo.sas.com:80"
   echo -e "\t-u User who has admin access of Metadata"
   echo -e "\t-p Admin User Password"
   echo -e "\t-P Source folder path of Content"
   exit 1 # Exit script after printing help
}

while getopts "e:u:p:P:" opt
do
   case "${opt}" in
      e )
				apiEndPoint="$OPTARG" 
				;;
      u ) 
				adminUser="$OPTARG"
				;;
      p )
				adminPassword="$OPTARG"
				;;
	  P )
				packagePath="$OPTARG"
				;;
      ? )
				helpFunction	
				;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$apiEndPoint" ] || [ -z "$adminUser" ] || [ -z "$adminPassword" ] || [ -z "$packagePath" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi
#chmod 775 sas-risk-content-cli
profileName="~/.sas/config.json"
export adminPassworddecoded=$(echo "$adminPassword" | base64 -di)
./sas-risk-content-cli --profile ${profileName} profile set-endpoint $apiEndPoint
./sas-risk-content-cli --profile ${profileName} profile set-output json
./sas-risk-content-cli --profile ${profileName} profile toggle-color y
export SSL_CERT_FILE=/sas/install/setup/ssl/loadbalancer.crt.pem
./sas-risk-content-cli --profile ${profileName} auth login --user $adminUser --password $adminPassworddecoded
./sas-risk-content-cli --profile ${profileName} content install --package-path $packagePath

