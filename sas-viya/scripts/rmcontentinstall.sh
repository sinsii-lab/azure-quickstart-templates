#!/bin/bash
## Do initial preperation of the non-ansible boxes. This should be restricted to preparing for ansible to 
## reach onto the box by installing its prerequest (should already be present on redhat), installing nfs to
## mount the ansible controller share, and copying the public key there into the authorized keys.
#
set -x
set -v
set -e

if [ -z "$1" ]; then
	INSTALL_USER="sas"
else
	INSTALL_USER="$2"
fi
export INSTALL_DIR="/sas/install"
export CODE_DIRECTORY="${INSTALL_DIR}"
export ADMINPASS="$3"
export PUBLIC_DNS_NAME="$4"

echo "Installing Risk Modeling Content"
su $INSTALL_USER -c	"cd ${CODE_DIRECTORY}/content/RiskModeling_v03_2020/install; chmod 775 cli_wrapper.sh; ./cli_wrapper.sh -e https://$PUBLIC_DNS_NAME -u sasadmin -p $ADMINPASS -P ."

