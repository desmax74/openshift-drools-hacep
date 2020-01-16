#!/bin/bash
echo "---------Starting Maven Script--------- ${user.home}"
echo "---------user.home--------- ${user.home}"
echo "---------user--------- ${user}"
echo "---------id--------- ${id}"
chmod a+x /deployments/maven.sh
/deployments/maven.sh
echo "---------End Maven Script---------"
echo "---------Starting App---------"
java -jar /deployments/app.jar