#!/bin/bash
echo "---------Starting Maven Script---------"
chmod a+x /deployments/maven.sh
/deployments/maven.sh
echo "---------End Maven Script---------"
echo "---------Starting App---------"
java -jar /deployments/app.jar