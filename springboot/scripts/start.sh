#!/bin/bash
echo "Home:"$HOME
echo "---------Starting Maven Script---------"
/deployments/maven.sh
echo "---------End Maven Script---------"
echo "---------Starting App---------"
java -jar /deployments/app.jar