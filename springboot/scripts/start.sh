#!/bin/bash
echo "---------Starting Maven Script---------"
/home/app/deployments/maven.sh
echo "---------End Maven Script---------"
echo "---------Starting App---------"
java -jar /home/app/deployments/app.jar