#!/bin/bash
echo "---------Copy Maven Scripts to /app folder"
mkdir /app/scripts
cp /scripts/maven.sh /scripts/logging.sh /app/scripts
chmod +x /app/scripts/maven.sh /app/scripts/logging.sh
mkdir /app/.m2
cp /scripts/jboss-settings.xml /app/.m2/settings.xml
echo "---------Starting Maven Script---------"
/app/scripts/maven.sh
echo "---------End Maven Script---------"
echo "---------Starting App---------"
java -Dkie.maven.settings.custom=/app/.m2/settings.xml -jar /app/app.jar
