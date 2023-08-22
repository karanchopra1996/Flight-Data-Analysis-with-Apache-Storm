#!/bin/sh
mvn exec:java -Dexec.mainClass="TopologyMain" -Dexec.args="src/main/resources/flights.txt src/main/resources/airports.txt"
