#!/bin/bash
# Compilation des sources
echo "On compile"
javac application/*.java
javac config/*.java
javac daemon/*.java
javac hdfs/*.java
javac interfaces/*.java
javac io/*.java
echo "On a compilé"