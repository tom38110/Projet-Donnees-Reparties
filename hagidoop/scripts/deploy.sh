#!/bin/bash
# Compilation
echo "On compile"
javac application/*.java
javac config/*.java
javac daemon/*.java
javac hdfs/*.java
javac interfaces/*.java
javac io/*.java
echo "On a compilé"

sleep 3

# Fragmentation du fichier donné en argument
# Lancement des HdfsServer
for ((i=0; i<$1; i++))
do
    java hdfs.HdfsServer "$i" &
    echo "On a lancé le serveur $i"
    sleep 3
done

# Lancement du write par HdfsClient
java hdfs.HdfsClient write txt "$2"
echo "On a écrit les fragments"

sleep 3

# Lancement des Worker
for ((i=0; i<$1; i++))
do
    java daemon.WorkerImpl "$i" &
    echo "On a lancé le worker $i"
    sleep 3
done

# Lancement de l'application
java application.MyMapReduce "$2"
sleep 3

# Effacer les fragments
for ((i=0; i<$1; i++))
do
    java hdfs.HdfsClient delete fragment_"$i".txt
    sleep 3
done

# Arrêt du Serveur et des Worker
for ((i=0; i<$1; i++))
do
    pkill -f "java hdfs.HdfsServer $i"
    pkill -f "java daemon.WorkerImpl $i"
done

