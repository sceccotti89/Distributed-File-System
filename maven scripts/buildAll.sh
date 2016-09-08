cd ../gossip
mvn install

cd ../server
mvn install:install-file -Dfile=libs/mr-udp-1.0.jar -DgroupId=rudp \
    -DartifactId=mr-udp -Dversion=1.0 -Dpackaging=jar -DgeneratePom=true
mvn install

cd ../client
mvn package
