cd ../gossip
call mvn install

cd ../server
call mvn install:install-file -Dfile=libs/mr-udp-1.0.jar -DgroupId=rudp -DartifactId=mr-udp ^
                              -Dversion=1.0 -Dpackaging=jar -DgeneratePom=true
call mvn install

cd../client
mvn package
