cd ../gossip
call mvn install

cd ../server
call mvn install

cd../client
mvn package
