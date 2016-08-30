cd ..

cd gossip
call mvn install
cd ..

cd server
call mvn install
cd..

cd client
mvn package
