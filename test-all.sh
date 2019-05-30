cd java/submit-java/
mvn -Dmaven.test.skip=true clean package
mvn test
cd ../..
cd scala/05-api
sbt package
sbt test
cd ../..
