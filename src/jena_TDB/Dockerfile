FROM library/maven:3.5.0-jdk-8

ADD pom.xml pom.xml
ADD src src

RUN mvn package

CMD java -version
