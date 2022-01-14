FROM maven:3.8.4-amazoncorretto-11 AS build                      

ADD pom.xml pom.xml
ADD src src

# jar is saved in /var/lib/docker/overlay2/<id>/diff/target
# Building jar: /target/tdbQuery-0.6.jar
RUN mvn clean package shade:shade

CMD java -version
