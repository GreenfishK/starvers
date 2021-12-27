# FROM maven:3.8.4-openjdk-17 AS build  
# ADD src src 
# ADD pom.xml pom.xml
# RUN mvn clean package

# FROM gcr.io/distroless/java  
# COPY --from=build /usr/src/app/target/tdbQuery-0.6.jar /usr/app/tdbQuery-0.6.jar
# EXPOSE 8080  
# ENTRYPOINT ["java","-jar","/usr/app/tdbQuery-0.6.jar"]  

FROM maven:3.8.4-openjdk-17 AS build                      
# FROM library/maven:3.5.0-jdk-8

ADD pom.xml pom.xml
ADD src src

# jar is saved in /var/lib/docker/overlay2/<id>/diff/target
# Building jar: /target/tdbQuery-0.6.jar
RUN mvn clean package assembly:single

CMD java -version
