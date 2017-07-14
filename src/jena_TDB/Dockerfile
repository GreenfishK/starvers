FROM library/maven:3.5.0-jdk-8

ADD pom.xml pom.xml
ADD src src

RUN mvn package

ENV POLICY ic
ENV ROLE s
ENV CATEGORY mat
ENV QUERY subjectLookup/queries-sel-10-e0.2.txt
# -e 1298
CMD java -cp target/tdbQuery-0.6-jar-with-dependencies.jar org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query \
    -e 89 \
    -j 1 \
    -p $POLICY \
    -d /var/data/dataset/$POLICY \
    -r $ROLE \
    -c $CATEGORY \
    -a /var/data/queries/$QUERY \
    -t /var/data/output/time-$POLICY-$CATEGORY-$(echo $QUERY | sed "s/\//-/g").txt

