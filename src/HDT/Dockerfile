FROM rfdhdt/hdt-cpp

ADD tools /tmp/tools
ADD make-all.sh /tmp/make-all.sh

RUN mkdir /opt/bear
# next line tackles changes to rfdhdt/hdt-cpp
RUN ln -s /usr/local/src/hdt-cpp /opt/hdt
RUN cd /opt/hdt/hdt-lib && /tmp/make-all.sh

ENV POLICY ic
ENV CATEGORY mat
ENV QUERY subjectLookup/queries-sel-10-e0.2.txt

#    -l 1298 \
WORKDIR /opt/bear
CMD /opt/bear/query-$POLICY-$CATEGORY \
    -d /var/data/dataset/$POLICY/ \
    -l 88 \
    -t spo \
    -i /var/data/queries/$QUERY\
    -o /var/data/output/time-hdt-$POLICY-$CATEGORY-$(echo $QUERY | sed "s/\//-/g").txt

