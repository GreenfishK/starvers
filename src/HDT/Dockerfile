# hdt-cpp, old "latest" image (mind the typo (rfd)) and corresponding environment variables
#FROM rfdhdt/hdt-cpp
#ENV HDT_DIR=/usr/local/src/hdt-cpp
#ENV HDT_LIBDIR=${HDT_DIR}/hdt-lib
#ENV CDS_LIBDIR=${HDT_DIR}/libcds-v1.0.12/lib
#ENV MAKE_ALL_RUNDIR=${HDT_DIR}/hdt-lib

# hdt-cpp, tagged versions v1.3.x and corresponding environment variables
FROM rdfhdt/hdt-cpp:v1.3.3
ENV HDT_DIR=/usr/local/src/hdt-cpp
ENV HDT_LIBDIR=${HDT_DIR}/libhdt/.libs
ENV CDS_LIBDIR=${HDT_DIR}/libcds/.libs
ENV MAKE_ALL_RUNDIR=${HDT_DIR}/libhdt

ADD tools /tmp/tools
ADD make-all.sh /tmp/make-all.sh

RUN mkdir /opt/bear
RUN cd ${MAKE_ALL_RUNDIR} && /tmp/make-all.sh

ENV POLICY=ic
ENV CATEGORY=mat
ENV QUERY=subjectLookup/queries-sel-10-e0.2.txt

#    -l 1298 \
WORKDIR /opt/bear
CMD /opt/bear/query-$POLICY-$CATEGORY \
    -d /var/data/dataset/$POLICY/ \
    -l 88 \
    -t spo \
    -i /var/data/queries/$QUERY\
    -o /var/data/output/time-hdt-$POLICY-$CATEGORY-$(echo $QUERY | sed "s/\//-/g").txt

