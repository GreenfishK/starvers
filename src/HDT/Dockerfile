FROM rdfhdt/hdt-cpp:v1.3.3
ENV HDT_DIR=/usr/local/src/hdt-cpp
ENV HDT_LIBDIR=${HDT_DIR}/libhdt/.libs
ENV CDS_LIBDIR=${HDT_DIR}/libcds/.libs
ENV MAKE_ALL_RUNDIR=${HDT_DIR}/libhdt

ADD tools /tmp/tools
ADD make-all.sh /tmp/make-all.sh

RUN mkdir /opt/bear
RUN cd ${MAKE_ALL_RUNDIR} && /tmp/make-all.sh

WORKDIR /opt/bear
CMD ls query-*
