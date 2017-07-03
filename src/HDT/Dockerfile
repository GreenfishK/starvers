FROM rfdhdt/hdt-cpp

ADD tools /tmp/tools
ADD make-all.sh /tmp/make-all.sh

RUN mkdir /opt/bear
RUN cd /opt/hdt/hdt-lib && /tmp/make-all.sh

WORKDIR /opt/bear
CMD /opt/bear/query-ic-mat

