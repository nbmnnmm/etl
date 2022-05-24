FROM dathoc/spark-base

LABEL maintainer="admin@dathoc.net"


ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077

ADD README.md / 
ADD  submit.sh /
ADD wordcount.py /


CMD ["/bin/bash", "/submit.sh"]
