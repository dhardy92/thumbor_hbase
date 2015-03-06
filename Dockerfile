FROM debian:squeeze

# Install basics
RUN \
  apt-get -qq update && \
  apt-get -y install wget python-software-properties && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Install Java
RUN \
  apt-get update && \
  apt-get install -y openjdk-6-jre-headless && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Install HBase 0.92.1 of CDH 4.1.2
RUN \
  echo "deb http://archive.cloudera.com/cdh4/debian/squeeze/amd64/cdh squeeze-cdh4 contrib" > /etc/apt/sources.list.d/cloudera_repository.list && \
  wget -qO - http://archive.cloudera.com/cdh4/debian/squeeze/amd64/cdh/archive.key | apt-key add - && \
  apt-get update && \
  apt-get install -y hbase-master hbase-thrift && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Supervisor
RUN \
  apt-get -qq update && \
  apt-get install -y supervisor && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

ADD supervisor.conf /etc/supervisor.conf

# Thrift API
EXPOSE 9090
# Thrift Web UI
EXPOSE 9095

CMD ["supervisord", "-c", "/etc/supervisor.conf"]
