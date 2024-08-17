# Use an official OpenJDK runtime as a parent image
FROM openjdk:11-jre-slim

# Set environment variables for Spark
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install dependencies
RUN apt-get update && \
    apt-get install -y curl procps build-essential cmake libgtk-3-dev \
                       libboost-all-dev python3 python3-pip python3-dev \
                       python3-numpy python3-scipy && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -L "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | \
    tar -xzC /opt && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark

# Install Python packages: dlib and opencv-python
RUN pip3 install dlib opencv-python

# Expose Spark ports
EXPOSE 7077 8080 4040

# Set up entry point for Spark
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
