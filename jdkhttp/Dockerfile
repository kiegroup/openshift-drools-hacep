FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
RUN microdnf install java-1.8.0-openjdk-headless && microdnf clean all
COPY target/*-jdkhttp-fat.jar /app/app.jar
COPY scripts /scripts
RUN chmod 777 /app
ENV HOME /app
EXPOSE 8080
CMD ["bash", "/scripts/start.sh"]
