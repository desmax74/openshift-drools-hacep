FROM registry.access.redhat.com/ubi8/ubi-minimal
RUN microdnf install --nodocs java-1.8.0-openjdk-headless && microdnf clean all
COPY target/*-jdkhttp-fat.jar /deployments/app.jar
COPY ./scripts/jboss-settings.xml /root/.m2/settings.xml
COPY ./scripts/logging.sh /deployments/logging.sh
COPY ./scripts/maven.sh /deployments/maven.sh
COPY ./scripts/start.sh /deployments/start.sh
EXPOSE 8080
CMD ["bash", "/deployments/start.sh"]