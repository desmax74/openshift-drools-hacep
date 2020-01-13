FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
RUN microdnf install java-1.8.0-openjdk-headless && microdnf clean all
ENV HOME /deployments
COPY target/*-springboot.jar /deployments/app.jar
COPY ./scripts/jboss-settings.xml /deployments/.m2/settings.xml
COPY ./scripts/logging.sh /deployments/logging.sh
COPY ./scripts/maven.sh /deployments/maven.sh
COPY ./scripts/start.sh /deployments/start.sh
RUN chmod 777 /deployments /deployments/.m2
RUN chmod 666 /deployments/.m2/settings.xml
RUN chmod 775 /deployments/logging.sh /deployments/maven.sh /deployments/start.sh /deployments/app.jar
EXPOSE 8080
CMD ["bash", "/deployments/start.sh"]