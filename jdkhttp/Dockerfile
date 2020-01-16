FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
RUN microdnf install java-1.8.0-openjdk-headless && microdnf install shadow-utils && microdnf clean all
RUN groupadd -r app -g <id_group> && useradd -u <id_user> -r -g app -m -d /app -s /sbin/nologin -c "App user" app && chmod 755 /app
WORKDIR /home/app/deployments
USER app
ENV HOME /app
COPY target/*-jdkhttp-fat.jar /home/app/deployments/app.jar
COPY ./scripts/logging.sh /home/app/deployments/logging.sh
COPY ./scripts/maven.sh /home/app/deployments/maven.sh
COPY ./scripts/start.sh /home/app/deployments/start.sh
COPY ./scripts/jboss-settings.xml /app/.m2/settings.xml
USER root
RUN chmod 777 /home/app/deployments /app/.m2
RUN chmod 666 /app/.m2/settings.xml
RUN chmod 775 /home/app/deployments/logging.sh /home/app/deployments/maven.sh /home/app/deployments/start.sh /home/app/deployments/app.jar
USER app
EXPOSE 8080
CMD ["bash", "/home/app/deployments/start.sh"]