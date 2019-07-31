FROM openjdk:8-jre-alpine
RUN apk add --no-cache curl
ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME
COPY target/*-jdkhttp-fat.jar /usr/app/
RUN chmod 775 /usr/app
EXPOSE 8080
CMD ["java", "-jar","openshift-kie-jdkhttp-fat.jar"]