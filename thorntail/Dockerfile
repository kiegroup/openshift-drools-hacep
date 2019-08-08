FROM openjdk:8-jre-alpine
RUN apk add --no-cache curl
ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME
COPY target/*-thorntail.jar ./app.jar
RUN chmod 775 /usr/app
EXPOSE 8080
CMD ["java", "-jar","app.jar"]