#https://github.com/fabric8io-images/java/blob/master/images/alpine/openjdk8/jre/Dockerfile
FROM alpine:3.9

# /dev/urandom is used as random source, which is perfectly safe
# according to http://www.2uo.de/myths-about-urandom/
RUN apk add --update \
    curl \
    openjdk8-jre-base=8.212.04-r0 \
 && rm /var/cache/apk/* \
 && echo "securerandom.source=file:/dev/urandom" >> /usr/lib/jvm/default-jvm/jre/lib/security/java.security

ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME
COPY target/*-thorntail.jar ./app.jar
RUN chmod 775 /usr/app
EXPOSE 8080
CMD ["java", "-jar","app.jar"]
