FROM gradle:9.6.0-jdk21 AS build

WORKDIR /app

COPY gradlew gradlew
COPY gradle gradle
COPY build.gradle settings.gradle ./
COPY src src

RUN chmod +x gradlew
RUN ./gradlew clean shadowJar --no-daemon

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /app/build/libs/message-relay-1.0.0.jar app.jar

RUN mkdir -p /app/data

EXPOSE 50051

CMD ["java", "-jar", "app.jar"]