# Dockerfile for scheduler-api
FROM maven:3.9.6-eclipse-temurin-21-alpine AS build
WORKDIR /app
COPY pom.xml .
COPY shared shared
COPY scheduler-api scheduler-api
COPY worker worker
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S appgroup && adduser -S app -G appgroup
WORKDIR /app
COPY --from=build /app/scheduler-api/target/scheduler-api-1.0.0-SNAPSHOT.jar app.jar
RUN chown -R app:appgroup /app
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
