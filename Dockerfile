# syntax=docker/dockerfile:1

# =============================================================================================
# IronFlow engine image. Multi-stage: build the executable jar with Maven, then run it on a
# slim JRE. The build stage's local Maven repo is cached as a BuildKit mount so repeated builds
# do not re-download the dependency tree.
# =============================================================================================

# ---- Build stage ----------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Copy the POM first and warm the dependency cache in its own layer, so a source-only change
# does not invalidate the (slow) dependency download.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -e -Dmaven.test.skip=true dependency:go-offline

COPY src ./src
# repackage produces the Spring Boot executable fat jar. Tests are skipped in the image build;
# they run in CI, not on every image assembly.
RUN --mount=type=cache,target=/root/.m2 mvn -q -e -Dmaven.test.skip=true clean package \
    && cp target/*.jar /build/app.jar

# ---- Runtime stage --------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

# Run as a non-root user. A workflow engine holds a database credential; there is no reason for
# its process to be uid 0 inside the container.
RUN groupadd --system ironflow && useradd --system --gid ironflow --home /app ironflow

COPY --from=build /build/app.jar /app/app.jar
RUN chown -R ironflow:ironflow /app
USER ironflow

EXPOSE 8080

# Container-aware heap: let the JVM size itself to the cgroup memory limit rather than the host.
# Virtual threads are enabled in application.yml; no thread-pool flags needed here.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+UseZGC"

# The health endpoint backs the compose healthcheck and any orchestrator readiness probe.
HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=5 \
    CMD ["sh", "-c", "wget -qO- http://localhost:8080/actuator/health | grep -q UP || exit 1"]

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
