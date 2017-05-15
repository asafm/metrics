package com.codahale.metrics.graphite;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.HostPortWaitStrategy;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.restassured.RestAssured.given;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class GraphiteITest {

    private static GenericContainer graphiteContainer;
    private final static Logger logger = LoggerFactory.getLogger(GraphiteITest.class);

    @BeforeClass
    public static void beforeClass() {
        graphiteContainer = new GenericContainer("sitespeedio/graphite:0.9.14")
                .withExposedPorts(2003, 80)
                .withFileSystemBind("/tmp/glog-itest/carbon.log", "/var/log/carbon.log", BindMode.READ_WRITE)
                .withClasspathResourceMapping("carbon.conf", "/opt/graphite/conf/carbon.conf", BindMode.READ_WRITE)
                .withClasspathResourceMapping("blacklist.conf", "/opt/graphite/conf/blacklist.conf", BindMode.READ_WRITE)
                .withClasspathResourceMapping("storage-schemas.conf", "/opt/graphite/conf/storage-schemas.conf", BindMode.READ_WRITE)
                .waitingFor(new HostPortWaitStrategy().withStartupTimeout(Duration.ofSeconds(10)));

        graphiteContainer.start();
        graphiteContainer.followOutput(new Slf4jLogConsumer(logger).withPrefix("graphite"),
                OutputFrame.OutputType.STDERR, OutputFrame.OutputType.STDOUT, OutputFrame.OutputType.END);
    }

    @AfterClass
    public static void afterClass() {
        if (graphiteContainer != null) graphiteContainer.stop();
    }

    @Test
    public void sanity() throws IOException, InterruptedException {
        Graphite graphite = new Graphite("localhost", graphiteContainer.getMappedPort(2003));
        graphite.connect();
    }

    @Test
    public void testWrite() throws IOException, InterruptedException {
        Graphite graphite = new Graphite("localhost", graphiteContainer.getMappedPort(2003));

        graphite.connect();

        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC")).truncatedTo(MINUTES);
        String metricName = "numOfLogz";
        String value = "100.0";
        graphite.send(metricName, value, now.toEpochSecond());

        graphite.flush();
        graphite.close();



        String result =
            given()
                .port(graphiteContainer.getMappedPort(80))
                .auth()
                .basic("guest", "guest")
            .when()
                .get("/render/?target="+metricName+"&format=csv&from=-5min")
                .asString();

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        assertThat(result).contains(metricName + "," + dateTimeFormatter.format(now) + "," + value);
    }
}
