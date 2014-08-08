package com.netflix.priam.defaultimpl;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.Sleeper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CassandraProcessManager implements ICassandraProcess {
    private static final Logger logger = LoggerFactory.getLogger(CassandraProcessManager.class);
    private static final String SUDO_STRING = "/usr/bin/sudo";
    private static final int SCRIPT_EXECUTE_WAIT_TIME_MS = 5000;
    private final CassandraConfiguration cassandraConfig;
    private final AmazonConfiguration amazonConfig;
    private final Sleeper sleeper;

    @Inject
    public CassandraProcessManager(CassandraConfiguration cassandraConfig, AmazonConfiguration amazonConfig, Sleeper sleeper) {
        this.cassandraConfig = cassandraConfig;
        this.amazonConfig = amazonConfig;
        this.sleeper = sleeper;
    }

    @Override
    public void start(boolean joinRing) throws IOException {
        logger.info("Starting cassandra server ....Join ring={}", joinRing);

        List<String> command = Lists.newArrayList();
        if (!"root".equals(System.getProperty("user.name"))) {
            command.add(SUDO_STRING);
            command.add("-n");
            command.add("-E");
        }
        command.addAll(getStartCommand());

        ProcessBuilder startCass = new ProcessBuilder(command);
        Map<String, String> env = startCass.environment();
        env.put("HEAP_NEWSIZE", cassandraConfig.getMaxNewGenHeapSize().get(amazonConfig.getInstanceType()));
        env.put("MAX_HEAP_SIZE", cassandraConfig.getMaxHeapSize().get(amazonConfig.getInstanceType()));
        env.put("CASSANDRA_HEAPDUMP_DIR", cassandraConfig.getHeapDumpLocation());
        env.put("JMX_PORT", Integer.toString(cassandraConfig.getJmxPort()));
        env.put("cassandra.join_ring", Boolean.toString(joinRing));
        startCass.directory(new File("/"));
        startCass.redirectErrorStream(true);
        Process starter = startCass.start();
        logger.info("Starting cassandra server ....");
        try {
            sleeper.sleepQuietly(SCRIPT_EXECUTE_WAIT_TIME_MS);
            int code = starter.exitValue();
            if (code == 0) {
                logger.info("Cassandra server has been started");
            } else {
                logger.error("Unable to start cassandra server. Error code: {}", code);
            }

            logProcessOutput(starter);
        } catch (Exception e) {
            logger.warn("Starting Cassandra has an error", e);
        }
    }

    protected List<String> getStartCommand() {
        List<String> startCmd = new LinkedList<>();
        for (String param : cassandraConfig.getCassStartScript().split(" ")) {
            if (StringUtils.isNotBlank(param)) {
                startCmd.add(param);
            }
        }
        return startCmd;
    }

    void logProcessOutput(Process p) {
        try {
            final String stdOut = readProcessStream(p.getInputStream());
            final String stdErr = readProcessStream(p.getErrorStream());
            logger.info("std_out: {}", stdOut);
            logger.info("std_err: {}", stdErr);
        } catch (IOException ioe) {
            logger.warn("Failed to read the std out/err streams", ioe);
        }
    }

    String readProcessStream(InputStream inputStream) throws IOException {
        final byte[] buffer = new byte[512];
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.length);
        int cnt;
        while ((cnt = inputStream.read(buffer)) != -1) {
            baos.write(buffer, 0, cnt);
        }
        return baos.toString();
    }

    @Override
    public void stop() throws IOException {
        logger.info("Stopping cassandra server ....");
        List<String> command = Lists.newArrayList();
        if (!"root".equals(System.getProperty("user.name"))) {
            command.add(SUDO_STRING);
            command.add("-n");
            command.add("-E");
        }
        for (String param : cassandraConfig.getCassStopScript().split(" ")) {
            if (StringUtils.isNotBlank(param)) {
                command.add(param);
            }
        }
        ProcessBuilder stopCass = new ProcessBuilder(command);
        stopCass.directory(new File("/"));
        stopCass.redirectErrorStream(true);
        Process stopper = stopCass.start();

        sleeper.sleepQuietly(SCRIPT_EXECUTE_WAIT_TIME_MS);
        try {
            int code = stopper.exitValue();
            if (code == 0) {
                logger.info("Cassandra server has been stopped");
            } else {
                logger.error("Unable to stop cassandra server. Error code: {}", code);
                logProcessOutput(stopper);
            }
        } catch (Exception e) {
            logger.warn("couldn't shut down cassandra correctly", e);
        }
    }
}
