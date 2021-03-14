package com.netflix.priam.utils;

import com.datastax.driver.core.VersionNumber;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.Task;
import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

@Singleton
public class TuneCassandra extends Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(TuneCassandra.class);

    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final CassandraTuner tuner;

    @Inject
    public TuneCassandra(CassandraConfiguration cassandraConfiguration,
                         AmazonConfiguration amazonConfiguration,
                         CassandraTuner tuner) {
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.tuner = tuner;
    }

    @Override
    public void execute() {
        boolean isDone = false;

        while (!isDone) {
            try {
                tuner.writeAllProperties(cassandraConfiguration.getYamlLocation(),
                        amazonConfiguration.getPrivateIP(),
                        cassandraConfiguration.getSeedProviderClassName(),
                        getCassandraVersionNumber(cassandraConfiguration.getCassVersionScript()));
                isDone = true;
            } catch (IOException e) {
                LOGGER.info("Fail writing {} file. Retry again! {}", cassandraConfiguration.getYamlLocation(), e.toString());
            }
        }
    }

    @Override
    public String getName() {
        return "Tune-Cassandra";
    }

    public String getTriggerName() {
        return "tunecassandra-trigger";
    }

    /**
     * Normally Priam can introspect Cassandra to determine the version number, see {@link NodeProbe#getReleaseVersion()}.
     * However, certain configuration parameters in <code>cassandra.yaml</code> are only valid for certain Cassandra
     * versions, and it is useful to know beforehand what the Cassandra version is to avoid configuring unsupported
     * parameters.  This creates a chicken-and-egg problem since Cassandra must be started to probe it for its version
     * but it cannot be started without configuring it first.
     *
     * The solution is to provide Priam with a script which returns the Cassandra version.  Naively this could be something
     * like <code>echo "2.2.4"</code> but ideally this would be a simple call to <code>cassandra -v</code>.  It is
     * up to the Priam configuration to ensure this script returns a version which matches the actual version of Cassandra
     * which will be launched.
     *
     * The version script is not required, but without it Priam will be unable to validate whether the configuration file
     * it produces is consistent with the Cassandra version being configured.
     * 
     * @param cassVersionScript Executable script which will output the Cassandra version to stdout
     * @return The version number, or null if no script was provided or the script failed.
     */
    @Nullable
    private static VersionNumber getCassandraVersionNumber(String cassVersionScript) {
        if (cassVersionScript == null) {
            return null;
        }

        List<String> lines;
        int exitCode = -1;

        // Typically the script is a simple call to "cassandra -v".  Regardless, since the script shouldn't actually
        // start Cassandra assume the current user has permission to execute it.

        ProcessBuilder versionProcess = new ProcessBuilder(cassVersionScript.split("\\s"));
        versionProcess.redirectErrorStream();
        Process process = null;
        try {
            process = versionProcess.start();
            Reader in = new InputStreamReader(process.getInputStream(), Charsets.UTF_8);
            lines = CharStreams.readLines(in);
            process.waitFor();
        } catch (IOException | InterruptedException e) {
            LOGGER.info("Fail getting Cassandra version.  {}", e.toString());
            return null;
        } finally {
            if (process != null) {
                exitCode = process.exitValue();
            }
        }

        if (exitCode != 0) {
            LOGGER.info("Cassandra version script returned non-zero exit code: {}", exitCode);
            return null;
        }

        if (lines.isEmpty()) {
            LOGGER.info("Cassandra version script returned no output");
            return null;
        }

        // Expecting only one line; if more than one was returned version should be the last line
        String versionString = lines.get(lines.size()-1);
        try {
            VersionNumber versionNumber = VersionNumber.parse(versionString);
            LOGGER.info("Prior to starting Cassandra version number script reports version to be {}", versionNumber);
            return versionNumber;
        } catch (IllegalArgumentException e) {
            LOGGER.info("Cassandra version script returned invalid version: {}", versionString);
            return null;
        }
    }
}
