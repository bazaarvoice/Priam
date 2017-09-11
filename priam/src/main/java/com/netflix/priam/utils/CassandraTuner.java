package com.netflix.priam.utils;

import com.datastax.driver.core.VersionNumber;
import com.google.inject.ImplementedBy;
import com.netflix.priam.defaultimpl.StandardTuner;

import javax.annotation.Nullable;
import java.io.IOException;

@ImplementedBy(StandardTuner.class)
public interface CassandraTuner {
    void writeAllProperties(String yamlLocation, String hostname, String seedProvider, @Nullable VersionNumber cassandraVersion) throws IOException;

    void updateAutoBootstrap(String yamlLocation, boolean autobootstrap) throws IOException;
}
