package com.netflix.priam;

import com.netflix.priam.config.BackupConfiguration;

public class TestBackupConfiguration extends BackupConfiguration {

    public TestBackupConfiguration() {
        setCommitLogBackupEnabled(false);
    }

}
