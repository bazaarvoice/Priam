package com.netflix.priam.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BackupConfiguration {
    @JsonProperty
    private boolean commitLogBackupEnabled;

    @JsonProperty
    private String commitLogBackupArchiveCmd;

    @JsonProperty
    private String commitLogBackupRestoreCmd;

    @JsonProperty
    private String commitLogBackupRestoreFromDirs;

    @JsonProperty
    private String commitLogBackupRestorePointInTime;

    @JsonProperty
    private boolean incrementalBackupEnabledForCassandra;

    public boolean isCommitLogBackupEnabled() {
        return commitLogBackupEnabled;
    }

    public String getCommitLogBackupArchiveCmd() {
        return commitLogBackupArchiveCmd;
    }

    public String getCommitLogBackupRestoreCmd() {
        return commitLogBackupRestoreCmd;
    }

    public String getCommitLogBackupRestoreFromDirs() {
        return commitLogBackupRestoreFromDirs;
    }

    public String getCommitLogBackupRestorePointInTime() {
        return commitLogBackupRestorePointInTime;
    }

    public boolean isIncrementalBackupEnabledForCassandra() {
        return incrementalBackupEnabledForCassandra;
    }

    public void setCommitLogBackupEnabled(boolean commitLogBackupEnabled) {
        this.commitLogBackupEnabled = commitLogBackupEnabled;
    }

    public void setCommitLogBackupArchiveCmd(String commitLogBackupArchiveCmd) {
        this.commitLogBackupArchiveCmd = commitLogBackupArchiveCmd;
    }

    public void setCommitLogBackupRestoreCmd(String commitLogBackupRestoreCmd) {
        this.commitLogBackupRestoreCmd = commitLogBackupRestoreCmd;
    }

    public void setCommitLogBackupRestoreFromDirs(String commitLogBackupRestoreFromDirs) {
        this.commitLogBackupRestoreFromDirs = commitLogBackupRestoreFromDirs;
    }

    public void setCommitLogBackupRestorePointInTime(String commitLogBackupRestorePointInTime) {
        this.commitLogBackupRestorePointInTime = commitLogBackupRestorePointInTime;
    }

    public void setIncrementalBackupEnabledForCassandra(boolean incrementalBackupEnabledForCassandra) {
        this.incrementalBackupEnabledForCassandra = incrementalBackupEnabledForCassandra;
    }
}
