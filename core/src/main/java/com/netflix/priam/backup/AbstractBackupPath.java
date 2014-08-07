/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.Date;

public abstract class AbstractBackupPath implements Comparable<AbstractBackupPath> {
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm");
    public static final char PATH_SEP = File.separatorChar;

    public static enum BackupFileType {
        SNAP, SST, CL, META
    }

    protected BackupFileType type;
    protected String clusterName;
    protected String keyspace;
    protected String columnFamily;
    protected String fileName;
    protected String baseDir;
    protected String token;
    protected String region;
    protected Date time;
    protected long size;
    protected boolean isCassandra1_0;

    protected final InstanceIdentity instanceIdentity;
    protected final CassandraConfiguration cassandraConfiguration;
    protected final AmazonConfiguration amazonConfiguration;
    protected final BackupConfiguration backupConfiguration;
    protected File backupFile;

    public AbstractBackupPath(CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration, BackupConfiguration backupConfiguration, InstanceIdentity instanceIdentity) {
        this.instanceIdentity = instanceIdentity;
        this.cassandraConfiguration = cassandraConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.backupConfiguration = backupConfiguration;
    }

    public String formatDate(Date d) {
        return DATE_FORMAT.print(new DateTime(d));
    }

    public Date parseDate(String s) {
        return DATE_FORMAT.parseDateTime(s).toDate();
    }

    public InputStream localReader() throws IOException {
        assert backupFile != null;
        return new RafInputStream(RandomAccessReader.open(backupFile, true));
    }

    public void parseLocal(File file, BackupFileType type) throws ParseException {
        // TODO cleanup.
        this.backupFile = file;

        String rpath = new File(cassandraConfiguration.getDataLocation()).toURI().relativize(file.toURI()).getPath();
        String[] elements = rpath.split("" + PATH_SEP);
        this.clusterName = cassandraConfiguration.getClusterName();
        this.baseDir = backupConfiguration.getS3BaseDir();
        this.region = amazonConfiguration.getRegionName();
        this.token = instanceIdentity.getInstance().getToken();
        this.type = type;
        if (type != BackupFileType.META && type != BackupFileType.CL) {
            this.keyspace = elements[0];
            if (!isCassandra1_0) {
                this.columnFamily = elements[1];
            }
        }
        if (type == BackupFileType.SNAP) {
            time = parseDate(elements[3]);
        }
        if (type == BackupFileType.SST || type == BackupFileType.CL) {
            time = new Date(file.lastModified());
        }
        this.fileName = file.getName();
        this.size = file.length();
    }

    /**
     * Given a date range, find a common string prefix Eg: 20120212, 20120213 =>
     * 2012021
     */
    public String match(Date start, Date end) {
        String sString = formatDate(start);
        String eString = formatDate(end);
        int diff = StringUtils.indexOfDifference(sString, eString);
        if (diff < 0) {
            return sString;
        }
        return sString.substring(0, diff);
    }

    /**
     * Local restore file
     */
    public File newRestoreFile() {
        StringBuilder buff = new StringBuilder();
        if (type == BackupFileType.CL) {
            buff.append(backupConfiguration.getCommitLogLocation()).append(PATH_SEP);
        } else {

            buff.append(cassandraConfiguration.getDataLocation()).append(PATH_SEP);
            if (type != BackupFileType.META) {
                buff.append(keyspace).append(PATH_SEP);
                if (!isCassandra1_0) {
                    buff.append(columnFamily).append(PATH_SEP);
                }
            }
        }
        buff.append(fileName);
        File return_ = new File(buff.toString());
        File parent = new File(return_.getParent());
        if (!parent.exists()) {
            parent.mkdirs();
        }
        return return_;
    }

    @Override
    public int compareTo(AbstractBackupPath o) {
        return getRemotePath().compareTo(o.getRemotePath());
    }

    @Override
    public boolean equals(Object obj) {
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }
        return getRemotePath().equals(((AbstractBackupPath) obj).getRemotePath());
    }

    /**
     * Get remote prefix for this path object
     */
    public abstract String getRemotePath();

    /**
     * Parses a fully constructed remote path
     */
    public abstract void parseRemote(String remoteFilePath);

    /**
     * Parses paths with just token prefixes
     */
    public abstract void parsePartialPrefix(String remoteFilePath);

    /**
     * Provides a common prefix that matches all objects that fall between
     * the start and end time
     */
    public abstract String remotePrefix(Date start, Date end, String location);

    /**
     * Provides the cluster prefix
     */
    public abstract String clusterPrefix(String location);

    public BackupFileType getType() {
        return type;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getFileName() {
        return fileName;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public String getToken() {
        return token;
    }

    public String getRegion() {
        return region;
    }

    public Date getTime() {
        return time;
    }

    public long getSize() {
        return size;
    }

    public File getBackupFile() {
        return backupFile;
    }

    public boolean isCassandra1_0() {
        return isCassandra1_0;
    }

    public void setCassandra1_0(boolean isCassandra1_0) {
        this.isCassandra1_0 = isCassandra1_0;
    }

    public static class RafInputStream extends InputStream {
        private final RandomAccessFile raf;

        public RafInputStream(RandomAccessFile raf) {
            this.raf = raf;
        }

        @Override
        public synchronized int read(byte[] bytes, int off, int len) throws IOException {
            return raf.read(bytes, off, len);
        }

        @Override
        public void close() {
            FileUtils.closeQuietly(raf);
        }

        @Override
        public int read() throws IOException {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "From: " + getRemotePath() + " To: " + newRestoreFile().getPath();
    }
}
