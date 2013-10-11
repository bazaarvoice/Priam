package com.netflix.priam.aws;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.Throttle;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of IBackupFileSystem for EBS
 */
@Singleton
public class EBSFileSystem implements IBackupFileSystem<File,File>, EBSFileSystemMBean {
    private static final Logger logger = LoggerFactory.getLogger(EBSFileSystem.class);
    private static final long UPLOAD_TIMEOUT = (2 * 60 * 60 * 1000L);
    private static final long AWS_API_POLL_INTERVAL = (30 * 1000L);

    private final Provider<AbstractBackupPath> pathProvider;
    private final BackupConfiguration backupConfiguration;
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final InstanceIdentity instanceIdentity;
    private final ICredential cred;
    private Throttle throttle;

    private AtomicLong bytesDownloaded = new AtomicLong();
    private AtomicLong bytesUploaded = new AtomicLong();
    private AtomicInteger uploadCount = new AtomicInteger();
    private AtomicInteger downloadCount = new AtomicInteger();
    private AmazonEC2Client ec2client;

    @Inject
    public EBSFileSystem(Provider<AbstractBackupPath> pathProvider, final BackupConfiguration backupConfiguration, CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration, InstanceIdentity instanceIdentity, ICredential cred)
        throws BackupRestoreException {
        this.pathProvider = pathProvider;
        this.cassandraConfiguration = cassandraConfiguration;
        this.backupConfiguration = backupConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.instanceIdentity = instanceIdentity;
        this.cred = cred;
        this.ec2client = new AmazonEC2Client(cred.getCredentials());

        this.throttle = new Throttle(this.getClass().getCanonicalName(), new Throttle.ThroughputFunction() {
            public int targetThroughput() {
                // @TODO: this is going to cause a stack overflow because an int can't hold as much as a long
                // converting from bytes per second to Mbps should help, but it doesn't provide guarantees
                int throttleLimit = (int)backupConfiguration.getUploadThrottleBytesPerSec();
                logger.info("throttle limit: {}", throttleLimit);
                if (throttleLimit < 1) {
                    return 0;
                }
                int totalBytesPerMS = throttleLimit / 1000;
                return totalBytesPerMS;
            }
        });

        // mount and attach EBS
        ebsMountAndAttach();

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = MBEAN_NAME;
        try {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Volume> getEbsVolumes() {
        DescribeVolumesRequest volumesRequest = new DescribeVolumesRequest()
                .withFilters(
                        new Filter()
                                .withName("tag:Name")
                                .withValues(cassandraConfiguration.getClusterName() + "-" + instanceIdentity.getInstance().getToken())
                );

        DescribeVolumesResult volumesResult = ec2client.describeVolumes(volumesRequest);

        return volumesResult.getVolumes();
    }

    public boolean isEbsAttached() {

        List<Volume> volumeList = getEbsVolumes();
        boolean isAttached = false;

        logger.info("{}", volumeList);

        // go ahead and return false if there are no results
        if (volumeList.isEmpty()){
            logger.info("No volumes match our criteria for this instance.");
            isAttached = false;
        } else {
            logger.info("Found volumes for this instance.");
            // go through all of the volumes with this tag name
            for (Volume vol : volumeList) {
                logger.info("Inspecting volume: {}", vol);
                // look at the attachments -- are any of them attached to the current instance?
                for (VolumeAttachment attachment : vol.getAttachments()) {
                    logger.info("Looking at attachment: {}", attachment);
                    // if yes, it's attached to us
                    if (attachment.getInstanceId().equals(instanceIdentity.getInstance().getInstanceId()) && attachment.getState().equals("attached")) {
                        logger.info("Found that the volume is attached to our instance.");
                        isAttached = true;
                        break;
                    }
                }
            }
        }

        return isAttached;
    }

    private void mountVolume(Volume volume) {

        logger.info("Attempting to mount volume: {}", volume);

        try {
            // if there's more than 1 attachment for this volume, then it's...attached to multiple instances?
            // this shouldn't happen because, currently, Amazon doesn't support that for EBS
            if (volume.getAttachments().size() > 1 ){
                logger.error("Failed to mount EBS volume {} because there were too many attachments for this volume. ", volume.getVolumeId());
            }

            // @TODO: make this platform-agnostic somehow
            // @TODO: don't do this every time we write to disk
            String mountEbsVolumeScript = new Scanner(getClass().getResourceAsStream("mountEbsVolume.sh")).toString();
            File mountEbsVolumeShell = File.createTempFile("mountEbsVolume", "sh");
            FileUtils.write(mountEbsVolumeShell, mountEbsVolumeScript);
            Process mountVolumeCmd = Runtime.getRuntime().exec(mountEbsVolumeShell.getAbsolutePath() + volume.getAttachments().get(0).getDevice() + " backup");

            logger.info("{}", CharStreams.toString(new InputStreamReader(mountVolumeCmd.getInputStream())));

        } catch (Exception e){ // runtime or IO
            logger.info("Failed to mount EBS volume. ", e.getMessage());
            logger.info(e.getMessage(), e);
            throw Throwables.propagate(e);
        }
    }

    // ideally, this list should always be of size 1 or 0
    // otherwise, what do we do with the other volumes??
    // leave it open in case for some reason we want to use multiple EBS volumes
    // but for now, just process the 1st volume only
    private void mountVolume(List<Volume> volumes) {
        for (Volume volume : volumes) {
            mountVolume(volume);
            // stop after mounting the first volume because we don't support multiple ebs volumes
            break;
        }
    }

    // attach volume
    private void attachVolume(Volume volume) {

        logger.info("Attaching volume: {}", volume);

        if (volume.getState().contentEquals("available")) {

            // define a sane default
            String devicePrefix = "/dev/xvd";
            String nextDeviceName = devicePrefix + "f";

            // @TODO: make this platform-agnostic?
            try {
                // grab the list of mounted devices
                Process getDeviceName = Runtime.getRuntime().exec("/bin/mount | grep '^" + devicePrefix + "' | awk '{print $1}'");
                // split the output into a String array
                String[] deviceNames = getDeviceName.getOutputStream().toString().split("\n");
                // sort the array
                Arrays.sort(deviceNames);
                // now just grab the last one in the array
                String lastDeviceName = deviceNames[deviceNames.length - 1];
                // make sure it starts with /dev/sd*, otherwise we have no EBS mounts present
                if (!lastDeviceName.startsWith(devicePrefix)) {
                    logger.debug("No devices beginning with " + devicePrefix + "* exist, so we should start with some sane default.");
                } else {
                    // now figure out the last letter of /dev/xvd*
                    char lastDeviceNameLastChar = lastDeviceName.substring(-1).charAt(0);
                    int lastCharNumericValue = Character.getNumericValue(lastDeviceNameLastChar);
                    // if we don't have a value greater than "f" we should ignore it
                    if (lastCharNumericValue < Character.getNumericValue('f') ){
                        logger.debug("Something is mounted at " + devicePrefix + "[a-e] but this won't map properly, so let's just go ahead and use 'f'.");
                    } else {
                        // our next device name in the sequence would be the next letter in the alphabet
                        // for Amazon request, we should mount to /dev/sd*, even though locally we want /dev/xvd*
                        nextDeviceName = devicePrefix + Character.forDigit(lastCharNumericValue + 1, 16);
                    }
                }

            } catch (Exception e){
                logger.error("Error figuring out device name", e);
                throw Throwables.propagate(e);
            }

            logger.info("Attaching volume {} on {}", volume.getVolumeId(), nextDeviceName);

            AttachVolumeRequest attachVolumeRequest = new AttachVolumeRequest()
                    .withVolumeId(volume.getVolumeId())
                    .withInstanceId(instanceIdentity.getInstance().getInstanceId())
                    .withDevice(nextDeviceName);
            AttachVolumeResult volumeAttachment = ec2client.attachVolume(attachVolumeRequest);

            String attachmentStatus = volumeAttachment.getAttachment().getState();

            while ("attaching".equals(attachmentStatus)) {
                try {

                    DescribeVolumesRequest describeVolumesRequest = new DescribeVolumesRequest().withVolumeIds(volume.getVolumeId());
                    List<Volume> attachmentVolumes = ec2client.describeVolumes(describeVolumesRequest).getVolumes();
                    // ensure our List is not null, not empty, and that getAttachments().get(0) doesn't throw an Index Out of Bounds exception
                    if (null != attachmentVolumes && attachmentVolumes.size() > 0
                            && null != attachmentVolumes.get(0).getAttachments() && attachmentVolumes.get(0).getAttachments().size() > 0) {
                        // seem weird? it is. You can't have more than 1 attachment, but Amazon provides a List instead of an Attachment object for some reason
                        attachmentStatus = attachmentVolumes.get(0).getAttachments().get(0).getState();
                    } else {
                        logger.info("Attachment status did not receive an update...");
                    }

                    logger.info("Attachment status: " + attachmentStatus);

                    if ("error".equals(attachmentStatus)){
                        logger.error("Error attaching EBS volume.");
                        break;
                    }

                    logger.info("Waiting for attachment...");
                    Thread.sleep(AWS_API_POLL_INTERVAL);

                } catch (InterruptedException e) {
                    logger.info("Failed to attach volume {}.", volume);
                    throw Throwables.propagate(e);
                }
            }

            if ("attached".equals(attachmentStatus)) {
                logger.info("Attached successfully on {}", volume.getAttachments().get(0).getDevice());
            }
        }
    }

    // create a new volume based on a snapshot
    public Volume createVolume(Optional<Snapshot> snapshot) throws RuntimeException {

        CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();

        // if a snapshot is supplied, use that for createVolumeRequest
        if (snapshot.isPresent()) {
            logger.info("Creating volume from snapshot: {}", snapshot);
            createVolumeRequest.setSize(snapshot.get().getVolumeSize());
            createVolumeRequest.setSnapshotId(snapshot.get().getSnapshotId());
        } else {
            logger.info("Creating volume from scratch");

            // figure out what disk size we need
            long raidSize = new File(cassandraConfiguration.getDataLocation()).getTotalSpace();

            // convert from bytes to gigabytes
            long volSize = (raidSize / 1024 / 1024 / 1024);

            // @TODO: potentially support striping volumes for nodes that need more than 1TiB of data on disk
            Preconditions.checkArgument(volSize <= 1024, "You cannot create EBS volumes greater than 1TiB. Scale up your ring so each node has less than 1TiB of data, or start striping EBS volumes.");

            logger.info("Create volume with size (GB): " + volSize);

            createVolumeRequest.setSize((int) volSize); // casting long to integer like this *should* be safe since we'll never have a drive with a number of gigabytes bigger than Int MaxValue
        }

        // always set availability zone to current AZ
        createVolumeRequest.setAvailabilityZone(amazonConfiguration.getAvailabilityZone());

        CreateVolumeResult createVolumeResult = ec2client.createVolume(createVolumeRequest);
        Volume createdVolume = new Volume().withState("creating"); // mock initially

        logger.info ("Creating volume... {}", createVolumeResult.getVolume());

        while ("creating".equals(createdVolume.getState())){

            try {
                DescribeVolumesResult createdVolumes = ec2client.describeVolumes(new DescribeVolumesRequest().withVolumeIds(createVolumeResult.getVolume().getVolumeId()));
                if (null != createdVolumes && createdVolumes.getVolumes().size() > 0){
                    createdVolume = createdVolumes.getVolumes().get(0);
                }

                logger.info("State: {}", createdVolume.getState());

                Thread.sleep(AWS_API_POLL_INTERVAL);
            } catch (Exception e){
                logger.info("Failed to create volume: " + e.getMessage());
                throw Throwables.propagate(e);
            }
        }

        if ("available".equals(createdVolume.getState())) {
            logger.info("Successfully created EBS volume.");
        } else {
            logger.error("Failed to create EBS volume.");
            throw Throwables.propagate(new BackupRestoreException("Failed to create EBS volume from snapshot"));
        }

        // tag the volume
        try {
            CreateTagsRequest createTagsRequest = new CreateTagsRequest()
                    .withTags(new Tag()
                            .withKey("Name")
                            .withValue(cassandraConfiguration.getClusterName() + "-" + instanceIdentity.getInstance().getToken()))
                    .withResources(createdVolume.getVolumeId());

            ec2client.createTags(createTagsRequest);
        } catch (Exception e){
            logger.error("Failed to create tags for EBS volume. {}", e.getMessage());
            throw Throwables.propagate(e);
        }

        return createdVolume;

    }

    public void ebsMountAndAttach() {

        logger.info("ebsMountAndAttach evoked");
        List<Volume> ebsVolumes = getEbsVolumes();

        // if an EBS volume is already mounted, we don't need to do anything here
        if (isEbsAttached()) {
            logger.info("EBS is already attached, so we just need to mount it.");
            mountVolume(ebsVolumes);
            return;
        }

        // locate any EBS volumes that should be attached
        // if we find an available volume matching our criteria, attach those volumes and mount them
        if (ebsVolumes.size() > 0) {

            for (Volume vol : ebsVolumes) {

                attachVolume(vol);
                mountVolume(vol);

            }

        } else { // never found an available volume to attach/reattach, so let's create one and then attach it

            logger.info("Never found a volume to attach, so creating one...");

            logger.info("Looking for the latest snapshot...");

            Snapshot useSnapshot = null;

            Filter[] snapshotFilters = new Filter[2];
            snapshotFilters[0] = new Filter()
                    .withName("tag:Name")
                    .withValues(cassandraConfiguration.getClusterName() + "-" + instanceIdentity.getInstance().getToken() + "-snap");
            snapshotFilters[1] = new Filter()
                    .withName("status")
                    .withValues("completed");


            DescribeSnapshotsResult describeSnapshotsResult = ec2client.describeSnapshots(
                    new DescribeSnapshotsRequest()
                            .withFilters(snapshotFilters)
            );

            List<Snapshot> snapshots = describeSnapshotsResult.getSnapshots();

            Calendar cal = new GregorianCalendar();
            cal.setTime(new Date());
            Date newestSnapshotTime = cal.getTime();

            for (Snapshot snapshot : snapshots ) {

                Date currentSnapshotTime = snapshot.getStartTime();

                if (currentSnapshotTime.after(newestSnapshotTime)) {
                    newestSnapshotTime = currentSnapshotTime;
                    useSnapshot = snapshot;
                }

                logger.info("Newest snapshot time: ");
                logger.info(newestSnapshotTime.toString());
            }

            Volume createdVolume;

            if (null != useSnapshot) {
                logger.info("Found snapshot: {}", useSnapshot);
                createdVolume = createVolume(Optional.of(useSnapshot));
            } else {
                logger.info("Did not find snapshot, so creating an empty EBS volume.");
                createdVolume = createVolume(Optional.<Snapshot>absent());
            }

            // attach the volume
            attachVolume(createdVolume);
        }
    }

    // "download" means copy from EBS volume to ephemeral disk
    @Override
    public void download(AbstractBackupPath path, File outputStream) throws BackupRestoreException {

//        logger.info("Ensure that EBS is attached for creating backups");
//        ebsMountAndAttach();

        logger.info("Downloading backup path {} to file {}", path, outputStream);

        try {
            //logger.info("Downloading " + path.getRemotePath());
            downloadCount.incrementAndGet();

            // copy from ebs to ephemeral
            logger.info("Remote path: {}", path.getRemotePath());
            File ebsPath = new File(path.getRemotePath());

            // write the file to outputstream
            // use this for FileChannels; Guava ByteStreams uses traditional copy
            // don't throttle copying from EBS to ephemeral -- we want to restore as quickly as possible
            FileUtils.copyFile(ebsPath, outputStream);
//            ByteStreams.copy(new FileInputStream(new File("/mnt/ephemeral/" + getPrefix())), outputStream);

            bytesDownloaded.addAndGet(FileUtils.sizeOf(outputStream));
        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage(), e);
        }
    }

    // "upload" means copy from ephemeral disk to EBS volume
    @Override
    public void upload(AbstractBackupPath path, File inputStream) throws BackupRestoreException {

//        logger.info("Ensure that EBS is attached for creating backups");
//        ebsMountAndAttach();

        logger.info("Uploading backup path {} to file {}", path, inputStream);
        logger.info("Full remote/ebs path for upload target: {}", backupConfiguration.getRestorePrefix() + EBSBackupPath.PATH_SEP + path.getRemotePath());

        try {
            uploadCount.incrementAndGet();

            // copy from ephemeral to EBS volume
            logger.info("Remote path: {}", path.getRemotePath());
            File ebsPath = new File(backupConfiguration.getRestorePrefix() + EBSBackupPath.PATH_SEP + path.getRemotePath());
            copyFile(inputStream, ebsPath);
//            FileUtils.copyFile(inputStream, ebsPath);
            bytesUploaded.addAndGet(FileUtils.sizeOf(inputStream));

        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage(), e);
        }

    }

    private void copyFile(File sourceFile, File destFile) throws IOException {

        // 65536 = 64k
        // convert to bytes
        long buffSize = backupConfiguration.getChunkSizeMB() * 1024 * 1024;

        if (sourceFile == null) {
            throw new NullPointerException("Source must not be null");
        }
        if (destFile == null) {
            throw new NullPointerException("Destination must not be null");
        }
        if (!sourceFile.exists()) {
            throw new FileNotFoundException("Source '" + sourceFile + "' does not exist");
        }
        if (sourceFile.isDirectory()) {
            throw new IOException("Source '" + sourceFile + "' exists but is a directory");
        }
        if (sourceFile.getCanonicalPath().equals(destFile.getCanonicalPath())) {
            throw new IOException("Source '" + sourceFile + "' and destination '" + destFile + "' are the same");
        }
        File parentFile = destFile.getParentFile();
        if (parentFile != null) {
            if (!parentFile.mkdirs() && !parentFile.isDirectory()) {
                throw new IOException("Destination '" + parentFile + "' directory cannot be created");
            }
        }
        if (destFile.exists() && !destFile.canWrite()) {
            throw new IOException("Destination '" + destFile + "' exists but is read-only");
        }

        if (destFile.exists() && destFile.isDirectory()){
            throw new IOException("Destination file '" + destFile + "' exists but is a directory.");
        }

        FileInputStream fis = null;
        FileOutputStream fos = null;
        FileChannel input = null;
        FileChannel output = null;

        try {
            fis = new FileInputStream(sourceFile);
            fos = new FileOutputStream(destFile);
            input = fis.getChannel();
            output = fos.getChannel();
            long size = input.size();
            long pos = 0;
            long count = 0;
            while (pos < size) {
                count = (size - pos) > buffSize ? buffSize : (size - pos);
                pos += output.transferFrom(input, pos, count);
                throttle.throttle(pos);
            }
        } catch (Exception e){
            logger.error("Error during copy: {}", e.getMessage());
            throw new IOException(e);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(fos);
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(fis);
        }

        if (sourceFile.length() != destFile.length()){
            throw new IOException("Failed to properly copy everything from " + sourceFile + " to " + destFile);
        }

        destFile.setLastModified(sourceFile.lastModified());

    }

    @Override
    public int getActivecount() {
        return 0;
    }

    @Override
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
        return new EBSFileIterator(pathProvider, path, start, till);
    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date) {
        return new EBSPrefixIterator(cassandraConfiguration, amazonConfiguration, backupConfiguration, pathProvider, date);
    }

    @Override
    public void snapshotEbs(String snapshotName) {
        // create a snapshot of the attached EBS volume
        // tag it with Name = [cassandra cluster name]-[token]
        // also tag it with Timestamp = [snapshotName which is a timestamp]
        List<Volume> ebsVolumes = getEbsVolumes();

        for(Volume vol : ebsVolumes) {

            logger.info("Creating snapshot for volume: {}", vol);

            try {

                // freeze filesystem
                new ProcessBuilder("xfs_freeze", "-f", backupConfiguration.getRestorePrefix()).start();

                CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest()
                        .withVolumeId(vol.getVolumeId())
                        .withDescription("Backup for " + cassandraConfiguration.getClusterName() + "-" + instanceIdentity.getInstance().getToken());
                CreateSnapshotResult createSnapshotResult = ec2client.createSnapshot(createSnapshotRequest);

                if (null == createSnapshotResult.getSnapshot()) {
                    logger.error("Failed to create EBS Snapshot.");
                    throw Throwables.propagate(new BackupRestoreException("Failed to create backup snapshot."));
                }

                // unfreeze filesystem
                new ProcessBuilder("xfs_freeze", "-u", backupConfiguration.getRestorePrefix()).start();

                CreateTagsRequest createTagsRequest = new CreateTagsRequest()
                        .withResources(createSnapshotResult.getSnapshot().getSnapshotId())
                        .withTags(
                                new Tag()
                                        .withKey("Name")
                                        .withValue(cassandraConfiguration.getClusterName() + "-" + instanceIdentity.getInstance().getToken() + "-snap"),
                                new Tag()
                                        .withKey("Timestamp")
                                        .withValue(snapshotName)
                        );
                ec2client.createTags(createTagsRequest);

            } catch (Exception e) {
                logger.error("Unable to create snapshot and tag volume {}, {}", vol, e.getMessage());
                throw Throwables.propagate(e);
            }

        }
    }

    /**
     * @TODO: We should cleanup our own EBS snapshots
     */
    @Override
    public void cleanup() {
        // noop
    }

    /**
     * Get prefix which will be used to locate files on EBS volume
     */
    public String getPrefix() {
        String prefix = "";
        if (StringUtils.isNotBlank(backupConfiguration.getRestorePrefix())) {
            prefix = backupConfiguration.getRestorePrefix();
        }

        String[] paths = prefix.split(String.valueOf(EBSBackupPath.PATH_SEP));
        return paths[0];
    }

    @Override
    public int downloadCount() {
        return downloadCount.get();
    }

    @Override
    public int uploadCount() {
        return uploadCount.get();
    }

    @Override
    public long bytesUploaded() {
        return bytesUploaded.get();
    }

    @Override
    public long bytesDownloaded() {
        return bytesDownloaded.get();
    }

}
