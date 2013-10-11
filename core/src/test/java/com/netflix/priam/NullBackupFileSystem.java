package com.netflix.priam;

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

public class NullBackupFileSystem implements IBackupFileSystem<InputStream,OutputStream>
{

    @Override
    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till)
    {
        return null;
    }

    @Override
    public int getActivecount()
    {
        return 0;
    }

    @Override
    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException
    {
    }

    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException
    {
    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date)
    {
        return null;
    }

    @Override
    public void cleanup()
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void snapshotEbs(String snapshotName) {
        // noop
    }
}