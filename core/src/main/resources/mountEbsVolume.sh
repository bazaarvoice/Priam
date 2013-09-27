#!/bin/bash

#
# Attaches EC2 EBS volume, formats and mounts it under a given mount point.
#
# First argument is the EBS volume mount point
# Second argument is mount point under /mnt/
#
# Example:
# mountEbsVolume.sh /dev/sdh backup
#

set -o errexit
set -o nounset
set -o xtrace

BLOCK_DEVICE="${1}"
MOUNT_POINT="${2}"

if [ -z $MOUNT_POINT ]; then
    MOUNT_POINT="backup"
fi

if mount | grep -qw /mnt/$MOUNT_POINT; then
  echo "EBS volume already mounted."
  exit 0
fi

# Unmount anything currently mounted on MOUNT_POINT
mount | awk '/$MOUNT_POINT/ {print $1}'| xargs -r umount
cp /etc/fstab /etc/fstab.orig
sed -i '/$MOUNT_POINT/d' /etc/fstab

yum install -y xfsprogs

mkfs.xfs -f $BLOCK_DEVICE

grep -q /mnt/$MOUNT_POINT /etc/fstab || {
    # Add the new FS for automount
    echo "$BLOCK_DEVICE  /mnt/$MOUNT_POINT xfs  async,noatime         0   2" >> /etc/fstab
}

mkdir -p /mnt/$MOUNT_POINT
mount /mnt/$MOUNT_POINT
