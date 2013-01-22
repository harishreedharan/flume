/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file;

import static org.apache.flume.channel.file.TestUtils.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import org.apache.flume.channel.file.proto.ProtosFactory;

public class TestFileChannelRestart extends TestFileChannelBase {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestFileChannelRestart.class);

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
  }
  @Test
  public void testRestartLogReplayV1() throws Exception {
    doTestRestart(true, false, false, false);
  }
  @Test
  public void testRestartLogReplayV2() throws Exception {
    doTestRestart(false, false, false, false);
  }

  @Test
  public void testFastReplayV1() throws Exception {
    doTestRestart(true, true, true, true);
  }

  @Test
  public void testFastReplayV2() throws Exception {
    doTestRestart(false, true, true, true);
  }

  @Test
  public void testFastReplayNegativeTestV1() throws Exception {
    doTestRestart(true, true, false, true);
  }

  @Test
  public void testFastReplayNegativeTestV2() throws Exception {
    doTestRestart(false, true, false, true);
  }

  @Test
  public void testNormalReplayV1() throws Exception {
    doTestRestart(true, true, true, false);
  }

  @Test
  public void testNormalReplayV2() throws Exception {
    doTestRestart(false, true, true, false);
  }

  public void doTestRestart(boolean useLogReplayV1,
          boolean forceCheckpoint, boolean deleteCheckpoint,
          boolean useFastReplay) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_LOG_REPLAY_V1,
            String.valueOf(useLogReplayV1));
    overrides.put(
            FileChannelConfiguration.USE_FAST_REPLAY,
            String.valueOf(useFastReplay));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "restart");
    if (forceCheckpoint) {
      forceCheckpoint(channel);
    }
    channel.stop();
    if(deleteCheckpoint) {
      File checkpoint = new File(checkpointDir, "checkpoint");
      Assert.assertTrue(checkpoint.delete());
      File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
      Assert.assertTrue(checkpointMetaData.delete());
    }
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testRestartWhenMetaDataExistsButCheckpointDoesNot() throws
      Exception {
    doTestRestartWhenMetaDataExistsButCheckpointDoesNot(false);
  }

  @Test
  public void testRestartWhenMetaDataExistsButCheckpointDoesNotWithBackup()
      throws Exception {
    doTestRestartWhenMetaDataExistsButCheckpointDoesNot(true);
  }

  private void doTestRestartWhenMetaDataExistsButCheckpointDoesNot(
      boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    Assert.assertTrue(checkpoint.delete());
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.exists());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(checkpoint.exists());
    Assert.assertTrue(checkpointMetaData.exists());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testRestartWhenCheckpointExistsButMetaDoesNot() throws Exception{
    doTestRestartWhenCheckpointExistsButMetaDoesNot(false);
  }

  @Test
  public void testRestartWhenCheckpointExistsButMetaDoesNotWithBackup() throws
      Exception{
    doTestRestartWhenCheckpointExistsButMetaDoesNot(true);
  }


  private void doTestRestartWhenCheckpointExistsButMetaDoesNot(boolean backup)
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.delete());
    Assert.assertTrue(checkpoint.exists());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(checkpoint.exists());
    Assert.assertTrue(checkpointMetaData.exists());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testRestartWhenNoCheckpointExists() throws Exception {
    doTestRestartWhenNoCheckpointExists(false);
  }

  @Test
  public void testRestartWhenNoCheckpointExistsWithBackup() throws Exception {
    doTestRestartWhenNoCheckpointExists(true);
  }

  private void doTestRestartWhenNoCheckpointExists(boolean backup) throws
      Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.delete());
    Assert.assertTrue(checkpoint.delete());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(checkpoint.exists());
    Assert.assertTrue(checkpointMetaData.exists());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testBadCheckpointVersion() throws Exception {
    doTestBadCheckpointVersion(false);
  }

  @Test
  public void testBadCheckpointVersionWithBackup() throws Exception {
    doTestBadCheckpointVersion(true);
  }

  private void doTestBadCheckpointVersion(boolean backup) throws Exception{
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    writer.seek(EventQueueBackingStoreFile.INDEX_VERSION *
            Serialization.SIZE_OF_LONG);
    writer.writeLong(2L);
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testBadCheckpointMetaVersion() throws Exception {
    doTestBadCheckpointMetaVersion(false);
  }

  @Test
  public void testBadCheckpointMetaVersionWithBackup() throws Exception {
    doTestBadCheckpointMetaVersion(true);
  }

  private void doTestBadCheckpointMetaVersion(boolean backup) throws
      Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    FileInputStream is = new FileInputStream(Serialization.getMetaDataFile(checkpoint));
    ProtosFactory.Checkpoint meta = ProtosFactory.Checkpoint.parseDelimitedFrom(is);
    Assert.assertNotNull(meta);
    is.close();
    FileOutputStream os = new FileOutputStream(
            Serialization.getMetaDataFile(checkpoint));
    meta.toBuilder().setVersion(2).build().writeDelimitedTo(os);
    os.flush();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testDifferingOrderIDCheckpointAndMetaVersion() throws Exception {
    doTestDifferingOrderIDCheckpointAndMetaVersion(false);
  }

  @Test
  public void testDifferingOrderIDCheckpointAndMetaVersionWithBackup() throws
      Exception {
    doTestDifferingOrderIDCheckpointAndMetaVersion(true);
  }

  private void doTestDifferingOrderIDCheckpointAndMetaVersion(boolean backup)
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    FileInputStream is = new FileInputStream(Serialization.getMetaDataFile(checkpoint));
    ProtosFactory.Checkpoint meta = ProtosFactory.Checkpoint.parseDelimitedFrom(is);
    Assert.assertNotNull(meta);
    is.close();
    FileOutputStream os = new FileOutputStream(
            Serialization.getMetaDataFile(checkpoint));
    meta.toBuilder().setWriteOrderID(12).build().writeDelimitedTo(os);
    os.flush();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testIncompleteCheckpoint() throws Exception{
    doTestIncompleteCheckpoint(false);
  }

  @Test
  public void testIncompleteCheckpointWithCheckpoint() throws Exception{
    doTestIncompleteCheckpoint(true);
  }

  private void doTestIncompleteCheckpoint(boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    writer.seek(EventQueueBackingStoreFile.INDEX_CHECKPOINT_MARKER
            * Serialization.SIZE_OF_LONG);
    writer.writeLong(EventQueueBackingStoreFile.CHECKPOINT_INCOMPLETE);
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testCorruptInflightPuts() throws Exception {
    doTestCorruptInflights("inflightPuts", false);
  }

  @Test
  public void testCorruptInflightPutsWithBackup() throws Exception {
    doTestCorruptInflights("inflightPuts", true);
  }

  @Test
  public void testCorruptInflightTakes() throws Exception {
    doTestCorruptInflights("inflightTakes", false);
  }

  @Test
  public void testCorruptInflightTakesWithBackup() throws Exception {
    doTestCorruptInflights("inflightTakes", true);
  }

  private void doTestCorruptInflights(String name, boolean backup) throws
      Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File inflight = new File(checkpointDir, name);
    RandomAccessFile writer = new RandomAccessFile(inflight, "rw");
    writer.write(new Random().nextInt());
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testTruncatedCheckpointMeta() throws Exception {
    doTestTruncatedCheckpointMeta(false);
  }

  @Test
  public void testTruncatedCheckpointMetaWithBackup() throws Exception {
    doTestTruncatedCheckpointMeta(true);
  }

  private void doTestTruncatedCheckpointMeta(boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(
            Serialization.getMetaDataFile(checkpoint), "rw");
    writer.setLength(0);
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testCorruptCheckpointMeta() throws Exception {
    doTestCorruptCheckpointMeta(false);
  }

  @Test
  public void testCorruptCheckpointMetaWithBackup() throws Exception {
    doTestCorruptCheckpointMeta(true);
  }

  private void doTestCorruptCheckpointMeta(boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, backup);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(
            Serialization.getMetaDataFile(checkpoint), "rw");
    writer.seek(10);
    writer.writeLong(new Random().nextLong());
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(backup);
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  private void insertBackUpParametersIntoMap(Map<String, String> overrides,
      boolean backup) throws IOException {
    if (backup) {
      overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, "true");
    }
    overrides.put(FileChannelConfiguration.CHECKPOINT_DIR,
        checkpointDir.getCanonicalPath() + "," + backupDir.getCanonicalPath());
  }

  private void checkIfBackupUsed(boolean backup) {
    boolean backupRestored = channel.checkpointBackupRestored();
    if (backup) {
      Assert.assertTrue(backupRestored);
    } else {
      Assert.assertFalse(backupRestored);
    }
  }

  @Test
  public void testWithExtraLogs()
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, "10");
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "extralogs");
    for (int i = 0; i < dataDirs.length; i++) {
      File file = new File(dataDirs[i], Log.PREFIX + (1000 + i));
      Assert.assertTrue(file.createNewFile());
      Assert.assertTrue(file.length() == 0);
      File metaDataFile = Serialization.getMetaDataFile(file);
      File metaDataTempFile = Serialization.getMetaDataTempFile(metaDataFile);
      Assert.assertTrue(metaDataTempFile.createNewFile());
    }
    channel.stop();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  // Make sure the entire channel was not replayed, only the events from the
  // backup.
  @Test
  public void testBackupUsedEnsureNoFullReplay() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, true);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    //This will cause 10 commits + 100 puts, and should be replayed.
    in = putEvents(channel, "restart", 10, 100);
    //This will cause 10 commits + 100 takes, and should be replayed.
    takeEvents(channel, 10, 100);
    Assert.assertEquals(100, in.size());
    Thread.sleep(2000);
    channel.stop();
    Serialization.deleteAllFiles(checkpointDir);
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(true);
    Assert.assertEquals(100, channel.getLog().getPutCount());
    //Commit count = 10 put batches + 10 take batches
    Assert.assertEquals(20, channel.getLog().getCommittedCount());
    Assert.assertEquals(100, channel.getLog().getTakeCount());
    Assert.assertEquals(0, channel.getLog().getRollbackCount());
    //Read Count = 100 puts + 10 commits + 100 takes + 10 commits
    Assert.assertEquals(220, channel.getLog().getReadCount());
    consumeChannel(channel);
  }

  //Make sure data files required by the backup checkpoint are not deleted.
  @Test
  public void testDataFilesRequiredByBackupNotDeleted() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    insertBackUpParametersIntoMap(overrides, true);
    overrides.put(FileChannelConfiguration.MAX_FILE_SIZE, "1000");
    channel = createFileChannel(overrides);
    channel.start();
    String prefix = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
    Assert.assertTrue(channel.isOpen());
    putEvents(channel, prefix, 10, 100);
    Set<String> origFiles = Sets.newHashSet();
    for(File dir : dataDirs) {
      origFiles.addAll(Lists.newArrayList(dir.list()));
    }
    forceCheckpoint(channel);
    takeEvents(channel, 10, 50);
    /*
     * At this point several data files should no longer be required by the
     * FEQ, since all events would be taken and committed. So the next
     * checkpoint should not update them, but should not delete them either as
     * backup is enabled, and the backup checkpoint still needs them.
     */
    long beforeSecondCheckpoint = System.currentTimeMillis();
    forceCheckpoint(channel);
    Set<String> newFiles = Sets.newHashSet();
    int olderThanCheckpoint = 0;
    int totalMetaFiles = 0;
    for(File dir : dataDirs) {
      File[] metadataFiles = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name.endsWith(".meta")) {
            return true;
          }
          return false;
        }
      });
      totalMetaFiles = metadataFiles.length;
      for(File metadataFile : metadataFiles) {
        if(metadataFile.lastModified() < beforeSecondCheckpoint) {
          olderThanCheckpoint++;
        }
      }
      newFiles.addAll(Lists.newArrayList(dir.list()));
    }
    /*
     * Files which are not required by the new checkpoint,
     * but required by the backup should not have been
     * updated by the checkpoint.
     */
    Assert.assertTrue(olderThanCheckpoint > 0);
    /*
     * But some files should have been updated by the checkpoint.
     */
    Assert.assertTrue(totalMetaFiles != olderThanCheckpoint);

    /*
     * All files needed by original checkpoint should still be there.
     */
    Assert.assertTrue(newFiles.containsAll(origFiles));
    takeEvents(channel, 10, 50);
    /*
     * This checkpoint can now delete a bunch of files which were originally
     * required by the first checkpoint, since the backup checkpoint is now the
     * 2nd checkpoint, not the first.
     */
    forceCheckpoint(channel);
    newFiles = Sets.newHashSet();
    for(File dir : dataDirs) {
      newFiles.addAll(Lists.newArrayList(dir.list()));
    }
    /*
     * Make sure the files required by checkpoint 1,
     * but not by checkpoint 2 were deleted.
     */
    Assert.assertTrue(!newFiles.containsAll(origFiles));
  }
}
