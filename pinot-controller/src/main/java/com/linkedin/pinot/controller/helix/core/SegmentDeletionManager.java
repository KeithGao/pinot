/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.filesystem.PinotFS;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentDeletionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionManager.class);
  private static final long MAX_DELETION_DELAY_SECONDS = 300L;  // Maximum of 5 minutes back-off to retry the deletion
  private static final long DEFAULT_DELETION_DELAY_SECONDS = 2L;

  private final ScheduledExecutorService _executorService;
  private final String _dataDir;
  private final String _helixClusterName;
  private final HelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String DELETED_SEGMENTS = "Deleted_Segments";

  public SegmentDeletionManager(String dataDir, HelixAdmin helixAdmin, String helixClusterName, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _dataDir = dataDir;
    _helixAdmin = helixAdmin;
    _helixClusterName = helixClusterName;
    _propertyStore = propertyStore;

    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotHelixResourceManagerExecutorService");
        return thread;
      }
    });
  }

  public void stop() {
    _executorService.shutdownNow();
  }

  public void deleteSegments(final String tableName, final Collection<String> segmentIds) {
    deleteSegmentsWithDelay(tableName, segmentIds, DEFAULT_DELETION_DELAY_SECONDS);
  }

  protected void deleteSegmentsWithDelay(final String tableName, final Collection<String> segmentIds,
      final long deletionDelaySeconds) {
    _executorService.schedule(new Runnable() {
      @Override
      public void run() {
        deleteSegmentFromPropertyStoreAndLocal(tableName, segmentIds, deletionDelaySeconds);
      }
    }, deletionDelaySeconds, TimeUnit.SECONDS);
  }

  protected synchronized void deleteSegmentFromPropertyStoreAndLocal(String tableName, Collection<String> segmentIds, long deletionDelay) {
    // Check if segment got removed from ExternalView and IdealStates
    if (_helixAdmin.getResourceExternalView(_helixClusterName, tableName) == null
        || _helixAdmin.getResourceIdealState(_helixClusterName, tableName) == null) {
      LOGGER.warn("Resource: {} is not set up in idealState or ExternalView, won't do anything", tableName);
      return;
    }

    List<String> segmentsToDelete = new ArrayList<>(segmentIds.size()); // Has the segments that will be deleted
    Set<String> segmentsToRetryLater = new HashSet<>(segmentIds.size());  // List of segments that we need to retry

    try {
      ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);

      for (String segmentId : segmentIds) {
        Map<String, String> segmentToInstancesMapFromExternalView = externalView.getStateMap(segmentId);
        Map<String, String> segmentToInstancesMapFromIdealStates = idealState.getInstanceStateMap(segmentId);
        if ((segmentToInstancesMapFromExternalView == null || segmentToInstancesMapFromExternalView.isEmpty()) && (
            segmentToInstancesMapFromIdealStates == null || segmentToInstancesMapFromIdealStates.isEmpty())) {
          segmentsToDelete.add(segmentId);
        } else {
          segmentsToRetryLater.add(segmentId);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while checking helix states for table {} " + tableName, e);
      segmentsToDelete.clear();
      segmentsToDelete.addAll(segmentIds);
      segmentsToRetryLater.clear();
    }

    if (!segmentsToDelete.isEmpty()) {
      List<String> propStorePathList = new ArrayList<>(segmentsToDelete.size());
      for (String segmentId : segmentsToDelete) {
        String segmentPropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentId);
        propStorePathList.add(segmentPropertyStorePath);
      }

      boolean[] deleteSuccessful = _propertyStore.remove(propStorePathList, AccessOption.PERSISTENT);
      List<String> propStoreFailedSegs = new ArrayList<>(segmentsToDelete.size());
      for (int i = 0; i < deleteSuccessful.length; i++) {
        final String segmentId = segmentsToDelete.get(i);
        if (!deleteSuccessful[i]) {
          // remove API can fail because the prop store entry did not exist, so check first.
          if (_propertyStore.exists(propStorePathList.get(i), AccessOption.PERSISTENT)) {
            LOGGER.info("Could not delete {} from propertystore", propStorePathList.get(i));
            segmentsToRetryLater.add(segmentId);
            propStoreFailedSegs.add(segmentId);
          }
        }
      }
      segmentsToDelete.removeAll(propStoreFailedSegs);

      removeSegmentsFromStore(tableName, segmentsToDelete);
    }

    LOGGER.info("Deleted {} segments from table {}:{}", segmentsToDelete.size(), tableName,
        segmentsToDelete.size() <= 5 ? segmentsToDelete : "");

    if (segmentsToRetryLater.size() > 0) {
      long effectiveDeletionDelay = Math.min(deletionDelay * 2, MAX_DELETION_DELAY_SECONDS);
      LOGGER.info("Postponing deletion of {} segments from table {}", segmentsToRetryLater.size(), tableName);
      deleteSegmentsWithDelay(tableName, segmentsToRetryLater, effectiveDeletionDelay);
      return;
    }
  }

  public void removeSegmentsFromStore(String tableNameWithType, List<String> segments) {
    for (String segment : segments) {
      removeSegmentFromStore(tableNameWithType, segment);
    }
  }

  protected void removeSegmentFromStore(String tableNameWithType, String segmentId) {
    final String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    if (_dataDir != null) {
      URI fileToMoveURI;
      PinotFS pinotFS;
      URI dataDirURI = ControllerConf.getUriFromPath(_dataDir);
      fileToMoveURI = ControllerConf.constructSegmentLocation(_dataDir, rawTableName, segmentId);
      URI deletedSegmentDestURI = ControllerConf.constructSegmentLocation(
          StringUtil.join(File.separator, _dataDir, DELETED_SEGMENTS), rawTableName, segmentId);
      pinotFS = PinotFSFactory.create(dataDirURI.getScheme());

      try {
        if (pinotFS.exists(fileToMoveURI)) {
          // Overwrites the file if it already exists in the target directory.
          pinotFS.move(fileToMoveURI, deletedSegmentDestURI);
          LOGGER.info("Moved segment {} from {} to {}", segmentId, fileToMoveURI.toString(), deletedSegmentDestURI.toString());
        } else {
          if (!SegmentName.isHighLevelConsumerSegmentName(segmentId)) {
            LOGGER.warn("Not found local segment file for segment {}" + fileToMoveURI.toString());
          }
        }
      } catch (IOException e) {
        LOGGER.warn("Could not move segment {} from {} to {}", segmentId, fileToMoveURI.toString(),
            deletedSegmentDestURI.toString(), e);
      }
    } else {
      LOGGER.info("dataDir is not configured, won't delete segment {} from disk", segmentId);
    }
  }

  /**
   * Removes aged deleted segments from the deleted directory
   * @param retentionInDays: retention for deleted segments in days
   */
  public void removeAgedDeletedSegments(int retentionInDays) {
    if (_dataDir != null) {
      File deletedDir = new File(_dataDir, DELETED_SEGMENTS);
      // Check that the directory for deleted segments exists
      if (!deletedDir.isDirectory()) {
        LOGGER.warn("Deleted segment directory {} does not exist or it is not directory.", deletedDir.getAbsolutePath());
        return;
      }

      URI dataDirURI = ControllerConf.getUriFromPath(_dataDir);
      URI deletedDirURI = ControllerConf.getUriFromPath(StringUtil.join(File.separator, _dataDir, DELETED_SEGMENTS));
      PinotFS pinotFS = PinotFSFactory.create(dataDirURI.getScheme());

      // Check that the directory for deleted segments exists
      if (!pinotFS.isDirectory(deletedDirURI)) {
        LOGGER.warn("Deleted segment directory {} does not exist or it is not directory.", deletedDirURI.toString());
        return;
      }

    try {
      String[] deletedDirFiles = pinotFS.listFiles(deletedDirURI, false);
      if (deletedDirFiles == null) {
        LOGGER.warn("Deleted segment directory {} does not exist.", deletedDirURI.toString());
        return;
      }

      for (String currentDir : deletedDirFiles) {
        URI currentURI = ControllerConf.getUriFromPath(currentDir);
        // Get files that are aged
        String[] targetFiles = pinotFS.listFiles(currentURI, false);
        int numFilesDeleted = 0;
        for (String targetFile : targetFiles) {
          URI targetURI = ControllerConf.getUriFromPath(targetFile);
          Date dateToDelete = DateTime.now().minusDays(retentionInDays).toDate();
          if (pinotFS.lastModified(targetURI) < dateToDelete.getTime()) {
            if (!pinotFS.delete(targetURI)) {
              LOGGER.warn("Cannot remove file {} from deleted directory.", targetURI.toString());
            } else {
              numFilesDeleted ++;
            }
          }
        }

        if (numFilesDeleted == targetFiles.length) {
          // Delete directory if it's empty
          if (!pinotFS.delete(currentURI)) {
            LOGGER.warn("The directory {} cannot be removed.", currentDir);
          }
        }
      }
    } catch (IOException e){
      LOGGER.error("Had trouble deleting directories", deletedDirURI.toString());
    }
    } else {
      LOGGER.info("dataDir is not configured, won't delete any expired segments from deleted directory.");
    }
  }
}
