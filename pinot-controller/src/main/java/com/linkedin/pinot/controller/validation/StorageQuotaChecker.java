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
package com.linkedin.pinot.controller.validation;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.controller.util.TableSizeReader;
import java.io.File;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to check if a new segment is within the configured storage quota for the table
 *
 */
public class StorageQuotaChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageQuotaChecker.class);

  private final TableSizeReader _tableSizeReader;
  private final TableConfig _tableConfig;
  private final ControllerMetrics _controllerMetrics;

  public StorageQuotaChecker(TableConfig tableConfig, TableSizeReader tableSizeReader,
      ControllerMetrics controllerMetrics) {
    _tableConfig = tableConfig;
    _tableSizeReader = tableSizeReader;
    _controllerMetrics = controllerMetrics;
  }

  public class QuotaCheckerResponse {
    public boolean isSegmentWithinQuota;
    public String reason;

    QuotaCheckerResponse(boolean isSegmentWithinQuota, String reason) {
      this.isSegmentWithinQuota = isSegmentWithinQuota;
      this.reason = reason;
    }
  }

  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param tableNameWithType table name with type (OFFLINE/REALTIME) information
   * @param segmentName name of the segment being added
   * @param timeoutMsec timeout in milliseconds for reading table sizes from server
   *
   */
  public QuotaCheckerResponse isSegmentStorageWithinQuota(@Nonnull File segmentFile, @Nonnull String tableNameWithType,
      @Nonnull String segmentName, @Nonnegative int timeoutMsec) throws InvalidConfigException {
    Preconditions.checkNotNull(segmentFile);
    Preconditions.checkNotNull(tableNameWithType);
    Preconditions.checkNotNull(segmentName);
    Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be > 0, input: %s", timeoutMsec);
    Preconditions.checkArgument(segmentFile.exists(), "Segment file: %s does not exist", segmentFile);
    Preconditions.checkArgument(segmentFile.isDirectory(), "Segment file: %s is not a directory", segmentFile);

    // 1. Read table config
    // 2. read table size from all the servers
    // 3. update predicted segment sizes
    // 4. is the updated size within quota
    QuotaConfig quotaConfig = _tableConfig.getQuotaConfig();
    int numReplicas = _tableConfig.getValidationConfig().getReplicationNumber();
    final String tableName = _tableConfig.getTableName();

    if (quotaConfig == null || Strings.isNullOrEmpty(quotaConfig.getStorage())) {
      // no quota configuration...so ignore for backwards compatibility
      LOGGER.warn("Quota configuration not set for table: {}", tableNameWithType);
      return new QuotaCheckerResponse(true, "Quota configuration not set for table: " + tableNameWithType);
    }

    long allowedStorageBytes = numReplicas * quotaConfig.storageSizeBytes();
    if (allowedStorageBytes < 0) {
      LOGGER.warn("Storage quota is not configured for table: {}", tableNameWithType);
      return new QuotaCheckerResponse(true, "Storage quota is not configured for table: " + tableNameWithType);
    }
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.TABLE_QUOTA, allowedStorageBytes);

    long incomingSegmentSizeBytes = FileUtils.sizeOfDirectory(segmentFile);

    // read table size
    TableSizeReader.TableSubTypeSizeDetails tableSubtypeSize = null;
    try {
      tableSubtypeSize = _tableSizeReader.getTableSubtypeSize(tableNameWithType, timeoutMsec);
    } catch (InvalidConfigException e) {
      LOGGER.error("Failed to get table size for table {}", tableNameWithType, e);
      throw e;
    }

    if (tableSubtypeSize.estimatedSizeInBytes == -1) {
      String msg = String.format("Failed to get size estimate for table %s.",
          tableNameWithType);
      // don't fail the quota check in this case
      return new QuotaCheckerResponse(true,
          "Failed to get size estimate for table: " + tableNameWithType);
    }

    // If the segment exists(refresh), get the existing size
    TableSizeReader.SegmentSizeDetails sizeDetails = tableSubtypeSize.segments.get(segmentName);
    long existingSegmentSizeBytes = sizeDetails != null ? sizeDetails.estimatedSizeInBytes : 0;

    // Since tableNameWithType comes with the table type(OFFLINE), thus we guarantee that
    // tableSubtypeSize.estimatedSizeInBytes is the offline table size.
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE,
        tableSubtypeSize.estimatedSizeInBytes);

    LOGGER.info("Table {}'s estimatedSizeInBytes is {}. ReportedSizeInBytes (actual reports from servers) is {}",
        tableName,
        tableSubtypeSize.estimatedSizeInBytes,
        tableSubtypeSize.reportedSizeInBytes);
    // incomingSegmentSizeBytes is compressed data size for just 1 replica.
    long estimatedFinalSizeBytes =
        tableSubtypeSize.estimatedSizeInBytes - existingSegmentSizeBytes + (incomingSegmentSizeBytes * numReplicas);
    if (estimatedFinalSizeBytes <= allowedStorageBytes) {
      String message = String.format(
          "Newly estimated size: %d bytes ( = existing estimated uncompressed size of all replicas: %d bytes - (existing segment sizes of all replicas: %d bytes) + (incoming compressed segment size: %d bytes * number replicas: %d)) is within total allowed storage size: %d bytes ( = configured quota: %d bytes * number replicas: %d) for table %s.",
          estimatedFinalSizeBytes, tableSubtypeSize.estimatedSizeInBytes, existingSegmentSizeBytes,
          incomingSegmentSizeBytes, numReplicas, allowedStorageBytes, quotaConfig.storageSizeBytes(), numReplicas,
          tableName);
      LOGGER.info(message);
      return new QuotaCheckerResponse(true, message);
    } else {
      String message;
      if (tableSubtypeSize.estimatedSizeInBytes > allowedStorageBytes) {
        message = String.format(
            "Table %s already over quota. Existing estimated uncompressed size of all replicas: %d bytes > total allowed storage size: %d bytes ( = configured quota: %d bytes * num replicas: %d). Check if indexes were enabled recently and adjust table quota accordingly.",
            tableName, tableSubtypeSize.estimatedSizeInBytes, allowedStorageBytes, quotaConfig.storageSizeBytes(),
            numReplicas);
      } else {
        message = String.format(
            "Storage quota exceeded. Newly estimated size: %d bytes ( = existing estimated uncompressed size of all replicas: %d bytes - (existing segment sizes of all replicas: %d bytes) + (incoming compressed segment size: %d bytes * number replicas: %d)) > total allowed storage size: %d bytes ( = configured quota: %d bytes * number replicas: %d) for table %s.",
            estimatedFinalSizeBytes, tableSubtypeSize.estimatedSizeInBytes, existingSegmentSizeBytes,
            incomingSegmentSizeBytes, numReplicas, allowedStorageBytes, quotaConfig.storageSizeBytes(), numReplicas,
            tableName);
      }
      LOGGER.warn(message);
      return new QuotaCheckerResponse(false, message);
    }
  }
}
