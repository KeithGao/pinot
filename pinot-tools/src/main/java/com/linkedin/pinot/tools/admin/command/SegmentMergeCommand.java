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
package com.linkedin.pinot.tools.admin.command;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.minion.rollup.MergeRollupSegmentConverter;
import com.linkedin.pinot.core.minion.rollup.MergeType;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment merge command.
 *
 * TODO: add support for m to n merge, add support for rollup type
 */
public class SegmentMergeCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMergeCommand.class);

  private static final String INPUT_SEGMENT_SEPARATOR = ",";
  private static final String DEFAULT_MERGE_TYPE = "CONCATENATE";

  @Option(name = "-inputSegments", required = true, metaVar = "<String>", usage = "Comma separated input segment paths that need to be merged")
  private String _inputSegmentPaths;

  @Option(name = "-outputPath", required = true, metaVar = "<String>", usage = "Output segment path. This should be different from working directory.")
  private String _outputPath;

  @Option(name = "-tableConfigFilePath", required = true, metaVar = "<String>", usage = "Table config file path.")
  private String _tableConfigFilePath;

  @Option(name = "-schemaFilePath", required = true, metaVar = "<String>", usage = "Schema file path")
  private String _schemaFilePath;

  @Option(name = "-inputSegmentTarred", required = true, metaVar = "<String>", usage = "Indicate whether the input segment is tarred (true, false)")
  private String _inputSegmentTarred;

  @Option(name = "-tarOutputSegment", required = true, metaVar = "<String>", usage = "Indicate whether to tar output segment (true, false)")
  private String _tarOutputSegment;

  @Option(name = "-outputSegmentName", required = false, metaVar = "<String>", usage = "The name of output segment file")
  private String _outputSegmentName;

  // TODO: once rollup mode is supported, make this field required
  @Option(name = "-mergeType", required = false, metaVar = "<String>", usage = "Merge type (\"CONCATENATE\" or \"ROLLUP\"). Currently, only \"CONCATENATE\" type is supported.")
  private String _mergeType = DEFAULT_MERGE_TYPE;

  @Option(name = "-workingDirectory", required = false, metaVar = "<String>", usage = "Path for working directory. This directory gets cleaned up after the job")
  private String _workingDirectory;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "SegmentConcatenation";
  }

  @Override
  public boolean execute() throws Exception {
    // Check merge type
    if (MergeType.valueOf(_mergeType) != MergeType.CONCATENATE) {
      throw new InvalidConfigException("Currently, only CONCATENATE merge type is supported");
    }

    LOGGER.info("Running segment merge command... mergeType: {}", _mergeType);

    // Set working directory
    File workingDir;
    if (_workingDirectory == null) {
      workingDir = FileUtils.getTempDirectory();
    } else {
      workingDir = new File(_workingDirectory);
    }

    try {
      // Get the list of input segment index directories
      List<File> inputIndexDirs = new ArrayList<>();
      boolean inputSegmentTarred = Boolean.valueOf(_inputSegmentTarred);
      LOGGER.info("inputSegmentTarred: {}", inputSegmentTarred);

      if (inputSegmentTarred) {
        File untarredSegments = new File(workingDir, "untarredSegments");
        Preconditions.checkState(untarredSegments.mkdirs());
        int segmentNum = 0;
        for (String tarredSegmentPath : _inputSegmentPaths.split(INPUT_SEGMENT_SEPARATOR)) {
          File tarredSegmentFile = new File(tarredSegmentPath.trim());
          File segmentDir = new File(untarredSegments, "segmentDir_" + segmentNum++);
          TarGzCompressionUtils.unTar(tarredSegmentFile, segmentDir);
          File[] files = segmentDir.listFiles();
          Preconditions.checkState(files != null && files.length == 1);
          File indexDir = files[0];
          inputIndexDirs.add(indexDir);
        }
      } else {
        // Simply add the given input paths when the input directories are not tarred
        for (String path : _inputSegmentPaths.split(INPUT_SEGMENT_SEPARATOR)) {
          inputIndexDirs.add(new File(path.trim()));
        }
      }
      LOGGER.info("Input segment paths: {}", inputIndexDirs);

      // Read table config
      String tableConfigString = new String(Files.readAllBytes(Paths.get(_tableConfigFilePath)), StandardCharsets.UTF_8);
      JSONObject tableConfigJson = new JSONObject(tableConfigString);
      TableConfig tableConfig = TableConfig.fromJSONConfig(tableConfigJson);

      // Read schema
      Schema schema = Schema.fromFile(new File(_schemaFilePath));

      LOGGER.info("Table config: {}", tableConfig);
      LOGGER.info("Schema : {}", schema);

      // Compute segment name if it is not specified
      if (_outputSegmentName == null) {
        _outputSegmentName = getDefaultSegmentName(tableConfig, schema, inputIndexDirs);
      }
      LOGGER.info("Output segment name: {}", _outputSegmentName);

      // Compute segment merge
      // TODO: add support for rollup
      MergeRollupSegmentConverter mergeRollupSegmentConverter =
          new MergeRollupSegmentConverter.Builder().setMergeType("CONCATENATE")
              .setSegmentName(_outputSegmentName)
              .setInputIndexDirs(inputIndexDirs)
              .setWorkingDir(workingDir)
              .setTableName(tableConfig.getTableName())
              .build();

      List<File> outputSegments = mergeRollupSegmentConverter.convert();
      Preconditions.checkState(outputSegments.size() == 1);
      File outputSegment = outputSegments.get(0);

      // Make sure to create output directory
      File outputDir = new File(_outputPath);
      if (!outputDir.exists()) {
        outputDir.mkdirs();
      }

      // Move the merged segment to output directory.
      boolean tarOutputSegment = Boolean.valueOf(_tarOutputSegment);
      LOGGER.info("tarOutputSegment: {}", tarOutputSegment);
      if (tarOutputSegment) {
        File tarredOutputSegmentsDir = new File(workingDir, "tarredOutputSegments");
        Preconditions.checkState(tarredOutputSegmentsDir.mkdir());
        File tarredOutputFile = new File(TarGzCompressionUtils.createTarGzOfDirectory(outputSegment.getPath(),
            new File(tarredOutputSegmentsDir, outputSegment.getName()).getPath()));
        outputSegment = tarredOutputFile;
        FileUtils.moveFile(outputSegment, new File(outputDir, outputSegment.getName()));
      } else {
        FileUtils.moveDirectory(outputSegment, new File(outputDir, outputSegment.getName()));
      }

      LOGGER.info("Segment has been merged correctly. Output file is located at {}", outputDir);
    } finally {
      // Clean up working directory
      FileUtils.deleteQuietly(workingDir);
    }
    return true;
  }

  @Override
  public String description() {
    return "Create the merged segment using concatenation";
  }

  // TODO: add a new segment name generator after porting from PBNJ and remove the below code
  private String getDefaultSegmentName(TableConfig tableConfig, Schema schema, List<File> inputIndexDirs)
      throws Exception {
    String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());

    // Compute mix/max time from segment metadata
    long minStartTime = Integer.MAX_VALUE;
    long maxEndTime = Integer.MIN_VALUE;
    for (File indexDir : inputIndexDirs) {
      SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
      long currentStartTime = segmentMetadata.getStartTime();
      if (currentStartTime < minStartTime) {
        minStartTime = currentStartTime;
      }

      long currentEndTime = segmentMetadata.getEndTime();
      if (currentEndTime > maxEndTime) {
        maxEndTime = currentEndTime;
      }
    }

    // Fetch time related configurations from schema and table config.
    String pushFrequency = tableConfig.getValidationConfig().getSegmentPushFrequency();
    String timeColumnType = tableConfig.getValidationConfig().getTimeType();
    String timeFormat = schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeFormat();

    // Compute normalized date
    String minStartTimeString = getNormalizedDate(minStartTime, pushFrequency, timeColumnType, timeFormat);
    String maxEndTimeString = getNormalizedDate(maxEndTime, pushFrequency, timeColumnType, timeFormat);

    return String.join("_", tableName, minStartTimeString, maxEndTimeString, "0");
  }

  private String getNormalizedDate(long time, String pushFrequency, String timeColumnType, String timeFormat) {
    // Compute since epoch
    Date sinceEpoch = null;
    if (timeFormat.equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())) {
      TimeUnit timeUnit = TimeUnit.valueOf(timeColumnType);

      switch (timeUnit) {
        case MILLISECONDS:
          sinceEpoch = new Date(time);
          break;
        case SECONDS:
          sinceEpoch = new Date(TimeUnit.SECONDS.toMillis(time));
          break;
        case MINUTES:
          sinceEpoch = new Date(TimeUnit.MINUTES.toMillis(time));
          break;
        case HOURS:
          sinceEpoch = new Date(TimeUnit.HOURS.toMillis(time));
          break;
        case DAYS:
          sinceEpoch = new Date(TimeUnit.DAYS.toMillis(time));
          break;
      }
    } else {
      try {
        SimpleDateFormat dateFormatPassedIn = new SimpleDateFormat(timeFormat);
        dateFormatPassedIn.setTimeZone(TimeZone.getTimeZone("UTC"));
        sinceEpoch = dateFormatPassedIn.parse(Long.toString(time));
      } catch (Exception e) {
        throw new RuntimeException("Could not parse simple date format: '" + timeFormat);
      }
    }

    // Pick the current time when having a parsing error
    if (sinceEpoch == null) {
      LOGGER.warn("Could not translate timeType '" + timeColumnType);
      sinceEpoch = new Date();
    }

    // Get date format
    SimpleDateFormat dateFormat;
    if (pushFrequency.equalsIgnoreCase("HOURLY")) {
      dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
    } else {
      dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    }
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    return dateFormat.format(sinceEpoch);
  }
}
