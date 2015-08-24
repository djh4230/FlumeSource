/*
   Copyright 2013 Vincent.Gu

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package org.apache.flume.source.syncdir;

public class SyncDirSourceConfigurationConstants {
  /** Directory to sync. */
  public static final String SYNC_DIRECTORY = "directory";
  /** All files in this directory would prepend this prefix */
  public static final String DIRECTORY_PREFIX = "directoryPrefix";
  public static final String DEFAULT_DIRECTORY_PREFIX = "";
  /**
   * When a file is fully written and won't be changed in the future, there
   * would be another file with the same name but having this suffix appended
   * to
   * it appeared in the same directory as the original file, to signal Flume to
   * stop reading this file any longer.
   */
  public static final String END_FILE_SUFFIX = "endFileSuffix";
  public static final String DEFAULT_END_FILE_SUFFIX = ".done";
  /**
   * Set the status files prefix and suffix while using
   * {@link org.apache.flume.serialization.ResettableFileLineReader}.
   */
  public static final String STATS_FILE_PREFIX = "syncingStatsFilePrefix";
  public static final String DEFAULT_STATS_FILE_PREFIX = ".";
  public static final String SYNCING_STATS_FILE_SUFFIX = "syncingStatsFileSuffix";
  public static final String DEFAULT_SYNCING_STATS_FILE_SUFFIX = ".FLUME-INCOMPLETE";
  /** Suffix appended to files when they are finished being sent. */
  /**
   * Set the status file suffix's ending form while using {@link
   * org.apache.flume.serialization.ResettableFileLineReader}.
   */
  public static final String SYNCED_STATS_FILE_SUFFIX = "syncedStatsFileSuffix";
  public static final String DEFAULT_SYNCED_STATS_FILE_SUFFIX = ".FLUME-COMPLETED";

  /** Header in which to put relative filename. */
  public static final String FILENAME_HEADER_KEY = "path";
  /** What size to batch with before sending to ChannelProcessor. */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;

  /** Flume scheduled poll delay by milliseconds */
  public static final String BACKOFF_INTERVAL ="backoffInteval";
  public static final int DEFAULT_BACKOFF_INTERVAL = 250;
  public static final String MAX_BACKOFF_INTERVAL = "maxBackoffInterval";
  public static final int DEFAULT_MAX_BACKOFF_INTERVAL = 4000;
  public static final String MESSAGE_HEADER_KEY="header";
}
