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

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;

/**
 * SyncDirSource is a source that will sync files like spool directory but also
 * copy the directory's original layout. Also unlike spool directory, this
 * source will also track changed files.
 * <p/>
 * For e.g., a file will be identified as finished and stops reading from it if
 * an empty file with suffix ".done" that present in the same directory of the
 * same name as of the original file.
 */
public class SyncDirSource extends AbstractSource implements Configurable,
		EventDrivenSource {

	private static final Logger logger = LoggerFactory
			.getLogger(SyncDirSource.class);
	// Delay used when polling for file changes
	private boolean backoff = true;
	private int backoffInterval;
	private int maxBackoffInterval;
	/* Config options */
	private File syncDirectory;
	private String directoryPrefix;
	private String endFileSuffix;
	private String statsFilePrefix;
	private String syncingStatsFileSuffix;
	private String syncedStatsFileSuffix;
	private String filenameHeaderKey = SyncDirSourceConfigurationConstants.FILENAME_HEADER_KEY;
	private String header;
	private int batchSize;
	private ScheduledExecutorService executor;
	private CounterGroup counterGroup;
	private Runnable runner;
	private SyncDirFileLineReader reader;

	private boolean hitChannelException;
	private boolean hasFatalError;

	@Override
	public synchronized void start() {
		logger.info("SyncDirSource source starting with directory:{}",
				syncDirectory);

		counterGroup = new CounterGroup();

		reader = new SyncDirFileLineReader(syncDirectory, endFileSuffix,
				statsFilePrefix, syncingStatsFileSuffix, syncedStatsFileSuffix);
		runner = new DirectorySyncRunnable(reader, counterGroup);

		executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleWithFixedDelay(runner, 0, 2000, TimeUnit.MILLISECONDS);

		super.start();
		logger.debug("SyncDirSource source started");
	}

	@Override
	public synchronized void stop() {
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			logger.info("Interrupted while awaiting termination", ex);
		}
		executor.shutdownNow();

		super.stop();
		logger.debug("SyncDirSource source stopped");
	}

	@Override
	public void configure(Context context) {
		String syncDirectoryStr = context
				.getString(SyncDirSourceConfigurationConstants.SYNC_DIRECTORY);
		Preconditions.checkState(syncDirectoryStr != null,
				"Configuration must specify a sync directory");
		syncDirectory = new File(syncDirectoryStr);

		directoryPrefix = context.getString(
				SyncDirSourceConfigurationConstants.DIRECTORY_PREFIX,
				SyncDirSourceConfigurationConstants.DEFAULT_DIRECTORY_PREFIX);
		endFileSuffix = context.getString(
				SyncDirSourceConfigurationConstants.END_FILE_SUFFIX,
				SyncDirSourceConfigurationConstants.DEFAULT_END_FILE_SUFFIX);
		statsFilePrefix = context.getString(
				SyncDirSourceConfigurationConstants.STATS_FILE_PREFIX,
				SyncDirSourceConfigurationConstants.DEFAULT_STATS_FILE_PREFIX);
		syncingStatsFileSuffix = context
				.getString(
						SyncDirSourceConfigurationConstants.SYNCING_STATS_FILE_SUFFIX,
						SyncDirSourceConfigurationConstants.DEFAULT_SYNCING_STATS_FILE_SUFFIX);
		syncedStatsFileSuffix = context
				.getString(
						SyncDirSourceConfigurationConstants.SYNCED_STATS_FILE_SUFFIX,
						SyncDirSourceConfigurationConstants.DEFAULT_SYNCED_STATS_FILE_SUFFIX);
		batchSize = context.getInteger(
				SyncDirSourceConfigurationConstants.BATCH_SIZE,
				SyncDirSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
		backoffInterval = context.getInteger(
				SyncDirSourceConfigurationConstants.BACKOFF_INTERVAL,
				SyncDirSourceConfigurationConstants.DEFAULT_BACKOFF_INTERVAL);
		maxBackoffInterval = context
				.getInteger(
						SyncDirSourceConfigurationConstants.MAX_BACKOFF_INTERVAL,
						SyncDirSourceConfigurationConstants.DEFAULT_MAX_BACKOFF_INTERVAL);
		header = context.getString(
				SyncDirSourceConfigurationConstants.MESSAGE_HEADER_KEY, "null");
	}

	private Event createEvent(byte[] lineEntry, String filename) {
		String body = new String(lineEntry);
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty(SyncDirSourceConfigurationConstants.MESSAGE_HEADER_KEY, header);
		jsonObject.addProperty("body", body);
		String json = jsonObject.toString();
		byte[] line = json.getBytes();
		Event out = EventBuilder.withBody(line);
		if (directoryPrefix.length() > 0) {
			out.getHeaders().put(filenameHeaderKey,
					directoryPrefix + File.separator + filename);
		} else {
			out.getHeaders().put(filenameHeaderKey, filename);
		}
		return out;
	}

	/** for testing */
	protected boolean hitChannelException() {
		return hitChannelException;
	}

	/** for testing */
	protected void setBackoff(final boolean backoff) {
		this.backoff = backoff;
	}

	/** for testing */
	protected boolean hasFatalError() {
		return hasFatalError;
	}

	private class DirectorySyncRunnable implements Runnable {
		private SyncDirFileLineReader reader;
		private CounterGroup counterGroup;

		public DirectorySyncRunnable(SyncDirFileLineReader reader,
				CounterGroup counterGroup) {
			this.reader = reader;
			this.counterGroup = counterGroup;
		}

		@Override
		public void run() {
			try {
				while (!Thread.interrupted()) {
					List<byte[]> lines = reader.readLines(batchSize);
					if (lines.size() == 0) {
						break;
					}
					String file = syncDirectory.toURI()
							.relativize(reader.getLastFileRead().toURI())
							.getPath();
					List<Event> events = Lists.newArrayList();
					for (byte[] l : lines) {
						counterGroup.incrementAndGet("syncdir.lines.read");
						events.add(createEvent(l, file));
					}
					try {
						getChannelProcessor().processEventBatch(events);
						reader.commit();
					} catch (ChannelException e) {
						hitChannelException = true;
						logger.warn("The channel is full, or this source's batch size is "
								+ "lager than channel's transaction capacity. This source will "
								+ "try again after "
								+ String.valueOf(backoffInterval)
								+ " milliseconds");

						if (backoff) {
							TimeUnit.MILLISECONDS.sleep(backoffInterval);
							backoffInterval = backoffInterval << 1;
							backoffInterval = backoffInterval >= maxBackoffInterval ? maxBackoffInterval
									: backoffInterval;
						}
						continue;
					}
				}
			} catch (Throwable t) {
				logger.error(
						"FATAL: "
								+ this.toString()
								+ ": "
								+ "Uncaught exception in SpoolDirectorySource thread. "
								+ "Restart or reconfigure Flume to continue processing.",
						t);
				hasFatalError = true;
				Throwables.propagate(t);
			}
		}
	}
}
