/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.bsp;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.InputSplitEvents;
import org.apache.giraph.graph.InputSplitPaths;
import org.apache.giraph.graph.Monitor;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public abstract class BspService<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements Watcher, CentralizedService<I, V, E, M> {
  /** Unset superstep */
  public static final long UNSET_SUPERSTEP = Long.MIN_VALUE;
  /** Input superstep (superstep when loading the vertices happens) */
  public static final long INPUT_SUPERSTEP = -1;
  /** Unset application attempt */
  public static final long UNSET_APPLICATION_ATTEMPT = Long.MIN_VALUE;
  /** Base ZooKeeper directory */
  public static final String BASE_DIR = "/_hadoopBsp";
  /** Master job state znode above base dir */
  public static final String MASTER_JOB_STATE_NODE = "/_masterJobState";
  /** Vertex input split directory about base dir */
  public static final String VERTEX_INPUT_SPLIT_DIR = "/_vertexInputSplitDir";
  /** Vertex input split done directory about base dir */
  public static final String VERTEX_INPUT_SPLIT_DONE_DIR =
      "/_vertexInputSplitDoneDir";
  /** Denotes a reserved vertex input split */
  public static final String VERTEX_INPUT_SPLIT_RESERVED_NODE =
      "/_vertexInputSplitReserved";
  /** Denotes a finished vertex input split */
  public static final String VERTEX_INPUT_SPLIT_FINISHED_NODE =
      "/_vertexInputSplitFinished";
  /** Denotes that all the vertex input splits are are ready for consumption */
  public static final String VERTEX_INPUT_SPLITS_ALL_READY_NODE =
      "/_vertexInputSplitsAllReady";
  /** Denotes that all the vertex input splits are done. */
  public static final String VERTEX_INPUT_SPLITS_ALL_DONE_NODE =
      "/_vertexInputSplitsAllDone";
  /** Edge input split directory about base dir */
  public static final String EDGE_INPUT_SPLIT_DIR = "/_edgeInputSplitDir";
  /** Edge input split done directory about base dir */
  public static final String EDGE_INPUT_SPLIT_DONE_DIR =
      "/_edgeInputSplitDoneDir";
  /** Denotes a reserved edge input split */
  public static final String EDGE_INPUT_SPLIT_RESERVED_NODE =
      "/_edgeInputSplitReserved";
  /** Denotes a finished edge input split */
  public static final String EDGE_INPUT_SPLIT_FINISHED_NODE =
      "/_edgeInputSplitFinished";
  /** Denotes that all the edge input splits are are ready for consumption */
  public static final String EDGE_INPUT_SPLITS_ALL_READY_NODE =
      "/_edgeInputSplitsAllReady";
  /** Denotes that all the edge input splits are done. */
  public static final String EDGE_INPUT_SPLITS_ALL_DONE_NODE =
      "/_edgeInputSplitsAllDone";
  /** Directory of attempts of this application */
  public static final String APPLICATION_ATTEMPTS_DIR =
      "/_applicationAttemptsDir";
  /** Where the master election happens */
  public static final String MASTER_ELECTION_DIR = "/_masterElectionDir";
  /** Superstep scope */
  public static final String SUPERSTEP_DIR = "/_superstepDir";
  /** Where the merged aggregators are located */
  public static final String MERGED_AGGREGATOR_DIR =
      "/_mergedAggregatorDir";
  /** Healthy workers register here. */
  public static final String WORKER_HEALTHY_DIR = "/_workerHealthyDir";
  /** Unhealthy workers register here. */
  public static final String WORKER_UNHEALTHY_DIR = "/_workerUnhealthyDir";
  /** Workers which wrote checkpoint notify here */
  public static final String WORKER_WROTE_CHECKPOINT_DIR =
      "/_workerWroteCheckpointDir";
  /** Finished workers notify here */
  public static final String WORKER_FINISHED_DIR = "/_workerFinishedDir";
  /** Where the master and worker addresses and partition assignments are set */
  public static final String ADDRESSES_AND_PARTITIONS_DIR =
      "/_addressesAndPartitions";
  /** Helps coordinate the partition exchnages */
  public static final String PARTITION_EXCHANGE_DIR =
      "/_partitionExchangeDir";
  /** Denotes that the superstep is done */
  public static final String SUPERSTEP_FINISHED_NODE = "/_superstepFinished";
  /** Denotes which workers have been cleaned up */
  public static final String CLEANED_UP_DIR = "/_cleanedUpDir";
  /** JSON partition stats key */
  public static final String JSONOBJ_PARTITION_STATS_KEY =
      "_partitionStatsKey";
  /** JSON message count key */
  public static final String JSONOBJ_NUM_MESSAGES_KEY = "_numMsgsKey";
  /** JSON metrics key */
  public static final String JSONOBJ_METRICS_KEY = "_metricsKey";

  /** JSON state key */
  public static final String JSONOBJ_STATE_KEY = "_stateKey";
  /** JSON application attempt key */
  public static final String JSONOBJ_APPLICATION_ATTEMPT_KEY =
      "_applicationAttemptKey";
  /** JSON superstep key */
  public static final String JSONOBJ_SUPERSTEP_KEY =
      "_superstepKey";
  /** Suffix denotes a worker */
  public static final String WORKER_SUFFIX = "_worker";
  /** Suffix denotes a master */
  public static final String MASTER_SUFFIX = "_master";
  /** If at the end of a checkpoint file, indicates metadata */
  public static final String CHECKPOINT_METADATA_POSTFIX = ".metadata";
  /**
   * If at the end of a checkpoint file, indicates vertices, edges,
   * messages, etc.
   */
  public static final String CHECKPOINT_VERTICES_POSTFIX = ".vertices";
  /**
   * If at the end of a checkpoint file, indicates metadata and data is valid
   * for the same filenames without .valid
   */
  public static final String CHECKPOINT_VALID_POSTFIX = ".valid";
  /**
   * If at the end of a checkpoint file, indicates the stitched checkpoint
   * file prefixes.  A checkpoint is not valid if this file does not exist.
   */
  public static final String CHECKPOINT_FINALIZED_POSTFIX = ".finalized";
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BspService.class);
  /** Path to the job's root */
  protected final String basePath;
  /** Path to the job state determined by the master (informative only) */
  protected final String masterJobStatePath;
  /** ZooKeeper paths for vertex input splits. */
  protected final InputSplitPaths vertexInputSplitsPaths;
  /** ZooKeeper paths for edge input splits. */
  protected final InputSplitPaths edgeInputSplitsPaths;
  /** Vertex input split events. */
  protected final InputSplitEvents vertexInputSplitsEvents;
  /** Edge input split events. */
  protected final InputSplitEvents edgeInputSplitsEvents;
  /** Path to the application attempts) */
  protected final String applicationAttemptsPath;
  /** Path to the cleaned up notifications */
  protected final String cleanedUpPath;
  /** Path to the checkpoint's root (including job id) */
  protected final String checkpointBasePath;
  /** Path to the master election path */
  protected final String masterElectionPath;
  /** Private ZooKeeper instance that implements the service */
  private final ZooKeeperExt zk;
  /** Has the Connection occurred? */
  private final BspEvent connectedEvent;
  /** Has worker registration changed (either healthy or unhealthy) */
  private final BspEvent workerHealthRegistrationChanged;
  /** Are the addresses and partition assignments to workers ready? */
  private final BspEvent addressesAndPartitionsReadyChanged;
  /** Application attempt changed */
  private final BspEvent applicationAttemptChanged;
  /** Superstep finished synchronization */
  private final BspEvent superstepFinished;
  /** Master election changed for any waited on attempt */
  private final BspEvent masterElectionChildrenChanged;
  /** Cleaned up directory children changed*/
  private final BspEvent cleanedUpChildrenChanged;
  /** Registered list of BspEvents */
  private final List<BspEvent> registeredBspEvents =
      new ArrayList<BspEvent>();
  /** Immutable configuration of the job*/
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Job context (mainly for progress) */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Cached superstep (from ZooKeeper) */
  private long cachedSuperstep = UNSET_SUPERSTEP;
  /** Restarted from a checkpoint (manual or automatic) */
  private long restartedSuperstep = UNSET_SUPERSTEP;
  /** Cached application attempt (from ZooKeeper) */
  private long cachedApplicationAttempt = UNSET_APPLICATION_ATTEMPT;
  /** Job id, to ensure uniqueness */
  private final String jobId;
  /** Task partition, to ensure uniqueness */
  private final int taskPartition;
  /** My hostname */
  private final String hostname;
  /** Combination of hostname '_' partition (unique id) */
  private final String hostnamePartitionId;
  /** Graph partitioner */
  private final GraphPartitionerFactory<I, V, E, M> graphPartitionerFactory;
  /** Mapper that will do the graph computation */
  private final GraphTaskManager<I, V, E, M> graphTaskManager;
  /** File system */
  private final FileSystem fs;
  /** Checkpoint frequency */
  private final int checkpointFrequency;

  /**
   * Constructor.
   *
   * @param serverPortList ZooKeeper server port list
   * @param sessionMsecTimeout ZooKeeper session timeount in milliseconds
   * @param context Mapper context
   * @param graphTaskManager GraphTaskManager for this compute node
   */
  public BspService(String serverPortList,
      int sessionMsecTimeout,
      Mapper<?, ?, ?, ?>.Context context,
      GraphTaskManager<I, V, E, M> graphTaskManager) {
    this.vertexInputSplitsEvents = new InputSplitEvents(context);
    this.edgeInputSplitsEvents = new InputSplitEvents(context);
    this.connectedEvent = new PredicateLock(context);
    this.workerHealthRegistrationChanged = new PredicateLock(context);
    this.addressesAndPartitionsReadyChanged = new PredicateLock(context);
    this.applicationAttemptChanged = new PredicateLock(context);
    this.superstepFinished = new PredicateLock(context);
    this.masterElectionChildrenChanged = new PredicateLock(context);
    this.cleanedUpChildrenChanged = new PredicateLock(context);

    registerBspEvent(connectedEvent);
    registerBspEvent(workerHealthRegistrationChanged);
    registerBspEvent(vertexInputSplitsEvents.getAllReadyChanged());
    registerBspEvent(vertexInputSplitsEvents.getStateChanged());
    registerBspEvent(edgeInputSplitsEvents.getAllReadyChanged());
    registerBspEvent(edgeInputSplitsEvents.getStateChanged());
    registerBspEvent(addressesAndPartitionsReadyChanged);
    registerBspEvent(applicationAttemptChanged);
    registerBspEvent(superstepFinished);
    registerBspEvent(masterElectionChildrenChanged);
    registerBspEvent(cleanedUpChildrenChanged);

    this.context = context;
    this.graphTaskManager = graphTaskManager;
    this.conf = new ImmutableClassesGiraphConfiguration<I, V, E, M>(
        context.getConfiguration());
    this.jobId = conf.get("mapred.job.id", "Unknown Job");
    this.taskPartition = conf.getTaskPartition();
    this.restartedSuperstep = conf.getLong(
        GiraphConstants.RESTART_SUPERSTEP, UNSET_SUPERSTEP);
    this.cachedSuperstep = restartedSuperstep;
    if ((restartedSuperstep != UNSET_SUPERSTEP) &&
        (restartedSuperstep < 0)) {
      throw new IllegalArgumentException(
          "BspService: Invalid superstep to restart - " +
              restartedSuperstep);
    }
    try {
      this.hostname = conf.getLocalHostname();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    this.hostnamePartitionId = hostname + "_" + getTaskPartition();
    this.graphPartitionerFactory = conf.createGraphPartitioner();

    this.checkpointFrequency = conf.getCheckpointFrequency();

    basePath = ZooKeeperManager.getBasePath(conf) + BASE_DIR + "/" + jobId;
    masterJobStatePath = basePath + MASTER_JOB_STATE_NODE;
    vertexInputSplitsPaths = new InputSplitPaths(basePath,
        VERTEX_INPUT_SPLIT_DIR, VERTEX_INPUT_SPLIT_DONE_DIR,
        VERTEX_INPUT_SPLITS_ALL_READY_NODE, VERTEX_INPUT_SPLITS_ALL_DONE_NODE);
    edgeInputSplitsPaths = new InputSplitPaths(basePath,
        EDGE_INPUT_SPLIT_DIR, EDGE_INPUT_SPLIT_DONE_DIR,
        EDGE_INPUT_SPLITS_ALL_READY_NODE, EDGE_INPUT_SPLITS_ALL_DONE_NODE);
    applicationAttemptsPath = basePath + APPLICATION_ATTEMPTS_DIR;
    cleanedUpPath = basePath + CLEANED_UP_DIR;
    checkpointBasePath = getConfiguration().get(
        GiraphConstants.CHECKPOINT_DIRECTORY,
        GiraphConstants.CHECKPOINT_DIRECTORY_DEFAULT + "/" + getJobId());
    masterElectionPath = basePath + MASTER_ELECTION_DIR;
    if (LOG.isInfoEnabled()) {
      LOG.info("BspService: Connecting to ZooKeeper with job " + jobId +
          ", " + getTaskPartition() + " on " + serverPortList);
    }
    try {
      this.zk = new ZooKeeperExt(serverPortList,
                                 sessionMsecTimeout,
                                 conf.getZookeeperOpsMaxAttempts(),
                                 conf.getZookeeperOpsRetryWaitMsecs(),
                                 this,
                                 context);
      connectedEvent.waitForever();
      this.fs = FileSystem.get(getConfiguration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Get the superstep from a ZooKeeper path
   *
   * @param path Path to parse for the superstep
   * @return Superstep from the path.
   */
  public static long getSuperstepFromPath(String path) {
    int foundSuperstepStart = path.indexOf(SUPERSTEP_DIR);
    if (foundSuperstepStart == -1) {
      throw new IllegalArgumentException(
          "getSuperstepFromPath: Cannot find " + SUPERSTEP_DIR +
          "from " + path);
    }
    foundSuperstepStart += SUPERSTEP_DIR.length() + 1;
    int endIndex = foundSuperstepStart +
        path.substring(foundSuperstepStart).indexOf("/");
    if (endIndex == -1) {
      throw new IllegalArgumentException(
          "getSuperstepFromPath: Cannot find end of superstep from " +
              path);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("getSuperstepFromPath: Got path=" + path +
          ", start=" + foundSuperstepStart + ", end=" + endIndex);
    }
    return Long.parseLong(path.substring(foundSuperstepStart, endIndex));
  }

  /**
   * Get the hostname and id from a "healthy" worker path
   *
   * @param path Path to check
   * @return Hostname and id from path
   */
  public static String getHealthyHostnameIdFromPath(String path) {
    int foundWorkerHealthyStart = path.indexOf(WORKER_HEALTHY_DIR);
    if (foundWorkerHealthyStart == -1) {
      throw new IllegalArgumentException(
          "getHealthyHostnameidFromPath: Couldn't find " +
              WORKER_HEALTHY_DIR + " from " + path);
    }
    foundWorkerHealthyStart += WORKER_HEALTHY_DIR.length();
    return path.substring(foundWorkerHealthyStart);
  }

  /**
   * Generate the base superstep directory path for a given application
   * attempt
   *
   * @param attempt application attempt number
   * @return directory path based on the an attempt
   */
  public final String getSuperstepPath(long attempt) {
    return applicationAttemptsPath + "/" + attempt + SUPERSTEP_DIR;
  }

  /**
   * Get the input split events for edge input.
   *
   * @return InputSplitEvents for edge input.
   */
  public InputSplitEvents getEdgeInputSplitsEvents() {
    return edgeInputSplitsEvents;
  }

  /**
   * Get the input split events for vertex input.
   *
   * @return InputSplitEvents for vertex input.
   */
  public InputSplitEvents getVertexInputSplitsEvents() {
    return vertexInputSplitsEvents;
  }

  /**
   * Generate the worker information "healthy" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerInfoHealthyPath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_HEALTHY_DIR;
  }

  /**
   * Generate the worker information "unhealthy" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerInfoUnhealthyPath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_UNHEALTHY_DIR;
  }

  /**
   * Generate the worker "wrote checkpoint" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerWroteCheckpointPath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_WROTE_CHECKPOINT_DIR;
  }

  /**
   * Generate the worker "finished" directory path for a
   * superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getWorkerFinishedPath(long attempt, long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + WORKER_FINISHED_DIR;
  }

  /**
   * Generate the "addresses and partitions" directory path for a superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getAddressesAndPartitionsPath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + ADDRESSES_AND_PARTITIONS_DIR;
  }

  /**
   * Generate the "partition exchange" directory path for a superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getPartitionExchangePath(long attempt,
      long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + PARTITION_EXCHANGE_DIR;
  }

  /**
   * Based on the superstep, worker info, and attempt, get the appropriate
   * worker path for the exchange.
   *
   * @param attempt Application attempt
   * @param superstep Superstep
   * @param workerInfo Worker info of the exchange.
   * @return Path of the desired worker
   */
  public final String getPartitionExchangeWorkerPath(long attempt,
      long superstep,
      WorkerInfo workerInfo) {
    return getPartitionExchangePath(attempt, superstep) +
        "/" + workerInfo.getHostnameId();
  }

  /**
   * Generate the "superstep finished" directory path for a superstep
   *
   * @param attempt application attempt number
   * @param superstep superstep to use
   * @return directory path based on the a superstep
   */
  public final String getSuperstepFinishedPath(long attempt, long superstep) {
    return applicationAttemptsPath + "/" + attempt +
        SUPERSTEP_DIR + "/" + superstep + SUPERSTEP_FINISHED_NODE;
  }

  /**
   * Generate the base superstep directory path for a given application
   * attempt
   *
   * @param superstep Superstep to use
   * @return Directory path based on the a superstep
   */
  public final String getCheckpointBasePath(long superstep) {
    return checkpointBasePath + "/" + superstep;
  }

  /**
   * Get the checkpoint from a finalized checkpoint path
   *
   * @param finalizedPath Path of the finalized checkpoint
   * @return Superstep referring to a checkpoint of the finalized path
   */
  public static long getCheckpoint(Path finalizedPath) {
    if (!finalizedPath.getName().endsWith(CHECKPOINT_FINALIZED_POSTFIX)) {
      throw new InvalidParameterException(
          "getCheckpoint: " + finalizedPath + "Doesn't end in " +
              CHECKPOINT_FINALIZED_POSTFIX);
    }
    String checkpointString =
        finalizedPath.getName().replace(CHECKPOINT_FINALIZED_POSTFIX, "");
    return Long.parseLong(checkpointString);
  }

  /**
   * Get the ZooKeeperExt instance.
   *
   * @return ZooKeeperExt instance.
   */
  public final ZooKeeperExt getZkExt() {
    return zk;
  }

  @Override
  public final long getRestartedSuperstep() {
    return restartedSuperstep;
  }

  /**
   * Set the restarted superstep
   *
   * @param superstep Set the manually restarted superstep
   */
  public final void setRestartedSuperstep(long superstep) {
    if (superstep < INPUT_SUPERSTEP) {
      throw new IllegalArgumentException(
          "setRestartedSuperstep: Bad argument " + superstep);
    }
    restartedSuperstep = superstep;
  }

  /**
   * Should checkpoint on this superstep?  If checkpointing, always
   * checkpoint the first user superstep.  If restarting, the first
   * checkpoint is after the frequency has been met.
   *
   * @param superstep Decide if checkpointing no this superstep
   * @return True if this superstep should be checkpointed, false otherwise
   */
  public final boolean checkpointFrequencyMet(long superstep) {
    if (checkpointFrequency == 0) {
      return false;
    }
    long firstCheckpoint = INPUT_SUPERSTEP + 1;
    if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
      firstCheckpoint = getRestartedSuperstep() + checkpointFrequency;
    }
    if (superstep < firstCheckpoint) {
      return false;
    }
    return ((superstep - firstCheckpoint) % checkpointFrequency) == 0;
  }

  /**
   * Get the file system
   *
   * @return file system
   */
  public final FileSystem getFs() {
    return fs;
  }

  public final ImmutableClassesGiraphConfiguration<I, V, E, M>
  getConfiguration() {
    return conf;
  }

  public final Mapper<?, ?, ?, ?>.Context getContext() {
    return context;
  }

  public final String getHostname() {
    return hostname;
  }

  public final String getHostnamePartitionId() {
    return hostnamePartitionId;
  }

  public final int getTaskPartition() {
    return taskPartition;
  }

  public final GraphTaskManager<I, V, E, M> getGraphTaskManager() {
    return graphTaskManager;
  }

  public final BspEvent getWorkerHealthRegistrationChangedEvent() {
    return workerHealthRegistrationChanged;
  }

  public final BspEvent getAddressesAndPartitionsReadyChangedEvent() {
    return addressesAndPartitionsReadyChanged;
  }


  public final BspEvent getApplicationAttemptChangedEvent() {
    return applicationAttemptChanged;
  }

  public final BspEvent getSuperstepFinishedEvent() {
    return superstepFinished;
  }


  public final BspEvent getMasterElectionChildrenChangedEvent() {
    return masterElectionChildrenChanged;
  }

  public final BspEvent getCleanedUpChildrenChangedEvent() {
    return cleanedUpChildrenChanged;
  }

  /**
   * Get the master commanded job state as a JSONObject.  Also sets the
   * watches to see if the master commanded job state changes.
   *
   * @return Last job state or null if none
   * @throws InterruptedException
   * @throws KeeperException
   */
  public final JSONObject getJobState() {
    try {
      getZkExt().createExt(masterJobStatePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("getJobState: Job state already exists (" +
          masterJobStatePath + ")");
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to create job state path " +
          "due to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Failed to create job state path " +
          "due to InterruptedException", e);
    }
    String jobState = null;
    try {
      List<String> childList =
          getZkExt().getChildrenExt(
              masterJobStatePath, true, true, true);
      if (childList.isEmpty()) {
        return null;
      }
      jobState =
          new String(getZkExt().getData(
              childList.get(childList.size() - 1), true, null));
    } catch (KeeperException.NoNodeException e) {
      LOG.info("getJobState: Job state path is empty! - " +
          masterJobStatePath);
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to get job state path " +
          "children due to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Failed to get job state path " +
          "children due to InterruptedException", e);
    }
    try {
      return new JSONObject(jobState);
    } catch (JSONException e) {
      throw new RuntimeException(
          "getJobState: Failed to parse job state " + jobState);
    }
  }

  /**
   * Get the job id
   *
   * @return job id
   */
  public final String getJobId() {
    return jobId;
  }

  /**
   * Get the latest application attempt and cache it.
   *
   * @return the latest application attempt
   */
  public final long getApplicationAttempt() {
    if (cachedApplicationAttempt != UNSET_APPLICATION_ATTEMPT) {
      return cachedApplicationAttempt;
    }
    try {
      getZkExt().createExt(applicationAttemptsPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("getApplicationAttempt: Node " +
          applicationAttemptsPath + " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException("Couldn't create application " +
          "attempts path due to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Couldn't create application " +
          "attempts path due to InterruptedException", e);
    }
    try {
      List<String> attemptList =
          getZkExt().getChildrenExt(
              applicationAttemptsPath, true, false, false);
      if (attemptList.isEmpty()) {
        cachedApplicationAttempt = 0;
      } else {
        cachedApplicationAttempt =
            Long.parseLong(Collections.max(attemptList));
      }
    } catch (KeeperException e) {
      throw new IllegalStateException("Couldn't get application " +
          "attempts to KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Couldn't get application " +
          "attempts to InterruptedException", e);
    }

    return cachedApplicationAttempt;
  }

  /**
   * Get the latest superstep and cache it.
   *
   * @return the latest superstep
   * @throws InterruptedException
   * @throws KeeperException
   */
  public final long getSuperstep() {
    if (cachedSuperstep != UNSET_SUPERSTEP) {
      return cachedSuperstep;
    }
    String superstepPath = getSuperstepPath(getApplicationAttempt());
    try {
      getZkExt().createExt(superstepPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("getApplicationAttempt: Node " +
            applicationAttemptsPath + " already exists!");
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "getSuperstep: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getSuperstep: InterruptedException", e);
    }

    List<String> superstepList;
    try {
      superstepList =
          getZkExt().getChildrenExt(superstepPath, true, false, false);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "getSuperstep: KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getSuperstep: InterruptedException", e);
    }
    if (superstepList.isEmpty()) {
      cachedSuperstep = INPUT_SUPERSTEP;
    } else {
      cachedSuperstep =
          Long.parseLong(Collections.max(superstepList));
    }

    return cachedSuperstep;
  }

  /**
   * Increment the cached superstep.  Shouldn't be the initial value anymore.
   */
  public final void incrCachedSuperstep() {
    if (cachedSuperstep == UNSET_SUPERSTEP) {
      throw new IllegalStateException(
          "incrSuperstep: Invalid unset cached superstep " +
              UNSET_SUPERSTEP);
    }
    ++cachedSuperstep;
  }

  /**
   * Set the cached superstep (should only be used for loading checkpoints
   * or recovering from failure).
   *
   * @param superstep will be used as the next superstep iteration
   */
  public final void setCachedSuperstep(long superstep) {
    cachedSuperstep = superstep;
  }

  /**
   * Set the cached application attempt (should only be used for restart from
   * failure by the master)
   *
   * @param applicationAttempt Will denote the new application attempt
   */
  public final void setApplicationAttempt(long applicationAttempt) {
    cachedApplicationAttempt = applicationAttempt;
    String superstepPath = getSuperstepPath(cachedApplicationAttempt);
    try {
      getZkExt().createExt(superstepPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      throw new IllegalArgumentException(
          "setApplicationAttempt: Attempt already exists! - " +
              superstepPath, e);
    } catch (KeeperException e) {
      throw new RuntimeException(
          "setApplicationAttempt: KeeperException - " +
              superstepPath, e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "setApplicationAttempt: InterruptedException - " +
              superstepPath, e);
    }
  }

  /**
   * Register a BspEvent.  Ensure that it will be signaled
   * by catastrophic failure so that threads waiting on an event signal
   * will be unblocked.
   *
   * @param event Event to be registered.
   */
  public void registerBspEvent(BspEvent event) {
    registeredBspEvents.add(event);
  }

  /**
   * Subclasses can use this to instantiate their respective partitioners
   *
   * @return Instantiated graph partitioner factory
   */
  protected GraphPartitionerFactory<I, V, E, M> getGraphPartitionerFactory() {
    return graphPartitionerFactory;
  }

  /**
   * Derived classes that want additional ZooKeeper events to take action
   * should override this.
   *
   * @param event Event that occurred
   * @return true if the event was processed here, false otherwise
   */
  protected boolean processEvent(WatchedEvent event) {
    return false;
  }

  @Override
  public final void process(WatchedEvent event) {
    // 1. Process all shared events
    // 2. Process specific derived class events
    if (LOG.isDebugEnabled()) {
      LOG.debug("process: Got a new event, path = " + event.getPath() +
          ", type = " + event.getType() + ", state = " +
          event.getState());
    }

    if ((event.getPath() == null) && (event.getType() == EventType.None)) {
      if (event.getState() == KeeperState.Disconnected) {
        // Watches may not be triggered for some time, so signal all BspEvents
        for (BspEvent bspEvent : registeredBspEvents) {
          bspEvent.signal();
        }
        LOG.warn("process: Disconnected from ZooKeeper (will automatically " +
            "try to recover) " + event);
      } else if (event.getState() == KeeperState.SyncConnected) {
        if (LOG.isInfoEnabled()) {
          LOG.info("process: Asynchronous connection complete.");
        }
        connectedEvent.signal();
      } else {
        LOG.warn("process: Got unknown null path event " + event);
      }
      return;
    }

    boolean eventProcessed = false;
    if (event.getPath().startsWith(masterJobStatePath)) {
      // This will cause all becomeMaster() MasterThreads to notice the
      // change in job state and quit trying to become the master.
      masterElectionChildrenChanged.signal();
      eventProcessed = true;
    } else if ((event.getPath().contains(WORKER_HEALTHY_DIR) ||
        event.getPath().contains(WORKER_UNHEALTHY_DIR)) &&
        (event.getType() == EventType.NodeChildrenChanged)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: workerHealthRegistrationChanged " +
            "(worker health reported - healthy/unhealthy )");
      }
      workerHealthRegistrationChanged.signal();
      eventProcessed = true;
    } else if (event.getPath().equals(
        vertexInputSplitsPaths.getAllReadyPath()) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: inputSplitsReadyChanged " +
            "(input splits ready)");
      }
      vertexInputSplitsEvents.getAllReadyChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(VERTEX_INPUT_SPLIT_RESERVED_NODE) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: vertexInputSplitsStateChanged " +
            "(made a reservation)");
      }
      vertexInputSplitsEvents.getStateChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(VERTEX_INPUT_SPLIT_RESERVED_NODE) &&
        (event.getType() == EventType.NodeDeleted)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: vertexInputSplitsStateChanged " +
            "(lost a reservation)");
      }
      vertexInputSplitsEvents.getStateChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(VERTEX_INPUT_SPLIT_FINISHED_NODE) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: vertexInputSplitsStateChanged " +
            "(finished inputsplit)");
      }
      vertexInputSplitsEvents.getStateChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(VERTEX_INPUT_SPLIT_DONE_DIR) &&
        (event.getType() == EventType.NodeChildrenChanged)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: vertexInputSplitsDoneStateChanged " +
            "(worker finished sending)");
      }
      vertexInputSplitsEvents.getDoneStateChanged().signal();
      eventProcessed = true;
    }  else if (event.getPath().equals(
        vertexInputSplitsPaths.getAllDonePath()) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: vertexInputSplitsAllDoneChanged " +
            "(all vertices sent from input splits)");
      }
      vertexInputSplitsEvents.getAllDoneChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().equals(
        edgeInputSplitsPaths.getAllReadyPath()) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: edgeInputSplitsReadyChanged " +
            "(input splits ready)");
      }
      edgeInputSplitsEvents.getAllReadyChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(EDGE_INPUT_SPLIT_RESERVED_NODE) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: edgeInputSplitsStateChanged " +
            "(made a reservation)");
      }
      edgeInputSplitsEvents.getStateChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(EDGE_INPUT_SPLIT_RESERVED_NODE) &&
        (event.getType() == EventType.NodeDeleted)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: edgeInputSplitsStateChanged " +
            "(lost a reservation)");
      }
      edgeInputSplitsEvents.getStateChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(EDGE_INPUT_SPLIT_FINISHED_NODE) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: edgeInputSplitsStateChanged " +
            "(finished inputsplit)");
      }
      edgeInputSplitsEvents.getStateChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(EDGE_INPUT_SPLIT_DONE_DIR) &&
        (event.getType() == EventType.NodeChildrenChanged)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("process: edgeInputSplitsDoneStateChanged " +
            "(worker finished sending)");
      }
      edgeInputSplitsEvents.getDoneStateChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().equals(
        edgeInputSplitsPaths.getAllDonePath()) &&
        (event.getType() == EventType.NodeCreated)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: edgeInputSplitsAllDoneChanged " +
            "(all vertices sent from input splits)");
      }
      edgeInputSplitsEvents.getAllDoneChanged().signal();
      eventProcessed = true;
    } else if (event.getPath().contains(ADDRESSES_AND_PARTITIONS_DIR) &&
        event.getType() == EventType.NodeCreated) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: partitionAssignmentsReadyChanged " +
            "(partitions are assigned)");
      }
      addressesAndPartitionsReadyChanged.signal();
      eventProcessed = true;
    } else if (event.getPath().contains(SUPERSTEP_FINISHED_NODE) &&
        event.getType() == EventType.NodeCreated) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: superstepFinished signaled");
      }
      superstepFinished.signal();
      eventProcessed = true;
    } else if (event.getPath().endsWith(applicationAttemptsPath) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: applicationAttemptChanged signaled");
      }
      applicationAttemptChanged.signal();
      eventProcessed = true;
    } else if (event.getPath().contains(MASTER_ELECTION_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: masterElectionChildrenChanged signaled");
      }
      masterElectionChildrenChanged.signal();
      eventProcessed = true;
    } else if (event.getPath().equals(cleanedUpPath) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("process: cleanedUpChildrenChanged signaled");
      }
      cleanedUpChildrenChanged.signal();
      eventProcessed = true;
    }

    if (!(processEvent(event)) && (!eventProcessed)) {
      LOG.warn("process: Unknown and unprocessed event (path=" +
          event.getPath() + ", type=" + event.getType() +
          ", state=" + event.getState() + ")");
    }
  }
  
  public void incLocalRequests() {
  }
  public void setStart() {
  	
  }
  public double getSp() {
  	return 0;
  }
  public double getSl() {
  	return 0;
  }
  public double getSr() {
  	return 0;
  }
  public void setFinishTime() {
  }
  public long getRunTime() {
  	return 0;
  }
  public void stopThreads(Monitor monitor) {
	  
  }
  public void restartThreads(Monitor Monitor) {
	  
  }
  public void addRemoteMessage(long time) {
	  
  }
  public void resetRemoteTime(){
  }
  public long getRemoteStartTime(){
	  return 0;
  }
  
  public void setRemoteStartTime(long value){
  }
  
  public long getRemoteFinishTime(){
	return 0;  
  }
  
  public void setRemoteFinishTime(long value){
	  
  }
  
  public double getEdgeCuts(){
	  return 0.0;
  }
}
