/**
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
package org.apache.hadoop.mapred;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.AuditLogger.Constants;
import org.apache.hadoop.mapred.JobInProgress.KillInterruptedException;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.DelegationTokenRenewal;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 * JobTracker通过在内存中维护有关Job、Task相关的所有信息，来跟踪他们运行、交互过程中所发生的数据交换，等等
 *
 *******************************************************/
public class JobTracker implements MRConstants, InterTrackerProtocol,
    JobSubmissionProtocol, TaskTrackerManager, RefreshUserMappingsProtocol,
    RefreshAuthorizationPolicyProtocol, AdminOperationsProtocol,
    JobTrackerMXBean {

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  static long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  static long RETIRE_JOB_INTERVAL;
  static long RETIRE_JOB_CHECK_INTERVAL;
  
  private final long DELEGATION_TOKEN_GC_INTERVAL = 3600000; // 1 hour
  private final DelegationTokenSecretManager secretManager;

  // The maximum fraction (range [0.0-1.0]) of nodes in cluster allowed to be
  // added to the all-jobs blacklist via heuristics.  By default, no more than
  // 50% of the cluster can be heuristically blacklisted, but the external
  // node-healthcheck script is not affected by this.
  private static double MAX_BLACKLIST_FRACTION = 0.5;

  // A tracker is blacklisted across jobs only if number of faults is more
  // than X% above the average number of faults (averaged across all nodes
  // in cluster).  X is the blacklist threshold here; 0.3 would correspond
  // to 130% of the average, for example.
  // 对应配置项：mapred.cluster.average.blacklist.threshold。
  // 默认是0.5，如果一个bad tasktracker失败的task个数超过了所有tasktracker平均值的mapred.cluster.average.blacklist.threshold倍，则加入灰名单，不仅会自动加入黑名单。
  private double AVERAGE_BLACKLIST_THRESHOLD = 0.5;

  // Fault threshold (number occurring within TRACKER_FAULT_TIMEOUT_WINDOW)
  // to consider a task tracker bad enough to blacklist heuristically.  This
  // is functionally the same as the older "MAX_BLACKLISTS_PER_TRACKER" value.
  // 对应配置项：mapred.max.tracker.blacklists。默认是4，bad tasktracker阈值，当一个tasktracker在时间窗口内失败个数超过该阈值，则认为该tasktracker是bad tasktracker
  private int TRACKER_FAULT_THRESHOLD; // = 4;

  // Width of overall fault-tracking sliding window (in minutes). (Default
  // of 24 hours matches previous "UPDATE_FAULTY_TRACKER_INTERVAL" value that
  // was used to forgive a single fault if no others occurred in the interval.)
  // 对应配置项：mapred.jobtracker.blacklist.fault-timeout-window，默认是3小时，时间窗口，计算该时间内失败的task个数
  // 以滑窗方式检测一个主机是否应该放入黑名单，，如果在TRACKER_FAULT_TIMEOUT_WINDOW=3小时范围内，出现了大于TRACKER_FAULT_THRESHOLD=4次失败，则应该放入黑名单。
  private int TRACKER_FAULT_TIMEOUT_WINDOW; // = 180 (3 hours)

  // Width of a single fault-tracking bucket (in minutes).
  private int TRACKER_FAULT_BUCKET_WIDTH;   // = 15
  private long TRACKER_FAULT_BUCKET_WIDTH_MSECS;

  // Basically TRACKER_FAULT_TIMEOUT_WINDOW / TRACKER_FAULT_BUCKET_WIDTH .
  private int NUM_FAULT_BUCKETS;

  /** the maximum allowed size of the jobconf **/
  long MAX_JOBCONF_SIZE = 5*1024*1024L;
  /** the config key for max user jobconf size **/
  public static final String MAX_USER_JOBCONF_SIZE_KEY = 
      "mapred.user.jobconf.limit";

  // Delegation token related keys
  public static final String  DELEGATION_KEY_UPDATE_INTERVAL_KEY =  
    "mapreduce.cluster.delegation.key.update-interval";
  public static final long    DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =  
    24*60*60*1000; // 1 day
  public static final String  DELEGATION_TOKEN_RENEW_INTERVAL_KEY =  
    "mapreduce.cluster.delegation.token.renew-interval";
  public static final long    DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =  
    24*60*60*1000;  // 1 day
  public static final String  DELEGATION_TOKEN_MAX_LIFETIME_KEY =  
    "mapreduce.cluster.delegation.token.max-lifetime";
  public static final long    DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =  
    7*24*60*60*1000; // 7 days
  
  // Approximate number of heartbeats that could arrive JobTracker
  // in a second
  static final String JT_HEARTBEATS_IN_SECOND = "mapred.heartbeats.in.second";
  private int NUM_HEARTBEATS_IN_SECOND;
  private static final int DEFAULT_NUM_HEARTBEATS_IN_SECOND = 100;
  private static final int MIN_NUM_HEARTBEATS_IN_SECOND = 1;
  
  // Scaling factor for heartbeats, used for testing only
  static final String JT_HEARTBEATS_SCALING_FACTOR = 
    "mapreduce.jobtracker.heartbeats.scaling.factor";
  private float HEARTBEATS_SCALING_FACTOR;
  private final float MIN_HEARTBEATS_SCALING_FACTOR = 0.01f;
  private final float DEFAULT_HEARTBEATS_SCALING_FACTOR = 1.0f;
  
  final static String JT_INIT_CONFIG_KEY_FOR_TESTS = 
      "mapreduce.jobtracker.init.for.tests";
  
  public static enum State { INITIALIZING, RUNNING }
  volatile State state = State.INITIALIZING;
  private static final int FS_ACCESS_RETRY_PERIOD = 1000;
  static final String JOB_INFO_FILE = "job-info";
  private DNSToSwitchMapping dnsToSwitchMapping;
  private NetworkTopology clusterMap;
  private int numTaskCacheLevels; // the max level to which we cache tasks
  private boolean isNodeGroupAware;
  /**
   * {@link #nodesAtMaxLevel} is using the keySet from {@link ConcurrentHashMap}
   * so that it can be safely written to and iterated on via 2 separate threads.
   * Note: It can only be iterated from a single thread which is feasible since
   *       the only iteration is done in {@link JobInProgress} under the 
   *       {@link JobTracker} lock.
   */
  private Set<Node> nodesAtMaxLevel = 
    Collections.newSetFromMap(new ConcurrentHashMap<Node, Boolean>());
  private final TaskScheduler taskScheduler;
  // JobTracker维护了一组JobInProgressListener监听器，在JobTracker运行过程中，发生某些事件会触发注册的JobInProgressListener的执行。
  // 比如，JobClient提交一个Job，JobTracker端会触发对应的JobInProgressListener调用jobAdded()初始化该Job；
  // 比如，Job执行过程中状态发生变更，会触发JobInProgressListener调用jobUpdated()执行；
  // 比如，Job运行完成，会触发jobInProgressListener调用jobRemoved()执行。
  // JobTracker初始化时会创建TaskScheduler，而启动TaskScheduler的时候，会把TaskScheduler所维护的JobInProgressListener添加到jobInProgressListeners列表中。
  private final List<JobInProgressListener> jobInProgressListeners =
    new CopyOnWriteArrayList<JobInProgressListener>();
  // 通过ServicePlugin接口，可以基于任意的RPC协议暴露DataNode或NameNode的功能。
  // 通过配置项mapreduce.jobtracker.plugins可以设置ServicePlugin，JobTracker启动的时候会加载初始化配置的ServicePlugin。
  private List<ServicePlugin> plugins;
  
  private static final LocalDirAllocator lDirAlloc = 
                              new LocalDirAllocator("mapred.local.dir");
  //system directory is completely owned by the JobTracker
  final static FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------

  // system files should have 700 permission
  final static FsPermission SYSTEM_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------
  
  private Clock clock;

  private final JobTokenSecretManager jobTokenSecretManager
    = new JobTokenSecretManager();

  JobTokenSecretManager getJobTokenSecretManager() {
    return jobTokenSecretManager;
  }

  /**
   * A client tried to submit a job before the Job Tracker was ready.
   */
  public static class IllegalStateException extends IOException {
 
    private static final long serialVersionUID = 1L;

    public IllegalStateException(String msg) {
      super(msg);
    }
  }

  /**
   * The maximum no. of 'completed' (successful/failed/killed)
   * jobs kept in memory per-user. 
   */
  final int MAX_COMPLETE_USER_JOBS_IN_MEMORY;

   /**
    * The minimum time (in ms) that a job's information has to remain
    * in the JobTracker's memory before it is retired.
    */
  static final int MIN_TIME_BEFORE_RETIRE = 0;

  private int nextJobId = 1;

  public static final Log LOG = LogFactory.getLog(JobTracker.class);

  static final String CONF_VERSION_KEY = "mapreduce.jobtracker.conf.version";
  static final String CONF_VERSION_DEFAULT = "default";
  
  public Clock getClock() {
    return clock;
  }
    
  static final String JT_HDFS_MONITOR_ENABLE = 
      "mapreduce.jt.hdfs.monitor.enable";
  static final boolean DEFAULT_JT_HDFS_MONITOR_THREAD_ENABLE = false;
  
  static final String JT_HDFS_MONITOR_THREAD_INTERVAL = 
      "mapreduce.jt.hdfs.monitor.interval.ms";
  static final int DEFAULT_JT_HDFS_MONITOR_THREAD_INTERVAL_MS = 5000;
  
  private Thread hdfsMonitor;

  /**
   * Start the JobTracker with given configuration.
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the JobTracker is up and running if the user passes the port as
   * <code>zero</code>.
   *   
   * @param conf configuration for the JobTracker.
   * @throws IOException
   */
  public static JobTracker startTracker(JobConf conf
                                        ) throws IOException,
                                                 InterruptedException {
    // generateNewIdentifier: 根据当前时间生成格式为yyyyMMddHHmm的字符串
    return startTracker(conf, generateNewIdentifier());
  }

  public static JobTracker startTracker(JobConf conf, String identifier) 
  throws IOException, InterruptedException {
  	return startTracker(conf, identifier, false);
  }
  
  public static JobTracker startTracker(JobConf conf, String identifier, boolean initialize) 
  throws IOException, InterruptedException {
    DefaultMetricsSystem.initialize("JobTracker");
    JobTracker result = null;
    while (true) {
      try {
        // 实例化一个JobTracker对象, 包含QueueManager实例化,myInstrumentation的实例化, taskScheduler的实例化以及各种初始化操作
        result = new JobTracker(conf, identifier);
        // 将jobTracker对象设置给taskScheduler的taskTrackerManager对象
        result.taskScheduler.setTaskTrackerManager(result);
        break;
      } catch (VersionMismatch e) {
        throw e;
      } catch (BindException e) {
        throw e;
      } catch (UnknownHostException e) {
        throw e;
      } catch (AccessControlException ace) {
        // in case of jobtracker not having right access
        // bail out
        throw ace;
      } catch (IOException e) {
        LOG.warn("Error starting tracker: " + 
                 StringUtils.stringifyException(e));
      }
      Thread.sleep(1000);
    }
    if (result != null) {
      JobEndNotifier.startNotifier();
      MBeans.register("JobTracker", "JobTrackerInfo", result);
      // 传进来的是false，所以这里不进行JobTracker的初始化操作，而是留到offerService()中进行。 到这里main()方法的JobTracker tracker = startTracker(new JobConf())完成，
      // 接下来回到main方法，执行tracker.offerService()。
      if(initialize == true) {
        result.setSafeModeInternal(SafeModeAction.SAFEMODE_ENTER);
        result.initializeFilesystem();
        result.setSafeModeInternal(SafeModeAction.SAFEMODE_LEAVE);
        result.initialize();
      }
    }
    return result;
  }

  public void stopTracker() throws IOException {
    JobEndNotifier.stopNotifier();
    close();
  }
    
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())){
      return JobSubmissionProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(AdminOperationsProtocol.class.getName())){
      return AdminOperationsProtocol.versionID;
    } else if (protocol.equals(RefreshUserMappingsProtocol.class.getName())){
      return RefreshUserMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to job tracker: " + protocol);
    }
  }
  
  public DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return secretManager;
  }
  
  /**
   * A thread to timeout tasks that have been assigned to task trackers,
   * but that haven't reported back yet.
   * Note that I included a stop() method, even though there is no place
   * where JobTrackers are cleaned up.
   */
  private class ExpireLaunchingTasks implements Runnable {
    /**
     * This is a map of the tasks that have been assigned to task trackers,
     * but that have not yet been seen in a status report.
     * map: task-id -> time-assigned 
     */
    private Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();
      
    public void run() {
      while (true) {
        try {
          // Every 3 minutes check for any tasks that are overdue
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
          long now = clock.getTime();
          if(LOG.isDebugEnabled()) {
            LOG.debug("Starting launching task sweep");
          }
          synchronized (JobTracker.this) {
            synchronized (launchingTasks) {
              Iterator<Map.Entry<TaskAttemptID, Long>> itr =
                launchingTasks.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<TaskAttemptID, Long> pair = itr.next();
                TaskAttemptID taskId = pair.getKey();
                long age = now - (pair.getValue()).longValue();
                LOG.info(taskId + " is " + age + " ms debug.");
                if (age > TASKTRACKER_EXPIRY_INTERVAL) {
                  LOG.info("Launching task " + taskId + " timed out.");
                  TaskInProgress tip = null;
                  tip = taskidToTIPMap.get(taskId);
                  if (tip != null) {
                    JobInProgress job = tip.getJob();
                    String trackerName = getAssignedTracker(taskId);
                    TaskTrackerStatus trackerStatus = 
                      getTaskTrackerStatus(trackerName); 
                      
                    // This might happen when the tasktracker has already
                    // expired and this thread tries to call failedtask
                    // again. expire tasktracker should have called failed
                    // task!
                    if (trackerStatus != null)
                      // 将Task状态标注为FAILED，并更新Task状态
                      job.failedTask(tip, taskId, "Error launching task", 
                                     tip.isMapTask()? TaskStatus.Phase.MAP:
                                     TaskStatus.Phase.STARTING,
                                     TaskStatus.State.FAILED,
                                     trackerName);
                  }
                  itr.remove();
                } else {
                  // the tasks are sorted by start time, so once we find
                  // one that we want to keep, we are done for this cycle.
                  break;
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          // all done
          break;
        } catch (Exception e) {
          LOG.error("Expire Launching Task Thread got exception: " +
                    StringUtils.stringifyException(e));
        }
      }
    }
      
    public void addNewTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.put(taskName, 
                           clock.getTime());
      }
    }
      
    public void removeTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.remove(taskName);
      }
    }
  }
    
  ///////////////////////////////////////////////////////
  // Used to expire TaskTrackers that have gone down
  ///////////////////////////////////////////////////////
  // TaskTracker与JobTracker失去连接，更新状态
  // 线程expireTracker Thread周期性的扫描队列trackerExpiryQueue，如果发现在某段时间（mapred.tasktracker.expiry.interval设置， 默认10分钟）内未汇报心跳，则将其从集群中移除。
  // 如果发现该任务处在运行或者等待状态或者是未完成的Task或者Reduce Task数目不为零的作业中已经完成的Map Task，则将会放入其他节点重新运行。
  class ExpireTrackers implements Runnable {
    public ExpireTrackers() {
    }
    /**
     * The run method lives for the life of the JobTracker, and removes TaskTrackers
     * that have not checked in for some time.
     */
    public void run() {
      while (true) {
        try {
          //
          // Thread runs periodically to check whether trackers should be expired.
          // The sleep interval must be no more than half the maximum expiry time
          // for a task tracker.
          //
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);

          //
          // Loop through all expired items in the queue
          //
          // Need to lock the JobTracker here since we are
          // manipulating it's data-structures via
          // ExpireTrackers.run -> JobTracker.lostTaskTracker ->
          // JobInProgress.failedTask -> JobTracker.markCompleteTaskAttempt
          // Also need to lock JobTracker before locking 'taskTracker' &
          // 'trackerExpiryQueue' to prevent deadlock:
          // @see {@link JobTracker.processHeartbeat(TaskTrackerStatus, boolean, long)} 
          synchronized (JobTracker.this) {
            synchronized (taskTrackers) {
              synchronized (trackerExpiryQueue) {
                long now = clock.getTime();
                TaskTrackerStatus leastRecent = null;
                // 取出队列中最早的一个TaskTrackerStatus，如果10分钟内没有收到心跳，则对TasTracker进行处理
                while ((trackerExpiryQueue.size() > 0) &&
                       (leastRecent = trackerExpiryQueue.first()) != null &&
                       ((now - leastRecent.getLastSeen()) > TASKTRACKER_EXPIRY_INTERVAL)) {

                        
                  // Remove profile from head of queue
                  trackerExpiryQueue.remove(leastRecent);
                  String trackerName = leastRecent.getTrackerName();
                        
                  // Figure out if last-seen time should be updated, or if tracker is dead
                  // 这里为什么用的是current.getStatus而不是leastRecent? 是因为trackerExpiryQueue放的初次初始化是的状态，
                  // 而current.getStatus是从taskTrackers获取的TaskTracker，里面放的是TaskTracker的最新状态信息
                  TaskTracker current = getTaskTracker(trackerName);
                  TaskTrackerStatus newProfile = 
                    (current == null ) ? null : current.getStatus();
                  // Items might leave the taskTracker set through other means; the
                  // status stored in 'taskTrackers' might be null, which means the
                  // tracker has already been destroyed.
                  if (newProfile != null) {
                    if ((now - newProfile.getLastSeen()) > TASKTRACKER_EXPIRY_INTERVAL) {
                      // 最关键的步骤，移除过期的TaskTracker，见该方法的具体实现。
                      removeTracker(current);
                      // remove the mapping from the hosts list
                      String hostname = newProfile.getHost();
                      hostnameToTaskTracker.get(hostname).remove(trackerName);
                    } else {
                      // Update time by inserting latest profile
                      // 更新trackerExpiryQueue队列中的TaskTrackerStatus对象。
                      trackerExpiryQueue.add(newProfile);
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException iex) {
          break;
        } catch (Exception t) {
          LOG.error("Tracker Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
        
  }

  synchronized void historyFileCopied(JobID jobid, String historyFile) {
    JobInProgress job = getJob(jobid);
    if (job != null) { //found in main cache
      if (historyFile != null) {
        job.setHistoryFile(historyFile);
      }
      return;
    }
    RetireJobInfo jobInfo = retireJobs.get(jobid);
    if (jobInfo != null) { //found in retired cache
      if (historyFile != null) {
        jobInfo.setHistoryFile(historyFile);
      }
    }
  }

  static class RetireJobInfo {
    final JobStatus status;
    final JobProfile profile;
    final long finishTime;
    final Counters counters;
    private String historyFile;
    RetireJobInfo(Counters counters, JobStatus status, JobProfile profile, 
        long finishTime, String historyFile) {
      this.counters = counters;
      this.status = status;
      this.profile = profile;
      this.finishTime = finishTime;
      this.historyFile = historyFile;
    }
    void setHistoryFile(String file) {
      this.historyFile = file;
    }
    String getHistoryFile() {
      return historyFile;
    }
  }
  ///////////////////////////////////////////////////////
  // Used to remove old finished Jobs that have been around for too long
  ///////////////////////////////////////////////////////
  class RetireJobs implements Runnable {
    private final Map<JobID, RetireJobInfo> jobIDStatusMap = 
      new HashMap<JobID, RetireJobInfo>();
    private final LinkedList<RetireJobInfo> jobRetireInfoQ = 
      new LinkedList<RetireJobInfo>();
    public RetireJobs() {
    }

    synchronized void addToCache(JobInProgress job) {
      Counters counters = new Counters();
      boolean isFine = job.getCounters(counters);
      counters = (isFine? counters: new Counters());
      RetireJobInfo info = new RetireJobInfo(counters, job.getStatus(),
          job.getProfile(), job.getFinishTime(), job.getHistoryFile());
      jobRetireInfoQ.add(info);
      jobIDStatusMap.put(info.status.getJobID(), info);
      if (jobRetireInfoQ.size() > retiredJobsCacheSize) {
        RetireJobInfo removed = jobRetireInfoQ.remove();
        jobIDStatusMap.remove(removed.status.getJobID());
        LOG.info("Retired job removed from cache " + removed.status.getJobID());
      }
    }

    synchronized RetireJobInfo get(JobID jobId) {
      return jobIDStatusMap.get(jobId);
    }

    @SuppressWarnings("unchecked")
    synchronized LinkedList<RetireJobInfo> getAll() {
      return (LinkedList<RetireJobInfo>) jobRetireInfoQ.clone();
    }

    synchronized LinkedList<JobStatus> getAllJobStatus() {
      LinkedList<JobStatus> list = new LinkedList<JobStatus>();
      for (RetireJobInfo info : jobRetireInfoQ) {
        list.add(info.status);
      }
      return list;
    }

    private boolean minConditionToRetire(JobInProgress job, long now) {
      return job.getStatus().getRunState() != JobStatus.RUNNING
          && job.getStatus().getRunState() != JobStatus.PREP
          && (job.getFinishTime() + MIN_TIME_BEFORE_RETIRE < now);
    }
    /**
     * The run method lives for the life of the JobTracker,
     * and removes Jobs that are not still running, but which
     * finished a long time ago.
     */
    public void run() {
      while (true) {
        try {
          // 默认检查间隔是1分钟
          Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
          List<JobInProgress> retiredJobs = new ArrayList<JobInProgress>();
          long now = clock.getTime();
          // RETIRE_JOB_INTERVAL默认值为24小时
          long retireBefore = now - RETIRE_JOB_INTERVAL;

          synchronized (jobs) {
            // 对于状态是SUCCEEDED，FAILED或者KILLED, 并且作业完成时间距离现在已经超过24小时，将作业加入过期队列
            for(JobInProgress job: jobs.values()) {
              if (minConditionToRetire(job, now) &&
                  (job.getFinishTime()  < retireBefore)) {
                retiredJobs.add(job);
              }
            }
          }
          synchronized (userToJobsMap) {
            Iterator<Map.Entry<String, ArrayList<JobInProgress>>> 
                userToJobsMapIt = userToJobsMap.entrySet().iterator();
            while (userToJobsMapIt.hasNext()) {
              Map.Entry<String, ArrayList<JobInProgress>> entry = 
                userToJobsMapIt.next();
              ArrayList<JobInProgress> userJobs = entry.getValue();

              // Remove retiredJobs from userToJobsMap to ensure we don't 
              // retire them multiple times
              Iterator<JobInProgress> it = userJobs.iterator();
              while (it.hasNext()) {
                JobInProgress jobUser = it.next();
                if (retiredJobs.contains(jobUser)) {
                  LOG.info("Removing from userToJobsMap: " + 
                      jobUser.getJobID());
                  it.remove();
                }
              }
              
              // Now, check for #jobs per user
              // 对于每个用户，userToJobsMap中如果用户的作业数量大于100,会将最早的已完成的作业信息移入过期队列。
              it = userJobs.iterator();
              while (it.hasNext() && 
                  userJobs.size() > MAX_COMPLETE_USER_JOBS_IN_MEMORY) {
                JobInProgress jobUser = it.next();
                if (minConditionToRetire(jobUser, now)) {
                  LOG.info("User limit exceeded. Marking job: " + 
                      jobUser.getJobID() + " for retire.");
                  retiredJobs.add(jobUser);
                  it.remove();
                }
              }
              // 如果用户的作业信息已经为空了，将用户从userToJobsMap中移除
              if (userJobs.isEmpty()) {
                userToJobsMapIt.remove();
              }
            }
          }
          if (!retiredJobs.isEmpty()) {
            synchronized (JobTracker.this) {
              synchronized (jobs) {
                synchronized (taskScheduler) {
                  for (JobInProgress job: retiredJobs) {
                    removeJobTasks(job);
                    jobs.remove(job.getProfile().getJobID());
                    for (JobInProgressListener l : jobInProgressListeners) {
                      l.jobRemoved(job);
                    }
                    String jobUser = job.getProfile().getUser();
                    LOG.info("Retired job with id: '" + 
                             job.getProfile().getJobID() + "' of user '" +
                             jobUser + "'");

                    // clean up job files from the local disk
                    JobHistory.JobInfo.cleanupJob(job.getProfile().getJobID());
                    addToCache(job);
                  }
                }
              }
            }
          }
        } catch (InterruptedException t) {
          break;
        } catch (Throwable t) {
          LOG.error("Error in retiring job:\n" +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }
  
  enum ReasonForBlackListing {
    EXCEEDING_FAILURES,
    NODE_UNHEALTHY
  }

  // FaultInfo:  data structure that tracks the number of faults of a single
  // TaskTracker, when the last fault occurred, and whether the TaskTracker
  // is blacklisted across all jobs or not.
  // FaultInfo：跟踪单个TaskTracker的故障数，上次故障发生时的数据结构，以及TaskTracker是否被列入所有作业的黑名单。
  private static class FaultInfo {
    static final String FAULT_FORMAT_STRING =  "%d failures on the tracker";
    int[] numFaults;      // timeslice buckets
    long lastRotated;     // 1st millisecond of current bucket
    boolean blacklisted; 
    boolean graylisted; 

    private int numFaultBuckets;
    private long bucketWidth;
    private HashMap<ReasonForBlackListing, String> blackRfbMap;
    private HashMap<ReasonForBlackListing, String> grayRfbMap;

    FaultInfo(long time, int numFaultBuckets, long bucketWidth) {
      this.numFaultBuckets = numFaultBuckets;
      this.bucketWidth = bucketWidth;
      numFaults = new int[numFaultBuckets];
      lastRotated = (time / bucketWidth) * bucketWidth;
      blacklisted = false;
      graylisted = false;
      blackRfbMap = new HashMap<ReasonForBlackListing, String>();
      grayRfbMap = new HashMap<ReasonForBlackListing, String>();
    }

    // timeStamp is presumed to be "now":  there are no checks for past or
    // future values, etc.
    private void checkRotation(long timeStamp) {
      long diff = timeStamp - lastRotated;
      // find index of the oldest bucket(s) and zero it (or them) out
      while (diff > bucketWidth) {
        // this is now the 1st millisecond of the oldest bucket, in a modular-
        // arithmetic sense (i.e., about to become the newest bucket):
        lastRotated += bucketWidth;
        // corresponding bucket index:
        int idx = (int)((lastRotated / bucketWidth) % numFaultBuckets);
        // clear the bucket's contents in preparation for new faults
        numFaults[idx] = 0;
        diff -= bucketWidth;
      }
    }

    private int bucketIndex(long timeStamp) {
      // stupid Java compiler thinks an int modulus can produce a long, sigh...
      return (int)((timeStamp / bucketWidth) % numFaultBuckets);
    }

    // no longer any need for corresponding decrFaultCount() method since we
    // implicitly auto-decrement when oldest bucket's contents get wiped on
    // rotation
    void incrFaultCount(long timeStamp) {
      checkRotation(timeStamp);
      ++numFaults[bucketIndex(timeStamp)];
    }

    int getFaultCount(long timeStamp) {
      checkRotation(timeStamp);
      int faultCount = 0;
      for (int faults : numFaults) { 
        faultCount += faults;
      }
      return faultCount;
    }

    boolean isBlacklisted() {
      return blacklisted;
    }

    boolean isGraylisted() {
      return graylisted;
    }

    void setBlacklist(ReasonForBlackListing rfb, String trackerFaultReport,
                      boolean gray) {
      if (gray) {
        graylisted = true;
        this.grayRfbMap.put(rfb, trackerFaultReport);
      } else {
        blacklisted = true;
        this.blackRfbMap.put(rfb, trackerFaultReport);
      }
    }

    public String getTrackerBlackOrGraylistReport(boolean gray) {
      StringBuffer sb = new StringBuffer();
      HashMap<ReasonForBlackListing, String> rfbMap =
        new HashMap<ReasonForBlackListing, String>();
      rfbMap.putAll(gray? grayRfbMap : blackRfbMap);
      for (String reasons : rfbMap.values()) {
        sb.append(reasons);
        sb.append("\n");
      }
      return sb.toString();
    }

    Set<ReasonForBlackListing> getReasonForBlacklisting(boolean gray) {
      return (gray? this.grayRfbMap.keySet() : this.blackRfbMap.keySet());
    }

    // no longer on the blacklist (or graylist), but we're still tracking any
    // faults in case issue is intermittent => don't clear numFaults[]
    public void unBlacklist(boolean gray) {
      if (gray) {
        graylisted = false;
        grayRfbMap.clear();
      } else {
        blacklisted = false;
        blackRfbMap.clear();
      }
    }

    public boolean removeBlacklistedReason(ReasonForBlackListing rfb,
                                           boolean gray) {
      String str = (gray? grayRfbMap.remove(rfb) : blackRfbMap.remove(rfb));
      return str!=null;
    }

    public void addBlacklistedReason(ReasonForBlackListing rfb,
                                     String reason, boolean gray) {
      if (gray) {
        grayRfbMap.put(rfb, reason);
      } else {
        blackRfbMap.put(rfb, reason);
      }
    }
    
  }

  private class FaultyTrackersInfo {
    // A map from hostName to its faults
    // potentiallyFaultyTrackers(潜在有错误的Tracker。当有task运行失败时，就将其加入该队列中)集合中移除。
    private Map<String, FaultInfo> potentiallyFaultyTrackers = 
              new HashMap<String, FaultInfo>();
    // This count gives the number of blacklisted trackers in the cluster 
    // at any time. This is maintained to avoid iteration over 
    // the potentiallyFaultyTrackers to get blacklisted trackers. And also
    // this count doesn't include blacklisted trackers which are lost, 
    // although the fault info is maintained for lost trackers.  
    private volatile int numBlacklistedTrackers = 0;
    private volatile int numGraylistedTrackers = 0;

    /**
     * Increments faults(blacklist by job) for the tracker by one.
     * 
     * Adds the tracker to the potentially faulty list. 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName 
     */
    void incrementFaults(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        long now = clock.getTime();
        FaultInfo fi = getFaultInfo(hostName, true);
        fi.incrFaultCount(now);
        // check heuristics, and add to the graylist if over the limit:
        if (exceedsFaults(fi, now)) {
          LOG.info("Adding " + hostName + " to the graylist across all jobs");
          String reason = String.format(FaultInfo.FAULT_FORMAT_STRING,
              fi.getFaultCount(now));
          blacklistTracker(hostName, reason,
              ReasonForBlackListing.EXCEEDING_FAILURES, true);
        }
      }        
    }

    /**
     * Graylists the tracker across all jobs (similar to blacklisting except
     * not actually removed from service) if all of the following heuristics
     * hold:
     * <ol>
     * <li>number of faults within TRACKER_FAULT_TIMEOUT_WINDOW is greater
     *     than or equal to TRACKER_FAULT_THRESHOLD (per-job blacklistings)
     *     (both configurable)</li>
     * <li>number of faults (per-job blacklistings) for given node is more
     *     than (1 + AVERAGE_BLACKLIST_THRESHOLD) times the average number
     *     of faults across all nodes (configurable)</li>
     * <li>less than 50% of the cluster is blacklisted (NOT configurable)</li>
     * </ol>
     * Note that the node health-check script is not explicitly limited by
     * the 50%-blacklist limit.
     */
    // this is the sole source of "heuristic blacklisting" == graylisting
    private boolean exceedsFaults(FaultInfo fi, long timeStamp) {
      int faultCount = fi.getFaultCount(timeStamp);
      if (faultCount >= TRACKER_FAULT_THRESHOLD) {
        // calculate average faults across all nodes
        long clusterSize = getClusterStatus().getTaskTrackers();
        long sum = 0;
        for (FaultInfo f : potentiallyFaultyTrackers.values()) {
          sum += f.getFaultCount(timeStamp);
        }
        double avg = (double) sum / clusterSize;   // avg num faults per node
        // graylisted trackers are already included in clusterSize:
        long totalCluster = clusterSize + numBlacklistedTrackers;
        if ((faultCount - avg) > (AVERAGE_BLACKLIST_THRESHOLD * avg) &&
            numGraylistedTrackers < (totalCluster * MAX_BLACKLIST_FRACTION)) {
          return true;
        }
      }
      return false;
    }

    private void incrBlacklistedTrackers(int count) {
      LOG.info("Incrementing blacklisted trackers by " + count);
      numBlacklistedTrackers += count;
      getInstrumentation().addBlackListedTrackers(count);
    }

    private void decrBlacklistedTrackers(int count) {
      LOG.info("Decrementing blacklisted trackers by " + count);
      numBlacklistedTrackers -= count;
      getInstrumentation().decBlackListedTrackers(count);
    }

    private void incrGraylistedTrackers(int count) {
      LOG.info("Incrementing graylisted trackers by " + count);
      numGraylistedTrackers += count;
      getInstrumentation().addGrayListedTrackers(count);
    }

    private void decrGraylistedTrackers(int count) {
      LOG.info("Decrementing graylisted trackers by " + count);
      numGraylistedTrackers -= count;
      getInstrumentation().decGrayListedTrackers(count);
    }

    // This may be called either as a result of the node health-check script
    // or because of heuristics based on single-job blacklist info.
    private void blacklistTracker(String hostName, String reason,
                                  ReasonForBlackListing rfb,
                                  boolean gray) {
      FaultInfo fi = getFaultInfo(hostName, true);
      String shade = gray? "gray" : "black";
      boolean listed = gray? fi.isGraylisted() : fi.isBlacklisted();
      if (listed) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding/overwriting reason for " + shade +
              "listed tracker : " + hostName + " Reason for " + shade +
              "listing is : " + rfb + " Reason details : " + reason);
        }
        if (!fi.getReasonForBlacklisting(gray).contains(rfb)) {
          LOG.info("Adding new reason for " + shade + "listed tracker : " +
              hostName + " Reason for " + shade + "listing is : " + rfb 
              + " Reason details : " + reason);
        }
        fi.addBlacklistedReason(rfb, reason, gray);
      } else {
        LOG.info("Adding new " + shade + "listed tracker : " + hostName 
            + " Reason for " + shade + "listing is : " + rfb 
            + " Reason details : " + reason);
        if (gray) {
          incrGraylistedTrackers(getNumTaskTrackersOnHost(hostName));
        } else {
          Set<TaskTracker> trackers = 
            hostnameToTaskTracker.get(hostName);
          synchronized (trackers) {
            for (TaskTracker tracker : trackers) {
              tracker.cancelAllReservations();
            }
          }
          removeHostCapacity(hostName);
        }
        fi.setBlacklist(rfb, reason, gray);
      }
    }

    /**
     * Check whether tasks can be assigned to the tracker.
     *
     * Faults are stored in a multi-bucket, circular sliding window; when
     * the implicit "time pointer" moves across a bucket boundary into the
     * oldest bucket, that bucket's faults are cleared, and it becomes the
     * newest ("current") bucket.  Thus TRACKER_FAULT_TIMEOUT_WINDOW
     * determines the timeout value for TaskTracker faults (in combination
     * with TRACKER_FAULT_BUCKET_WIDTH), and the sum over all buckets is
     * compared with TRACKER_FAULT_THRESHOLD to determine whether graylisting
     * is warranted (or, alternatively, if it should be lifted).
     *
     * Assumes JobTracker is locked on entry.
     * 
     * @param hostName The tracker name
     * @param now      The current time (milliseconds)
     */
    // 检查是否可以向TaskTracker指派运行Task
    // 当TaskTracker发送Heartbeat标志其没有重启，那么会执行该子流程
    void checkTrackerFaultTimeout(String hostName, long now) {
      synchronized (potentiallyFaultyTrackers) {
        // 获取TaskTracker的Fault information
        FaultInfo fi = potentiallyFaultyTrackers.get(hostName);
        // getFaultCount() auto-rotates the buckets, clearing out the oldest
        // as needed, before summing the faults:
        if (fi != null && fi.getFaultCount(now) < TRACKER_FAULT_THRESHOLD) {
          // 注意下面调用方法中参数 gray的值为true
          unBlacklistTracker(hostName, ReasonForBlackListing.EXCEEDING_FAILURES,
                             true, now);
        }
      }
    }

    private void unBlacklistTracker(String hostName,
                                    ReasonForBlackListing rfb,
                                    boolean gray,
                                    long timeStamp) {
      FaultInfo fi = getFaultInfo(hostName, false);
      if (fi == null) {
        return;
      }
      Set<ReasonForBlackListing> rfbSet = fi.getReasonForBlacklisting(gray);
      boolean listed = gray? fi.isGraylisted() : fi.isBlacklisted();
      if (listed && rfbSet.contains(rfb)) {
        // 从grapRfbMap中移除reason for black Listing(rfb)
        if (fi.removeBlacklistedReason(rfb, gray)) {
          if (fi.getReasonForBlacklisting(gray).isEmpty()) {
            LOG.info("Un" + (gray? "gray" : "black") + "listing tracker : " +
                     hostName);
            if (gray) {
              // 减少graylisted trackers数值, getNumTaskTrackersOnHost方法是获取hostName对应的节点上运行的TaskTracker的个数
              decrGraylistedTrackers(getNumTaskTrackersOnHost(hostName));
            } else {
              addHostCapacity(hostName);
            }
            // 清除grayRfbMap
            fi.unBlacklist(gray);
            // We have unblack/graylisted tracker, so tracker should definitely
            // be healthy. Check fault count; if zero, don't keep it in memory.
            // 如果faultCount(now)==0,则remove the task tracker from the potentially-faulty list
            if (fi.getFaultCount(timeStamp) == 0) {
              potentiallyFaultyTrackers.remove(hostName);
            }
          }
        }
      }
    }

    // Assumes JobTracker is locked on the entry
    private FaultInfo getFaultInfo(String hostName, boolean createIfNecessary) {
      FaultInfo fi = null;
      synchronized (potentiallyFaultyTrackers) {
        fi = potentiallyFaultyTrackers.get(hostName);
        if (fi == null && createIfNecessary) {
          fi = new FaultInfo(clock.getTime(), NUM_FAULT_BUCKETS,
                             TRACKER_FAULT_BUCKET_WIDTH_MSECS);
          potentiallyFaultyTrackers.put(hostName, fi);
        }
      }
      return fi;
    }

    /**
     * Removes the tracker from the blacklist, graylist, and
     * potentially-faulty list, when it is restarted.
     * 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName
     */
    // 标记TaskTracker为Health状态
    // 当TaskTracker重启了，然后再次连接JobTracker时，发送Heartbeat的过程中，会执行该流程.
    // 很有可能TaskTracker重启之前，其上运行Task失败了很多次，在JobTracker端记录该失败计数，当满足一定条件后，会将TaskTracker加入灰名单，如果TaskTracker重启了，应该将其从灰名单中移除，以便不影响任务分派
    void markTrackerHealthy(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        // 从potentiallyFaultyTrackers(潜在有错误的Tracker。当有task运行失败时，就将其加入该队列中)集合中移除host
        FaultInfo fi = potentiallyFaultyTrackers.remove(hostName);
        if (fi != null) {
          // a tracker can be both blacklisted and graylisted, so check both
          // 一个TaskTracker可能同时存在blacklisted和graylisted中
          if (fi.isGraylisted()) {
            LOG.info("Marking " + hostName + " healthy from graylist");
            // 更新numGraylistedTrackers。从getNumTaskTrackersOnHost可以看出，一个Host上可能存在多个TT的可能
            decrGraylistedTrackers(getNumTaskTrackersOnHost(hostName));
          }
          if (fi.isBlacklisted()) {
            LOG.info("Marking " + hostName + " healthy from blacklist");
            // 更新numBlacklistedTrackers等数量以及totalMapTaskCapacity、totalReduceTaskCapacity的数量，见该方法的实现
            addHostCapacity(hostName);
          }
          // no need for fi.unBlacklist() for either one:  fi is already gone
        }
      }
    }

    private void removeHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        // remove the capacity of trackers on this host
        int numTrackersOnHost = 0;
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          int mapSlots = status.getMaxMapSlots();
          totalMapTaskCapacity -= mapSlots;
          int reduceSlots = status.getMaxReduceSlots();
          totalReduceTaskCapacity -= reduceSlots;
          ++numTrackersOnHost;
          getInstrumentation().addBlackListedMapSlots(mapSlots);
          getInstrumentation().addBlackListedReduceSlots(reduceSlots);
        }
        uniqueHostsMap.remove(hostName);
        incrBlacklistedTrackers(numTrackersOnHost);
      }
    }
    
    // This is called when a tracker is restarted or when the health-check
    // script reports it healthy.  (Not called for graylisting.)
    private void addHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        int numTrackersOnHost = 0;
        // add the capacity of trackers on the host
        // 从taskTrackers集合中获取节点hostName上所有的TaskTracker的TaskTrackerStatus
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          int mapSlots = status.getMaxMapSlots();
          totalMapTaskCapacity += mapSlots;
          int reduceSlots = status.getMaxReduceSlots();
          totalReduceTaskCapacity += reduceSlots;
          numTrackersOnHost++;
          getInstrumentation().decBlackListedMapSlots(mapSlots);
          getInstrumentation().decBlackListedReduceSlots(reduceSlots);
        }
        uniqueHostsMap.put(hostName, numTrackersOnHost);
        // 从blacklisted中减去
        decrBlacklistedTrackers(numTrackersOnHost);
      }
    }

    /**
     * Whether a host is blacklisted (by health-check script) across all jobs.
     * 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName - hostname to check
     * @return true if blacklisted
     */
    boolean isBlacklisted(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.isBlacklisted();
        }
      }
      return false;
    }

    /**
     * Whether a host is graylisted (by heuristics) "across all jobs".
     * 
     * Assumes JobTracker is locked on the entry.
     * 
     * @param hostName - hostname to check
     * @return true if graylisted
     */
    boolean isGraylisted(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.isGraylisted();
        }
      }
      return false;
    }

    // Assumes JobTracker is locked on the entry.
    int getFaultCount(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.getFaultCount(clock.getTime());
        }
      }
      return 0;
    }


    // Assumes JobTracker is locked on the entry.
    void setNodeHealthStatus(String hostName, boolean isHealthy, String reason,
                             long timeStamp) {
      FaultInfo fi = null;
      // If TaskTracker node is not healthy, get or create a fault info object
      // and blacklist it.  (This path to blacklisting ultimately comes from
      // the health-check script called in NodeHealthCheckerService; see JIRA
      // MAPREDUCE-211 for details.  We never use graylisting for this path.)
      if (!isHealthy) {
        fi = getFaultInfo(hostName, true);
        synchronized (potentiallyFaultyTrackers) {
          blacklistTracker(hostName, reason,
                           ReasonForBlackListing.NODE_UNHEALTHY, false);
        }
      } else {
        if ((fi = getFaultInfo(hostName, false)) != null) {
          unBlacklistTracker(hostName, ReasonForBlackListing.NODE_UNHEALTHY,
                             false, timeStamp);
        }
      }
    }
  }

  /**
   * Get all task tracker statuses on given host
   * 
   * Assumes JobTracker is locked on the entry
   * @param hostName
   * @return {@link java.util.List} of {@link TaskTrackerStatus}
   */
  private List<TaskTrackerStatus> getStatusesOnHost(String hostName) {
    List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus(); 
        if (hostName.equals(status.getHost())) {
          statuses.add(status);
        }
      }
    }
    return statuses;
  }

  /**
   * Get total number of task trackers on given host
   * 
   * Assumes JobTracker is locked on the entry
   * @param hostName
   * @return number of task trackers running on given host
   */
  private int getNumTaskTrackersOnHost(String hostName) {
    int numTrackers = 0;
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus(); 
        if (hostName.equals(status.getHost())) {
          ++numTrackers;
        }
      }
    }
    return numTrackers;
  }

  ///////////////////////////////////////////////////////
  // Used to recover the jobs upon restart
  ///////////////////////////////////////////////////////
  class RecoveryManager {
    Set<JobID> jobsToRecover; // set of jobs to be recovered
    
    private int totalEventsRecovered = 0;
    private int restartCount = 0;
    private boolean shouldRecover = false;

    Set<String> recoveredTrackers = 
      Collections.synchronizedSet(new HashSet<String>());
    
    /** A custom listener that replays the events in the order in which the 
     * events (task attempts) occurred. 
     */
    
    public RecoveryManager() {
      jobsToRecover = new TreeSet<JobID>();
    }

    public boolean contains(JobID id) {
      return jobsToRecover.contains(id);
    }

    void addJobForRecovery(JobID id) {
      jobsToRecover.add(id);
    }

    public boolean shouldRecover() {
      return shouldRecover;
    }

    public boolean shouldSchedule() {
      return recoveredTrackers.isEmpty();
    }

    private void markTracker(String trackerName) {
      recoveredTrackers.add(trackerName);
    }

    void unMarkTracker(String trackerName) {
      recoveredTrackers.remove(trackerName);
    }

    Set<JobID> getJobsToRecover() {
      return jobsToRecover;
    }

    /** Check if the given string represents a job-id or not 
     */
    private boolean isJobNameValid(String str) {
      if(str == null) {
        return false;
      }
      String[] parts = str.split("_");
      if(parts.length == 3) {
        if(parts[0].equals("job")) {
            // other 2 parts should be parseable
            return JobTracker.validateIdentifier(parts[1])
                   && JobTracker.validateJobNumber(parts[2]);
        }
      }
      return false;
    }
    
    // checks if the job dir has the required files
    public void checkAndAddJob(FileStatus status) throws IOException {
      // 前提：job默认是可恢复的，我们提交job的时候在submitjob方法中会把每个job的信息放在${mapred.system.dir}/jobID/job-info 和${mapred.system.dir}/jobID/jobToken 文件中，作用就是用来恢复失败的job
      String fileName = status.getPath().getName();
      // 根据文件名是否是jobID以及在mapred.system.dir/jobID/下是否存在job-info 和 jobToken信息文件，都存在，则将该jobID添加到recoveryManager队列中，并设置shouldRecover = true，表示需要进行作业恢复。
      if (isJobNameValid(fileName) && isJobDirValid(JobID.forName(fileName))) {
        // 将需要恢复的JobID加入到jobsToRecover集合中
        recoveryManager.addJobForRecovery(JobID.forName(fileName));
        shouldRecover = true; // enable actual recovery if num-files > 1
      }
    }
    
    private boolean isJobDirValid(JobID jobId) throws IOException {
      boolean ret = false;
      // ${mapred.system.dir}/jobID/job-info
      Path jobInfoFile = getSystemFileForJob(jobId);
      // ${mapred.system.dir}/jobID/jobToken
      final Path jobTokenFile = getTokenFileForJob(jobId);
      JobConf job = new JobConf();
      // 判断在mapred.system.dir/jobID/下是否存在job-info 和 jobToken信息文件
      if (jobTokenFile.getFileSystem(job).exists(jobTokenFile)
          && jobInfoFile.getFileSystem(job).exists(jobInfoFile)) {
        ret = true;
      } else {
        LOG.warn("Job " + jobId
            + " does not have valid info/token file so ignoring for recovery");
      }
      return ret;
    }
  
    Path getRestartCountFile() {
      return new Path(getSystemDir(), "jobtracker.info");
    }

    Path getTempRestartCountFile() {
      return new Path(getSystemDir(), "jobtracker.info.recover");
    }

    /**
     * Initialize the recovery process. It simply creates a jobtracker.info file
     * in the jobtracker's system directory and writes its restart count in it.
     * For the first start, the jobtracker writes '0' in it. Upon subsequent 
     * restarts the jobtracker replaces the count with its current count which 
     * is (old count + 1). The whole purpose of this api is to obtain restart 
     * counts across restarts to avoid attempt-id clashes.
     * 
     * Note that in between if the jobtracker.info files goes missing then the
     * jobtracker will disable recovery and continue. 
     *  
     */
    void updateRestartCount() throws IOException {
      Path restartFile = getRestartCountFile();
      Path tmpRestartFile = getTempRestartCountFile();
      FsPermission filePerm = new FsPermission(SYSTEM_FILE_PERMISSION);

      // read the count from the jobtracker info file
      if (fs.exists(restartFile)) {
        fs.delete(tmpRestartFile, false); // delete the tmp file
      } else if (fs.exists(tmpRestartFile)) {
        // if .rec exists then delete the main file and rename the .rec to main
        fs.rename(tmpRestartFile, restartFile); // rename .rec to main file
      } else {
        // For the very first time the jobtracker will create a jobtracker.info
        // file.
        // enable recovery if this is a restart
        shouldRecover = true;

        // write the jobtracker.info file
        try {
          FSDataOutputStream out = FileSystem.create(fs, restartFile, 
                                                     filePerm);
          out.writeInt(0);
          out.close();
        } catch (IOException ioe) {
          LOG.warn("Writing to file " + restartFile + " failed!");
          LOG.warn("FileSystem is not ready yet!");
          fs.delete(restartFile, false);
          throw ioe;
        }
        return;
      }

      FSDataInputStream in = fs.open(restartFile);
      try {
        // read the old count
        restartCount = in.readInt();
        ++restartCount; // increment the restart count
      } catch (IOException ioe) {
        LOG.warn("System directory is garbled. Failed to read file " 
                 + restartFile);
        LOG.warn("Jobtracker recovery is not possible with garbled"
                 + " system directory! Please delete the system directory and"
                 + " restart the jobtracker. Note that deleting the system" 
                 + " directory will result in loss of all the running jobs.");
        throw new RuntimeException(ioe);
      } finally {
        if (in != null) {
          in.close();
        }
      }

      // Write back the new restart count and rename the old info file
      //TODO This is similar to jobhistory recovery, maybe this common code
      //      can be factored out.
      
      // write to the tmp file
      FSDataOutputStream out = FileSystem.create(fs, tmpRestartFile, filePerm);
      out.writeInt(restartCount);
      out.close();

      // delete the main file
      fs.delete(restartFile, false);
      
      // rename the .rec to main file
      fs.rename(tmpRestartFile, restartFile);
    }

    public void recover() {
      int recovered = 0;
      long recoveryProcessStartTime = clock.getTime();
      if (!shouldRecover()) {
        // 如果是false即不恢复，会清除jobsToRecover中的内容
        // clean up jobs structure
        jobsToRecover.clear();
        return;
      }

      LOG.info("Starting the recovery process for " + jobsToRecover.size()
          + " jobs ...");
      // jobsToRecover队列是在jobTracker初始化时添加的(初始化recoveryManager 调用构造方法的时候会创建)，对其中每个作业进行恢复。
      // 主要是读取job信息，然后调用JobTracker.submitJob(JobID.downgrade(token.getJobID()), token .getJobSubmitDir().toString(), ugi, ts, true)进行作业再次提交。
      for (JobID jobId : jobsToRecover) {
        LOG.info("Submitting job " + jobId);
        try {
          Path jobInfoFile = getSystemFileForJob(jobId);
          final Path jobTokenFile = getTokenFileForJob(jobId);
          FSDataInputStream in = fs.open(jobInfoFile);
          final JobInfo token = new JobInfo();
          token.readFields(in);
          in.close();
          
          // Read tokens as JT user
          JobConf job = new JobConf();
          final Credentials ts = 
              (jobTokenFile.getFileSystem(job).exists(jobTokenFile)) ?
            Credentials.readTokenStorageFile(jobTokenFile, job) : null;

          // Re-submit job
          // 再次提交job
          final UserGroupInformation ugi = UserGroupInformation
              .createRemoteUser(token.getUser().toString());
          JobStatus status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
            public JobStatus run() throws IOException, InterruptedException {
              return submitJob(JobID.downgrade(token.getJobID()), token
                  .getJobSubmitDir().toString(), ugi, ts, true);
            }
          });
          if (status == null) {
            LOG.info("Job " + jobId + " was not recovered since it " +
              "disabled recovery on restart (" + JobConf.MAPREDUCE_RECOVER_JOB +
              " set to 'false').");
          } else {
            recovered++;
          }
        } catch (Exception e) {
          LOG.warn("Could not recover job " + jobId, e);
        }
      }
      recoveryDuration = clock.getTime() - recoveryProcessStartTime;
      hasRecovered = true;

      LOG.info("Recovery done! Recoverd " + recovered + " of "
          + jobsToRecover.size() + " jobs.");
      LOG.info("Recovery Duration (ms):" + recoveryDuration);
    }
    
    int totalEventsRecovered() {
      return totalEventsRecovered;
    }
  }

  private JobTrackerInstrumentation myInstrumentation;

  private void createInstrumentation() {
    // Initialize instrumentation
    JobTrackerInstrumentation tmp;
    Class<? extends JobTrackerInstrumentation> metricsInst =
        getInstrumentationClass(conf);
    LOG.debug("instrumentation class="+ metricsInst);
    // 如果没有设置mapred.jobtracker.instrumentation, 返回JobTrackerMetricsSource实例
    if (metricsInst == null) {
      myInstrumentation = JobTrackerInstrumentation.create(this, conf);
      return;
    }
    try {
      java.lang.reflect.Constructor<? extends JobTrackerInstrumentation> c =
          metricsInst.getConstructor(new Class<?>[]{JobTracker.class,
          JobConf.class});
      tmp = c.newInstance(this, conf);
    } catch (Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by
      //falling back on the default.
      LOG.error("failed to initialize job tracker metrics", e);
      tmp = JobTrackerInstrumentation.create(this, conf);
    }
    myInstrumentation = tmp;
  }
    
  /////////////////////////////////////////////////////////////////
  // The real JobTracker
  ////////////////////////////////////////////////////////////////
  int port;
  String localMachine;
  // 格式为yyyyMMddHHmm的时间字符串
  private String trackerIdentifier;
  long startTime;
  int totalSubmissions = 0;
  // 该TaskTracker上最大Map Slot总数
  private int totalMapTaskCapacity;
  // 该TaskTracker上最大Reduce Slot总数
  private int totalReduceTaskCapacity;
  private HostsFileReader hostsReader;
  
  // JobTracker recovery variables
  private volatile boolean hasRestarted = false;
  private volatile boolean hasRecovered = false;
  private volatile long recoveryDuration;

  //
  // Properties to maintain while running Jobs and Tasks:
  //
  // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its 
  //     Tasks are complete.
  //
  // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
  //
  // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
  //
  // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
  //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
  //     reexecution if all downstream Tasks that require its output have already obtained
  //     the necessary files.
  //

  // All the known jobs.  (jobid->JobInProgress)
  // JobTracker维护一个JobID->JobInProgress映射的列表，JobID标识一个提交的Job，JobInProgress是JobTracker端维护的Job的所有信息的数据结构。在如下情况下，会检索/操作该jobs数据结构：

  // * JobClient提交Job的时候，会创建JobInProgress，并加入到jobs集合中
  // * JobClient远程调用Kill掉指定Job的时候，会根据JobID从jobs中获取JobInProgress信息，并Kill掉该Job，更新状态信息
  // * JobClient查询当前运行的所有Job信息时，会检索jobs列表
  // * 在JobTracker端检索一个Job所维护的Task信息时，会根据JobInProgress所维护的数据结构获取到对应的Task的信息TaskInProgress
  // * Job运行状态不为RUNNING，并且也不为PREP，并且完成时间早于当前时间，会将Job从jobs列表删除
  // * JobTracker解析接收到的TaskTracker发送的心跳的过程中，会检索并更新jobs列表中的Job信息，找到可以分配给该TaskTracker的属于满足条件的Job所包含的Task
  Map<JobID, JobInProgress> jobs =  
    Collections.synchronizedMap(new TreeMap<JobID, JobInProgress>());

  // (user -> list of JobInProgress)
  // 用来跟踪某个用户提交的需要运行的Job集合的数据结构。
  // 当Job完成（success/failure/killed）后，会在JobTracker内存中保存一些Job，这些Job属于哪些用户的。
  // 默认情况下会保存MAX_COMPLETE_USER_JOBS_IN_MEMORY=100个用户的已完成的Job，当超过该值时，会清理掉最早的用户以及对应的完成的Job信息。
  // 可以通过配置项mapred.jobtracker.completeuserjobs.maximum来设置该值。
  TreeMap<String, ArrayList<JobInProgress>> userToJobsMap =
    new TreeMap<String, ArrayList<JobInProgress>>();
    
  // (TaskTracker ID --> list of jobs to cleanup)
  // 用来跟踪某个TaskTracker上运行的Job集合的数据结构。
  // 当一个Job已经运行完成，TaskTracker需要知道哪些运行在该节点上的Job已经完成，并等待通知进行清理，这时会在JobTracker端检索该Map，取出该TaskTracker对应的需要进行清理的Job的集合。
  // 另外，还有一种情况，当JobTracker一段时间内没有收到TaskTracker发送的心跳报告，这时会将该TaskTracker对应的Job集合从trackerToJobsToCleanup中删除，
  // 后续会重新调度这些运行在该有问题的TaskTracker上的Task（这些Task属于某些Job，JobTracker分配任务的单位是Task）。
  Map<String, Set<JobID>> trackerToJobsToCleanup = 
    new HashMap<String, Set<JobID>>();
  
  // (TaskTracker ID --> list of tasks to cleanup)
  // 用来跟踪某个TaskTracker上运行的Task集合的数据结构。
  // 当Job运行完成（成功或者失败）后，一个TaskTracker需要知道属于该Job的哪些Task运行在该TaskTracker上，需要对这些Task进行清理。
  // JobTracker端会查询出这类Task，并通过心跳的响应，向对应的TaskTracker发送KillTaskAction指令，通知TaskTracker清理这些Task运行时生成的临时文件等。
  Map<String, Set<TaskAttemptID>> trackerToTasksToCleanup = 
    new HashMap<String, Set<TaskAttemptID>>();
  
  // All the known TaskInProgress items, mapped to by taskids ( TAID(TaskAttemptID) --> TaskInProgress(TIP) )
  // TaskAttemptID用来标识一个MapTask或一个ReduceTask，通过该数据结构可以根据TaskAttemptID获取到MapTask/ReduceTask的运行信息，也就是TaskInProgress对象。
  // 当需要检索MapTask/ReduceTask，或者对JobTracker端所维护的该Task的状态信息进行更新的时候，需要通过该数据结构获取到。
  Map<TaskAttemptID, TaskInProgress> taskidToTIPMap =
    new TreeMap<TaskAttemptID, TaskInProgress>();
  // This is used to keep track of all trackers running on one host. While
  // decommissioning the host, all the trackers on the host will be lost.
  // (TaskTracker HOST --> TaskTracker)
  // 一台主机上，可能运行着多个TaskTracker进程，该数据结构用来维护host到TaskTracker集合的映射关系。
  // 如果一个host被加入了黑名单，则该host上面的所有TaskTracker都无法接收任务。
  Map<String, Set<TaskTracker>> hostnameToTaskTracker = 
    Collections.synchronizedMap(new TreeMap<String, Set<TaskTracker>>());
  

  // ( TAID(TaskAttemptID) --> TaskTracker ID )
  // 维护TaskAttemptID到TaskTracker的映射关系，可以通过一个Task的ID获取到该Task运行在哪个TaskTracker上。
  TreeMap<TaskAttemptID, String> taskidToTrackerMap = new TreeMap<TaskAttemptID, String>();

  // ( TaskTracker ID --> TreeSet of taskids running at that tracker)
  // 某个TaskTracker上都运行着哪些Task，通过该数据结构来维护这种映射关系。
  TreeMap<String, Set<TaskAttemptID>> trackerToTaskMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // ( TaskTracker ID -> TreeSet of completed taskids running at that tracker)
  // 在某个TaskTracker上都运行完成了哪些Task，通过该数据结构来维护这种映射关系。
  TreeMap<String, Set<TaskAttemptID>> trackerToMarkedTasksMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // ( TaskTracker ID --> last sent HeartBeatResponse)
  // TaskTracker会周期性地向JobTracker发送心跳报告，最近一次发送的心跳报告，JobTracker会给其一个响应，最后的这个响应的数据保存在该数据结构中。
  Map<String, HeartbeatResponse> trackerToHeartbeatResponseMap = 
    new TreeMap<String, HeartbeatResponse>();

  // ( task tracker hostname --> Node (NetworkTopology))
  // JobTracker维护了一个网络拓扑结构（NetworkTopology），组成该拓扑结构的是一个一个的Node，每个Node都包含了网络位置信息、继承关系信息、名称等。
  // 每个TaskTracker都是整个Hadoop集群的一个节点，通过该数据结构维护了TaskTracker在集群拓扑结构中相关信息。
  // 比如，根据给定TaskTracker ID，从hostnameToNodeMap中检索出其对应的Node信息，在调度一个Job的MapTask运行时（MapTask运行具有Locality特性），
  // 可以基于local、rack-local、off-switch的顺序优先选择前面的Node运行该MapTask。
  Map<String, Node> hostnameToNodeMap = 
    Collections.synchronizedMap(new TreeMap<String, Node>());
  
  // Number of resolved entries
  int numResolved;

  // statistics about TaskTrackers with faults; may lead to graylisting
  private FaultyTrackersInfo faultyTrackers = new FaultyTrackersInfo();
  
  private JobTrackerStatistics statistics = 
    new JobTrackerStatistics();
  //
  // Watch and expire TaskTracker objects using these structures.
  // We can map from Name->TaskTrackerStatus, or we can expire by time.
  //
  // 全局计数器：MapTask总数
  int totalMaps = 0;
  // 全局计数器： ReduceTask总数
  int totalReduces = 0;
  // 全局计数器： 占用的Map Slot总数
  private int occupiedMapSlots = 0;
  // 全局计数器： 占用的Reduce Slot总数
  private int occupiedReduceSlots = 0;
  private int reservedMapSlots = 0;
  private int reservedReduceSlots = 0;
  // 在updateTaskTrackerStatus方法中会初始化taskTrackers：taskTrackers.put(trackerName, taskTracker);
  private HashMap<String, TaskTracker> taskTrackers =
    new HashMap<String, TaskTracker>();
  // host --> host上TaskTracker的数量
  Map<String,Integer>uniqueHostsMap = new ConcurrentHashMap<String, Integer>();
  ExpireTrackers expireTrackers = new ExpireTrackers();
  Thread expireTrackersThread = null;
  RetireJobs retireJobs = new RetireJobs();
  Thread retireJobsThread = null;
  final int retiredJobsCacheSize;
  ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
  Thread expireLaunchingTaskThread = new Thread(expireLaunchingTasks,
                                                "expireLaunchingTasks");

  CompletedJobStatusStore completedJobStatusStore = null;
  Thread completedJobsStoreThread = null;
  RecoveryManager recoveryManager;
  JobHistoryServer jobHistoryServer;

  /**
   * It might seem like a bug to maintain a TreeSet of tasktracker objects,
   * which can be updated at any time.  But that's not what happens!  We
   * only update status objects in the taskTrackers table.  Status objects
   * are never updated once they enter the expiry queue.  Instead, we wait
   * for them to expire and remove them from the expiry queue.  If a status
   * object has been updated in the taskTracker table, the latest status is 
   * reinserted.  Otherwise, we assume the tracker has expired.
   */
  // TaskTracker汇报心跳后，JobTracker会将他放到过期队列：trackerExpiryQueue中
  TreeSet<TaskTrackerStatus> trackerExpiryQueue =
    new TreeSet<TaskTrackerStatus>(
                                   new Comparator<TaskTrackerStatus>() {
                                     public int compare(TaskTrackerStatus p1, TaskTrackerStatus p2) {
                                       if (p1.getLastSeen() < p2.getLastSeen()) {
                                         return -1;
                                       } else if (p1.getLastSeen() > p2.getLastSeen()) {
                                         return 1;
                                       } else {
                                         return (p1.getTrackerName().compareTo(p2.getTrackerName()));
                                       }
                                     }
                                   }
                                   );

  // Used to provide an HTML view on Job, Task, and TaskTracker structures
  final HttpServer infoServer;
  int infoPort;

  Server interTrackerServer;

  // Some jobs are stored in a local system directory.  We can delete
  // the files when we're done with the job.
  static final String SUBDIR = "jobTracker";
  final LocalFileSystem localFs;
  FileSystem fs = null;
  Path systemDir = null;
  JobConf conf;

  private final ACLsManager aclsManager;

  long limitMaxMemForMapTasks;
  long limitMaxMemForReduceTasks;
  long memSizeForMapSlotOnJT;
  long memSizeForReduceSlotOnJT;

  private QueueManager queueManager;

  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf) throws IOException, InterruptedException {
    this(conf, generateNewIdentifier());
  }

  JobTracker(JobConf conf, QueueManager qm) 
  throws IOException, InterruptedException {
    this(conf, generateNewIdentifier(), new Clock(), qm);
  }

  JobTracker(JobConf conf, Clock clock) 
  throws IOException, InterruptedException {
    this(conf, generateNewIdentifier(), clock);
  }
  
  public static final String JT_USER_NAME = "mapreduce.jobtracker.kerberos.principal";
  public static final String JT_KEYTAB_FILE =
    "mapreduce.jobtracker.keytab.file";
  
  JobTracker(final JobConf conf, String identifier) 
  throws IOException, InterruptedException {   
    this(conf, identifier, new Clock());
  }

  JobTracker(final JobConf conf, String identifier, Clock clock) 
  throws IOException, InterruptedException { 
    // 调用QueueManager的构造方法实例化QueueManager, QueueManager中会实例化队列,如果用户没有配置队列信息的话默认初始化一个名字为default的队列。
    // 继续调用JobTracker的构造方法（里面包括各种初始化，包括TaskScheduler的初始化）
    this(conf, identifier, clock, new QueueManager(new Configuration(conf)));
  } 
  
  private void initJTConf(JobConf conf) {
    if (conf.getBoolean(
        DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, false)) {
      LOG.warn(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY + 
          " is enabled, disabling it");
      conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, false);
    }
  }
 
  @InterfaceAudience.Private
  void initializeFilesystem() throws IOException, InterruptedException {
    // Connect to HDFS NameNode
    while (!Thread.currentThread().isInterrupted() && fs == null) {
      try {
        fs = getMROwner().doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException {
            return FileSystem.get(conf);
          }});
      } catch (IOException ie) {
        fs = null;
        LOG.info("Problem connecting to HDFS Namenode... re-trying", ie);
        Thread.sleep(FS_ACCESS_RETRY_PERIOD);
      }
    }
    
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    
    // Ensure HDFS is healthy
    if ("hdfs".equalsIgnoreCase(fs.getUri().getScheme())) {
      while (!DistributedFileSystem.isHealthy(fs.getUri())) {
        LOG.info("HDFS initialized but not 'healthy' yet, waiting...");
        Thread.sleep(FS_ACCESS_RETRY_PERIOD);
      }
    }
  }
  
  @InterfaceAudience.Private
  void initialize() 
      throws IOException, InterruptedException {
    // initialize history parameters.
    final JobTracker jtFinal = this;

    // 下面是初始化JobHistory
    getMROwner().doAs(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        JobHistory.init(jtFinal, conf, jtFinal.localMachine, 
            jtFinal.startTime);
        return true;
      }
    });

    // start the recovery manager
    // 初始化recoveryManager，用于作业恢复管理，即JobTracker启动时，恢复上次停止时正在运行的作业，并恢复各个任务的运行状态
    recoveryManager = new RecoveryManager();
    
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // if we haven't contacted the namenode go ahead and do it
        // clean up the system dir, which will only work if hdfs is out of 
        // safe mode
        if(systemDir == null) {
          // mapred.system.dir为mapreduce共享目录，不能为本地目录，只能为HDFS目录，可以填 写相对目录如：mapred-system-dir，假设以hadp登录系统，并启动hadoop
          // $ ./hadoop fs -ls /user/hadp  /user/hadp/mapred-system-dir
          systemDir = new Path(getSystemDir());    
        }
        try {
          FileStatus systemDirStatus = fs.getFileStatus(systemDir);
          if (!systemDirStatus.getOwner().equals(
              getMROwner().getShortUserName())) {
            throw new AccessControlException("The systemdir " + systemDir +
                " is not owned by " + getMROwner().getShortUserName());
          }
          if (!systemDirStatus.getPermission().equals(SYSTEM_DIR_PERMISSION)) {
            LOG.warn("Incorrect permissions on " + systemDir +
                ". Setting it to " + SYSTEM_DIR_PERMISSION);
            fs.setPermission(systemDir,new FsPermission(SYSTEM_DIR_PERMISSION));
          }
        } catch (FileNotFoundException fnf) {} //ignore
        // Make sure that the backup data is preserved
        // 列出mapred.system.dir参数指定的目录下所有的文件信息
        FileStatus[] systemDirData = fs.listStatus(this.systemDir);
        // Check if the history is enabled .. as we cant have persistence with 
        // history disabled
        // 如果mapred.jobtracker.restart.recover=true，默认是false，且systemDir下存在文件时，调用recoveryManager.checkAndAddJob(status)判断是否需要进行作业恢复
        if (conf.getBoolean("mapred.jobtracker.restart.recover", false) 
            && systemDirData != null) {
          for (FileStatus status : systemDirData) {
            try {
              // 判断是否需要进行作业恢复, 见方法具体实现
              recoveryManager.checkAndAddJob(status);
            } catch (Throwable t) {
              LOG.warn("Failed to add the job " + status.getPath().getName(), 
                       t);
            }
          }
          
          // Check if there are jobs to be recovered
          // 判断是否需要进行作业恢复，hasRestarted = shouldRecover， 而shouldRecover有上面的recoveryManager.checkAndAddJob(status);决定。
          // 需要则直接退出（这里还没有开始作业恢复，现在只是在做初始化相关的工作），不需要则会执行删除systemDir命令，并重新创建systemDir目录
          hasRestarted = recoveryManager.shouldRecover();
          if (hasRestarted) {
            break; // if there is something to recover else clean the sys dir
          }
        }
        LOG.info("Cleaning up the system directory");
        // 删除systemDir
        fs.delete(systemDir, true);
        // 新创建systemDir目录
        if (FileSystem.mkdirs(fs, systemDir, 
            new FsPermission(SYSTEM_DIR_PERMISSION))) {
          break;
        }
        LOG.error("Mkdirs failed to create " + systemDir);
      } catch (AccessControlException ace) {
        LOG.warn("Failed to operate on mapred.system.dir (" + systemDir 
                 + ") because of permissions.");
        LOG.warn("Manually delete the mapred.system.dir (" + systemDir 
                 + ") and then start the JobTracker.");
        LOG.warn("Bailing out ... ", ace);
        throw ace;
      } catch (IOException ie) {
        LOG.info("problem cleaning system directory: " + systemDir, ie);
      }
      Thread.sleep(FS_ACCESS_RETRY_PERIOD);
    }
    
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    
    // Same with 'localDir' except it's always on the local disk.
    // 之后根据是否需要进行作业恢复，判断是否清除localDir目录，需要进行恢复则不清楚。
    if (!hasRestarted) {
      conf.deleteLocalFiles(SUBDIR);
    }

    // 接下来是一些关于JobHistory和infoServer的操作，同时会启动调用jobHistoryServer = new JobHistoryServer(conf, aclsManager, infoServer);
    // jobHistoryServer.start()，启动jobHistoryServer用于查看作业历史信息。

    // Initialize history DONE folder
    FileSystem historyFS = getMROwner().doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws IOException {
        JobHistory.initDone(conf, fs);
        final String historyLogDir = 
          JobHistory.getCompletedJobHistoryLocation().toString();
        infoServer.setAttribute("historyLogDir", historyLogDir);

        infoServer.setAttribute
          ("serialNumberDirectoryDigits",
           Integer.valueOf(JobHistory.serialNumberDirectoryDigits()));

        infoServer.setAttribute
          ("serialNumberTotalDigits",
           Integer.valueOf(JobHistory.serialNumberTotalDigits()));
        
        return new Path(historyLogDir).getFileSystem(conf);
      }
    });
    infoServer.setAttribute("fileSys", historyFS);
    infoServer.setAttribute("jobConf", conf);
    infoServer.setAttribute("aclManager", aclsManager);

    if (JobHistoryServer.isEmbedded(conf)) {
      LOG.info("History server being initialized in embedded mode");
      // 用于查看作业历史信息的Server
      jobHistoryServer = new JobHistoryServer(conf, aclsManager, infoServer);
      // 启动jobHistoryServer
      jobHistoryServer.start();
      LOG.info("Job History Server web address: " + JobHistoryServer.getAddress(conf));
    }

    //initializes the job status store
    // 新建completedJobStatusStore线程，用于将已完成的作业信息保存到hdfs中。
    // 具体是在一个job完成之后调用completedJobStatusStore的store()方法将job信息保存到hdfs中，而该线程的run()方法是清除掉已保存到hdfs的job信息文件，
    // 该功能的启用由mapred.job.tracker.persist.jobstatus.active参数决定，默认是关闭的。
    completedJobStatusStore = new CompletedJobStatusStore(conf, aclsManager);
    
    // Setup HDFS monitoring
    if (this.conf.getBoolean(
        JT_HDFS_MONITOR_ENABLE, DEFAULT_JT_HDFS_MONITOR_THREAD_ENABLE)) {
      // hdfsMonitor用于监控hdfs是否正常，默认关闭该功能。
      // 到这里initialize()方法完成了。回到offerService()方法
      hdfsMonitor = new HDFSMonitorThread(this.conf, this, this.fs);
      hdfsMonitor.start();
    }
  }
  
  JobTracker(final JobConf conf, String identifier, Clock clock, QueueManager qm) 
  throws IOException, InterruptedException {

    // dfs.client.retry.policy.enabled设置为false
    initJTConf(conf);
    
    this.queueManager = qm;
    this.clock = clock;
    // Set ports, start RPC servers, setup security policy etc.
    // 这句是获得配置文件mapred-site.xml中的mapred.job.tracker配置项的值 ip:端口号
    // 并根据该值新建一个InetSocketAddress对象
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();
    // find the owner of the process
    // get the desired principal to load
    UserGroupInformation.setConfiguration(conf);
    // 这句有点像是使用ssh登陆jobTracker所在主机，这里应该就是在搭建hadoop集群时需要设置ssh无密码访问的原因。
    SecurityUtil.login(conf, JT_KEYTAB_FILE, JT_USER_NAME, localMachine);

    long secretKeyInterval = 
    conf.getLong(DELEGATION_KEY_UPDATE_INTERVAL_KEY, 
                   DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
      conf.getLong(DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                   DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
      conf.getLong(DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 
                   DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    // 新建一个MapReduce安全管理相关的类
    secretManager =
      new DelegationTokenSecretManager(secretKeyInterval,
                                       tokenMaxLifetime,
                                       tokenRenewInterval,
                                       DELEGATION_TOKEN_GC_INTERVAL);
    // 以后台线程的方法启动secretManager
    secretManager.startThreads();
       
    MAX_JOBCONF_SIZE = conf.getLong(MAX_USER_JOBCONF_SIZE_KEY, MAX_JOBCONF_SIZE);
    //
    // Grab some static constants
    // 作业终止间隔
    TASKTRACKER_EXPIRY_INTERVAL = 
      conf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);
    // 已经完成的作业信息在内存中的最长保留时常，默认24小时，超过该时间会被移入过期队列
    RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
    // 对已经完成的作业信息进行清除的线程检查间隔，默认1分钟
    RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
    // 过期队列中保留的已完成作业信息数量，默认是1000,超过该值，将会从内存中彻底删除
    retiredJobsCacheSize =
             conf.getInt("mapred.job.tracker.retiredjobs.cache.size", 1000);
    // userToJobsMap中每个用户可以保存的作业信息数量，默认值为100, 超过该值后最早的已完成的作业信息会被移入过期队列，该条件和上面的RETIRE_JOB_INTERVAL只要有一个满足就会被移入过期队列
    MAX_COMPLETE_USER_JOBS_IN_MEMORY = conf.getInt("mapred.jobtracker.completeuserjobs.maximum", 100);

    // values related to heuristic graylisting (a "fault" is a per-job
    // blacklisting; too many faults => node is graylisted across all jobs):
    // 默认是3小时，时间窗口，计算该时间内失败的task个数
    TRACKER_FAULT_TIMEOUT_WINDOW =  // 3 hours
      conf.getInt("mapred.jobtracker.blacklist.fault-timeout-window", 3 * 60);
    TRACKER_FAULT_BUCKET_WIDTH =    // 15 minutes
      conf.getInt("mapred.jobtracker.blacklist.fault-bucket-width", 15);
    TRACKER_FAULT_THRESHOLD =
      conf.getInt("mapred.max.tracker.blacklists", 4);
      // future:  rename to "mapred.jobtracker.blacklist.fault-threshold" for
      // namespace consistency

    if (TRACKER_FAULT_BUCKET_WIDTH > TRACKER_FAULT_TIMEOUT_WINDOW) {
      TRACKER_FAULT_BUCKET_WIDTH = TRACKER_FAULT_TIMEOUT_WINDOW;
    }
    TRACKER_FAULT_BUCKET_WIDTH_MSECS =
      (long)TRACKER_FAULT_BUCKET_WIDTH * 60 * 1000;

    // ideally, TRACKER_FAULT_TIMEOUT_WINDOW should be an integral multiple of
    // TRACKER_FAULT_BUCKET_WIDTH, but round up just in case:
    NUM_FAULT_BUCKETS =
      (TRACKER_FAULT_TIMEOUT_WINDOW + TRACKER_FAULT_BUCKET_WIDTH - 1) /
      TRACKER_FAULT_BUCKET_WIDTH;

    NUM_HEARTBEATS_IN_SECOND = 
      conf.getInt(JT_HEARTBEATS_IN_SECOND, DEFAULT_NUM_HEARTBEATS_IN_SECOND);
    if (NUM_HEARTBEATS_IN_SECOND < MIN_NUM_HEARTBEATS_IN_SECOND) {
      NUM_HEARTBEATS_IN_SECOND = DEFAULT_NUM_HEARTBEATS_IN_SECOND;
    }
    
    HEARTBEATS_SCALING_FACTOR = 
      conf.getFloat(JT_HEARTBEATS_SCALING_FACTOR, 
                    DEFAULT_HEARTBEATS_SCALING_FACTOR);
    if (HEARTBEATS_SCALING_FACTOR < MIN_HEARTBEATS_SCALING_FACTOR) {
      HEARTBEATS_SCALING_FACTOR = DEFAULT_HEARTBEATS_SCALING_FACTOR;
    }

    // This configuration is there solely for tuning purposes and
    // once this feature has been tested in real clusters and an appropriate
    // value for the threshold has been found, this config might be taken out.
    AVERAGE_BLACKLIST_THRESHOLD =
      conf.getFloat("mapred.cluster.average.blacklist.threshold", 0.5f);

    // This is a directory of temporary submission files.  We delete it
    // on startup, and can delete any files that we're done with
    this.conf = conf;
    JobConf jobConf = new JobConf(conf);

    // 初始化一些有关内存的参数值
    initializeTaskMemoryRelatedConfig();

    // Read the hosts/exclude files to restrict access to the jobtracker.
    // 涉及到两个参数：mapred.hosts和mapred.hosts.exclude，参考mapred-default.xml关于这两个参数的描述，
    // 大致意思是这两个参数决定哪些node能够访问jobTracker以及哪些node不能访问jobTracker，null则无任何限制，
    // 这是一种安全策略，可以防止非法机器访问jobTracker
    this.hostsReader = new HostsFileReader(conf.get("mapred.hosts", ""),
                                           conf.get("mapred.hosts.exclude", ""));

    // 新建一个作业级别和队列级别的管理和访问权限控制对象，所以将queueManager对象传递给aclsManager，同时new一个JobACLsManager对象，
    // queueManager负责队列级别的管理和访问权限控制，而JobACLsManager对象负责作业级别的管理和访问权限控制。
    // 作业级别权限包括VIEW_JOB和MODIFY_JOB，队列级别的权限包括ADMINISTER_JOBS和SUBMIT_JOB
    aclsManager = new ACLsManager(conf, new JobACLsManager(conf), queueManager);

    LOG.info("Starting jobtracker with owner as " +
        getMROwner().getShortUserName());

    // Create network topology 构建网络拓扑结构
    clusterMap = (NetworkTopology) ReflectionUtils.newInstance(
            conf.getClass("net.topology.impl", NetworkTopology.class,
                NetworkTopology.class), conf);

    // Create the taskScheduler
    // 实例化taskScheduler, 具体使用的任务调度方案则是由mapred.jobtracker.taskScheduler设置，该参数在mapred-site.xml中配置，用户可配置自己的属性值来覆盖默认的调度策略。
    // 默认是使用JobQueueTaskScheduler调度器，也就是遵循FIFO原则的作业调度器。同时hadoop还自带了FairScheduler和CapacityScheduler两个调度器
    // TaskScheduler的实现类的构造方法中都有一个设置JobListener的初始化操作
    Class<? extends TaskScheduler> schedulerClass
      = conf.getClass("mapred.jobtracker.taskScheduler",
          JobQueueTaskScheduler.class, TaskScheduler.class);
    // 调用ReflectionUtils类的newInstance方法，该方法中通过反射构造一个TaskScheduler对象，然后调用了TaskScheduler对象的setConf(conf)方法
    taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(schedulerClass, conf);
    
    // Set service-level authorization security policy
    // 服务级别的安全策略，默认是关闭的
    if (conf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
          (PolicyProvider)(ReflectionUtils.newInstance(
              conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                  MapReducePolicyProvider.class, PolicyProvider.class), 
              conf));
        ServiceAuthorizationManager.refresh(conf, policyProvider);
    }

    // 这个mapred.job.tracker.handler.count决定jobTracker启动多少个handler用来接收rpc请求，默认是10，这里是一个hadoop的优化点。
    int handlerCount = conf.getInt("mapred.job.tracker.handler.count", 10);
    // 获得一个Server对象(RPC Server)，在新建Server对象时会新建一个listener = new Listener()和一个responder = new Responder()对象，用于监听rpc请求和用于发送rpc请求结果。
    this.interTrackerServer = 
      RPC.getServer(this, addr.getHostName(), addr.getPort(), handlerCount, 
          false, conf, secretManager);
    if (LOG.isDebugEnabled()) {
      Properties p = System.getProperties();
      for (Iterator it = p.keySet().iterator(); it.hasNext();) {
        String key = (String) it.next();
        String val = p.getProperty(key);
        LOG.debug("Property '" + key + "' is " + val);
      }
    }

    String infoAddr =
      NetUtils.getServerAddress(conf, "mapred.job.tracker.info.bindAddress",
                                "mapred.job.tracker.info.port",
                                "mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.startTime = clock.getTime();
    // 这里是新建一个infoServer线程用于web服务的，也就是将集群的一些信息(Job, Task, TaskTracker相关信息)显示到web页面，端口是50030。
    infoServer = new HttpServer("job", infoBindAddress, tmpInfoPort, 
        tmpInfoPort == 0, conf, aclsManager.getAdminsAcl());
    infoServer.setAttribute("job.tracker", this);
    
    infoServer.addServlet("reducegraph", "/taskgraph", TaskGraphServlet.class);
    infoServer.start();
    
    this.trackerIdentifier = identifier;
    // 实例话myInstrumentation
    createInstrumentation();
    
    // The rpc/web-server ports can be ephemeral ports... 
    // ... ensure we have the correct info
    this.port = interTrackerServer.getListenerAddress().getPort();
    this.conf.set("mapred.job.tracker", (this.localMachine + ":" + this.port));
    this.localFs = FileSystem.getLocal(conf);
    LOG.info("JobTracker up at: " + this.port);
    this.infoPort = this.infoServer.getPort();
    this.conf.set("mapred.job.tracker.http.address", 
        infoBindAddress + ":" + this.infoPort); 
    LOG.info("JobTracker webserver: " + this.infoServer.getPort());

    // 新建一个dnsToSwitchMapping用于构造集群的网络拓扑结构
    // 该接口定义了将DNS名称或者节点IP地址转换成网络位置的规则。hadoop以层次树的方式定义节点的网络位置，并依据该位置存取数据或者调度任务。
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);
    this.numTaskCacheLevels = conf.getInt("mapred.task.cache.levels", 
        NetworkTopology.DEFAULT_HOST_LEVEL);
    this.isNodeGroupAware = conf.getBoolean(
            "mapred.jobtracker.nodegroup.aware", false);

    // 插件，但是没有实现，应该目前是一个扩展点吧。
    plugins = conf.getInstances("mapreduce.jobtracker.plugins",
        ServicePlugin.class);
    for (ServicePlugin p : plugins) {
      try {
        p.start(this);
        LOG.info("Started plug-in " + p + " of type " + p.getClass());
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " of type " + p.getClass()
            + " could not be started", t);
      }
    }
    // 设置JobTracker初始化完成。到这里整个JobTracker的构造函数结束了
    // 总结一下，主要对一些重要的对象进行了初始化，有secretManager/aclsManager/taskScheduler/interTrackerServer/infoServicer。
    this.initDone.set(conf.getBoolean(JT_INIT_CONFIG_KEY_FOR_TESTS, true));
  }

  private static SimpleDateFormat getDateFormat() {
    return new SimpleDateFormat("yyyyMMddHHmm");
  }

  private static String generateNewIdentifier() {
    return getDateFormat().format(new Date());
  }
  
  static boolean validateIdentifier(String id) {
    try {
      // the jobtracker id should be 'date' parseable
      getDateFormat().parse(id);
      return true;
    } catch (ParseException pe) {}
    return false;
  }

  static boolean validateJobNumber(String id) {
    try {
      // the job number should be integer parseable
      Integer.parseInt(id);
      return true;
    } catch (IllegalArgumentException pe) {}
    return false;
  }

  /**
   * Whether the JT has restarted
   */
  public boolean hasRestarted() {
    return hasRestarted;
  }

  /**
   * Whether the JT has recovered upon restart
   */
  public boolean hasRecovered() {
    return hasRecovered;
  }

  /**
   * How long the jobtracker took to recover from restart.
   */
  public long getRecoveryDuration() {
    return hasRestarted() 
           ? recoveryDuration
           : 0;
  }

  /**
   * Get JobTracker's FileSystem. This is the filesystem for mapred.system.dir.
   */
  FileSystem getFileSystem() {
    return fs;
  }

  /**
   * Get JobTracker's LocalFileSystem handle. This is used by jobs for 
   * localizing job files to the local disk.
   */
  LocalFileSystem getLocalFileSystem() throws IOException {
    return localFs;
  }

  static Class<? extends JobTrackerInstrumentation> getInstrumentationClass(Configuration conf) {
    return conf.getClass("mapred.jobtracker.instrumentation", null,
                         JobTrackerInstrumentation.class);
  }
  
  static void setInstrumentationClass(Configuration conf, Class<? extends JobTrackerInstrumentation> t) {
    conf.setClass("mapred.jobtracker.instrumentation",
        t, JobTrackerInstrumentation.class);
  }

  JobTrackerInstrumentation getInstrumentation() {
    return myInstrumentation;
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr =
      conf.get("mapred.job.tracker", "localhost:8012");
    return NetUtils.createSocketAddr(jobTrackerStr);
  }

  /**
   * Run forever
   * 该方法主要是初始化并启动一些重要线程，以及进行作业恢复和启动taskScheduler。   offerService()结束main()也就结束了
   */
  public void offerService() throws InterruptedException, IOException {
    // start the inter-tracker server
    // 启动interTrackerServe(RPC server), 进入start方法查看具体实现
    this.interTrackerServer.start();
    
    // Initialize the JobTracker FileSystem within safemode
    // 设置为safemode进行FileSystem的初始化，初始化完成后离开safemode
    setSafeModeInternal(SafeModeAction.SAFEMODE_ENTER);
    initializeFilesystem();
    setSafeModeInternal(SafeModeAction.SAFEMODE_LEAVE);
    
    // Initialize JobTracker
    // 初始化JobTracker，里面包括很多东西（如构造RecoveryManager，CompletedJobStatusStore线程），进入该方法查看具体过程
    initialize();
    
    // Prepare for recovery. This is done irrespective of the status of restart
    // flag.
    // 开始进行作业恢复
    while (true) {
      try {
        // 更新恢复次数
        // 具体逻辑：第一次恢复向mapred.system.dir/jobtracker.info文件中写入0，之后每次进行恢复都会对文件中的值进行+1操作。该方法是用来记录恢复次数。
        recoveryManager.updateRestartCount();
        break;
      } catch (IOException ioe) {
        LOG.warn("Failed to initialize recovery manager. ", ioe);
        // wait for some time
        Thread.sleep(FS_ACCESS_RETRY_PERIOD);
        LOG.warn("Retrying...");
      }
    }
    // 将taskScheduler初始化时创建的JobListener添加到JobTracker的jobInProgressListeners集合中.
    // 启动JobTracker,  参考JobQueueTaskScheduler的start方法中会调用eagerTaskInitializationListener.start()
    // eagerTaskInitializationListener启动线程, 对队列中的job进行监听, 详见EagerTaskInitializationListener的start()
    taskScheduler.start();
    
    //  Start the recovery after starting the scheduler
    try {
      // 进行作业恢复， 具体实现进入该方法查看
      recoveryManager.recover();
    } catch (Throwable t) {
      LOG.warn("Recovery manager crashed! Ignoring.", t);
    }
    // refresh the node list as the recovery manager might have added 
    // disallowed trackers
    // 更新节点信息， 具体实现进入该方法查看
    refreshHosts();

    // 下面分别启动expireTrackersThread，retireJobsThread，expireLaunchingTaskThread，completedJobStatusStore线程。

    // expireTrackersThread：该线程用于发现和清除死掉的TaskTracker。
    // 主要根据一个TaskTracker自上一次的心跳汇报以来在TASKTRACKER_EXPIRY_INTERVAL时间（由mapred.tasktracker.expiry.interval参数设置，默认是10 * 60 * 1000，单位ms, 即10分钟）内未汇报心跳，则将其清除
    // 即如果某个TaskTracker在10分钟内未汇报心跳，则JobTracker认为它已经死掉，并将它的相关信息从数据结构trackerToJobsToCleanup, trackerToTasksToCleanup, trackerToTaskMap, trackerToMarkedTasksMap
    // 中清除，同时将正在运行的任务状态标注为KILLED_UNCLEAN
    this.expireTrackersThread = new Thread(this.expireTrackers,
                                          "expireTrackers");
    this.expireTrackersThread.start();
    // retireJobsThread：该线程用于清除已完成的作业信息。JobTracker会将已经完成的作业信息存放到内存中，以便外部查询，但随着job越来越多，会占用JobTracker大量内存，
    // 为此，JobTracker通过该线程清理驻留在内存中较长时间的已经运行完成的作业信息，当job满足如下条件1,2或者条件1,3时，将被从数据结构jobs转移到过期作业队列中。
    // 1. 作业已经运行完成，即运行状态为SUCCEEDED、FAILED、KILLED的job，
    // 2. 作业完成时间距现在已经超过24小时，即RETIRE_JOB_INTERVAL时间间隔（由mapred.jobtracker.retirejob.interval参数设置，默认是24 * 60 * 60 * 1000，单位ms, 即24小时）；
    // 3. 作业拥有者已经完成作业总数超过100，即MAX_COMPLETE_USER_JOBS_IN_MEMORY（由mapred.jobtracker.completeuserjobs.maximum参数设置，默认是100）
    this.retireJobsThread = new Thread(this.retireJobs, "retireJobs");
    this.retireJobsThread.start();
    // expireLaunchingTaskThread：该线程用于发现已经分配给某个TaskTracker但一直未汇报信息的任务。
    // 主要是根据每个TaskAttemptID启动的时间与当前时间的间隔是否大于TASKTRACKER_EXPIRY_INTERVAL（由mapred.tasktracker.expiry.interval参数设置，默认是10 * 60 * 1000，单位ms, 即10分钟）决定。
    // 当JobTracker将某个任务分配给TaskTracker后，如果该任务在10分钟内未汇报进度，则JobTracker认为该任务分配失败，并将其状态标注为FAILED
    expireLaunchingTaskThread.start();

    if (completedJobStatusStore.isActive()) {
      // completedJobsStoreThread：该线程用于将已完成的作业信息保存到hdfs中, 并提供了一套存取这些信息的API。该线程能够解决以下两个问题：
      // 1.用户无法获取很久之前的作业运行信息：前面提到线程retireJobsThread会清除长时间驻留在内存中的完成作业，这会导致用户无法查询很久之前某个作业的运行信息。
      // 2.JobTracker重启后作业运行信息丢失：当JobTracker因故障重启后，所有原本保存到内存中的作业信息将会全部丢失。
      // 该线程通过保存作业运行日志的方式，使得用户可以查询任意时间提交的作业和还原作业的运行信息。默认情况下，该线程不会启用。
      completedJobsStoreThread = new Thread(completedJobStatusStore,
                                            "completedjobsStore-housekeeper");
      completedJobsStoreThread.start();
    }

    // Just for unit-tests 
    waitForInit();
    synchronized (this) {
      state = State.RUNNING;
    }
    
    LOG.info("Starting RUNNING");
    
    this.interTrackerServer.join();
    LOG.info("Stopped interTrackerServer");
  }

  AtomicBoolean initDone = new AtomicBoolean(true);
  Object initDoneLock = new Object();
  
  private void waitForInit() {
    synchronized (initDoneLock) {
      while (!initDone.get()) {
        try {
          LOG.debug("About to wait since initDone = false");
          initDoneLock.wait();
        } catch (InterruptedException ie) {
          LOG.debug("Ignoring ", ie);
        }
      }
    }
  }
  
  void setInitDone(boolean done) {
    synchronized (initDoneLock) {
      initDone.set(done);
      initDoneLock.notify();
    }
  }
  
  void close() throws IOException {
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
          LOG.info("Stopped plug-in " + p + " of type " + p.getClass());
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " of type " + p.getClass()
              + " could not be stopped", t);
        }
      }
    }
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        LOG.warn("Exception shutting down JobTracker", ex);
      }
    }
    if (this.interTrackerServer != null) {
      LOG.info("Stopping interTrackerServer");
      this.interTrackerServer.stop();
    }
    if (this.expireTrackersThread != null && this.expireTrackersThread.isAlive()) {
      LOG.info("Stopping expireTrackers");
      this.expireTrackersThread.interrupt();
      try {
        this.expireTrackersThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.retireJobsThread != null && this.retireJobsThread.isAlive()) {
      LOG.info("Stopping retirer");
      this.retireJobsThread.interrupt();
      try {
        this.retireJobsThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (taskScheduler != null) {
      taskScheduler.terminate();
    }
    if (this.expireLaunchingTaskThread != null && this.expireLaunchingTaskThread.isAlive()) {
      LOG.info("Stopping expireLaunchingTasks");
      this.expireLaunchingTaskThread.interrupt();
      try {
        this.expireLaunchingTaskThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (this.completedJobsStoreThread != null &&
        this.completedJobsStoreThread.isAlive()) {
      LOG.info("Stopping completedJobsStore thread");
      this.completedJobsStoreThread.interrupt();
      try {
        this.completedJobsStoreThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    if (jobHistoryServer != null) {
      LOG.info("Stopping job history server");
      try {
        jobHistoryServer.shutdown();
      } catch (Exception ex) {
        LOG.warn("Exception shutting down Job History server", ex);
      }
  }
    DelegationTokenRenewal.close();
    LOG.info("stopped all jobtracker services");
    return;
  }
    
  ///////////////////////////////////////////////////////
  // Maintain lookup tables; called by JobInProgress
  // and TaskInProgress
  ///////////////////////////////////////////////////////
  void createTaskEntry(TaskAttemptID taskid, String taskTracker, TaskInProgress tip) {
    LOG.info("Adding task (" + tip.getAttemptType(taskid) + ") " + 
      "'"  + taskid + "' to tip " + 
      tip.getTIPId() + ", for tracker '" + taskTracker + "'");

    // taskid --> tracker
    taskidToTrackerMap.put(taskid, taskTracker);

    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToTaskMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToTaskMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);

    // taskid --> TIP
    taskidToTIPMap.put(taskid, tip);
    
  }
    
  void removeTaskEntry(TaskAttemptID taskid) {
    // taskid --> tracker
    String tracker = taskidToTrackerMap.remove(taskid);

    // tracker --> taskid
    if (tracker != null) {
      Set<TaskAttemptID> trackerSet = trackerToTaskMap.get(tracker);
      if (trackerSet != null) {
        trackerSet.remove(taskid);
      }
    }

    // taskid --> TIP
    if (taskidToTIPMap.remove(taskid) != null) {   
      LOG.info("Removing task '" + taskid + "'");
    }
  }
    
  /**
   * Mark a 'task' for removal later.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker the tasktracker at which the 'task' was running
   * @param taskid completed (success/failure/killed) task
   */
  void markCompletedTaskAttempt(String taskTracker, TaskAttemptID taskid) {
    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToMarkedTasksMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToMarkedTasksMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Marked '" + taskid + "' from '" + taskTracker + "'");
    }
  }

  /**
   * Mark all 'non-running' jobs of the job for pruning.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param job the completed job
   */
  void markCompletedJob(JobInProgress job) {
    for (TaskInProgress tip : job.getTasks(TaskType.JOB_SETUP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.MAP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING && 
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.REDUCE)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(), 
                                   taskStatus.getTaskID());
        }
      }
    }
  }
    
  /**
   * Remove all 'marked' tasks running on a given {@link TaskTracker}
   * from the {@link JobTracker}'s data-structures.
   * This function assumes that the JobTracker is locked on entry.
   * 
   * @param taskTracker tasktracker whose 'non-running' tasks are to be purged
   */
  private void removeMarkedTasks(String taskTracker) {
    // Purge all the 'marked' tasks which were running at taskTracker
    Set<TaskAttemptID> markedTaskSet = 
      trackerToMarkedTasksMap.get(taskTracker);
    if (markedTaskSet != null) {
      for (TaskAttemptID taskid : markedTaskSet) {
        // 同时更新JobTracker内部维护的如下3个队列：TreeMap<TaskAttemptID, String> taskidToTrackerMap、TreeMap<String, Set<TaskAttemptID>> trackerToTaskMap、Map<TaskAttemptID, TaskInProgress> taskidToTIPMap。
        removeTaskEntry(taskid);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removed marked completed task '" + taskid + "' from '" + 
                    taskTracker + "'");
        }
      }
      // Clear
      // 从队列TreeMap<String, Set<TaskAttemptID>> trackerToMarkedTasksMap中移除所有被标记完成的Task
      trackerToMarkedTasksMap.remove(taskTracker);
    }
  }
    
  /**
   * Call {@link #removeTaskEntry(String)} for each of the
   * job's tasks.
   * When the JobTracker is retiring the long-completed
   * job, either because it has outlived {@link #RETIRE_JOB_INTERVAL}
   * or the limit of {@link #MAX_COMPLETE_USER_JOBS_IN_MEMORY} jobs 
   * has been reached, we can afford to nuke all it's tasks; a little
   * unsafe, but practically feasible. 
   * 
   * @param job the job about to be 'retired'
   */
  synchronized void removeJobTasks(JobInProgress job) { 
    // iterate over all the task types
    for (TaskType type : TaskType.values()) {
      // iterate over all the tips of the type under consideration
      for (TaskInProgress tip : job.getTasks(type)) {
        // iterate over all the task-ids in the tip under consideration
        for (TaskAttemptID id : tip.getAllTaskAttemptIDs()) {
          // remove the task-id entry from the jobtracker
          removeTaskEntry(id);
        }
      }
    }
  }
    
  /**
   * Safe clean-up all data structures at the end of the 
   * job (success/failure/killed).
   * Here we also ensure that for a given user we maintain 
   * information for only MAX_COMPLETE_USER_JOBS_IN_MEMORY jobs 
   * on the JobTracker.
   *  
   * @param job completed job.
   */
  synchronized void finalizeJob(JobInProgress job) {
    // Mark the 'non-running' tasks for pruning
    markCompletedJob(job);
    
    JobEndNotifier.registerNotification(job.getJobConf(), job.getStatus());

    // start the merge of log files
    JobID id = job.getStatus().getJobID();
    if (job.hasRestarted()) {
      try {
        JobHistory.JobInfo.finalizeRecovery(id, job.getJobConf());
      } catch (IOException ioe) {
        LOG.info("Failed to finalize the log file recovery for job " + id, ioe);
      }
    }

    // mark the job as completed
    try {
      JobHistory.JobInfo.markCompleted(id);
    } catch (IOException ioe) {
      LOG.info("Failed to mark job " + id + " as completed!", ioe);
    }

    final JobTrackerInstrumentation metrics = getInstrumentation();
    metrics.finalizeJob(conf, id);
    
    long now = clock.getTime();
    
    // mark the job for cleanup at all the trackers
    addJobForCleanup(id);

    // add the (single-job) blacklisted trackers to potentially faulty list
    // for possible heuristic graylisting across all jobs
    if (job.getStatus().getRunState() == JobStatus.SUCCEEDED) {
      if (job.getNoOfBlackListedTrackers() > 0) {
        for (String hostName : job.getBlackListedTrackers()) {
          faultyTrackers.incrementFaults(hostName);
        }
      }
    }

    String jobUser = job.getProfile().getUser();
    // add to the user to jobs mapping
    synchronized (userToJobsMap) {
      ArrayList<JobInProgress> userJobs = userToJobsMap.get(jobUser);
      if (userJobs == null) {
        userJobs =  new ArrayList<JobInProgress>();
        userToJobsMap.put(jobUser, userJobs);
      }
      userJobs.add(job);
    }
  }

  ///////////////////////////////////////////////////////
  // Accessors for objects that want info on jobs, tasks,
  // trackers, etc.
  ///////////////////////////////////////////////////////
  public int getTotalSubmissions() {
    return totalSubmissions;
  }
  public String getJobTrackerMachine() {
    return localMachine;
  }
  
  /**
   * Get the unique identifier (ie. timestamp) of this job tracker start.
   * @return a string with a unique identifier
   */
  public String getTrackerIdentifier() {
    return trackerIdentifier;
  }

  public int getTrackerPort() {
    return port;
  }
  public int getInfoPort() {
    return infoPort;
  }
  public long getStartTime() {
    return startTime;
  }
  public Vector<JobInProgress> runningJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.RUNNING) {
        v.add(jip);
      }
    }
    return v;
  }
  /**
   * Version that is called from a timer thread, and therefore needs to be
   * careful to synchronize.
   */
  public synchronized List<JobInProgress> getRunningJobs() {
    synchronized (jobs) {
      return runningJobs();
    }
  }
  public Vector<JobInProgress> failedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if ((status.getRunState() == JobStatus.FAILED)
          || (status.getRunState() == JobStatus.KILLED)) {
        v.add(jip);
      }
    }
    return v;
  }
  public Vector<JobInProgress> completedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.SUCCEEDED) {
        v.add(jip);
      }
    }
    return v;
  }

  /**
   * Get all the task trackers in the cluster
   * 
   * @return {@link Collection} of {@link TaskTrackerStatus} 
   */
  // lock to taskTrackers should hold JT lock first.
  public synchronized Collection<TaskTrackerStatus> taskTrackers() {
    Collection<TaskTrackerStatus> ttStatuses;
    synchronized (taskTrackers) {
      ttStatuses = 
        new ArrayList<TaskTrackerStatus>(taskTrackers.values().size());
      for (TaskTracker tt : taskTrackers.values()) {
        ttStatuses.add(tt.getStatus());
      }
    }
    return ttStatuses;
  }
  
  /**
   * Get the active task tracker statuses in the cluster
   *  
   * @return {@link Collection} of active {@link TaskTrackerStatus} 
   */
  // This method is synchronized to make sure that the locking order 
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers 
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public Collection<TaskTrackerStatus> activeTaskTrackers() {
    Collection<TaskTrackerStatus> activeTrackers = 
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for ( TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (!faultyTrackers.isBlacklisted(status.getHost())) {
          activeTrackers.add(status);
        }
      }
    }
    return activeTrackers;
  }
  
  /**
   * Get the active, blacklisted, and graylisted task tracker names in the
   * cluster. The first element in the returned list contains the list of
   * active tracker names; the second element in the returned list contains
   * the list of blacklisted tracker names; and the third contains the list
   * of graylisted tracker names.  Note that the blacklist is disjoint
   * from the active list, but the graylist is not:  initially, graylisted
   * trackers are still active and therefore will appear in both lists.
   * (Graylisted trackers can later be blacklisted, in which case they'll
   * be removed from the active list and added to the blacklist, but they
   * remain on the graylist in this case.  Blacklisting comes about via the
   * health-check script, while graylisting is heuristically based on the
   * number of per-job blacklistings in a specified time interval.)
   */
  // This method is synchronized to make sure that the locking order
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public List<List<String>> taskTrackerNames() {
    List<String> activeTrackers = new ArrayList<String>();
    List<String> blacklistedTrackers = new ArrayList<String>();
    List<String> graylistedTrackers = new ArrayList<String>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        String hostName = status.getHost();
        String trackerName = status.getTrackerName();
        if (!faultyTrackers.isBlacklisted(hostName)) {
          activeTrackers.add(trackerName);
        } else {
          blacklistedTrackers.add(trackerName);
        }
        if (faultyTrackers.isGraylisted(hostName)) {
          graylistedTrackers.add(trackerName);
        }
      }
    }
    List<List<String>> result = new ArrayList<List<String>>(3);
    result.add(activeTrackers);
    result.add(blacklistedTrackers);
    result.add(graylistedTrackers);
    return result;
  }

  /**
   * Get the statuses of the blacklisted task trackers in the cluster.
   *
   * @return {@link Collection} of blacklisted {@link TaskTrackerStatus}
   */
  // used by the web UI (machines.jsp)
  public Collection<TaskTrackerStatus> blacklistedTaskTrackers() {
    return blackOrGraylistedTaskTrackers(false);
  }

  /**
   * Get the statuses of the graylisted task trackers in the cluster.
   *
   * @return {@link Collection} of graylisted {@link TaskTrackerStatus}
   */
  public Collection<TaskTrackerStatus> graylistedTaskTrackers() {
    return blackOrGraylistedTaskTrackers(true);
  }

  // This method is synchronized to make sure that the locking order
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized private Collection<TaskTrackerStatus>
        blackOrGraylistedTaskTrackers(boolean gray) {
    Collection<TaskTrackerStatus> listedTrackers =
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        boolean listed = gray? faultyTrackers.isGraylisted(status.getHost()) :
                               faultyTrackers.isBlacklisted(status.getHost());
        if (listed) {
          listedTrackers.add(status);
        }
      }
    }
    return listedTrackers;
  }

  synchronized int getFaultCount(String hostName) {
    return faultyTrackers.getFaultCount(hostName);
  }

  /**
   * Get the number of task trackers that are blacklisted (via health-check
   * script) across all jobs.
   *
   * @return
   */
  int getBlacklistedTrackerCount() {
    return faultyTrackers.numBlacklistedTrackers;
  }

  /**
   * Get the number of task trackers that are graylisted (via heuristics on
   * single-job blacklistings) across all jobs.
   *
   * @return
   */
  int getGraylistedTrackerCount() {
    return faultyTrackers.numGraylistedTrackers;
  }

  /**
   * Whether the tracker is blacklisted or not
   *
   * @param trackerID
   *
   * @return true if blacklisted, false otherwise
   */
  synchronized public boolean isBlacklisted(String trackerID) {
    TaskTrackerStatus status = getTaskTrackerStatus(trackerID);
    if (status != null) {
      return faultyTrackers.isBlacklisted(status.getHost());
    }
    return false;
  }

  /**
   * Whether the tracker is graylisted or not
   *
   * @param trackerID
   *
   * @return true if graylisted, false otherwise
   */
  synchronized public boolean isGraylisted(String trackerID) {
    TaskTrackerStatus status = getTaskTrackerStatus(trackerID);
    if (status != null) {
      return faultyTrackers.isGraylisted(status.getHost());
    }
    return false;
  }

  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTrackerStatus getTaskTrackerStatus(String trackerID) {
    TaskTracker taskTracker;
    synchronized (taskTrackers) {
      taskTracker = taskTrackers.get(trackerID);
    }
    return (taskTracker == null) ? null : taskTracker.getStatus();
  }

  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTracker getTaskTracker(String trackerID) {
    synchronized (taskTrackers) {
      return taskTrackers.get(trackerID);
    }
  }

  JobTrackerStatistics getStatistics() {
    return statistics;
  }
  /**
   * Adds a new node to the jobtracker. It involves adding it to the expiry
   * thread and adding it for resolution
   * 
   * Assumes JobTracker, taskTrackers and trackerExpiryQueue is locked on entry
   * 
   * @param status Task Tracker's status
   */
  private void addNewTracker(TaskTracker taskTracker) throws UnknownHostException {
    TaskTrackerStatus status = taskTracker.getStatus();
    // 放入到过期队列中
    trackerExpiryQueue.add(status);

    //  Register the tracker if its not registered
    String hostname = status.getHost();
    if (getNode(status.getTrackerName()) == null) {
      // Making the network location resolution inline .. 
      resolveAndAddToTopology(hostname);
    }

    // add it to the set of tracker per host
    Set<TaskTracker> trackers = hostnameToTaskTracker.get(hostname);
    if (trackers == null) {
      trackers = Collections.synchronizedSet(new HashSet<TaskTracker>());
      hostnameToTaskTracker.put(hostname, trackers);
    }
    statistics.taskTrackerAdded(status.getTrackerName());
    getInstrumentation().addTrackers(1);
    LOG.info("Adding tracker " + status.getTrackerName() + " to host " 
             + hostname);
    trackers.add(taskTracker);
  }

  public Node resolveAndAddToTopology(String name) throws UnknownHostException {
    List <String> tmpList = new ArrayList<String>(1);
    tmpList.add(name);
    List <String> rNameList = dnsToSwitchMapping.resolve(tmpList);
    String rName = rNameList.get(0);
    String networkLoc = NodeBase.normalize(rName);
    return addHostToNodeMapping(name, networkLoc);
  }
  
  private Node addHostToNodeMapping(String host, String networkLoc) {
    Node node = null;
    synchronized (nodesAtMaxLevel) {
      if ((node = clusterMap.getNode(networkLoc+"/"+host)) == null) {
        node = new NodeBase(host, networkLoc);
        clusterMap.add(node);
        if (node.getLevel() < getNumTaskCacheLevels()) {
          LOG.fatal("Got a host whose level is: " + node.getLevel() + "." 
              + " Should get at least a level of value: " 
              + getNumTaskCacheLevels());
          try {
            stopTracker();
          } catch (IOException ie) {
            LOG.warn("Exception encountered during shutdown: " 
                + StringUtils.stringifyException(ie));
            System.exit(-1);
          }
        }
        hostnameToNodeMap.put(host, node);
        // Make an entry for the node at the max level in the cache
        nodesAtMaxLevel.add(getParentNode(node, getNumTaskCacheLevels() - 1));
      }
    }
    return node;
  }

  /**
   * Returns a collection of nodes at the max level
   */
  public Collection<Node> getNodesAtMaxLevel() {
    return nodesAtMaxLevel;
  }

  public static Node getParentNode(Node node, int level) {
    for (int i = 0; i < level; ++i) {
      node = node.getParent();
    }
    return node;
  }

  /**
   * Return the Node in the network topology that corresponds to the hostname
   */
  public Node getNode(String name) {
    return hostnameToNodeMap.get(name);
  }
  public int getNumTaskCacheLevels() {
    return numTaskCacheLevels;
  }
  public int getNumResolvedTaskTrackers() {
    return numResolved;
  }
  
  public int getNumberOfUniqueHosts() {
    return uniqueHostsMap.size();
  }
  public boolean isNodeGroupAware() {
    return isNodeGroupAware;
  }
  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }
  
  // Update the listeners about the job
  // Assuming JobTracker is locked on entry.
  private void updateJobInProgressListeners(JobChangeEvent event) {
    // 分别调用JobQueueJobInProgressListener和EagerTaskInitializationListener的jobUpdated方法
    for (JobInProgressListener listener : jobInProgressListeners) {
      listener.jobUpdated(event);
    }
  }
  
  /**
   * Return the {@link QueueManager} associated with the JobTracker.
   */
  public QueueManager getQueueManager() {
    return queueManager;
  }
  
  ////////////////////////////////////////////////////
  // InterTrackerProtocol
  ////////////////////////////////////////////////////

  // Just returns the VersionInfo version (unlike MXBean#getVersion)
  public String getVIVersion() throws IOException {
    return VersionInfo.getVersion();
  }

  public String getBuildVersion() throws IOException {
    return VersionInfo.getBuildVersion();
  }

  /**
   * The periodic heartbeat mechanism between the {@link TaskTracker} and
   * the {@link JobTracker}.
   * 
   * The {@link JobTracker} processes the status information sent by the 
   * {@link TaskTracker} and responds with instructions to start/stop 
   * tasks or jobs, and also 'reset' instructions during contingencies.
   *
   * JobTracker与TaskTracker之间通过org.apache.hadoop.mapred.InterTrackerProtocol协议来进行通信，
   * 心跳机制实际上是一个RPC请求，JobTracker作为Server，而TaskTracker作为Client，TaskTracker通过RPC调用JT的heartbeat方法，将TT自身的一些状态信息发送给JT，同时JT通过返回值返回对TT的指令。
   * 心跳有三个作用：
   * 1）判断TT是否活着
   * 2）报告TT的资源情况以及任务运行情况
   * 3）为TT发送指令（如运行task，kill task等）

   * TaskTracker通过该接口进行远程调用实现Heartbeat消息的发送，协议方法定义如下所示：
   *
   * 通过该方法可以看出，最核心的Heartbeat报告数据都封装在TaskTrackerStatus对象中，JobTracker端会接收TaskTracker周期性地发送的心跳报告，
   * 根据这些心跳信息来更新整个Hadoop集群中计算资源的状态/数量，以及Task的运行状态。
   *
   * @param status          该参数封装了TaskTracker上的各种状态信息
   * @param restarted       如果TaskTracker刚刚重新启动，该值为true，否则为false
   * @param initialContact  如果TaskTracker是初次连接JobTracker，该值为true，否则为false
   * @param acceptNewTasks  如果Tasktracker可以接收新任务，该值为true，否则该值为false，这通常取决于slot是否有剩余和节点健康状况等。
   * @param responseId      表示心跳响应编号，用于防止重复发送心跳。每接收一次心跳后，该值加1。
   * @return                该函数的返回值为一个HeartbeatResponse对象，该对象主要封装了JobTracker向TaskTracker下达的命令。
   * @throws IOException
   *
   */
  public synchronized HeartbeatResponse heartbeat(TaskTrackerStatus status,
                                                  boolean restarted,
                                                  boolean initialContact,
                                                  boolean acceptNewTasks, 
                                                  short responseId) 
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got heartbeat from: " + status.getTrackerName() + 
                " (restarted: " + restarted + 
                " initialContact: " + initialContact + 
                " acceptNewTasks: " + acceptNewTasks + ")" +
                " with responseId: " + responseId);
    }

    // Make sure heartbeat is from a tasktracker allowed by the jobtracker.
    // 第一步是检查发送心跳请求的TT是否属于可允许的TT，这个是根据一个HostsFileReader对象进行判断的，该对象是在实例化JT的时候创建的，这个类保存了两个队列，分别是includes和excludes队列，
    // includes表示可以访问的host列表，excludes表示不可访问的host列表，这两个列表的内容根据两个mapred.hosts和mapred.hosts.exclude（mapred-site,xml中，默认是null，即没有限制）这两个参数指定的文件名读取的。
    // 具体可参考 this.hostsReader = new HostsFileReader(conf.get("mapred.hosts", ""), conf.get("mapred.hosts.exclude", ""));
    if (!acceptTaskTracker(status)) {
      throw new DisallowedTaskTrackerException(status);
    }

    // First check if the last heartbeat response got through
    String trackerName = status.getTrackerName();
    long now = clock.getTime();
    // 检查TT是否重启，是重启的话标识该TT的状态为健康的，否则检查TT的健康状态。
    // faultyTrackers.markTrackerHealthy(status.getHost())内部将该TT所在的Host上所有的TT（从这里可以看出hadoop考虑到一个Host上可能存在多个TT的可能）从黑名单，灰名单和可能存在错误的列表上删除，
    // 也就是从potentiallyFaultyTrackers队列中移除该Host，通过更新JT的numGraylistedTrackers/numBlacklistedTrackers数量以及JT的totalMapTaskCapacity和totalReduceTaskCapacity数量。
    // 至于如何检查TT健康状态，具体是根据JT上记录的关于TT执行任务失败的次数来判断的（具体不是太理解）。
    if (restarted) {
      // 标记TaskTracker为Healthy状态，当TaskTracker重启了，然后再次连接JobTracker时，发送Heartbeat的过程中，会执行该流程。具体看该方法的实现
      faultyTrackers.markTrackerHealthy(status.getHost());
    } else {
      // 检查是否可以指派任务在该TaskTracker上进行。需要检查该TaskTracker对应的黑名单和灰名单情况，如果TaskTracker状态一切正常，则恢复其正常被指派任务并运行Task的能力。具体看方法的实现
      faultyTrackers.checkTrackerFaultTimeout(status.getHost(), now);
    }

    // 从JT记录的HeartbeatResponse队列中获取该TT的HeartbeatResponse信息，即判断JT之前是否收到过该TT的心跳请求。
    HeartbeatResponse prevHeartbeatResponse =
      trackerToHeartbeatResponseMap.get(trackerName);
    boolean addRestartInfo = false;

    // 如果initialContact != true,表明TT不是首次连接JT
    if (initialContact != true) {
      // If this isn't the 'initial contact' from the tasktracker,
      // there is something seriously wrong if the JobTracker has
      // no record of the 'previous heartbeat'; if so, ask the 
      // tasktracker to re-initialize itself.
      // 如果prevHeartbeatResponse == null，即TT不是首次连接JT，而且JT中并没有该TT之前的心跳请求信息，说明是JobTracker重启了(这时候要判断是否有job需要恢复)
      if (prevHeartbeatResponse == null) {
        // This is the first heartbeat from the old tracker to the newly started JobTracker
        // 判断hasRestarted是否为true，hasRestarted是在JT初始化(main-> tracker.offerService -> initialize()方法中来决定的，而
        // initialize方法中recoveryManager.checkAndAddJob(status) 会判断systemDir是否有job需要恢复，如果有shouldRecover = true;然后设置hasRestarted = recoveryManager.shouldRecover();
        // 所以当有job需要恢复时，hasRestarted()=true，addRestartInfo会被设置为true，即需要TT进行job恢复操作，同时从recoveryManager的recoveredTrackers队列中移除该TT。
        // 如果没有job需要恢复，则直接返回HeartbeatResponse，并对TT下重新初始化指令，注意此处返回的responseId还是TT发过来的responseId，即responseId不变。
        if (hasRestarted()) {// 1. JobTracker重启了，hasRestarted()=true,这时候需要TaskTracker进行job恢复
          addRestartInfo = true;
          // inform the recovery manager about this tracker joining back
          recoveryManager.unMarkTracker(trackerName);
        } else {  // 2.
          // Jobtracker might have restarted but no recovery is needed
          // otherwise this code should not be reached
          LOG.warn("Serious problem, cannot find record of 'previous' " +
                   "heartbeat for '" + trackerName + 
                   "'; reinitializing the tasktracker");
          return new HeartbeatResponse(responseId, 
              new TaskTrackerAction[] {new ReinitTrackerAction()});
        }

      } else {
        // 当prevHeartbeatResponse!=null时, 如果prevHeartbeatResponse.getResponseId() != responseId， 则认为这是一条重复的心跳请求
        // 说明TaskTracker因为失去了与JobTracker之间的RPC连接而没有接收到，会直接返回prevHeartbeatResponse，而忽略本次心跳请求。
        // 如果prevHeartbeatResponse.getResponseId() = responseId，则继续往下执行。
                
        // It is completely safe to not process a 'duplicate' heartbeat from a 
        // {@link TaskTracker} since it resends the heartbeat when rpcs are 
        // lost see {@link TaskTracker.transmitHeartbeat()};
        // acknowledge it by re-sending the previous response to let the 
        // {@link TaskTracker} go forward. 
        if (prevHeartbeatResponse.getResponseId() != responseId) {
          LOG.info("Ignoring 'duplicate' heartbeat from '" + 
              trackerName + "'; resending the previous 'lost' response");
          return prevHeartbeatResponse;
        }
      }
    }
      
    // Process this heartbeat
    // 首先将responseId + 1，然后记录心跳发送时间。
    short newResponseId = (short)(responseId + 1);
    status.setLastSeen(now);
    // processHeartbeat方法，用来判断TaskTracker是否需要重新初始化，返回false则需要重新初始化，返回true则不需要重新初始化(具体查看该方法的实现)，
    // 如果processHeartbeat()返回false，则返回HeartbeatResponse()，并下达重新初始化TT指令。只有当 initialContact = false,并且不存在old tastTrackerStatus时才返回false。
    if (!processHeartbeat(status, initialContact, now)) {
      if (prevHeartbeatResponse != null) {
        trackerToHeartbeatResponseMap.remove(trackerName);
      }
      return new HeartbeatResponse(newResponseId, 
                   new TaskTrackerAction[] {new ReinitTrackerAction()});
    }
      
    // Initialize the response to be sent for the heartbeat
    // 此处会实例化一个HeartbeatResponse对象，作为本次心跳的返回值，
    HeartbeatResponse response = new HeartbeatResponse(newResponseId, null);
    // 再初始化一个TaskTrackerAction队列，用于存放JT对TT下达的指令。
    List<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();
    boolean isBlacklisted = faultyTrackers.isBlacklisted(status.getHost());
    // Check for new tasks to be executed on the tasktracker
    // 检查是否可以向该TaskTracker指派任务，如果可以向该TaskTracker指派任务，则直接使用TaskScheduler指定的调度策略，选择当前可以指派给TaskTracker的一组需要启动的Task（对应指令LaunchTaskAction）。
    // 首先需要判断recoveryManager的recoveredTrackers是否为空，即是否有需要恢复的TT，没有则返回true。
    // 然后根据TT心跳发送的acceptNewTasks值，即表明TT是否可接收新任务，并且该TT不在黑名单中，
    // 同上满足以上条件，则JT可以为TT分配任务。分配任务的选择方式是优先CleanupTask，然后是SetupTask，然后才是Map/Reduce Task
    if (recoveryManager.shouldSchedule() && acceptNewTasks && !isBlacklisted) {
      TaskTrackerStatus taskTrackerStatus = getTaskTrackerStatus(trackerName);
      if (taskTrackerStatus == null) {
        LOG.warn("Unknown task tracker polling; ignoring: " + trackerName);
      } else {
        // 优先选择辅助型任务，选择优先级从高到低依次为：job-cleanup task, task-cleanup task和job-setup task, 这样可以让运行完成的作业快速结束，新提交的作业立刻进入运行状态
        // 该方法只会返回一个Task或0个Task（tasks的size为1或0），如果返回null，则表示没有cleanup或者setup任务需要执行，则有执行调度器分配的map/reduce任务。
        // 具体看一下getSetupAndCleanupTasks方法的具体实现
        List<Task> tasks = getSetupAndCleanupTasks(taskTrackerStatus);
        if (tasks == null ) {
          // 由任务调度器选择一个或多个计算型任务，此处是使用TaskScheduler调度任务(默认是JObQueueTaskScheduler)，一大难点，后期分析。
          // assignTasks方法最后返回分配任务列表assignedTasks。调度器只分配MapTask和ReduceTask。而作业的其它辅助任务都是交由JobTracker来调度的，如JobSetup、JobCleanup、TaskCleanup任务等。
          tasks = taskScheduler.assignTasks(taskTrackers.get(trackerName));
        }
        if (tasks != null) {
          // 遍历任务列表tasks
          for (Task task : tasks) {
            // 分配的Task加入expireLaunchingTasks，expireLaunchingTaskThread会定期检查expireLaunchingTasks的Task在规定的时间内是否汇报了进度。
            expireLaunchingTasks.addNewTask(task.getTaskID());
            if(LOG.isDebugEnabled()) {
              LOG.debug(trackerName + " -> LaunchTask: " + task.getTaskID());
            }
            // 生成一个LaunchTaskAction指令。
            actions.add(new LaunchTaskAction(task));
          }
        }
      }
    }

    // 以下分别是下达kill task（KillTaskAction）指令，kill/cleanedup job（KillJobAction）指令，commit task（CommitTaskAction）指令。
    // 加上LaunchTaskAction和另一个ReinitTackerAction，这是心跳JT对TT下达的所有五种指令
    // Check for tasks to be killed
    // 先检查在该TaskTracker上是否有完成的Job，计算属于这些Job的需要被Kill掉（对应指令KillTaskAction）的Task；
    List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
    if (killTasksList != null) {
      actions.addAll(killTasksList);
    }
     
    // Check for jobs to be killed/cleanedup
    // 再检查是否有完成的Job，并且对应在该TaskTracker上的Task需要被清理（对应指令KillJobAction）；
    List<TaskTrackerAction> killJobsList = getJobsForCleanup(trackerName);
    if (killJobsList != null) {
      actions.addAll(killJobsList);
    }

    // Check for tasks whose outputs can be saved
    // 最后检查是否有已经完成需要被提交的Task（以此来通知TaskTracker提交Task完成并更新状态，对应指令CommitTaskAction）。
    List<TaskTrackerAction> commitTasksList = getTasksToSave(status);
    if (commitTasksList != null) {
      actions.addAll(commitTasksList);
    }

    // 剩下一些收尾工作，如计算下次发送心跳的时间，以及设置需要TT进行恢复的任务，更新trackerToHeartbeatResponseMap队列，移除标记的task。最后返回HeartbeatResponse对象，完成心跳请求响应。
    // calculate next heartbeat interval and put in heartbeat response
    int nextInterval = getNextHeartbeatInterval();
    response.setHeartbeatInterval(nextInterval);
    response.setActions(
                        actions.toArray(new TaskTrackerAction[actions.size()]));
    
    // check if the restart info is req
    // 如果JobTracker重启了，则需要将需要恢复的Job列表加入response
    if (addRestartInfo) {
      response.setRecoveredJobs(recoveryManager.getJobsToRecover());
    }
        
    // Update the trackerToHeartbeatResponseMap
    // 更新JobTracker内部维护的trackerToHeartbeatResponseMap映射。
    trackerToHeartbeatResponseMap.put(trackerName, response);

    // Done processing the hearbeat, now remove 'marked' tasks
    // 根据TaskTracker的Heartbeat报告的Task状态信息，对标记为完成的Task，更新JobTracker内部维护的多个队列和Map：trackerToMarkedTasksMap、taskidToTrackerMap、trackerToTaskMap、taskidToTIPMap
    removeMarkedTasks(trackerName);
        
    return response;
  }
  
  /**
   * Calculates next heartbeat interval using cluster size.
   * Heartbeat interval is incremented by 1 second for every 100 nodes by default. 
   * @return next heartbeat interval.
   */
  public int getNextHeartbeatInterval() {
    // get the no of task trackers
    int clusterSize = getClusterStatus().getTaskTrackers();
    // JobTracker会根据集群规模动态调整TaskTracker汇报心跳的时间间隔。
    // HEARTBEATS_SCALING_FACTOR默认值是1, NUM_HEARTBEATS_IN_SECOND默认值是100, HEARTBEAT_INTERVAL_MIN默认值是300(毫秒)， 即最小间隔是300毫秒
    int heartbeatInterval =  Math.max(
                                (int)(1000 * HEARTBEATS_SCALING_FACTOR *
                                      ((double)clusterSize / 
                                                NUM_HEARTBEATS_IN_SECOND)),
                                HEARTBEAT_INTERVAL_MIN) ;
    return heartbeatInterval;
  }

  /**
   * Return if the specified tasktracker is in the hosts list, 
   * if one was configured.  If none was configured, then this 
   * returns true.
   */
  private boolean inHostsList(TaskTrackerStatus status) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || hostsList.contains(status.getHost()));
  }

  /**
   * Return if the specified tasktracker is in the exclude list.
   */
  private boolean inExcludedHostsList(TaskTrackerStatus status) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return excludeList.contains(status.getHost());
  }

  /**
   * Returns true if the tasktracker is in the hosts list and 
   * not in the exclude list. 
   */
  private boolean acceptTaskTracker(TaskTrackerStatus status) {
    return (inHostsList(status) && !inExcludedHostsList(status));
  }
    
  /**
   * Update the last recorded status for the given task tracker.
   * It assumes that the taskTrackers are locked on entry.
   * @param trackerName The name of the tracker
   * @param status The new status for the task tracker
   * @return Was an old status found?
   */
  // 更新TaskTracker状态
  private boolean updateTaskTrackerStatus(String trackerName,
                                          TaskTrackerStatus status) {
    TaskTracker tt = getTaskTracker(trackerName);
    // oldStatus == null,返回false；oldStatus !=null，返回true。
    TaskTrackerStatus oldStatus = (tt == null) ? null : tt.getStatus();
    // TaskTracker不是首次连接
    if (oldStatus != null) {
      // 下面4个是全局计数器，在当前计数器的基础上，减去上次汇报的报告中的数量（实际上是假定上次汇报的全部指标都已完成，如果没完成，再通过本次汇报的状态报告再加回去）
      // MapTask总数
      totalMaps -= oldStatus.countMapTasks();
      // ReduceTask总数
      totalReduces -= oldStatus.countReduceTasks();
      // 占用的Map Slot总数
      occupiedMapSlots -= oldStatus.countOccupiedMapSlots();
      // 占用的Reduce Slot总数
      occupiedReduceSlots -= oldStatus.countOccupiedReduceSlots();
      getInstrumentation().decRunningMaps(oldStatus.countMapTasks());
      getInstrumentation().decRunningReduces(oldStatus.countReduceTasks());
      getInstrumentation().decOccupiedMapSlots(oldStatus.countOccupiedMapSlots());
      getInstrumentation().decOccupiedReduceSlots(oldStatus.countOccupiedReduceSlots());
      // is host in oldTaskTrackerStatus not black listed ? 如果TaskTracker没有被加入到黑名单中，还要更新下面两个JobTracker端全局计数器
      if (!faultyTrackers.isBlacklisted(oldStatus.getHost())) {
        int mapSlots = oldStatus.getMaxMapSlots();
        // 该JobTracker上最大Map Slot总数, 先将该TaskTracker的mapSlots总数减掉，如果status != null，会把最新的mapSlots再加到totalMapTaskCapacity之中，其实就是更新totalMapTaskCapacity
        // 下面的totalReduceTaskCapacity操作类似
        totalMapTaskCapacity -= mapSlots;
        int reduceSlots = oldStatus.getMaxReduceSlots();
        // 该JobTracker上最大Reduce Slot总数
        totalReduceTaskCapacity -= reduceSlots;
      }
      if (status == null) {
        // status == null,将TaskTracker移除
        taskTrackers.remove(trackerName);
        // 考虑到一个host上面可能有多个TaskTracker，将该TaskTracker所在的host总的TaskTracker数量减一，然后更新uniqueHostsMap
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(oldStatus.getHost());
        if (numTaskTrackersInHost != null) {
          numTaskTrackersInHost --;
          if (numTaskTrackersInHost > 0)  {
            uniqueHostsMap.put(oldStatus.getHost(), numTaskTrackersInHost);
          }
          else {
            uniqueHostsMap.remove(oldStatus.getHost());
          }
        }
      }
    }
    // taskTrackers中没有当前传入的trackerName信息，则说明是初次链接，初始化一些信息，并将TaskTracker加入到taskTrackers中
    // 首先更新JobTracker内部维护的6个全局计数器：totalMaps、totalReduces、occupiedMapSlots、occupiedReduceSlots、totalMapTaskCapacity、totalReduceTaskCapacity，各个计数器具体含义见上面说明。
    if (status != null) {
      totalMaps += status.countMapTasks();
      totalReduces += status.countReduceTasks();
      occupiedMapSlots += status.countOccupiedMapSlots();
      occupiedReduceSlots += status.countOccupiedReduceSlots();
      getInstrumentation().addRunningMaps(status.countMapTasks());
      getInstrumentation().addRunningReduces(status.countReduceTasks());
      getInstrumentation().addOccupiedMapSlots(status.countOccupiedMapSlots());
      getInstrumentation().addOccupiedReduceSlots(status.countOccupiedReduceSlots());
      // is host in oldTaskTrackerStatus not black listed ?
      if (!faultyTrackers.isBlacklisted(status.getHost())) {
        int mapSlots = status.getMaxMapSlots();
        totalMapTaskCapacity += mapSlots;
        int reduceSlots = status.getMaxReduceSlots();
        totalReduceTaskCapacity += reduceSlots;
      }
      boolean alreadyPresent = false;
      TaskTracker taskTracker = taskTrackers.get(trackerName);
      // 如果TaskTracker是第一次汇报状态报告，则需要在JobTracker内部注册，构造一个org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker对象（该TaskTracker对象是在JobTracker的视角看到的结构），
      // 加入到队列HashMap<String, TaskTracker> taskTrackers中，同时还要计算该TaskTracker所在的host节点上TaskTracker进程的个数，更新队列Map<String, Integer> uniqueHostsMap。
      if (taskTracker != null) {
        alreadyPresent = true;
      } else {
        taskTracker = new TaskTracker(trackerName);
      }
      
      taskTracker.setStatus(status);
      // 将TaskTracker加入到taskTrackers
      taskTrackers.put(trackerName, taskTracker);
      
      if (LOG.isDebugEnabled()) {
        int runningMaps = 0, runningReduces = 0;
        int commitPendingMaps = 0, commitPendingReduces = 0;
        int unassignedMaps = 0, unassignedReduces = 0;
        int miscMaps = 0, miscReduces = 0;
        List<TaskStatus> taskReports = status.getTaskReports();
        for (Iterator<TaskStatus> it = taskReports.iterator(); it.hasNext();) {
          TaskStatus ts = (TaskStatus) it.next();
          boolean isMap = ts.getIsMap();
          TaskStatus.State state = ts.getRunState();
          if (state == TaskStatus.State.RUNNING) {
            if (isMap) { ++runningMaps; }
            else { ++runningReduces; }
          } else if (state == TaskStatus.State.UNASSIGNED) {
            if (isMap) { ++unassignedMaps; }
            else { ++unassignedReduces; }
          } else if (state == TaskStatus.State.COMMIT_PENDING) {
            if (isMap) { ++commitPendingMaps; }
            else { ++commitPendingReduces; }
          } else {
            if (isMap) { ++miscMaps; } 
            else { ++miscReduces; } 
          }
        }
        LOG.debug(trackerName + ": Status -" +
                  " running(m) = " + runningMaps + 
                  " unassigned(m) = " + unassignedMaps + 
                  " commit_pending(m) = " + commitPendingMaps +
                  " misc(m) = " + miscMaps +
                  " running(r) = " + runningReduces + 
                  " unassigned(r) = " + unassignedReduces + 
                  " commit_pending(r) = " + commitPendingReduces +
                  " misc(r) = " + miscReduces); 
      }

      // 如果TaskTracker是第一次汇报状态报告，还需更新uniqueHostsMap
      if (!alreadyPresent)  {
        Integer numTaskTrackersInHost = 
          uniqueHostsMap.get(status.getHost());
        if (numTaskTrackersInHost == null) {
          numTaskTrackersInHost = 0;
        }
        numTaskTrackersInHost ++;
        uniqueHostsMap.put(status.getHost(), numTaskTrackersInHost);
      }
    }
    getInstrumentation().setMapSlots(totalMapTaskCapacity);
    getInstrumentation().setReduceSlots(totalReduceTaskCapacity);
    return oldStatus != null;
  }
  
  // Increment the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void incrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots += reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots += reservedSlots;
    }
  }

  // Decrement the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void decrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots -= reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots -= reservedSlots;
    }
  }

  private void updateNodeHealthStatus(TaskTrackerStatus trackerStatus,
                                      long timeStamp) {
    TaskTrackerHealthStatus status = trackerStatus.getHealthStatus();
    synchronized (faultyTrackers) {
      faultyTrackers.setNodeHealthStatus(trackerStatus.getHost(), 
          status.isNodeHealthy(), status.getHealthReport(), timeStamp);
    }
  }
    
  /**
   * Process incoming heartbeat messages from the task trackers.
   */
  private synchronized boolean processHeartbeat(
                                 TaskTrackerStatus trackerStatus, 
                                 boolean initialContact,
                                 long timeStamp) throws UnknownHostException {

    getInstrumentation().heartbeat();

    String trackerName = trackerStatus.getTrackerName();

    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        // 根据该TT的上一次心跳发送的状态信息更新JT的一些信息，如totalMaps,totalReduces,occupiedMapSlots,occupiedReduceSlots等，接着根据本次心跳发送的TT状态信息再次更新这些变量
        // 返回值根据 is old taskTrackerStatus found? 存在返回true，不存在返回false
        boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                     trackerStatus);

        TaskTracker taskTracker = getTaskTracker(trackerName);
        if (initialContact) {
          // 如果该TT是首次连接JT，且存在oldStatus，则表明JT丢失了TT，具体意思应该是JT在一段时间内与TT失去了联系，之后TT恢复了，所以发送心跳时显示首次连接。
          // If it's first contact, then clear out 
          // any state hanging around
          if (seenBefore) {
            // lostTaskTracker(taskTracker)：会将该TT从所有的队列中移除，并将该TT上记录的job清除掉(kill掉)，当然对那些已经完成的Job不会进行次操作。
            lostTaskTracker(taskTracker);
          }
        } else {
          // If not first contact, there should be some record of the tracker
          // 当TT不是首次连接到JT，但是JT却没有该TT的历史status信息，则表示JT对该TT未知，所以重新更新TaskTracker状态信息。
          if (!seenBefore) {
            LOG.warn("Status from unknown Tracker : " + trackerName);
            updateTaskTrackerStatus(trackerName, null);
            return false;
          }
        }

        if (initialContact) {
          // if this is lost tracker that came back now, and if it's blacklisted
          // increment the count of blacklisted trackers in the cluster
          // 如果初次链接JobTracker且包含在黑名单中，则increment the count of blacklisted trackers，然后加入trackerExpiryQueue和hostnameToTaskTracker
          if (isBlacklisted(trackerName)) {
            faultyTrackers.incrBlacklistedTrackers(1);
          }
          // This could now throw an UnknownHostException but only if the
          // TaskTracker status itself has an invalid name
          // JobTracker将刚刚初始化的TaskTracker的TaskTrackerStatus对象放到过期队列：trackerExpiryQueue中(线程ExpireTrackers Thread会周期性的扫描队列trackerExpiryQueue)，并将其加入网络拓扑结构中。
          // TaskTrackerStatus对象中有TaskTracker最近的心跳时间（TaskTrackerStatus.lastSeen)
          addNewTracker(taskTracker);
        }
      }
    }

    // 更新TaskTracker上所有Task状态
    updateTaskStatuses(trackerStatus);
    // 更新NodeHealth信息
    updateNodeHealthStatus(trackerStatus, timeStamp);
    
    return true;
  }

  /**
   * A tracker wants to know if any of its Tasks have been
   * closed (because the job completed, whether successfully or not)
   */
  private synchronized List<TaskTrackerAction> getTasksToKill(
                                                              String taskTracker) {
    
    Set<TaskAttemptID> taskIds = trackerToTaskMap.get(taskTracker);
    List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
    if (taskIds != null) {
      for (TaskAttemptID killTaskId : taskIds) {
        TaskInProgress tip = taskidToTIPMap.get(killTaskId);
        if (tip == null) {
          continue;
        }
        if (tip.shouldClose(killTaskId)) {
          // 
          // This is how the JobTracker ends a task at the TaskTracker.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (!tip.getJob().isComplete()) {
            killList.add(new KillTaskAction(killTaskId));
            if (LOG.isDebugEnabled()) {
              LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
            }
          }
        }
      }
    }
    
    // add the stray attempts for uninited jobs
    // trackerToTasksToCleanup中该taskTracker需要清理的Task
    synchronized (trackerToTasksToCleanup) {
      Set<TaskAttemptID> set = trackerToTasksToCleanup.remove(taskTracker);
      if (set != null) {
        for (TaskAttemptID id : set) {
          killList.add(new KillTaskAction(id));
        }
      }
    }
    return killList;
  }

  /**
   * Add a job to cleanup for the tracker.
   */
  private void addJobForCleanup(JobID id) {
    for (String taskTracker : taskTrackers.keySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Marking job " + id + " for cleanup by tracker " + taskTracker);
      }
      synchronized (trackerToJobsToCleanup) {
        Set<JobID> jobsToKill = trackerToJobsToCleanup.get(taskTracker);
        if (jobsToKill == null) {
          jobsToKill = new HashSet<JobID>();
          trackerToJobsToCleanup.put(taskTracker, jobsToKill);
        }
        jobsToKill.add(id);
      }
    }
  }
  
  /**
   * A tracker wants to know if any job needs cleanup because the job completed.
   */
  private List<TaskTrackerAction> getJobsForCleanup(String taskTracker) {
    Set<JobID> jobs = null;
    // 获取trackerToJobsToCleanup中对应此tasktracker的所有jobs，封装成KillJobAction，加入actions中。
    synchronized (trackerToJobsToCleanup) {
      jobs = trackerToJobsToCleanup.remove(taskTracker);
    }
    if (jobs != null) {
      // prepare the actions list
      List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
      for (JobID killJobId : jobs) {
        killList.add(new KillJobAction(killJobId));
        if(LOG.isDebugEnabled()) {
          LOG.debug(taskTracker + " -> KillJobAction: " + killJobId);
        }
      }

      return killList;
    }
    return null;
  }

  /**
   * A tracker wants to know if any of its Tasks can be committed 
   */
  private synchronized List<TaskTrackerAction> getTasksToSave(
                                                 TaskTrackerStatus tts) {
    // 检查tasktracker的所有的task中状态等于TaskStatus.State.COMMIT_PENDING的，封装成CommitTaskAction，加入actions中。表示这个task的输出可以保存。
    List<TaskStatus> taskStatuses = tts.getTaskReports();
    if (taskStatuses != null) {
      List<TaskTrackerAction> saveList = new ArrayList<TaskTrackerAction>();
      for (TaskStatus taskStatus : taskStatuses) {
        if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING) {
          TaskAttemptID taskId = taskStatus.getTaskID();
          TaskInProgress tip = taskidToTIPMap.get(taskId);
          if (tip == null) {
            continue;
          }
          if (tip.shouldCommit(taskId)) {
            saveList.add(new CommitTaskAction(taskId));
            if (LOG.isDebugEnabled()) {
              LOG.debug(tts.getTrackerName() + 
                      " -> CommitTaskAction: " + taskId);
            }
          }
        }
      }
      return saveList;
    }
    return null;
  }
  
  // returns cleanup tasks first, then setup tasks.
  synchronized List<Task> getSetupAndCleanupTasks(
    TaskTrackerStatus taskTracker) throws IOException {
    
    // Don't assign *any* new task in safemode
    // 如果集群处于safe模式，则不分配任务
    if (isInSafeMode()) {
      return null;
    }

    // 计算TT的最大map/reduce slot，以及已占用的map/reduce slot，以及集群可使用的TT数量，和集群的host数量。
    int maxMapTasks = taskTracker.getMaxMapSlots();
    int maxReduceTasks = taskTracker.getMaxReduceSlots();
    int numMaps = taskTracker.countOccupiedMapSlots();
    int numReduces = taskTracker.countOccupiedReduceSlots();
    int numTaskTrackers = getClusterStatus().getTaskTrackers();
    int numUniqueHosts = getNumberOfUniqueHosts();

    Task t = null;
    synchronized (jobs) {
      if (numMaps < maxMapTasks) {
        // 首先获取Job的 map类型的cleanup task，每个Job有两个Cleanup任务(cleanup = new TaskInProgress[2])，cleanup[0]是map task, cleanup[1]是reduce task。
        // 这里实际是取cleanup[0]
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          // 进入方法obtainJobCleanupTask
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        // 然后从job(JobInProgress)的mapCleanupTasks中取第一个需要进行进行清理操作的map task（取出后该task会从mapCleanupTasks中移除）。
        // mapCleanupTasks中存放的是运行状态为FAILED_UNCLEAN/KILLED_UNCLEAN的task
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        // 然后在获取Job的 map类型的setup task，每个Job有两个setup task(setup = new TaskInProgress[2])，setup[0]是map task, setup[1]是reduce task。
        // 这里实际是取setup[0]
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                  numUniqueHosts, true);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
      // 上面这三个全部是获取的map任务，而下面是获取reduce任务，方法基本一样。
      if (numReduces < maxReduceTasks) {
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainTaskCleanupTask(taskTracker, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
        for (Iterator<JobInProgress> it = jobs.values().iterator();
             it.hasNext();) {
          JobInProgress job = it.next();
          t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                    numUniqueHosts, false);
          if (t != null) {
            return Collections.singletonList(t);
          }
        }
      }
    }
    return null;
  }

  /**
   * Grab the local fs name
   */
  public synchronized String getFilesystemName() throws IOException {
    if (fs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return fs.getUri().toString();
  }

  /**
   * Returns a handle to the JobTracker's Configuration
   */
  public JobConf getConf() {
    return conf;
  }
  
  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) throws IOException {
    LOG.warn("Report from " + taskTracker + ": " + errorMessage);        
  }

  /**
   * Remove the job_ from jobids to get the unique string.
   */
  static String getJobUniqueString(String jobid) {
    return jobid.substring(4);
  }

  ////////////////////////////////////////////////////
  // JobSubmissionProtocol
  ////////////////////////////////////////////////////

  /**
   * Allocates a new JobId string.
   */
  public synchronized JobID getNewJobId() throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();

    // getTrackerIdentifier方法获取属性trackerIdentifier的值，JobTracker在调用main方法进行初始化时会调用generateNewIdentifier方法设置trackerIdentifier的值
    // trackerIdentifier的值是对new Date()格式化(yyyyMMddHHmm)的字符串(new Date()实际为启动JobTracker的时间)， 这样就构造了JobID实例，JobID的toString方法是 job_jtIdentifier_0001
    // 'job'是固定值，两个'_'是固定的分隔符，jtIdentifier就是传入的yyyyMMddHHmm格式的字符串，0001是将nextJobId格式化成4位，不足在前面补0，但是当nextJobId大于9999后，就会直接显示该数字，
    // 比如nextJobId=10001,则jobid可能为：job_200912121733_10001
    return new JobID(getTrackerIdentifier(), nextJobId++);
  }
  
  /**
   * JobTracker.submitJob() kicks off a new job.  
   *
   * Create a 'JobInProgress' object, which contains both JobProfile
   * and JobStatus.  Those two sub-objects are sometimes shipped outside
   * of the JobTracker.  But JobInProgress adds info that's useful for
   * the JobTracker alone.
   */
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
      throws IOException {
    // Check for JobTracker operational state
    // 检查JobTracker进程是否启动, JobTracker是在hadoop集群启动的时候启动的.
    checkJobTrackerState();
    
    return submitJob(jobId, jobSubmitDir, null, ts, false);
  }

  /**
   * JobTracker.submitJob() kicks off a new job.
   * 
   * Create a 'JobInProgress' object, which contains both JobProfile and
   * JobStatus. Those two sub-objects are sometimes shipped outside of the
   * JobTracker. But JobInProgress adds info that's useful for the JobTracker
   * alone.
   * @return null if the job is being recovered but mapred.job.restart.recover
   * is false.
   */
  JobStatus submitJob(JobID jobId, String jobSubmitDir,
      UserGroupInformation ugi, Credentials ts, boolean recovered)
      throws IOException {
    // Check for safe-mode
    // 检查是否在安全模式, 在安全模式则抛出异常.
    checkSafeMode();
    
    JobInfo jobInfo = null;
    if (ugi == null) {
      ugi = UserGroupInformation.getCurrentUser();
    }
    synchronized (this) {
      if (jobs.containsKey(jobId)) {
        // job already running, don't start twice
        return jobs.get(jobId).getStatus();
      }
      // 生成jobInfo对象
      jobInfo = new JobInfo(jobId, new Text(ugi.getShortUserName()),
          new Path(jobSubmitDir));
    }
    
    // Store the job-info in a file so that the job can be recovered
    // later (if at all)
    // Note: jobDir & jobInfo are owned by JT user since we are using
    // his fs object
    // 判断job是否可以被recovered, 默认可以, 将jobInfo对象序列化到job-info文件中
    if (!recovered) {
      // 获取保存jobInfo的文件路径: ${mapred.system.dir}/jobId,
      // ${mapred.system.dir}的默认值为：/tmp/hadoop/mapred/system
      // job-info信息保存在 ${mapred.system.dir}/jobId/job-info
      Path jobDir = getSystemDirectoryForJob(jobId);
      FileSystem.mkdirs(fs, jobDir, new FsPermission(SYSTEM_DIR_PERMISSION));
      FSDataOutputStream out = fs.create(getSystemFileForJob(jobId));
      // 将JobInfo结构对应的数据写入到Job文件,具体写入内容查看write方法
      jobInfo.write(out);
      out.close();
    }

    // Create the JobInProgress, do not lock the JobTracker since
    // we are about to copy job.xml from HDFS and write jobToken file to HDFS
    JobInProgress job = null;
    try {
      if (ts == null) {
        ts = new Credentials();
      }
      // 将job tokens写入文件: ${mapred.system.dir}/jobId/jobToken
      generateAndStoreJobTokens(jobId, ts);
      // 为job实例化一个JobInProgress对象, 这个对象将会对job以后的所有情况进行负责，如初始化，执行等
      // client端提交的每一个job都会封装一个对应的JobInProgress对象, 通过new JobInProgress构造的job初始状态是JobStatus.PREP
      // JobTracker通过维护Map<JobID, JobInProgress> jobs来维护各个job的整个生命周期(创建执行清理)
      job = new JobInProgress(this, this.conf, jobInfo, 0, ts);
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    if (recovered && 
        !job.getJobConf().getBoolean(
            JobConf.MAPREDUCE_RECOVER_JOB, 
            JobConf.DEFAULT_MAPREDUCE_RECOVER_JOB)) {
      LOG.info("Job "+ jobId.toString() + " is not enable for recovery, cleaning up job files");
      job.cleanupJob();
      return null;
    }
    
    synchronized (this) {
      // check if queue is RUNNING
      // 获取job的队列信息, 并判断相应的队列是否在运行中，没有运行的话则任务失败。
      String queue = job.getProfile().getQueueName();
      if (!queueManager.isRunning(queue)) {
        throw new IOException("Queue \"" + queue + "\" is not running");
      }
      try {
        aclsManager.checkAccess(job, ugi, Operation.SUBMIT_JOB);
      } catch (IOException ioe) {
        LOG.warn("Access denied for user " + job.getJobConf().getUser()
            + ". Ignoring job " + jobId, ioe);
        job.fail();
        throw ioe;
      }

      // Check the job if it cannot run in the cluster because of invalid memory
      // requirements.
      // 检查内存情况
      try {
        checkMemoryRequirements(job);
      } catch (IOException ioe) {
        throw ioe;
      }

      try {
        // 检查任务提交情况, 如果是默认的JobQueueTaskScheduler, 则该方法中没有进行任何操作.
        this.taskScheduler.checkJobSubmission(job);
      } catch (IOException ioe){
        LOG.error("Problem in submitting job " + jobId, ioe);
        throw ioe;
      }

      // Submit the job
      // 提交job
      JobStatus status;
      try {
        // 将job(JobInProgress)加入到JobTracker的Map<JobID, JobInProgress> jobs中
        // 并设置job的Listener, Listener是TaskScheduler中创建的,
        // 该方法中会为job添加JobInProgressListener并触发listener执行
        status = addJob(jobId, job);
      } catch (IOException ioe) {
        LOG.info("Job " + jobId + " submission failed!", ioe);
        status = job.getStatus();
        status.setFailureInfo(StringUtils.stringifyException(ioe));
        failJob(job);
        throw ioe;
      }
      return status;
    }
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getStagingAreaDir()
   */
  public String getStagingAreaDir() throws IOException {
    // Check for safe-mode
    checkSafeMode();

    try{
      final String user =
        UserGroupInformation.getCurrentUser().getShortUserName();
      return getMROwner().doAs(new PrivilegedExceptionAction<String>() {
        @Override
        public String run() throws Exception {
          // 调用getStagingAreaDirInternal 方法
          return getStagingAreaDirInternal(user);
        }
      });
    } catch(InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  private String getStagingAreaDirInternal(String user) throws IOException {
    final Path stagingRootDir =
      new Path(conf.get("mapreduce.jobtracker.staging.root.dir",
            "/tmp/hadoop/mapred/staging"));
    final FileSystem fs = stagingRootDir.getFileSystem(conf);
    // 返回值是： ${mapreduce.jobtracker.staging.root.dir}/${user}/.staging/
    return fs.makeQualified(new Path(stagingRootDir,
                              user+"/.staging")).toString();
  }

  /**
   * Adds a job to the jobtracker. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * @param jobId The id for the job submitted which needs to be added
   */
  private synchronized JobStatus addJob(JobID jobId, JobInProgress job) 
  throws IOException {
    totalSubmissions++;

    synchronized (jobs) {
      synchronized (taskScheduler) {
        jobs.put(job.getProfile().getJobID(), job);
        // 这里的JobInProgressListener对象就是相应的taskScheduler的JobListener，这里为job添加了JobListener.
        // jobAdded方法是一个trigger
        // 对应默认的JobQueueTaskScheduler, 包含JobQueueJobInProgressListener和EagerTaskInitializationListener
        // 所以分别调用这两个类的jobAdded() 方法, 这两个listener会把job加入到两个队列中
        // EagerTaskInitializationListener把job加入到队列之后会触发job的初始化操作，具体见里面的实现
        for (JobInProgressListener listener : jobInProgressListeners) {
          listener.jobAdded(job);
        }
      }
    }
    // 计数器加一
    myInstrumentation.submitJob(job.getJobConf(), jobId);
    job.getQueueMetrics().submitJob(job.getJobConf(), jobId);

    LOG.info("Job " + jobId + " added successfully for user '" 
             + job.getJobConf().getUser() + "' to queue '" 
             + job.getJobConf().getQueueName() + "'");
    AuditLogger.logSuccess(job.getUser(), 
        Operation.SUBMIT_JOB.name(), jobId.toString());
    return job.getStatus();
  }

  /**
   * Are ACLs for authorization checks enabled on the JT?
   * 
   * @return
   */
  boolean areACLsEnabled() {
    return conf.getBoolean(JobConf.MR_ACLS_ENABLED, false);
  }

  /**@deprecated use {@link #getClusterStatus(boolean)}*/
  @Deprecated
  public synchronized ClusterStatus getClusterStatus() {
    return getClusterStatus(false);
  }

  public synchronized ClusterStatus getClusterStatus(boolean detailed) {
    synchronized (taskTrackers) {
      if (detailed) {
        List<List<String>> trackerNames = taskTrackerNames();
        return new ClusterStatus(trackerNames.get(0),
            trackerNames.get(1),
            trackerNames.get(2),
            TASKTRACKER_EXPIRY_INTERVAL,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity, 
            state, getExcludedNodes().size()
            );
      } else {
        return new ClusterStatus(
            // active trackers include graylisted but not blacklisted ones:
            taskTrackers.size() - getBlacklistedTrackerCount(),
            getBlacklistedTrackerCount(),
            getGraylistedTrackerCount(),
            TASKTRACKER_EXPIRY_INTERVAL,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity, 
            state, getExcludedNodes().size());          
      }
    }
  }

  public synchronized ClusterMetrics getClusterMetrics() {
    return new ClusterMetrics(totalMaps,
      totalReduces, occupiedMapSlots, occupiedReduceSlots,
      reservedMapSlots, reservedReduceSlots,
      totalMapTaskCapacity, totalReduceTaskCapacity,
      totalSubmissions,
      taskTrackers.size() - getBlacklistedTrackerCount(), 
      getBlacklistedTrackerCount(), getGraylistedTrackerCount(),
      getExcludedNodes().size()) ;
  }

  /**
   * @see JobSubmissionProtocol#killJob
   */
  public synchronized void killJob(JobID jobid) throws IOException {
    if (null == jobid) {
      LOG.info("Null jobid object sent to JobTracker.killJob()");
      return;
    }
    
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    // No 'killJob' in safe-mode
    checkSafeMode();
    
    JobInProgress job = jobs.get(jobid);
    
    if (null == job) {
      LOG.info("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }
        
    // check both queue-level and job-level access
    aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
        Operation.KILL_JOB);

    killJob(job);
  }
  
  private synchronized void killJob(JobInProgress job) {
    LOG.info("Killing job " + job.getJobID());
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    job.kill();
    
    // Inform the listeners if the job is killed
    // Note : 
    //   If the job is killed in the PREP state then the listeners will be 
    //   invoked
    //   If the job is killed in the RUNNING state then cleanup tasks will be 
    //   launched and the updateTaskStatuses() will take care of it
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()
        && newStatus.getRunState() == JobStatus.KILLED) {
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
            newStatus);
      updateJobInProgressListeners(event);
    }
  }
  /**
   * Discard a current delegation token.
   */ 
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                       ) throws IOException,
                                                InterruptedException {
    String user = UserGroupInformation.getCurrentUser().getUserName();
    secretManager.cancelToken(token, user);
  }  
  /**
   * Get a new delegation token.
   */ 
  @Override
  public Token<DelegationTokenIdentifier> 
     getDelegationToken(Text renewer
                        )throws IOException, InterruptedException {
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be issued only with kerberos authentication");
    }
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Text owner = new Text(ugi.getUserName());
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }  
    DelegationTokenIdentifier ident =  
      new DelegationTokenIdentifier(owner, renewer, realUser);
    return new Token<DelegationTokenIdentifier>(ident, secretManager);
  }  
  /**
   * Renew a delegation token to extend its lifetime.
   */ 
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                      ) throws IOException,
                                               InterruptedException {
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be issued only with kerberos authentication");
    }
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    return secretManager.renewToken(token, user);
  }  

  // 提交job后, Listener会调用该方法初始化Job
  public void initJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Init on null job is not valid");
      return;
    }
	        
    try {
      JobStatus prevStatus = (JobStatus)job.getStatus().clone();
      LOG.info("Initializing " + job.getJobID());
      // 调用JobInProgress的initTasks方法初始化该Job对应的Tasks, 即创建map和reduce task
      job.initTasks();
      // Inform the listeners if the job state has changed
      // Note : that the job will be in PREP state.
      JobStatus newStatus = (JobStatus)job.getStatus().clone();
      if (prevStatus.getRunState() != newStatus.getRunState()) {
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
              newStatus);
        synchronized (JobTracker.this) {
          // 更新Job相关队列的状态
          updateJobInProgressListeners(event);
        }
      }
    } catch (KillInterruptedException kie) {
      //   If job was killed during initialization, job state will be KILLED
      LOG.error("Job initialization interrupted:\n" +
          StringUtils.stringifyException(kie));
      killJob(job);
    } catch (Throwable t) {
      String failureInfo = 
        "Job initialization failed:\n" + StringUtils.stringifyException(t);
      // If the job initialization is failed, job state will be FAILED
      LOG.error(failureInfo);
      job.getStatus().setFailureInfo(failureInfo);
      failJob(job);
    }
  }

  /**
   * Fail a job and inform the listeners. Other components in the framework 
   * should use this to fail a job.
   */
  public synchronized void failJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Fail on null job is not valid");
      return;
    }
         
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    LOG.info("Failing job " + job.getJobID());
    job.fail();
     
    // Inform the listeners if the job state has changed
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()) {
      JobStatusChangeEvent event = 
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus, 
            newStatus);
      updateJobInProgressListeners(event);
    }
  }
  
  public synchronized void setJobPriority(JobID jobid, 
                                          String priority)
                                          throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    JobInProgress job = jobs.get(jobid);
    if (null == job) {
        LOG.info("setJobPriority(): JobId " + jobid.toString()
            + " is not a valid job");
        return;
    }

    JobPriority newPriority = JobPriority.valueOf(priority);
    setJobPriority(jobid, newPriority);
  }
                           
  void storeCompletedJob(JobInProgress job) {
    //persists the job info in DFS
    completedJobStatusStore.store(job);
  }

  /**
   * Check if the <code>job</code> has been initialized.
   * 
   * @param job {@link JobInProgress} to be checked
   * @return <code>true</code> if the job has been initialized,
   *         <code>false</code> otherwise
   */
  private boolean isJobInited(JobInProgress job) {
    return job.inited(); 
  }
  
  public JobProfile getJobProfile(JobID jobid) throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getProfile while holding the lock
        // on the JobTracker since it isn't a synchronized method
        return job.getProfile();
      }  else {
        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          return info.profile;
        }
      }
    }
    return completedJobStatusStore.readJobProfile(jobid);
  }
  
  public JobStatus getJobStatus(JobID jobid) throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    if (null == jobid) {
      LOG.warn("JobTracker.getJobStatus() cannot get status for null jobid");
      return null;
    }
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getStatus while holding the lock
        // on the JobTracker since it isn't a synchronized method
        return job.getStatus();
      } else {
        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          return info.status;
        }
      }
    }
    return completedJobStatusStore.readJobStatus(jobid);
  }
  
  private static final Counters EMPTY_COUNTERS = new Counters();
  public Counters getJobCounters(JobID jobid) throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    UserGroupInformation callerUGI = UserGroupInformation.getCurrentUser();
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {

        // check the job-access
        aclsManager.checkAccess(job, callerUGI, Operation.VIEW_JOB_COUNTERS);
        Counters counters = new Counters();
        if (isJobInited(job)) {
          boolean isFine = job.getCounters(counters);
          if (!isFine) {
            throw new IOException("Counters Exceeded limit: " + 
                Counters.MAX_COUNTER_LIMIT);
          }
          return counters;
        }
        else {
          return EMPTY_COUNTERS;
        }
      } else {
        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          return info.counters;
        }
      }
    }

    return completedJobStatusStore.readCounters(jobid);
  }
  
  private static final TaskReport[] EMPTY_TASK_REPORTS = new TaskReport[0];
  
  public synchronized TaskReport[] getMapTaskReports(JobID jobid)
      throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeMapTasks =
        job.reportTasksInProgress(true, true);
      for (Iterator it = completeMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteMapTasks =
        job.reportTasksInProgress(true, false);
      for (Iterator it = incompleteMapTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  public synchronized TaskReport[] getReduceTaskReports(JobID jobid)
      throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector completeReduceTasks = job.reportTasksInProgress(false, true);
      for (Iterator it = completeReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector incompleteReduceTasks = job.reportTasksInProgress(false, false);
      for (Iterator it = incompleteReduceTasks.iterator(); it.hasNext();) {
        TaskInProgress tip = (TaskInProgress) it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }

  public synchronized TaskReport[] getCleanupTaskReports(JobID jobid)
      throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportCleanupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip = it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportCleanupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip = it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  
  }
  
  public synchronized TaskReport[] getSetupTaskReports(JobID jobid)
      throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    JobInProgress job = jobs.get(jobid);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job == null || !isJobInited(job)) {
      return EMPTY_TASK_REPORTS;
    } else {
      Vector<TaskReport> reports = new Vector<TaskReport>();
      Vector<TaskInProgress> completeTasks = job.reportSetupTIPs(true);
      for (Iterator<TaskInProgress> it = completeTasks.iterator();
           it.hasNext();) {
        TaskInProgress tip =  it.next();
        reports.add(tip.generateSingleReport());
      }
      Vector<TaskInProgress> incompleteTasks = job.reportSetupTIPs(false);
      for (Iterator<TaskInProgress> it = incompleteTasks.iterator(); 
           it.hasNext();) {
        TaskInProgress tip =  it.next();
        reports.add(tip.generateSingleReport());
      }
      return reports.toArray(new TaskReport[reports.size()]);
    }
  }
  
  public static final String MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY =
      "mapred.cluster.map.memory.mb";
  public static final String MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY =
      "mapred.cluster.reduce.memory.mb";

  public static final String MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY =
      "mapred.cluster.max.map.memory.mb";
  public static final String MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY =
      "mapred.cluster.max.reduce.memory.mb";

  /* 
   * Returns a list of TaskCompletionEvent for the given job, 
   * starting from fromEventId.
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getTaskCompletionEvents(java.lang.String, int, int)
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(
      JobID jobid, int fromEventId, int maxEvents) throws IOException{
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    JobInProgress job = this.jobs.get(jobid);
      
    if (null != job) {
      return isJobInited(job) ? 
          job.getTaskCompletionEvents(fromEventId, maxEvents) : 
          TaskCompletionEvent.EMPTY_ARRAY;
    }

    return completedJobStatusStore.readJobTaskCompletionEvents(jobid, 
                                                               fromEventId, 
                                                               maxEvents);
  }

  private static final String[] EMPTY_TASK_DIAGNOSTICS = new String[0];
  /**
   * Get the diagnostics for a given task
   * @param taskId the id of the task
   * @return an array of the diagnostic messages
   */
  public synchronized String[] getTaskDiagnostics(TaskAttemptID taskId)  
    throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    List<String> taskDiagnosticInfo = null;
    JobID jobId = taskId.getJobID();
    TaskID tipId = taskId.getTaskID();
    JobInProgress job = jobs.get(jobId);
    if (job != null) {
      // Check authorization
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.VIEW_JOB_DETAILS);
    }
    if (job != null && isJobInited(job)) {
      TaskInProgress tip = job.getTaskInProgress(tipId);
      if (tip != null) {
        taskDiagnosticInfo = tip.getDiagnosticInfo(taskId);
      }
      
    }
    
    return ((taskDiagnosticInfo == null) ? EMPTY_TASK_DIAGNOSTICS :
             taskDiagnosticInfo.toArray(new String[taskDiagnosticInfo.size()]));
  }
    
  /** Get all the TaskStatuses from the tipid. */
  TaskStatus[] getTaskStatuses(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? new TaskStatus[0] 
            : tip.getTaskStatuses());
  }

  /** Returns the TaskStatus for a particular taskid. */
  TaskStatus getTaskStatus(TaskAttemptID taskid) {
    TaskInProgress tip = getTip(taskid.getTaskID());
    return (tip == null ? null 
            : tip.getTaskStatus(taskid));
  }
    
  /**
   * Returns the counters for the specified task in progress.
   */
  Counters getTipCounters(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? null : tip.getCounters());
  }

  /**
   * Returns the configured task scheduler for this job tracker.
   * @return the configured task scheduler
   */
  TaskScheduler getTaskScheduler() {
    return taskScheduler;
  }
  
  /**
   * Returns specified TaskInProgress, or null.
   */
  public TaskInProgress getTip(TaskID tipid) {
    JobInProgress job = jobs.get(tipid.getJobID());
    return (job == null ? null : job.getTaskInProgress(tipid));
  }
    
  /**
   * @see JobSubmissionProtocol#killTask(TaskAttemptID, boolean)
   */
  public synchronized boolean killTask(TaskAttemptID taskid, boolean shouldFail)
      throws IOException {
    // Check for JobTracker operational state
    checkJobTrackerState();
    
    // No 'killTask' in safe-mode
    checkSafeMode();

    TaskInProgress tip = taskidToTIPMap.get(taskid);
    if(tip != null) {
      // check both queue-level and job-level access
      aclsManager.checkAccess(tip.getJob(),
          UserGroupInformation.getCurrentUser(),
          shouldFail ? Operation.FAIL_TASK : Operation.KILL_TASK);

      return tip.killTask(taskid, shouldFail);
    }
    else {
      LOG.info("Kill task attempt failed since task " + taskid + " was not found");
      return false;
    }
  }
  
  /**
   * Get tracker name for a given task id.
   * @param taskId the name of the task
   * @return The name of the task tracker
   */
  public synchronized String getAssignedTracker(TaskAttemptID taskId) {
    return taskidToTrackerMap.get(taskId);
  }
    
  public JobStatus[] jobsToComplete() {
    return getJobStatus(jobs.values(), true);
  } 
  /**
   * @see JobSubmissionProtocol#getAllJobs()
   */
  public JobStatus[] getAllJobs() {
    List<JobStatus> list = new ArrayList<JobStatus>();
    list.addAll(Arrays.asList(getJobStatus(jobs.values(),false)));
    list.addAll(retireJobs.getAllJobStatus());
    return list.toArray(new JobStatus[list.size()]);
  }
    
  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    // Might not be initialized yet, TT handles this
    if (isInSafeMode()) {
      return null;
    }
    
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getQueueAdmins(String)
   */
  public AccessControlList getQueueAdmins(String queueName) throws IOException {
    AccessControlList acl =
        queueManager.getQueueACL(queueName, QueueACL.ADMINISTER_JOBS);
    if (acl == null) {
      acl = new AccessControlList(" ");
    }
    return acl;
  }

  ///////////////////////////////////////////////////////////////
  // JobTracker methods
  ///////////////////////////////////////////////////////////////
  public JobInProgress getJob(JobID jobid) {
    return jobs.get(jobid);
  }

  // Get the job directory in system directory
  Path getSystemDirectoryForJob(JobID id) {
    return new Path(getSystemDir(), id.toString());
  }
  
  //Get the job token file in system directory
  Path getSystemFileForJob(JobID id) {
    return new Path(getSystemDirectoryForJob(id), JOB_INFO_FILE);
  }

  //Get the job token file in system directory
  Path getTokenFileForJob(JobID id) {
    return new Path(
        getSystemDirectoryForJob(id), TokenCache.JOB_TOKEN_HDFS_FILE);
  }
  
  /**
   * Change the run-time priority of the given job.
   * 
   * @param jobId job id
   * @param priority new {@link JobPriority} for the job
   * @throws IOException
   * @throws AccessControlException
   */
  synchronized void setJobPriority(JobID jobId, JobPriority priority)
      throws AccessControlException, IOException {
    JobInProgress job = jobs.get(jobId);
    if (job != null) {

      // check both queue-level and job-level access
      aclsManager.checkAccess(job, UserGroupInformation.getCurrentUser(),
          Operation.SET_JOB_PRIORITY);

      synchronized (taskScheduler) {
        JobStatus oldStatus = (JobStatus)job.getStatus().clone();
        job.setPriority(priority);
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        JobStatusChangeEvent event = 
          new JobStatusChangeEvent(job, EventType.PRIORITY_CHANGED, oldStatus, 
                                   newStatus);
        updateJobInProgressListeners(event);
      }
    } else {
      LOG.warn("Trying to change the priority of an unknown job: " + jobId);
    }
  }
  
  ////////////////////////////////////////////////////
  // Methods to track all the TaskTrackers
  ////////////////////////////////////////////////////
  /**
   * Accept and process a new TaskTracker profile.  We might
   * have known about the TaskTracker previously, or it might
   * be brand-new.  All task-tracker structures have already
   * been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  // 更新TaskTracker上所有Task状态
  void updateTaskStatuses(TaskTrackerStatus status) {
    String trackerName = status.getTrackerName();
    // 获取该TaskTracker上所有的Task的TaskStatus, 注意这个status是TaskTracker通过heartbeat发送过来的
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(trackerName);
      TaskAttemptID taskId = report.getTaskID();
      
      // don't expire the task if it is not unassigned
      // 如果一个Task的运行状态不为TaskStatus.State.UNASSIGNED，说明该Task还没有在TaskTracker上获得运行机会，则并不让该Task失败,等待下一次被调度分配给TaskTracker去运行
      // （当一个Task指派给一个TaskTracker运行时，会首先在JobTracker端加入到一个超时列表中，由一个独立的线程JobTracker.ExpireLaunchingTasks去检测，该Task是否在给定的时间内（默认是10分钟 ）
      // 是否在TaskTracker上启动而且一直没有报告状态，如果没有报告，则会将该Task标记为失败）
      if (report.getRunState() != TaskStatus.State.UNASSIGNED) {
        // 从expireLaunchingTasks中移除该Task，即不让该Task失败,等待下一次被调度分配给TaskTracker去运行
        expireLaunchingTasks.removeTask(taskId);
      }

      // 根据Task的ID，获取到它对应的JobInProgress信息，
      JobInProgress job = getJob(taskId.getJobID());
      // 如果没有获取到则将该Task对应的JobInProgress对象加入到cleanup列表Map<String, Set<JobID>> trackerToJobsToCleanup中，直接返回继续处理下一个TaskStatus报告；
      if (job == null) {
        // if job is not there in the cleanup list ... add it
        synchronized (trackerToJobsToCleanup) {
          Set<JobID> jobs = trackerToJobsToCleanup.get(trackerName);
          if (jobs == null) {
            jobs = new HashSet<JobID>();
            trackerToJobsToCleanup.put(trackerName, jobs);
          }
          jobs.add(taskId.getJobID());
        }
        continue;
      }

      // 如果能够获取到对应的JobInProgress信息，则检查该JobInProgress中包含的Job是否设置初始化完成状态，如果没有设置，则直接将该Task加入到队列Map<String, Set<TaskAttemptID>> trackerToTasksToCleanup中，
      // 等待JobTracker调度Kill掉该Task，直接返回继续处理下一个TaskStatus报告。
      if (!job.inited()) {
        // if job is not yet initialized ... kill the attempt
        synchronized (trackerToTasksToCleanup) {
          Set<TaskAttemptID> tasks = trackerToTasksToCleanup.get(trackerName);
          if (tasks == null) {
            tasks = new HashSet<TaskAttemptID>();
            trackerToTasksToCleanup.put(trackerName, tasks);
          }
          tasks.add(taskId);
        }
        continue;
      }

      // 检查该TaskStatus报告中对应的TaskAttemptID（taskId），是否在JobTracker端存在对应的TaskInProgress对象
      TaskInProgress tip = taskidToTIPMap.get(taskId);
      // Check if the tip is known to the jobtracker. In case of a restarted
      // jt, some tasks might join in later
      if (tip != null || hasRestarted()) {
        if (tip == null) {
          // 很有可能JobTracker重启，内存中维护的Map<TaskAttemptID, TaskInProgress> taskidToTIPMap队列中没有TaskInProgress对象，这时JobInProgress对象一定存在，
          // 可以通过JobInProgress对象获取到该Task对应的TaskInProgress对象（因为在JobTracker端创建Job的时候，会分别创建4类TIP：map、reduce、cleanup、setup）
          tip = job.getTaskInProgress(taskId.getTaskID());
          // 将其加入到Map<TaskAttemptID, TaskInProgress> taskidToTIPMap队列中
          job.addRunningTaskToTIP(tip, taskId, status, false);
        }
        
        // Update the job and inform the listeners if necessary
        JobStatus prevStatus = (JobStatus)job.getStatus().clone();
        // Clone TaskStatus object here, because JobInProgress
        // or TaskInProgress can modify this object and
        // the changes should not get reflected in TaskTrackerStatus.
        // An old TaskTrackerStatus is used later in countMapTasks, etc.
        // 更新Task状态, 具体看该方法的实现
        job.updateTaskStatus(tip, (TaskStatus)report.clone());
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        
        // Update the listeners if an incomplete job completes
        // 触发已知的一组JobInProgressListener的jobUpdated方法，去更新Job状态。
        if (prevStatus.getRunState() != newStatus.getRunState()) {
          JobStatusChangeEvent event = 
            new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, 
                                     prevStatus, newStatus);
          updateJobInProgressListeners(event);
        }
      } else {
        LOG.info("Serious problem.  While updating status, cannot find taskid " 
                 + report.getTaskID());
      }
      
      // Process 'failed fetch' notifications
      // 根据TaskStatus能够获取到所有Fetch失败的Task，查询该Task对应的TaskInProgress对象，从而进一步通知JobInProgress对象，
      // 根据设定的允许Task Fetch失败的最大次数限制，确定是否要让该Task失败，并更新TaskInProgress状态。
      List<TaskAttemptID> failedFetchMaps = report.getFetchFailedMaps();
      if (failedFetchMaps != null) {
        for (TaskAttemptID mapTaskId : failedFetchMaps) {
          TaskInProgress failedFetchMap = taskidToTIPMap.get(mapTaskId);
          
          if (failedFetchMap != null) {
            // Gather information about the map which has to be failed, if need be
            String failedFetchTrackerName = getAssignedTracker(mapTaskId);
            if (failedFetchTrackerName == null) {
              failedFetchTrackerName = "Lost task tracker";
            }
            // 更新TaskInProgress状态
            failedFetchMap.getJob().fetchFailureNotification(failedFetchMap,
                                                             mapTaskId,
                                                             failedFetchTrackerName,
                                                             taskId,
                                                             trackerName);
          }
        }
      }
    }
  }

  /**
   * We lost the task tracker!  All task-tracker structures have 
   * already been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void lostTaskTracker(TaskTracker taskTracker) {
    String trackerName = taskTracker.getTrackerName();
    LOG.info("Lost tracker '" + trackerName + "'");
    
    // remove the tracker from the local structures
    // 从队列Map<String, Set<JobID>> trackerToJobsToCleanup中移除在该TaskTracker上已经完成且需要清理的所有Job。
    synchronized (trackerToJobsToCleanup) {
      trackerToJobsToCleanup.remove(trackerName);
    }

    // 从队列Map<String, Set<TaskAttemptID>> trackerToTasksToCleanup中移除在TaskTracker上已经运行完成且需要清理的所有Task。
    synchronized (trackerToTasksToCleanup) {
      trackerToTasksToCleanup.remove(trackerName);
    }
    
    // Inform the recovery manager
    // 通知Recovery Manager从其维护的Set<String>类型的恢复列表JobTracker.RecoveryManager.recoveredTrackers中移除该TaskTracker。
    recoveryManager.unMarkTracker(trackerName);
    
    Set<TaskAttemptID> lostTasks = trackerToTaskMap.get(trackerName);
    // 从TreeMap<String, Set<TaskAttemptID>> trackerToTaskMap中删除在该TaskTracker上运行的所有Task。
    trackerToTaskMap.remove(trackerName);

    if (lostTasks != null) {
      // List of jobs which had any of their tasks fail on this tracker
      Set<JobInProgress> jobsWithFailures = new HashSet<JobInProgress>();
      // 对在该TaskTracker上的运行的每一个Task（在队列trackerToTaskMap中），进行如下2步处理：
      for (TaskAttemptID taskId : lostTasks) {
        // 1. 从队列 taskidToTIPMap中取出TaskAttemptID对应的TaskInProgress tip结构，再根据tip获取到JobInProgress：JobInProgress job = tip.getJob();；
        TaskInProgress tip = taskidToTIPMap.get(taskId);
        JobInProgress job = tip.getJob();

        // Completed reduce tasks never need to be failed, because 
        // their outputs go to dfs
        // And completed maps with zero reducers of the job 
        // never need to be failed.
        // 2.如果tip标记Task没有完成，或者满足条件tip.isMapTask() && !tip.isJobSetupTask() && job.desiredReduces() != 0，
        if (!tip.isComplete() ||
            (tip.isMapTask() && !tip.isJobSetupTask() && 
             job.desiredReduces() != 0)) {
          // if the job is done, we don't want to change anything
          // 检查Job运行状态，当job.getStatus().getRunState() == JobStatus.RUNNING || job.getStatus().getRunState() == JobStatus.PREP成立时，则该Task运行失败，并更新Task状态，
          if (job.getStatus().getRunState() == JobStatus.RUNNING ||
              job.getStatus().getRunState() == JobStatus.PREP) {
            // the state will be KILLED_UNCLEAN, if the task(map or reduce) 
            // was RUNNING on the tracker
            TaskStatus.State killState = (tip.isRunningTask(taskId) && 
              !tip.isJobSetupTask() && !tip.isJobCleanupTask()) ? 
              TaskStatus.State.KILLED_UNCLEAN : TaskStatus.State.KILLED;
            // 更新Task状态
            job.failedTask(tip, taskId, ("Lost task tracker: " + trackerName), 
                           (tip.isMapTask() ? 
                               TaskStatus.Phase.MAP : 
                               TaskStatus.Phase.REDUCE), 
                            killState,
                            trackerName);
            // 同时收集这类Job，放入集合Set<JobInProgress> jobsWithFailures中，后续对这些Job进行处理；
            jobsWithFailures.add(job);
          }
        } else { // 如果ReduceTask已经完成，以及具有0个ReduceTask的所有MapTask已经完成，则将这些Task放入到队列 trackerToMarkedTasksMap中, 即else中markCompletedTaskAttempt(trackerName, taskId)方法；
          // Completed 'reduce' task and completed 'maps' with zero 
          // reducers of the job, not failed;
          // only removed from data-structures.
          markCompletedTaskAttempt(trackerName, taskId);
        }
      }
      
      // Penalize this tracker for each of the jobs which   
      // had any tasks running on it when it was 'lost' 
      // Also, remove any reserved slots on this tasktracker
      // 由于该TaskTracker被JobTracker标记为lost状态，则对上面收集到的jobsWithFailures集合中的Job，只要存在属于该Job的Task被分配到该TaskTracker上运行，
      // 会通过累加计算在该TaskTracker上失败的Task计数，给该TaskTracker以惩罚
      for (JobInProgress job : jobsWithFailures) {
        job.addTrackerTaskFailure(trackerName, taskTracker);
      }

      // Cleanup
      // 并释放所有在该TaskTracker上预留的Slot
      taskTracker.cancelAllReservations();

      // Purge 'marked' tasks, needs to be done  
      // here to prevent hanging references!
      // 从队列TreeMap<String, Set<TaskAttemptID>> trackerToMarkedTasksMap中移除所有被标记完成的Task，同时更新JobTracker内部维护的如下3个队列：
      // TreeMap<TaskAttemptID, String> taskidToTrackerMap、TreeMap<String, Set<TaskAttemptID>> trackerToTaskMap、Map<TaskAttemptID, TaskInProgress> taskidToTIPMap
      removeMarkedTasks(trackerName);
    }
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.
   */
  public synchronized void refreshNodes() throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    // check access
    if (!aclsManager.isMRAdmin(UserGroupInformation.getCurrentUser())) {
      AuditLogger.logFailure(user, Constants.REFRESH_NODES, 
          aclsManager.getAdminsAcl().toString(), Constants.JOBTRACKER, 
          Constants.UNAUTHORIZED_USER);
      throw new AccessControlException(user + 
                                       " is not authorized to refresh nodes.");
    }
    
    AuditLogger.logSuccess(user, Constants.REFRESH_NODES, Constants.JOBTRACKER);
    // call the actual api
    refreshHosts();
  }

  UserGroupInformation getMROwner() {
    return aclsManager.getMROwner();
  }

  private synchronized void refreshHosts() throws IOException {
    // Reread the config to get mapred.hosts and mapred.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    // 由hostsReader重新读取mapred.hosts以及mapred.hosts.exclude值，并添加到相应队列中，然后遍历现有的taskTrackers节点，
    // 把不在mapred.hosts或者在mapred.hosts.exclude中的节点从hostnameToTaskTracker中移除
    LOG.info("Refreshing hosts information");
    Configuration conf = new Configuration();

    hostsReader.updateFileNames(conf.get("mapred.hosts",""), 
                                conf.get("mapred.hosts.exclude", ""));
    hostsReader.refresh();
    
    Set<String> excludeSet = new HashSet<String>();
    for(Map.Entry<String, TaskTracker> eSet : taskTrackers.entrySet()) {
      String trackerName = eSet.getKey();
      TaskTrackerStatus status = eSet.getValue().getStatus();
      // Check if not include i.e not in host list or in hosts list but excluded
      if (!inHostsList(status) || inExcludedHostsList(status)) {
          excludeSet.add(status.getHost()); // add to rejected trackers
      }
    }
    decommissionNodes(excludeSet);
  }

  // Assumes JobTracker, taskTrackers and trackerExpiryQueue is locked on entry
  // Remove a tracker from the system
  private void removeTracker(TaskTracker tracker) {
    String trackerName = tracker.getTrackerName();
    String hostName = JobInProgress.convertTrackerNameToHostName(trackerName);
    // Remove completely after marking the tasks as 'KILLED'
    lostTaskTracker(tracker);
    // tracker is lost; if it is blacklisted and/or graylisted, remove
    // it from the relevant count(s) of trackers in the cluster
    if (isBlacklisted(trackerName)) {
      LOG.info("Removing " + hostName + " from blacklist");
      faultyTrackers.decrBlacklistedTrackers(1);
    }
    if (isGraylisted(trackerName)) {
      LOG.info("Removing " + hostName + " from graylist");
      faultyTrackers.decrGraylistedTrackers(1);
    }
    // JobTracker端与该TaskTracker相关的数据结构都需要更新，受到影响的Job和Task的数据结构也需要更新，具体处理流程见updateTaskTrackerStatus方法的实现
    updateTaskTrackerStatus(trackerName, null);
    statistics.taskTrackerRemoved(trackerName);
    getInstrumentation().decTrackers(1);
  }

  // main decommission
  synchronized void decommissionNodes(Set<String> hosts) 
  throws IOException {  
    LOG.info("Decommissioning " + hosts.size() + " nodes");
    // create a list of tracker hostnames
    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        int trackersDecommissioned = 0;
        for (String host : hosts) {
          LOG.info("Decommissioning host " + host);
          Set<TaskTracker> trackers = hostnameToTaskTracker.remove(host);
          if (trackers != null) {
            for (TaskTracker tracker : trackers) {
              LOG.info("Decommission: Losing tracker " + tracker.getTrackerName() + 
                       " on host " + host);
              removeTracker(tracker);
            }
            trackersDecommissioned += trackers.size();
          }
          LOG.info("Host " + host + " is ready for decommissioning");
        }
        getInstrumentation().setDecommissionedTrackers(trackersDecommissioned);
      }
    }
  }

  /**
   * Returns a set of excluded nodes.
   */
  Collection<String> getExcludedNodes() {
    return hostsReader.getExcludedHosts();
  }

  /**
   * Get the localized job file path on the job trackers local file system
   * @param jobId id of the job
   * @return the path of the job conf file on the local file system
   */
  public static String getLocalJobFilePath(JobID jobId){
    return JobHistory.JobInfo.getLocalJobFilePath(jobId);
  }
  ////////////////////////////////////////////////////////////
  // main()
  ////////////////////////////////////////////////////////////

  /**
   * Start the JobTracker process.  This is used only for debugging.  As a rule,
   * JobTracker should be run as part of the DFS Namenode process.
   *
   * 在hadoop集群启动的时候会调用JobTracker的main方法将jobtracker进程启动。
   */
  public static void main(String argv[]
                          ) throws IOException, InterruptedException {
    StringUtils.startupShutdownMessage(JobTracker.class, argv, LOG);
    
    try {
      if(argv.length == 0) {
        // 构造JobTracker对象(包括实例化QueueManager,实例化 myInstrumentation, 构造TaskScheduler实例及将当前JobTracker设置为TaskScheduler的TaskTrackerManager)
        JobTracker tracker = startTracker(new JobConf());
        // 启动JobTracker内部一些重要的服务或者线程, 包括taskScheduler的启动, 各种监控线程(比如对超时时间范围之内没有收到TaskTracker的Heartbeat报告，对过期TaskTracker的处理线程expireTrackerThread,
        // retireJobsThread线程， expireLaunchingTaskThread线程， completedJobsStoreThread线程)的启动。
        tracker.offerService();
      }
      else {
        if ("-dumpConfiguration".equals(argv[0]) && argv.length == 1) {
          dumpConfiguration(new PrintWriter(System.out));
        }
        else {
          System.out.println("usage: JobTracker [-dumpConfiguration]");
          System.exit(-1);
        }
      }
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
  /**
   * Dumps the configuration properties in Json format
   * @param writer {@link}Writer object to which the output is written
   * @throws IOException
   */
  private static void dumpConfiguration(Writer writer) throws IOException {
    Configuration.dumpConfiguration(new JobConf(), writer);
    writer.write("\n");
    // get the QueueManager configuration properties
    QueueManager.dumpConfiguration(writer);
    writer.write("\n");
  }

  @Override
  public JobQueueInfo[] getQueues() throws IOException {
    return queueManager.getJobQueueInfos();
  }


  @Override
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return queueManager.getJobQueueInfo(queue);
  }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) throws IOException {
    Collection<JobInProgress> jips = taskScheduler.getJobs(queue);
    return getJobStatus(jips,false);
  }
  
  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException{
    return queueManager.getQueueAcls(
            UserGroupInformation.getCurrentUser());
  }
  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if(jips == null || jips.isEmpty()) {
      return new JobStatus[]{};
    }
    ArrayList<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for(JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();
      status.setStartTime(jip.getStartTime());
      status.setUsername(jip.getProfile().getUser());
      if(toComplete) {
        if(status.getRunState() == JobStatus.RUNNING || 
            status.getRunState() == JobStatus.PREP) {
          jobStatusList.add(status);
        }
      }else {
        jobStatusList.add(status);
      }
    }
    return  jobStatusList.toArray(
        new JobStatus[jobStatusList.size()]);
  }

  /**
   * Returns the confgiured maximum number of tasks for a single job
   */
  int getMaxTasksPerJob() {
    return conf.getInt("mapred.jobtracker.maxtasks.per.job", -1);
  }
  
  @Override
  public void refreshServiceAcl() throws IOException {
    if (!conf.getBoolean(
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }
    ServiceAuthorizationManager.refresh(conf, new MapReducePolicyProvider());
  }

  private void initializeTaskMemoryRelatedConfig() {
    memSizeForMapSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    memSizeForReduceSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));

    if (conf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY)+
          " instead use "+JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY+
          " and " + JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY
      );

      limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      if (limitMaxMemForMapTasks != JobConf.DISABLED_MEMORY_LIMIT &&
        limitMaxMemForMapTasks >= 0) {
        limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
          limitMaxMemForMapTasks /
            (1024 * 1024); //Converting old values in bytes to MB
      }
    } else {
      limitMaxMemForMapTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    }

    LOG.info(new StringBuilder().append("Scheduler configured with ").append(
        "(memSizeForMapSlotOnJT, memSizeForReduceSlotOnJT,").append(
        " limitMaxMemForMapTasks, limitMaxMemForReduceTasks) (").append(
        memSizeForMapSlotOnJT).append(", ").append(memSizeForReduceSlotOnJT)
        .append(", ").append(limitMaxMemForMapTasks).append(", ").append(
            limitMaxMemForReduceTasks).append(")"));
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() {
    LOG.info("Refreshing superuser proxy groups mapping ");
    
    ProxyUsers.refreshSuperUserGroupsConfiguration();
  }
    
  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    
    Groups.getUserToGroupsMappingService().refresh();
  }
  
  private boolean perTaskMemoryConfigurationSetOnJT() {
    if (limitMaxMemForMapTasks == JobConf.DISABLED_MEMORY_LIMIT
        || limitMaxMemForReduceTasks == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForMapSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForReduceSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
  }

  /**
   * Check the job if it has invalid requirements and throw and IOException if does have.
   * 
   * @param job
   * @throws IOException 
   */
  private void checkMemoryRequirements(JobInProgress job)
      throws IOException {
    if (!perTaskMemoryConfigurationSetOnJT()) {
      LOG.debug("Per-Task memory configuration is not set on JT. "
          + "Not checking the job for invalid memory requirements.");
      return;
    }

    boolean invalidJob = false;
    String msg = "";
    long maxMemForMapTask = job.getMemoryForMapTask();
    long maxMemForReduceTask = job.getMemoryForReduceTask();

    if (maxMemForMapTask == JobConf.DISABLED_MEMORY_LIMIT
        || maxMemForReduceTask == JobConf.DISABLED_MEMORY_LIMIT) {
      invalidJob = true;
      msg = "Invalid job requirements.";
    }

    if (maxMemForMapTask > limitMaxMemForMapTasks
        || maxMemForReduceTask > limitMaxMemForReduceTasks) {
      invalidJob = true;
      msg = "Exceeds the cluster's max-memory-limit.";
    }

    if (invalidJob) {
      StringBuilder jobStr =
          new StringBuilder().append(job.getJobID().toString()).append("(")
              .append(maxMemForMapTask).append(" memForMapTasks ").append(
                  maxMemForReduceTask).append(" memForReduceTasks): ");
      LOG.warn(jobStr.toString() + msg);

      throw new IOException(jobStr.toString() + msg);
    }
  }

  @Override
  public void refreshQueues() throws IOException {
    LOG.info("Refreshing queue information. requested by : " +
        UserGroupInformation.getCurrentUser().getShortUserName());
    this.queueManager.refreshQueues(new Configuration());
    
    synchronized (taskScheduler) {
      taskScheduler.refresh();
    }

  }

  // used by the web UI (machines.jsp)
  public String getReasonsForBlacklisting(String host) {
    return getReasonsForBlackOrGraylisting(host, false);
  }

  public String getReasonsForGraylisting(String host) {
    return getReasonsForBlackOrGraylisting(host, true);
  }

  synchronized private String getReasonsForBlackOrGraylisting(String host,
                                                              boolean gray) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, gray);
    if (fi == null) {
      return "";
    }
    return fi.getTrackerBlackOrGraylistReport(gray);
  }

  /** Test Methods */
  synchronized Set<ReasonForBlackListing> getReasonForBlackList(String host) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, false);
    if (fi == null) {
      return new HashSet<ReasonForBlackListing>();
    }
    return fi.getReasonForBlacklisting(false);
  }

  /**
   * 
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }
  
  /**
   * Returns authentication method used to establish the connection
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  JobACLsManager getJobACLsManager() {
    return aclsManager.getJobACLsManager();
  }

  ACLsManager getACLsManager() {
    return aclsManager;
  }

  // Begin MXBean implementation
  @Override
  public String getHostname() {
    return StringUtils.simpleHostname(getJobTrackerMachine());
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() +", r"+ VersionInfo.getRevision();
  }

  @Override
  public String getConfigVersion() {
    return conf.get(CONF_VERSION_KEY, CONF_VERSION_DEFAULT);
  }

  @Override
  public int getThreadCount() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  @Override
  public String getSummaryJson() {
    return getSummary().toJson();
  }

  InfoMap getSummary() {
    final ClusterMetrics metrics = getClusterMetrics();
    InfoMap map = new InfoMap();
    map.put("nodes", metrics.getTaskTrackerCount()
            + getBlacklistedTrackerCount());
    map.put("alive", metrics.getTaskTrackerCount());
    map.put("blacklisted", getBlacklistedTrackerCount());
    map.put("graylisted", getGraylistedTrackerCount());
    map.put("slots", new InfoMap() {{
      put("map_slots", metrics.getMapSlotCapacity());
      put("map_slots_used", metrics.getOccupiedMapSlots());
      put("reduce_slots", metrics.getReduceSlotCapacity());
      put("reduce_slots_used", metrics.getOccupiedReduceSlots());
    }});
    map.put("jobs", metrics.getTotalJobSubmissions());
    return map;
  }

  @Override
  public String getAliveNodesInfoJson() {
    return JSON.toString(getAliveNodesInfo());
  }

  List<InfoMap> getAliveNodesInfo() {
    List<InfoMap> info = new ArrayList<InfoMap>();
    for (final TaskTrackerStatus  tts : activeTaskTrackers()) {
      final int mapSlots = tts.getMaxMapSlots();
      final int redSlots = tts.getMaxReduceSlots();
      info.add(new InfoMap() {{
        put("hostname", tts.getHost());
        put("last_seen", tts.getLastSeen());
        put("health", tts.getHealthStatus().isNodeHealthy() ? "OK" : "");
        put("slots", new InfoMap() {{
          put("map_slots", mapSlots);
          put("map_slots_used", mapSlots - tts.getAvailableMapSlots());
          put("reduce_slots", redSlots);
          put("reduce_slots_used", redSlots - tts.getAvailableReduceSlots());
        }});
        put("failures", tts.getTaskFailures());
        put("dir_failures", tts.getDirFailures());
      }});
    }
    return info;
  }

  @Override
  public String getBlacklistedNodesInfoJson() {
    return JSON.toString(getUnhealthyNodesInfo(blacklistedTaskTrackers()));
  }

  @Override
  public String getGraylistedNodesInfoJson() {
    return JSON.toString(getUnhealthyNodesInfo(graylistedTaskTrackers()));
  }

  List<InfoMap> getUnhealthyNodesInfo(Collection<TaskTrackerStatus> list) {
    List<InfoMap> info = new ArrayList<InfoMap>();
    for (final TaskTrackerStatus tts : list) {
      info.add(new InfoMap() {{
        put("hostname", tts.getHost());
        put("last_seen", tts.getLastSeen());
        put("reason", tts.getHealthStatus().getHealthReport());
      }});
    }
    return info;
  }
  
  @Override
  public String getQueueInfoJson() {
    return getQueueInfo().toJson();
  }

  InfoMap getQueueInfo() {
    InfoMap map = new InfoMap();
    try {
      for (final JobQueueInfo q : getQueues()) {
        map.put(q.getQueueName(), new InfoMap() {{
          put("state", q.getQueueState());
          put("info", q.getSchedulingInfo());
        }});
      }
    }
    catch (Exception e) {
      throw new RuntimeException("Getting queue info", e);
    }
    return map;
  }
  // End MXbean implementaiton

  /**
   * JobTracker SafeMode
   */
  // SafeMode actions
  public enum SafeModeAction{ SAFEMODE_LEAVE, SAFEMODE_ENTER, SAFEMODE_GET; }
  
  private AtomicBoolean safeMode = new AtomicBoolean(false);
  private AtomicBoolean adminSafeMode = new AtomicBoolean(false);
  private String adminSafeModeUser = "";
  
  public boolean setSafeMode(JobTracker.SafeModeAction safeModeAction) 
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    // Anyone can check JT safe-mode
    if (safeModeAction == SafeModeAction.SAFEMODE_GET) {
      boolean safeMode = this.safeMode.get();
      LOG.info("Getting safemode information: safemode=" + safeMode + ". " +
          "Requested by : " +
          UserGroupInformation.getCurrentUser().getShortUserName());
      AuditLogger.logSuccess(user, Constants.GET_SAFEMODE, 
          Constants.JOBTRACKER);
      return safeMode;
    }
    
    // Check access for modifications to safe-mode
    if (!aclsManager.isMRAdmin(UserGroupInformation.getCurrentUser())) {
      AuditLogger.logFailure(user, Constants.SET_SAFEMODE, 
          aclsManager.getAdminsAcl().toString(), Constants.JOBTRACKER, 
          Constants.UNAUTHORIZED_USER);
      throw new AccessControlException(user + 
                                       " is not authorized to set " +
                                       " JobTracker safemode.");
    }
    AuditLogger.logSuccess(user, Constants.SET_SAFEMODE, Constants.JOBTRACKER);

    boolean currSafeMode = setSafeModeInternal(safeModeAction);
    adminSafeMode.set(currSafeMode);
    adminSafeModeUser = user;
    return currSafeMode;
  }
  
  boolean isInAdminSafeMode() {
    return adminSafeMode.get();
  }
  
  boolean setSafeModeInternal(JobTracker.SafeModeAction safeModeAction) 
      throws IOException {
    if (safeModeAction != SafeModeAction.SAFEMODE_GET) {
      boolean safeMode = false;
      if (safeModeAction == SafeModeAction.SAFEMODE_ENTER) {
        safeMode = true;
      } else if (safeModeAction == SafeModeAction.SAFEMODE_LEAVE) {
        safeMode = false;
      }
      LOG.info("Setting safe mode to " + safeMode + ". Requested by : " +
          UserGroupInformation.getCurrentUser().getShortUserName());
      this.safeMode.set(safeMode);
    }
    return this.safeMode.get();
  }

  public boolean isInSafeMode() {
    return safeMode.get();
  }
  
  String getSafeModeText() {
    if (!isInSafeMode())
      return "OFF";
    String safeModeInfo = 
        adminSafeMode.get() ? 
            "Set by admin <strong>" + adminSafeModeUser + "</strong>": 
            "HDFS unavailable";
    return "<em>ON - " + safeModeInfo + "</em>";
  }
  
  private void checkSafeMode() throws SafeModeException {
    if (isInSafeMode()) {
      SafeModeException sme = 
          new SafeModeException(
              (isInAdminSafeMode()) ? adminSafeModeUser : null);
      LOG.info("JobTracker in safe-mode, aborting operation: ", sme); 
      throw sme;
    }
  }
  
  private void checkJobTrackerState() 
      throws JobTrackerNotYetInitializedException {
    if (state != State.RUNNING) {
      JobTrackerNotYetInitializedException jtnyie =
          new JobTrackerNotYetInitializedException();
      LOG.info("JobTracker not yet in RUNNING state, aborting operation: ", 
          jtnyie); 
      throw jtnyie;
    }
  }

  /**
   * generate job token and save it into the file
   * @throws IOException
   * @throws InterruptedException 
   */
  private void 
  generateAndStoreJobTokens(final JobID jobId, final Credentials tokenStorage) 
      throws IOException {

    // Write out jobToken as JT user
    try {
      getMROwner().doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {

          Path jobDir = getSystemDirectoryForJob(jobId);
          Path keysFile = new Path(jobDir, TokenCache.JOB_TOKEN_HDFS_FILE);
          //create JobToken file and write token to it
          JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(jobId
              .toString()));
          Token<JobTokenIdentifier> token = 
              new Token<JobTokenIdentifier>(
                  identifier, getJobTokenSecretManager());
          token.setService(identifier.getJobId());

          TokenCache.setJobToken(token, tokenStorage);

          // write TokenStorage out
          tokenStorage.writeTokenStorageFile(keysFile, getConf());
          LOG.info("jobToken generated and stored with users keys in "
              + keysFile.toUri().getPath());
          
          return null;

        }
      });
    } catch (InterruptedException ie) {
      // TODO Auto-generated catch block
      throw new IOException(ie);
    }

  }

}
