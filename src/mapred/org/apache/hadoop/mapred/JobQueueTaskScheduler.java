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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link TaskScheduler} that keeps jobs in a queue in priority order (FIFO
 * by default).
 */
class JobQueueTaskScheduler extends TaskScheduler {
  
  private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
  public static final Log LOG = LogFactory.getLog(JobQueueTaskScheduler.class);
  
  protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
  protected EagerTaskInitializationListener eagerTaskInitializationListener;
  private float padFraction;
  
  public JobQueueTaskScheduler() {
    // 初始化一个JobInProgressListener对象, 该对象中初始化了一个Map<JobSchedulingInfo,JobInProgress>,并且向该map中传入了一个FIFO的比较器。
    this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
    // JobTracker通过main函数启动时是调用ReflectionUtils通过反射构造该类, 之后还会调用setConf方法. 该方法中初始化了一个EagerTaskInitializationListener对象。
    // 调用完setConf方法后，JobTracker还会调用JobQueueTaskScheduler的setTaskTrackerManager方法 setTaskTrackerManager(jobTracker)
    // JobInProgressListener是用来监听由JobClient端提交后在JobTracker端Job（在JobTracker端维护的JobInProgress）生命周期变化，并触发相应事件（jobAdded/jobUpdated/jobRemoved）
  }

  // JobTracker实例化后会调用offerService方法, 该方法中会调用下面的start方法
  @Override
  public synchronized void start() throws IOException {
    super.start();
    // JobTracker通过main方法启动的过程中调用JobQueueTaskScheduler的构造方法初始化JobQueueTaskScheduler，然后调用JobQueueTaskScheduler的setConf方法
    // 让后调用JobQueueTaskScheduler的setTaskTrackerManager方法（该方法继承自父类）将taskTrackerManager设置为JobTracker实例
    // 这行程序的作用就是为jobTracker添加jobListener, 用来监听job的.
    // 实际是调用jobTracker的jobInProgressListeners集合的add(listener)方法，eagerTaskInitializationListener通过start方法已经处于监听状态
    taskTrackerManager.addJobInProgressListener(jobQueueJobInProgressListener);
    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(
        eagerTaskInitializationListener);
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    if (jobQueueJobInProgressListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          jobQueueJobInProgressListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);
    this.eagerTaskInitializationListener =
      new EagerTaskInitializationListener(conf);
  }

  // JobQueueTaskScheduler最重要的方法是assignTasks，他实现了工作调度。具体实现原理参考：http://blog.csdn.net/xhh198781/article/details/7046389, http://www.cnblogs.com/lxf20061900/p/3775963.html
  // 对于JobQueueTaskScheduler的任务调度实现原则可总结如下：
  //   1.先调度优先级高的作业，统一优先级的作业则先进先出；
  //   2.尽量使集群每一个TaskTracker达到负载均衡(这个均衡是task数量上的而不是实际的工作强度)；
  //   3.尽量分配作业的本地任务给TaskTracker，但不是尽快分配作业的本地任务给TaskTracker，最多分配一个非本地任务给TaskTracker(一是保证任务的并发性，二是避免有些TaskTracker的本地任务被偷走)，最多分配一个reduce任务；
  //   4.为优先级或者紧急的Task预留一定的slot；
  @Override
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
    // Check for JT safe-mode
    if (taskTrackerManager.isInSafeMode()) {
      LOG.info("JobTracker is in safe-mode, not scheduling any tasks.");
      return null;
    } 

    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus(); 
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
    final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();

    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    //
    // Get map + reduce counts for the current tracker.
    // 首先它会检查 TaskTracker 端还可以做多少个 map 和 reduce 任务，将要派发的任务数是否超出这个数，
    // 是否超出集群的任务平均剩余可负载数。如果都没超出，则为此TaskTracker 分配一个 MapTask 或 ReduceTask 。
    final int trackerMapCapacity = taskTrackerStatus.getMaxMapSlots();
    final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceSlots();
    final int trackerRunningMaps = taskTrackerStatus.countMapTasks();
    final int trackerRunningReduces = taskTrackerStatus.countReduceTasks();

    // Assigned tasks
    List<Task> assignedTasks = new ArrayList<Task>();

    //
    // Compute (running + pending) map and reduce task numbers across pool
    // 计算剩余的map和reduce的工作量：remaining
    int remainingReduceLoad = 0;
    int remainingMapLoad = 0;
    synchronized (jobQueue) {
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          remainingMapLoad += (job.desiredMaps() - job.finishedMaps());
          if (job.scheduleReduces()) {
            remainingReduceLoad += 
              (job.desiredReduces() - job.finishedReduces());
          }
        }
      }
    }

    // Compute the 'load factor' for maps and reduces
    // 计算map和reduce的负载因子 剩余占对应的最大容量比
    double mapLoadFactor = 0.0;
    if (clusterMapCapacity > 0) {
      mapLoadFactor = (double)remainingMapLoad / clusterMapCapacity;
    }
    double reduceLoadFactor = 0.0;
    if (clusterReduceCapacity > 0) {
      reduceLoadFactor = (double)remainingReduceLoad / clusterReduceCapacity;
    }
        
    //
    // In the below steps, we allocate first map tasks (if appropriate),
    // and then reduce tasks if appropriate.  We go through all jobs
    // in order of job arrival; jobs only get serviced if their 
    // predecessors are serviced, too.
    //

    //
    // We assign tasks to the current taskTracker if the given machine 
    // has a workload that's less than the maximum load of that kind of
    // task.
    // However, if the cluster is close to getting loaded i.e. we don't
    // have enough _padding_ for speculative executions etc., we only 
    // schedule the "highest priority" task i.e. the task from the job 
    // with the highest priority.
    //

    // mapLoadFactor * trackerMapCapacity会使得该节点当前map的容量和集群整体的负载相近
    final int trackerCurrentMapCapacity = 
      Math.min((int)Math.ceil(mapLoadFactor * trackerMapCapacity), 
                              trackerMapCapacity);
    // 获取当前tasktracker可用的mapslot，该tasktracker超过集群目前的负载水平后就不分配task，否则会有空闲的slot等待分配task
    int availableMapSlots = trackerCurrentMapCapacity - trackerRunningMaps;
    boolean exceededMapPadding = false;
    if (availableMapSlots > 0) {
      exceededMapPadding = 
        exceededPadding(true, clusterStatus, trackerMapCapacity);
    }
    
    int numLocalMaps = 0;
    int numNonLocalMaps = 0;
    // 下面是为每个mapslot选择一个map task
    scheduleMaps:
    for (int i=0; i < availableMapSlots; ++i) {
      synchronized (jobQueue) {
        // 首先遍历jobQueue中的每个处于非运行状态的JobInProgress
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }

          Task t = null;
          
          // Try to schedule a Map task with locality between node-local 
          // and rack-local
          // 调用JobInProgress的obtainNewNodeOrRackLocalMapTask方法获取基于节点本地或者机架本地的map task，obtainNewNodeOrRackLocalMapTask会通过调用findNewMapTask获取map数组中的索引值
          // obtainNewNodeOrRackLocalMapTask要么返回一个MapTask要么返回null
          t = 
            job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
          if (t != null) {
            assignedTasks.add(t); // 加入分配task列表assignedTasks
            ++numLocalMaps;
            
            // Don't assign map tasks to the hilt!
            // Leave some free slots in the cluster for future task-failures,
            // speculative tasks etc. beyond the highest priority job
            if (exceededMapPadding) {
              break scheduleMaps; // 跳出内层for循环，准备为下一个mapslot找合适的MapTask
            }
           
            // Try all jobs again for the next Map task 
            break;  // 遍历所有的job，直到为该mapslot找到合适的MapTask
          }
          
          // Try to schedule a node-local or rack-local Map task
          // 如果没有合适的MapTask(node-local或者rack-local),则调用obtainNewNonLocalMapTask获取MapTask
          // 产生 Map 任务使用 JobInProgress 的obtainNewMapTask() 方法, 实质上最后调用了 JobInProgress 的 findNewMapTask() 访问nonRunningMapCache 。
          t = 
            job.obtainNewNonLocalMapTask(taskTrackerStatus, numTaskTrackers,
                                   taskTrackerManager.getNumberOfUniqueHosts());
          
          if (t != null) {
            assignedTasks.add(t);
            ++numNonLocalMaps;
            
            // We assign at most 1 off-switch or speculative task
            // This is to prevent TaskTrackers from stealing local-tasks
            // from other TaskTrackers.
            break scheduleMaps;
          }
        }
      }
    }
    int assignedMaps = assignedTasks.size();

    //
    // Same thing, but for reduce tasks
    // However we _never_ assign more than 1 reduce task per heartbeat
    // 分配完map task，再分配reduce task, 每次心跳分配不超过一个ReduceTask
    final int trackerCurrentReduceCapacity = 
      Math.min((int)Math.ceil(reduceLoadFactor * trackerReduceCapacity), 
               trackerReduceCapacity);
    final int availableReduceSlots = 
      Math.min((trackerCurrentReduceCapacity - trackerRunningReduces), 1);
    boolean exceededReducePadding = false;
    if (availableReduceSlots > 0) {
      exceededReducePadding = exceededPadding(false, clusterStatus, 
                                              trackerReduceCapacity);
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
              job.numReduceTasks == 0) {
            continue;
          }

          // 使用JobInProgress.obtainNewReduceTask() 方法，实质上最后调用了JobInProgress的 findNewReduceTask() 访问 nonRuningReduceCache
          Task t = 
            job.obtainNewReduceTask(taskTrackerStatus, numTaskTrackers, 
                                    taskTrackerManager.getNumberOfUniqueHosts()
                                    );
          if (t != null) {
            assignedTasks.add(t);
            break;
          }
          
          // Don't assign reduce tasks to the hilt!
          // Leave some free slots in the cluster for future task-failures,
          // speculative tasks etc. beyond the highest priority job
          if (exceededReducePadding) {
            break;
          }
        }
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Task assignments for " + taskTrackerStatus.getTrackerName() + " --> " +
                "[" + mapLoadFactor + ", " + trackerMapCapacity + ", " + 
                trackerCurrentMapCapacity + ", " + trackerRunningMaps + "] -> [" + 
                (trackerCurrentMapCapacity - trackerRunningMaps) + ", " +
                assignedMaps + " (" + numLocalMaps + ", " + numNonLocalMaps + 
                ")] [" + reduceLoadFactor + ", " + trackerReduceCapacity + ", " + 
                trackerCurrentReduceCapacity + "," + trackerRunningReduces + 
                "] -> [" + (trackerCurrentReduceCapacity - trackerRunningReduces) + 
                ", " + (assignedTasks.size()-assignedMaps) + "]");
    }

    return assignedTasks;
  }

  // 在任务调度器JobQueueTaskScheduler的实现中，如果在集群中的TaskTracker节点比较多的情况下，它总是会想办法让若干个TaskTracker节点预留一些空闲的slots(计算能力)，
  // 以便能够快速的处理优先级比较高的Job的Task或者发生错误的Task，以保证已经被调度的作业的完成。exceededPadding方法判断当前集群是否需要预留一部分map/reduce计算能力来执行那些失败的、紧急的或特殊的任务。
  private boolean exceededPadding(boolean isMapTask, 
                                  ClusterStatus clusterStatus, 
                                  int maxTaskTrackerSlots) { 
    int numTaskTrackers = clusterStatus.getTaskTrackers();
    int totalTasks = 
      (isMapTask) ? clusterStatus.getMapTasks() : 
        clusterStatus.getReduceTasks();
    int totalTaskCapacity = 
      isMapTask ? clusterStatus.getMaxMapTasks() : 
                  clusterStatus.getMaxReduceTasks();

    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    boolean exceededPadding = false;
    synchronized (jobQueue) {
      int totalNeededTasks = 0;
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() != JobStatus.RUNNING ||
            job.numReduceTasks == 0) {
          continue;
        }

        //
        // Beyond the highest-priority task, reserve a little 
        // room for failures and speculative executions; don't 
        // schedule tasks to the hilt.
        //
        totalNeededTasks += 
          isMapTask ? job.desiredMaps() : job.desiredReduces();
        int padding = 0;
        if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
          padding = 
            Math.min(maxTaskTrackerSlots,
                     (int) (totalNeededTasks * padFraction));
        }
        if (totalTasks + padding >= totalTaskCapacity) {
          exceededPadding = true;
          break;
        }
      }
    }

    return exceededPadding;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return jobQueueJobInProgressListener.getJobQueue();
  }  
}
