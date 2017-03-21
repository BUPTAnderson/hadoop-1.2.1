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
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A {@link JobInProgressListener} which initializes the tasks for a job as soon
 * as the job is added (using the {@link #jobAdded(JobInProgress)} method).
 */
class EagerTaskInitializationListener extends JobInProgressListener {
  
  private static final int DEFAULT_NUM_THREADS = 4;
  private static final Log LOG = LogFactory.getLog(
      EagerTaskInitializationListener.class.getName());
  
  /////////////////////////////////////////////////////////////////
  //  Used to init new jobs that have just been created
  /////////////////////////////////////////////////////////////////
  class JobInitManager implements Runnable {
   
    public void run() {
      JobInProgress job = null;
      while (true) {
        try {
          synchronized (jobInitQueue) {
            while (jobInitQueue.isEmpty()) {
              jobInitQueue.wait();
            }
            // 取出JobInProgress
            job = jobInitQueue.remove(0);
          }
          // 创建一个InitJob线程去初始化该JobInProgress
          threadPool.execute(new InitJob(job));
        } catch (InterruptedException t) {
          LOG.info("JobInitManagerThread interrupted.");
          break;
        } 
      }
      LOG.info("Shutting down thread pool");
      threadPool.shutdownNow();
    }
  }
  
  class InitJob implements Runnable {
  
    private JobInProgress job;
    
    public InitJob(JobInProgress job) {
      this.job = job;
    }
    
    public void run() {
      // 调用JobTracker的initJob方法
      ttm.initJob(job);
    }
  }
  // 负责监听队列中有没有新的job加入, 如果有, 出发初始化线程new InitJob
  private JobInitManager jobInitManager = new JobInitManager();
  private Thread jobInitManagerThread;
  private List<JobInProgress> jobInitQueue = new ArrayList<JobInProgress>();
  private ExecutorService threadPool;
  private int numThreads;
  private TaskTrackerManager ttm;

  // JobTacker初始化JobQueueTaskScheduler后会调用JobQueueTaskScheduler的setConf,
  // setConf方法中会调用下面的构造方法初始化EagerTaskInitializationListener
  public EagerTaskInitializationListener(Configuration conf) {
    // 设置可以同时初始化的job数量
    numThreads = conf.getInt("mapred.jobinit.threads", DEFAULT_NUM_THREADS);
    threadPool = Executors.newFixedThreadPool(numThreads);
  }

  // ttm实际为JobTracker
  public void setTaskTrackerManager(TaskTrackerManager ttm) {
    this.ttm = ttm;
  }

  // JobTracker通过main方法启动时会调用自己的offerService()方法，该方法中会调用JobQueueTaskScheduler的start方法, start方法中会依次调用
  // EagerTaskInitializationListener的setTaskTrackerManager和start方法
  // 下面方法的作用是监听jobInitQueue是否有job加入
  public void start() throws IOException {
    this.jobInitManagerThread = new Thread(jobInitManager, "jobInitManager");
    jobInitManagerThread.setDaemon(true);
    this.jobInitManagerThread.start();
  }
  
  public void terminate() throws IOException {
    if (jobInitManagerThread != null && jobInitManagerThread.isAlive()) {
      LOG.info("Stopping Job Init Manager thread");
      jobInitManagerThread.interrupt();
      try {
        jobInitManagerThread.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }

  /**
   * We add the JIP to the jobInitQueue, which is processed 
   * asynchronously to handle split-computation and build up
   * the right TaskTracker/Block mapping.
   *
   * 我们将JIP添加到jobInitQueue，它被异步处理以处理分割计算并建立正确的TaskTracker / Block映射。
   */
  @Override
  public void jobAdded(JobInProgress job) {
    synchronized (jobInitQueue) {
      jobInitQueue.add(job);
      // 对jobInitQueue按job的优先级进行排序
      resortInitQueue();
      // jobInitManagerThread刚开始由于jobInitQueue为空，处于阻塞状态，现在不为空了，被唤醒，在start()方法中jobInitQueue是由JobInitManager构造的，下面的语句实际会触发
      // JobInitManager的run()方法
      jobInitQueue.notifyAll();
    }

  }
  
  /**
   * Sort jobs by priority and then by start time.
   */
  private synchronized void resortInitQueue() {
    Comparator<JobInProgress> comp = new Comparator<JobInProgress>() {
      public int compare(JobInProgress o1, JobInProgress o2) {
        int res = o1.getPriority().compareTo(o2.getPriority());
        if(res == 0) {
          if(o1.getStartTime() < o2.getStartTime())
            res = -1;
          else
            res = (o1.getStartTime()==o2.getStartTime() ? 0 : 1);
        }
          
        return res;
      }
    };
    
    synchronized (jobInitQueue) {
      Collections.sort(jobInitQueue, comp);
    }
  }

  @Override
  public void jobRemoved(JobInProgress job) {
    synchronized (jobInitQueue) {
      jobInitQueue.remove(job);
    }
  }

  @Override
  public void jobUpdated(JobChangeEvent event) {
    // 如果Job优先级（Priority）/开始时间发生变更，则对List<JobInProgress> jobInitQueue队列进行重新排序
    if (event instanceof JobStatusChangeEvent) {
      jobStateChanged((JobStatusChangeEvent)event);
    }
  }
  
  // called when the job's status is changed
  private void jobStateChanged(JobStatusChangeEvent event) {
    // Resort the job queue if the job-start-time or job-priority changes
    if (event.getEventType() == EventType.START_TIME_CHANGED
        || event.getEventType() == EventType.PRIORITY_CHANGED) {
      synchronized (jobInitQueue) {
        resortInitQueue();
      }
    }
  }

}
