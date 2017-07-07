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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * The job submitter's view of the Job. It allows the user to configure the
 * job, submit it, control its execution, and query the state. The set methods
 * only work until the job is submitted, afterwards they will throw an 
 * IllegalStateException.
 */
public class Job extends JobContext {  
  public static enum JobState {DEFINE, RUNNING};
  private JobState state = JobState.DEFINE;
  private JobClient jobClient;
  private RunningJob info;

  /**
   * Creates a new {@link Job}
   * A Job will be created with a generic {@link Configuration}.
   *
   * @return the {@link Job}
   * @throws IOException
   */
  public static Job getInstance() throws IOException {
    // create with a null Cluster
    return getInstance(new Configuration());
  }

  /**
   * Creates a new {@link Job} with a given {@link Configuration}.
   *
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so
   * that any necessary internal modifications do not reflect on the incoming
   * parameter.
   *
   * @param conf the {@link Configuration}
   * @return the {@link Job}
   * @throws IOException
   */
  public static Job getInstance(Configuration conf) throws IOException {
    // create with a null Cluster
    JobConf jobConf = new JobConf(conf);
    return new Job(jobConf);
  }

  /**
   * Creates a new {@link Job} with a given {@link Configuration}
   * and a given jobName.
   *
   * The <code>Job</code> makes a copy of the <code>Configuration</code> so
   * that any necessary internal modifications do not reflect on the incoming
   * parameter.
   *
   * @param conf the {@link Configuration}
   * @param jobName the job instance's name
   * @return the {@link Job}
   * @throws IOException
   */
  public static Job getInstance(Configuration conf, String jobName)
           throws IOException {
    // create with a null Cluster
    Job result = getInstance(conf);
    result.setJobName(jobName);
    return result;
  }

  public Job() throws IOException {
    this(new Configuration());
  }

  public Job(Configuration conf) throws IOException {
    super(conf, null);
  }

  public Job(Configuration conf, String jobName) throws IOException {
    this(conf);
    // setJobName中会确认当前job的状态
    setJobName(jobName);
  }

  JobClient getJobClient() {
    return jobClient;
  }
  
  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state "+ this.state + 
                                      " instead of " + state);
    }

    if (state == JobState.RUNNING && jobClient == null) {
      throw new IllegalStateException("Job in state " + JobState.RUNNING + 
                                      " however jobClient is not initialized!");
    }
  }

  /**
   * Set the number of reduce tasks for the job.
   * @param tasks the number of reduce tasks
   * @throws IllegalStateException if the job is submitted
   */
  public void setNumReduceTasks(int tasks) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setNumReduceTasks(tasks);
  }

  /**
   * Set the current working directory for the default file system.
   * 
   * @param dir the new current working directory.
   * @throws IllegalStateException if the job is submitted
   */
  public void setWorkingDirectory(Path dir) throws IOException {
    ensureState(JobState.DEFINE);
    conf.setWorkingDirectory(dir);
  }

  /**
   * Set the {@link InputFormat} for the job.
   * @param cls the <code>InputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setInputFormatClass(Class<? extends InputFormat> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(INPUT_FORMAT_CLASS_ATTR, cls, InputFormat.class);
  }

  /**
   * Set the {@link OutputFormat} for the job.
   * @param cls the <code>OutputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputFormatClass(Class<? extends OutputFormat> cls
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(OUTPUT_FORMAT_CLASS_ATTR, cls, OutputFormat.class);
  }

  /**
   * Set the {@link Mapper} for the job.
   * @param cls the <code>Mapper</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapperClass(Class<? extends Mapper> cls
                             ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(MAP_CLASS_ATTR, cls, Mapper.class);
  }

  /**
   * Set the Jar by finding where a given class came from.
   * @param cls the example class
   */
  public void setJarByClass(Class<?> cls) {
    conf.setJarByClass(cls);
  }
  
  /**
   * Get the pathname of the job's jar.
   * @return the pathname
   */
  public String getJar() {
    return conf.getJar();
  }

  /**
   * Set the combiner class for the job.
   * @param cls the combiner to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setCombinerClass(Class<? extends Reducer> cls
                               ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(COMBINE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Reducer} for the job.
   * @param cls the <code>Reducer</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setReducerClass(Class<? extends Reducer> cls
                              ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(REDUCE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Partitioner} for the job.
   * @param cls the <code>Partitioner</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setPartitionerClass(Class<? extends Partitioner> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(PARTITIONER_CLASS_ATTR, cls, Partitioner.class);
  }

  /**
   * Set the key class for the map output data. This allows the user to
   * specify the map output key class to be different than the final output
   * value class.
   * 
   * @param theClass the map output key class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputKeyClass(Class<?> theClass
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputKeyClass(theClass);
  }

  /**
   * Set the value class for the map output data. This allows the user to
   * specify the map output value class to be different than the final output
   * value class.
   * 
   * @param theClass the map output value class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputValueClass(Class<?> theClass
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputValueClass(theClass);
  }

  /**
   * Set the key class for the job output data.
   * 
   * @param theClass the key class for the job output data.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputKeyClass(Class<?> theClass
                                ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyClass(theClass);
  }

  /**
   * Set the value class for job outputs.
   * 
   * @param theClass the value class for job outputs.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputValueClass(Class<?> theClass
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueClass(theClass);
  }

  /**
   * Define the comparator that controls how the keys are sorted before they
   * are passed to the {@link Reducer}.
   * @param cls the raw comparator
   * @throws IllegalStateException if the job is submitted
   */
  public void setSortComparatorClass(Class<? extends RawComparator> cls
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyComparatorClass(cls);
  }

  /**
   * Define the comparator that controls which keys are grouped together
   * for a single call to 
   * {@link Reducer#reduce(Object, Iterable, 
   *                       org.apache.hadoop.mapreduce.Reducer.Context)}
   * @param cls the raw comparator to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setGroupingComparatorClass(Class<? extends RawComparator> cls
                                         ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueGroupingComparator(cls);
  }

  /**
   * Set the user-specified job name.
   * 
   * @param name the job's new name.
   * @throws IllegalStateException if the job is submitted
   */
  public void setJobName(String name) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setJobName(name);
  }
  
  /**
   * Turn speculative execution on or off for this job. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on, else <code>false</code>.
   */
  public void setSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setSpeculativeExecution(speculativeExecution);
  }

  /**
   * Turn speculative execution on or off for this job for map tasks. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on for map tasks,
   *                             else <code>false</code>.
   */
  public void setMapSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setMapSpeculativeExecution(speculativeExecution);
  }

  /**
   * Turn speculative execution on or off for this job for reduce tasks. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on for reduce tasks,
   *                             else <code>false</code>.
   */
  public void setReduceSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setReduceSpeculativeExecution(speculativeExecution);
  }

  /**
   * Get the URL where some job progress information will be displayed.
   * 
   * @return the URL where some job progress information will be displayed.
   */
  public String getTrackingURL() {
    ensureState(JobState.RUNNING);
    return info.getTrackingURL();
  }

  /**
   * Get the <i>progress</i> of the job's setup, as a float between 0.0 
   * and 1.0.  When the job setup is completed, the function returns 1.0.
   * 
   * @return the progress of the job's setup.
   * @throws IOException
   */
  public float setupProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.setupProgress();
  }

  /**
   * Get the <i>progress</i> of the job's map-tasks, as a float between 0.0 
   * and 1.0.  When all map tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's map-tasks.
   * @throws IOException
   */
  public float mapProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.mapProgress();
  }

  /**
   * Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0 
   * and 1.0.  When all reduce tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's reduce-tasks.
   * @throws IOException
   */
  public float reduceProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.reduceProgress();
  }

  /**
   * Check if the job is finished or not. 
   * This is a non-blocking call.
   * 
   * @return <code>true</code> if the job is complete, else <code>false</code>.
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isComplete();
  }

  /**
   * Check if the job completed successfully. 
   * 
   * @return <code>true</code> if the job succeeded, else <code>false</code>.
   * @throws IOException
   */
  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isSuccessful();
  }

  /**
   * Kill the running job.  Blocks until all job tasks have been
   * killed as well.  If the job is no longer running, it simply returns.
   * 
   * @throws IOException
   */
  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    info.killJob();
  }
    
  /**
   * Get events indicating completion (success/failure) of component tasks.
   *  
   * @param startFrom index to start fetching events from
   * @return an array of {@link TaskCompletionEvent}s
   * @throws IOException
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom
                                                       ) throws IOException {
    ensureState(JobState.RUNNING);
    return info.getTaskCompletionEvents(startFrom);
  }
  
  /**
   * Kill indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void killTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId), 
                  false);
  }

  /**
   * Fail indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void failTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId), 
                  true);
  }

  /**
   * Gets the counters for this job.
   * 
   * @return the counters for this job.
   * @throws IOException
   */
  public Counters getCounters() throws IOException {
    ensureState(JobState.RUNNING);
    return new Counters(info.getCounters());
  }

  private void ensureNotSet(String attr, String msg) throws IOException {
    if (conf.get(attr) != null) {
      throw new IOException(attr + " is incompatible with " + msg + " mode.");
    }    
  }
  
  /**
   * Sets the flag that will allow the JobTracker to cancel the HDFS delegation
   * tokens upon job completion. Defaults to true.
   */
  public void setCancelDelegationTokenUponJobCompletion(boolean value) {
    ensureState(JobState.DEFINE);
    conf.setBoolean(JOB_CANCEL_DELEGATION_TOKEN, value);
  }

  /**
   * 设定为新的API, 除非新API已经被明确的设定或者旧的mapper或reduce被使用了
   * Default to the new APIs unless they are explicitly set or the old mapper or
   * reduce attributes are used.
   * @throws IOException if the configuration is inconsistant
   */
  private void setUseNewAPI() throws IOException {
    // 获取用户设置的reduce个数, 没有设置的话默认值为1
    int numReduces = conf.getNumReduceTasks();
    String oldMapperClass = "mapred.mapper.class";
    String oldReduceClass = "mapred.reducer.class";
    // 如果没有设置mapred.mapper.class属性, 则设置mapred.reducer.class为true, 否则为false
    conf.setBooleanIfUnset("mapred.mapper.new-api",
                           conf.get(oldMapperClass) == null);
    // 如果设置为使用新的mapper API, 则确认旧的mapper API的相关属性没有设置
    if (conf.getUseNewMapper()) {
      String mode = "new map API";
      ensureNotSet("mapred.input.format.class", mode);
      ensureNotSet(oldMapperClass, mode);
      if (numReduces != 0) {
        ensureNotSet("mapred.partitioner.class", mode);
       } else {
        ensureNotSet("mapred.output.format.class", mode);
      }      
    } else {
      String mode = "map compatability";
      // 确认没有设置mapreduce.inputformat.class, 即inputformat class
      ensureNotSet(JobContext.INPUT_FORMAT_CLASS_ATTR, mode);
      ensureNotSet(JobContext.MAP_CLASS_ATTR, mode);
      if (numReduces != 0) {
        ensureNotSet(JobContext.PARTITIONER_CLASS_ATTR, mode);
       } else {
        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
      }
    }
    if (numReduces != 0) {
      // 如果没有设置mapred.reducer.class属性, 则设置mapred.reducer.new-api为true, 否则为false
      conf.setBooleanIfUnset("mapred.reducer.new-api",
                             conf.get(oldReduceClass) == null);
      // 如果设置为使用新的reduce API, 则确认旧的reduce API的相关属性没有设置
      if (conf.getUseNewReducer()) {
        String mode = "new reduce API";
        ensureNotSet("mapred.output.format.class", mode);
        ensureNotSet(oldReduceClass, mode);   
      } else {
        String mode = "reduce compatability";
        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
        ensureNotSet(JobContext.REDUCE_CLASS_ATTR, mode);   
      }
    }   
  }

  /**
   * Submit the job to the cluster and return immediately.
   * @throws IOException
   */
  public void submit() throws IOException, InterruptedException, 
                              ClassNotFoundException {
    ensureState(JobState.DEFINE);
    // 设置使用新的API
    setUseNewAPI();
    
    // Connect to the JobTracker and submit the job
    // connect()方法中会初始化jobClient，比如初始化jobSubmitClient
    connect();
    // 调用JobClient的submitJobInternal方法,该方法中会最终会通过jobSubmitClient把作业提交到JobTracker
    // 不管是新的mapreduce api还是旧的api, 都会调用submitJobInternal方法.
    info = jobClient.submitJobInternal(conf);
    super.setJobID(info.getID());
    // 作业已经提交，将作业的状态置为RUNNING
    state = JobState.RUNNING;
   }
  
  /**
   * Open a connection to the JobTracker
   * @throws IOException
   * @throws InterruptedException 
   */
  private void connect() throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        jobClient = new JobClient((JobConf) getConfiguration());    
        return null;
      }
    });
  }
  
  /**
   * 提交作业入口
   * Submit the job to the cluster and wait for it to finish.
   * @param verbose print the progress to the user
   * @return true if the job succeeded
   * @throws IOException thrown if the communication with the 
   *         <code>JobTracker</code> is lost
   */
  public boolean waitForCompletion(boolean verbose
                                   ) throws IOException, InterruptedException,
                                            ClassNotFoundException {
    if (state == JobState.DEFINE) {
      // 在new Job(Configuration conf, String jobName)时, Job默认的属性JobState state = JobState.DEFINE，及job是定义状态
      // 调用submit方法， 该方法中会将Job的state设置为JobState.RUNNING
      submit();
    }
    if (verbose) {
      jobClient.monitorAndPrintJob(conf, info);
    } else {
      info.waitForCompletion();
    }
    return isSuccessful();
  }
  
}
