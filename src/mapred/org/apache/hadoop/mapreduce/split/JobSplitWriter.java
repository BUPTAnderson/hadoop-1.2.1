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

package org.apache.hadoop.mapreduce.split;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

/**
 * The class that is used by the Job clients to write splits (both the meta
 * and the raw bytes parts)
 */
public class JobSplitWriter {

  private static final Log LOG = LogFactory.getLog(JobSplitWriter.class);
  private static final int splitVersion = JobSplit.META_SPLIT_VERSION;
  private static final byte[] SPLIT_FILE_HEADER;
  static final String MAX_SPLIT_LOCATIONS = "mapreduce.job.max.split.locations";
  
  static {
    try {
      SPLIT_FILE_HEADER = "SPL".getBytes("UTF-8");
    } catch (UnsupportedEncodingException u) {
      throw new RuntimeException(u);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, List<InputSplit> splits) 
  throws IOException, InterruptedException {
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
    createSplitFiles(jobSubmitDir, conf, fs, array);
  }
  
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, T[] splits) 
  throws IOException, InterruptedException {
    // 创建文件job.split
    FSDataOutputStream out = createFile(fs,
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    // 向文件job.split中写入原始InputSplit, 并返回SplitMetaInfo信息
    SplitMetaInfo[] info = writeNewSplits(conf, splits, out);
    out.close();
    // 创建job.splitmetainfo文件, 将返回的SplitMetaInfo对象数组信息序列化写入, 如: /tmp/hadoop/mapred/staging/shirdrn/.staging/job_200912121733_0002/job.splitmetainfo
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  public static void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem   fs, 
      org.apache.hadoop.mapred.InputSplit[] splits) 
  throws IOException {
    FSDataOutputStream out = createFile(fs,
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    SplitMetaInfo[] info = writeOldSplits(splits, out, conf);
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  private static FSDataOutputStream createFile(FileSystem fs, Path splitFile,
      Configuration job)  throws IOException {
    FSDataOutputStream out = FileSystem.create(fs, splitFile, 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    int replication = job.getInt("mapred.submit.replication", 10);
    fs.setReplication(splitFile, (short)replication);
    writeSplitHeader(out);
    return out;
  }
  private static void writeSplitHeader(FSDataOutputStream out) 
  throws IOException {
    out.write(SPLIT_FILE_HEADER);
    out.writeInt(splitVersion);
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends InputSplit> 
  SplitMetaInfo[] writeNewSplits(Configuration conf, 
      T[] array, FSDataOutputStream out)
  throws IOException, InterruptedException {

    SplitMetaInfo[] info = new SplitMetaInfo[array.length];
    // 下面是获取序列化类，将每一个InputSplit对象进行序列化写入到输出流中
    // 输出流就是:${mapreduce.jobtracker.staging.root.dir}/${user}/.staging/${jobId}/job.split, 如: /tmp/hadoop/mapred/staging/shirdrn/.staging/job_200912121733_0002/job.split
    // 1. 先写入：split.getClass().getName()
    // 2. 写入：serializer.serialize(split)将split对象序列化写入
    // 3. 对应每个InputSplit对象, 创建对应的SplitMetaInfo对象
    //该对象包括InputSpilt元信息在job.split文件中的偏移量，InputSplit的长度
    //最后将该SplitMetaInfo对象数组返回.
    if (array.length != 0) {
      SerializationFactory factory = new SerializationFactory(conf);
      int i = 0;
      long offset = out.getPos();
      for(T split: array) {
        long prevCount = out.getPos();
        Text.writeString(out, split.getClass().getName());
        Serializer<T> serializer = 
          factory.getSerializer((Class<T>) split.getClass());
        serializer.open(out);
        // 默认调用WritableSerialization的serialize(Writable w)
        // public void serialize(Writable w) throws IOException {
        //  w.write(dataOut);
        // }
        // 所以实际是调用的FileSplit的write(DataOutput out), 进入该方法发现并没有将FileSplit的hosts信息写入输出流
        // 即向 job.split文件中没有保存每个split对应的hosts信息, 该信息保存到了SplitMetaInfo中, 对应信息如下
        //   job.split:  |split_0.getclass.getName,分片0所在file name, split_0.start(分片在file中的起始位置), split_0.length(分片长度)|split_1.getclass.getName,分片1所在file name, split_1.start, split_1.length|....
        //split.log中位置:0                                                                                                       t                                                                        m
        //每个split对应的信息： SplitMetaInfo(split_0.getLocations, 0(split_0在job.split中的偏移量), split_0.length(split_0的分片大小)), SplitMetaInfo(split_1.getLocations, t, split_1.length)
        serializer.serialize(split);
        long currCount = out.getPos();
        String[] locations = split.getLocations();
        final int max_loc = conf.getInt(MAX_SPLIT_LOCATIONS, 10);
        if (locations.length > max_loc) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + max_loc);
          locations = Arrays.copyOf(locations, max_loc);
        }
        info[i++] = 
          new JobSplit.SplitMetaInfo( 
              locations, offset,
              split.getLength());
        offset += currCount - prevCount;
      }
    }
    return info;
  }
  
  private static SplitMetaInfo[] writeOldSplits(
      org.apache.hadoop.mapred.InputSplit[] splits,
      FSDataOutputStream out, Configuration conf) throws IOException {
    SplitMetaInfo[] info = new SplitMetaInfo[splits.length];
    if (splits.length != 0) {
      int i = 0;
      long offset = out.getPos();
      for(org.apache.hadoop.mapred.InputSplit split: splits) {
        long prevLen = out.getPos();
        Text.writeString(out, split.getClass().getName());
        split.write(out);
        long currLen = out.getPos();
        String[] locations = split.getLocations();
        final int max_loc = conf.getInt(MAX_SPLIT_LOCATIONS, 10);
        if (locations.length > max_loc) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + max_loc);
          locations = Arrays.copyOf(locations, max_loc);
        }
        info[i++] = new JobSplit.SplitMetaInfo( 
            locations, offset,
            split.getLength());
        offset += currLen - prevLen;
      }
    }
    return info;
  }

  private static void writeJobSplitMetaInfo(FileSystem fs, Path filename, 
      FsPermission p, int splitMetaInfoVersion, 
      JobSplit.SplitMetaInfo[] allSplitMetaInfo) 
  throws IOException {
    // write the splits meta-info to a file for the job tracker
    // job.splitmetainfo 内容:
    // header|version|分片数|split_0.getlocations 个数, split_0.getlocations 各个location位置, split_0.在job.split中的偏移量, split_0.length|
    //                     |split_1.getlocations 个数, split_1.getlocations 各个location位置, split_1.在job.split中的偏移量, split_1.length|
    //                     |...
    FSDataOutputStream out = 
      FileSystem.create(fs, filename, p);
    out.write(JobSplit.META_SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(out, splitMetaInfoVersion);
    WritableUtils.writeVInt(out, allSplitMetaInfo.length);
    for (JobSplit.SplitMetaInfo splitMetaInfo : allSplitMetaInfo) {
      splitMetaInfo.write(out);
    }
    out.close();
  }
}

