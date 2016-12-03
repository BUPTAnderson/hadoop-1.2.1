
    这是hadoop-1.2.1的源码，我在学习阅读该源码的时候加入了自己的注释，方便自己查询，
 任何人都可以查看该注释后的源码，水平有限，如有不对，欢迎指正。
    我们基于新的mapreduce API, 正常我们写好一个hadoop的mapreduce程序, 将程序编译
 好的jar包上传到一台可以运行hadoop mapreduce程序的节点上, 执行命令：
 hadoop jar xxx.jar -files=blacklist.txt,whitelist.txt -libjars=third-party.jar
 -archives=directionary.zip -input /test/input -output /test/output

 一个word count的mapreduce程序示例：
 public static void main(String[] args) throws Exception {
   Configuration conf = new Configuration();
   String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
   if (otherArgs.length != 2) {
     System.err.println("Usage: wordcount <in> <out>");
     System.exit(2);
   }
   Job job = new Job(conf, "word count");
   job.setJarByClass(WordCount.class);
   job.setMapperClass(TokenizerMapper.class);
   job.setCombinerClass(IntSumReducer.class);
   job.setReducerClass(IntSumReducer.class);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(IntWritable.class);
   FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
   FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
   System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
     在我们的mapreduce程序, 会有job实例, 调用job.waitForCompletion(true)? 0:1
 这里的waitForCompletion()就是程序的入口了, 源码的分析从调用这个方法开始.

PS: GenericOptionsParser是一个工具类, 对我们执行的命令中的各种参数进行解析, 像:
-files=blacklist.txt,whitelist.txt, 会在conf中设置
<tmpfiles, blacklist.txt,whitelist.txt>, 而new Job(conf, "word count")
Job实例内部有jobconf, jobconf = new org.apache.hadoop.mapred.JobConf(conf)
conf里面的配置信息都会copy到jobconf中, 所以对命令解析后的参数信息都会保存在jobconf
里面。
