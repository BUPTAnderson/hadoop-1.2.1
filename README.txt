#
#    这是hadoop-1.2.1的源码，我在学习阅读该源码的时候加入了自己的注释，方便自己查询，
# 任何人都可以查看该注释后的源码，水平有限，如有不对，欢迎指正。
#    我们基于新的mapreduce API, 正常我们写好一个hadoop的mapreduce程序, 将程序编译
# 好的jar包上传到一台可以运行hadoop mapreduce程序的节点上, 执行命令：
# hadoop jar xxx.jar -files=blacklist.txt,whitelist.txt -libjars=third-party.jar
# -archives=directionary.zip -input /test/input -output /test/output
# 在我们的mapreduce程序, 会有job实例, 调用job.waitForCompletion(true)? 0:1
# 这里的waitForCompletion()就是程序的入口了, 源码的分析从调用这个方法开始.
For the latest information about Hadoop, please visit our website at:

   http://hadoop.apache.org/core/

and our wiki, at:

   http://wiki.apache.org/hadoop/

This distribution includes cryptographic software.  The country in 
which you currently reside may have restrictions on the import, 
possession, use, and/or re-export to another country, of 
encryption software.  BEFORE using any encryption software, please 
check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to 
see if this is permitted.  See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and
Security (BIS), has classified this software as Export Commodity 
Control Number (ECCN) 5D002.C.1, which includes information security
software using or performing cryptographic functions with asymmetric
algorithms.  The form and manner of this Apache Software Foundation
distribution makes it eligible for export under the License Exception
ENC Technology Software Unrestricted (TSU) exception (see the BIS 
Export Administration Regulations, Section 740.13) for both object 
code and source code.

The following provides more details on the included cryptographic
software:
  Hadoop Core uses the SSL libraries from the Jetty project written 
by mortbay.org.
