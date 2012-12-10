HADOOP        = /usr/lib/hadoop
HADOOP_MR     = /usr/lib/hadoop-0.20-mapreduce/
HADOOP_HDFS   = /usr/lib/hadoop-hdfs
CLASSPATH=$(HADOOP)/hadoop-common.jar:$(HADOOP)/hadoop-auth.jar:$(HADOOP)/hadoop-annotations.jar:$(HADOOP_MR)/hadoop-core.jar:$(HADOOP_HDFS)/hadoop-hdfs.jar
JAR	= jar
JAVAC	= javac

PatentJoin.jar: PatentJoin.java
	-mkdir patentjoin_classes
	javac -classpath $(CLASSPATH) PatentJoin.java -d patentjoin_classes
	$(JAR) -cvf PatentJoin.jar -C patentjoin_classes/ .

run:	PatentJoin.jar	
	-hadoop fs -rm -r output
	-hadoop fs -rm -r augmented
#	-hadoop fs -rm -r intermediate
#	hadoop jar ./PatentJoin.jar org.myorg.PatentJoin patents/pat63_99.txt patents/cite75_99.txt intermediate output
	hadoop jar ./PatentJoin.jar org.myorg.PatentJoin patents/pat63_99.txt patents/cite75_99.txt intermediate augmented output

clean::	
	-rm PatentJoin.jar
	-rm -rf patentjoin_classes
	-hadoop fs -rm -r output
	-hadoop fs -rm -r augmented
	-hadoop fs -rm -r intermediate
