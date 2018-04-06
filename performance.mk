OVERRIDING=default
NUMREDUCER = 1
CC = javac
HD = hadoop
OUTPUT = /user/sharanprasad/output25
DIR = mkdir

all: create_directory compile create_jar run_hadoop


create_directory :
        $(DIR) sharan_wordcount

compile :
        $(CC) -d sharan_wordcount WordCount.java

create_jar :
        jar cvf sharan_wordcount.jar -C  sharan_wordcount/ .


run_hadoop :
        $(HD) jar sharan_wordcount.jar nz.ac.vuw.ecs.sharanprasad.WordCount /tmp/aol $(OUTPUT) $(NUMREDUCER)

clean : hadoopclean directory_clean

hadoopclean :
        hdfs dfs -rm -r -f $(OUTPUT)

directory_clean :
        rm -rf sharan_wordcount sharan_wordcount.jar