CC = javac
HD = hadoop
OUTPUT = /user/sharanprasad/output26
DIR = mkdir

all: create_directory compile create_jar run_hadoop display_output


create_directory :
        $(DIR) sharan_task2

compile :
        $(CC) -d sharan_task2 StatsCounter.java

create_jar :
        jar cvf sharan_task2.jar -C  sharan_task2/ .


run_hadoop :
        $(HD) jar sharan_task2.jar nz.ac.vuw.ecs.sharanprasad.StatsCounter /tmp/aol $(OUTPUT)

display_output :
        hdfs dfs -cat $(OUTPUT)/results.txt

clean :  vihadoopclean directory_clean

hadoopclean :
        hdfs dfs -rm -r -f $(OUTPUT)

directory_clean :
        rm -rf sharan_task2 sharan_task2.jar