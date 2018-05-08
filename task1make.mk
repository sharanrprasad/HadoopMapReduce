OVERRIDING=default
SEARCHID = 7980225
CC = javac
HD = hadoop
OUTPUT = /user/sharanprasad/output24
DIR = mkdir

all: create_directory compile create_jar run_hadoop display_output


create_directory :
        $(DIR) sharan_task1

compile :
        $(CC) -d sharan_task1 SearchHistory.java

create_jar :
        jar cvf sharan_task1.jar -C  sharan_task1/ .


run_hadoop :
        $(HD) jar sharan_task1.jar nz.ac.vuw.ecs.sharanprasad.SearchHistory /tmp/aol $(OUTPUT) $(SEARCHID)

display_output :
        hdfs dfs -cat $(OUTPUT)/part-r-00000

clean : hadoopclean directory_clean

hadoopclean :
        hdfs dfs -rm -r -f $(OUTPUT)

directory_clean :
        rm -rf sharan_task1 sharan_task1.jar