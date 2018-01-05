Bigram analysis on Latin Texts using Spark 

Environment to be used for running:
Virtual Machine provided for Spark.
Download Here ( https://buffalo.box.com/s/t72vy6py6r6v852anwa55whl4mmibfaw )


Copy the JAR file bigram.jar , input files and new_lemmatizer.csv

Open terminal inside the folder, for example let's say the files are stored in a folder 'bigrams' 
having path as /home/hadoop/bigrams/

Then you can execute the spark job using spark-submit as follows:
 (the Input folder contains sample input files, with various quantities, I'm providing the command for the files kept in /input1)

************************************************************************************

spark-submit --class "bigram.WordCount"  bigram.jar /home/hadoop/bigrams/Input/input1/   Output/out1

*************************************************************************************

The above command will use the input files from Input/input1 and store the output in a new folder 'Output/out1' inside the directory
from which the command was run

A sample output has been provided in the folder 'Sample Output'
