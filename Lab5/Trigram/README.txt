Trigram analysis on Latin Texts using Spark 

Environment to be used for running:
Virtual Machine provided for Spark.
Download Here ( https://buffalo.box.com/s/t72vy6py6r6v852anwa55whl4mmibfaw )


Copy the JAR file trigram.jar , input files and new_lemmatizer.csv

Open terminal inside the folder, for example let's say the files are stored in a folder 'trigrams' 
having path as /home/hadoop/trigrams/

Then you can execute the spark job using spark-submit as follows:
 (the Input folder contains many sample input files, with various quantities, I'm providing the command for the files kept in /input1files)

************************************************************************************

spark-submit --class "trigram.WordCount"  trigram.jar /home/hadoop/trigrams/Input/input1files/   Output/out1

************************************************************************************

The above command will use the input files from Input/input1files and store the output in a new folder 'Output/out1' inside the directory
from which the command was run

A sample output has been provided in the folder 'Sample Output'
