Data Intensive Computing - Lab5

Partners: Ajay Gandhi (agandhi3@buffalo.edu)
		  Siddharth Pateriya (spateriy@buffalo.edu)

Data Analysis using Spark:

Environment chosen for the Lab:

Virtual Machine provided for Spark.
Download Here ( https://buffalo.box.com/s/t72vy6py6r6v852anwa55whl4mmibfaw )

The files/folders are as follows:

1. Plots.pdf contains the required plots for bi-gram, tri-gram analysis on Latin Texts
2. Folder Bigram contains the JAR files, input files and sample outputs for running Bigram analysis on Latin texts using Spark
3. Folder Trigram contains the JAR files, input files and sample outputs for running Trigram analysis on Latin texts using Spark
4. Folder Vignette contains the Scala file and the titanic.csv dataset

Each folder also contains a README file containing instructions on how to execute the Spark jobs.

The output for the bigram and trigram jobs is explained below:

For 2 words W1 and W2, having Lemmas Lem1W1,Lem2W1, Lem1W2 and Lem2W2, the output is Lem1W1,Lem1W2  <location1><location2>....<location n>

Lem1W1,Lem1W2  <location1><location2>....<location n>
Lem1W1,Lem2W2  <location1><location2>....<location n>
Lem2W1,Lem1W2  <location1><location2>....<location n>
Lem2W1,Lem2W2  <location1><location2>....<location n>

This pattern continues for 3Grams as well, like
Lem1W1,Lem1W2,Lem1W3  <location1><location2>....<location n>
Lem1W1,Lem1W2,Lem2W3  <location1><location2>....<location n>
and so on