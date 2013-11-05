# Mining Example

We give an example about how the FIM mining methods work on a synthetic dataset.

## Horizontal Transaction Database

_sample.dat_ is a _horizontal transaction database_. Each number represents
and item and each line represents a transaction. **BigFim** algorithm works on
this type of database.

## Vertical Transaction Database

**Dist-Eclat** algorithm works on _vertical transaction database_, where each
line represents an item and the transactions that this item appears. Format of
each line is:

    item_id{tab}[space separated transactions]

_sample-tids.dat_ is a vertical transaction database.

## Configuring the Parameters

The methods accept the parameters as a configuration file. Config file is a
standart properties file, which include key value pairs at each line. Parameters
are explained in detail in the example configuration files.

## Running the miner

Here is the step by step introductions to run one of the miners on a single-node
setup hadoop platform.

Make sure that you have the database in both formats:

    cd example
    java -cp ../bigfim-*.jar org.apache.mahout.fpm.DbTransposer sample.dat

Put the datafiles in to HDFS:

    $HADOOP_PREFIX/bin/hadoop fs -mkdir data
    $HADOOP_PREFIX/bin/hadoop fs -put sample.dat data
    $HADOOP_PREFIX/bin/hadoop fs -put sample-tids.dat data
    
Run the dist-eclat miner:
    
    $HADOOP_PREFIX/bin/hadoop jar ../bigfim-*.jar org.apache.mahout.fpm.disteclat.DistEclatDriver config-sample-Dist-Eclat.properties
   
It will run for a while. Get the frequent itemsets:

    $HADOOP_PREFIX/bin/hadoop fs -get output/sample-60-3 . 
   
The file `sample-60-3/fis/part-r-00000` lists all the frequent itemsets.
Please note that the file is encoded for compression. To decode the file and 
write all the individual frequent itemsets to `/tmp/out.txt` use the following 
command:

    java -cp ../bigfim-*.jar org.apache.mahout.fpm.disteclat.util.TriePrinter output/sample-60-3/fis/part-r-00000 /tmp/out.txt
 
For the format of the output please refer to the project [wiki][wiki].

## Preparing your own database

To run BigFim and Dist-Eclat on your own datasets you should prepare their 
formats accordingly. We provide a tool to convert a horizontal database to a 
vertical one. We provide an example over the well known _mushroom_ dataset. 

Firstly, download the datasets from [fimi repository][fimi]. Use the following 
commands to download the dataset and uncompress it:

    wget http://fimi.ua.ac.be/data/mushroom.dat.gz
    gunzip mushroom.dat.gz


To convert the _horizontal_ database to _vertical_ database, use the following
command:

    java -cp ../bigfim-*.jar org.apache.mahout.fpm.util.DbTransposer sample.dat
   
It will create a file called _mushroom-tids.dat_ in the same folder. You can 
provide this file to Dist-Eclat. Config files for _mushroom_ dataset is provided 
as well.

## Sample dataset generation

Sample dataset is genereted by IBM Quest Synthetic data generator (the version 
that generates a three column file) with the following commands.

    ./gen lit -ascii -ntrans 0.1 -tlen 10 -nitems 0.02 -npats 20 -patlen 4 -fname T20I4D100 -randseed -100
    sed T20I4D100.data -e "s/ \+/ /g"|awk '{ a[$2] = a[$2] $3 " " } END { for (i in a) print substr(a[i],1,length(a[i])-1) }' > sample.dat



[fimi]: http://fimi.ua.ac.be/data/
[wiki]: https://gitlab.com/adrem/bigfim/wikis/home
