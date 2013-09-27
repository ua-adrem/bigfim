# Mining Example

First download the datasets from [fimi repository][fimi]. For this example, we
will need mushroom dataset. Use the following commands to download the dataset
and uncompress it:

    wget http://fimi.ua.ac.be/data/mushroom.dat.gz
    gunzip mushroom.dat.gz

## Horizontal Transaction Database

_mushroom.dat.gz_ is a _horizontal transaction database_. Each number represents
and item and each line represents a transaction. **BigFim** algorithm works on
this type of database.

## Vertical Transaction Database

**Dist-Eclat** algorithm works on _vertical transaction database_, where each
line represents an item and the transactions that this item appears. Format of
each line is:

    item_id{tab}[space separated transactions]

To convert the _horizontal_ database to _vertical_ database, use the following
command:

    java -cp ../bigfim.0.1.jar ua.util.DbTransposer mushroom.dat
   
It will create a file called _mushroom-tids.dat_ in the same folder.


## Configuring the Parameters

The methods accept the parameters as a configuration file. Config file is a
standart properties files, which include key value pairs at each line.
Parameters are explained in detail in the example configuration files.

## Running the miner

Here is the step by step introductions to run one of the miners on a single-node
setup hadoop platform.

First download the dataset and convert it:

    cd example
    wget http://fimi.ua.ac.be/data/mushroom.dat.gz
    gunzip mushroom.dat.gz
    java -cp ../bigfim.0.1.jar ua.util.DbTransposer mushroom.dat

Run the dist-eclat miner:

    $HADOOP_PREFIX/bin/hadoop fs -mkdir data
    $HADOOP_PREFIX/bin/hadoop fs -put mushroom.dat data
    $HADOOP_PREFIX/bin/hadoop fs -put mushroom-tids.dat data
    $HADOOP_PREFIX/bin/hadoop jar ../bigfim.0.1.jar config-mushroom-Dist-Eclat.properties
   
It will run for a while. Get the frequent itemsets:

    $HADOOP_PREFIX/bin/hadoop fs -get output/mushroom-800-10-3 .
   
The file `mushroom-800-10-3/fis/part-r-00000` lists all the frequent itemsets.
For the format of the output please refer to the project [wiki][wiki].






[fimi]: http://fimi.ua.ac.be/data/
[wiki]: https://gitlab.com/adrem/bigfim/wikis/home
