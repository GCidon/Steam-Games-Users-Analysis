# Steam Games/Users Analysis

## How to run the code

### Local Mode

In order to run our applications in local mode using Linux Ubuntu or an Ubuntu virtual machine in Windows, we need:

+ Python installed
+ Spark installed
+ Steam.csv downloaded (you can find it in the data folder)

Once we have all the requirements, we can easily run the code with the following command line:

``` $spark-submit file_name.py "argument" ```

Where "argument" is only included for the execution of codes that need an argument, such as GameRecommendation.py, which needs a game as an argument in order to operate correctly.

### Cluster Mode

In order to run our applications in local mode using Linux Ubuntu or an Ubuntu virtual machine in Windows, we need:

+ Python installed in our instance
+ Spark installedin our instance
+ Steam.csv downloaded in our instance (you can find it in the data folder)

Once we have all the requirements, we can easily run the code with the following command line:

``` $spark-submit --num-executors N --executor-cores M  "argument" ```

Where 
+ N is the number of worker nodes
+ M is the number of cores per worker node
+ "argument" is only included for the execution of codes that need an argument, such as GameRecommendation.py, which needs a game as an argument in order to operate correctly.

### Python scripts

Some scripts were made just for graphic images generation, such as the file from the folder "Fechas". To run this code, you will only need to have the Python Packages needed to generate that kind of data, eg. MatPlotLib, Numpy or Pandas.

Running that code, the program will only show you the bar graphics, but if you uncomment the last line (just by erasing the "#"), it will save it as a PNG in the same folder you run it.
 
