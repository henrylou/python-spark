from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("hostsIntersection").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv")
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv")

    julyFirstHosts = julyFirstLogs.map(lambda line: line.split('\t')[0])
    augustFirstHosts = augustFirstLogs.map(lambda line: line.split('\t')[0])

    intersectedLogHosts = julyFirstHosts.intersection(augustFirstHosts)
    cleanLogHosts = intersectedLogHosts.filter(lambda line: line != 'host')

    cleanLogHosts.saveAsTextFile("out/nasa_logs_same_hosts.csv")

'''
"in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
"in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

Example output:
vagrant.vf.mmc.com
www-a1.proxy.aol.com
.....    

Keep in mind, that the original log files contains the following header lines.
host    logname    time    method    url    response    bytes

Make sure the head lines are removed in the resulting RDD.
'''
