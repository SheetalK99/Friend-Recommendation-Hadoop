Author: Sheetal Kadam (sak170006)
CS6350 
Big data Management Analytics and Management
Fall 2019
Homework 1  

Assuming user input directory is :/user/sk/input/ 
Input directory contains soc-LiveJournal1Adj.txt and userdata.txt

Q1:

hadoop jar HW1_Q1.jar MutualFriends /user/sk/input/soc-LiveJournal1Adj.txt /user/sk/out_Q1
hdfs dfs -cat /user/sk/out_Q1/part-r-00000

Q2:
hadoop jar mutualfriends.jar Top10Friends /user/sk/input/soc-LiveJournal1Adj.txt /user/sk/out_Q2_temp /user/sk/out_Q2
hdfs dfs -cat /user/sk/out_Q2/part-r-00000

Q3:
Last 2 parameters are friendid pairs

hadoop jar HW1_Q3.jar FriendStates /user/sk/input/soc-LiveJournal1Adj.txt /user/sk/input/userdata.txt /user/sk/out_Q3 0 1

hdfs dfs -cat /user/sk/out_Q3/part-r-00000


Q4:
hadoop jar HW1_Q4.jar AvgAgeFriends /user/sk/input/soc-LiveJournal1Adj.txt /user/sk/input/userdata.txt /user/sk/out_Q4_temp /user/sk/out_Q4
hdfs dfs -cat /user/sk/out_Q4/part-r-00000

