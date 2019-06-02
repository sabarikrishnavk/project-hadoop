Pre condition
--

Create/copy the sample input.txt under "input" folder.

Sample entry : xGe_ZX-U,df9a9dd86f161d059dc5c0e03a792617,1513252366,11,2017-12-14
in songid,userId,timestamp,hr,date format


Program logic
--
First map reduce to generate "score for a song" in the entire data set 
Combiner class (SongScoreReducer) is used to improve performance of score calculation.

Second map reduce to sort the songs per day based on score and output into different partitions.

Comparators are used for sorting the results based on a group (Song,date,score) and based on date as well.


To run the program in eclips
--

Run As Java program and update the program arguments in Run As configuratio


Java program : com.pgbde.hadoop.MusicScoreJob

Arguments : input/input.txt output/score


Java program : com.pgbde.hadoop.MusicSortJob

Arguments : output/score output/results


To run the program in command prompt
--

Run  >> mvn install
Execute >> 
hadoop jar target/project-hadoop-0.0.1-SNAPSHOT.jar com.pgbde.hadoop.MusicScoreJob input/input.txt output/score/

hadoop jar target/project-hadoop-0.0.1-SNAPSHOT.jar com.pgbde.hadoop.MusicSortJob output/score/ output/results

To run in AWS
--
Copy the jar via winscp to cloudera ec2 instance
Connect via putty

Copy target/project-hadoop-0.0.1-SNAPSHOT.jar to /media/sf_VM/musicproject.jar
Execute >>
sudo rm -rf /yarn/nm/usercache/ec2-user

nohup hadoop jar musicproject.jar com.pgbde.hadoop.MusicScoreJob s3a://mapreduce-project-bde/part-00000 s3a://sabari-musicproject-output/score >> job1.txt

nohup hadoop jar musicproject.jar com.pgbde.hadoop.MusicSortJob s3a://sabari-saavn-output/score s3a://sabari-musicproject-output/results >> job2.txt
