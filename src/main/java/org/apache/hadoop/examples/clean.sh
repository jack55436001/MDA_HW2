hadoop fs -rm -R /user/root/data/tmp1
hadoop fs -rm -R /user/root/output/pagerank5*
hadoop fs -rm -R /user/root/data/input_pr5*
hadoop fs -copyFromLocal input_pr5.txt /user/root/data/input_pr5
