transactions= LOAD '/user/hadoop/project/Transactions.txt' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
clean1= FOREACH transactions GENERATE CustID;
trans_group= GROUP clean1 BY CustID;
trans_count= FOREACH trans_group GENERATE group as CustID,COUNT(clean1)as numTrans,(int)1;
min_trans= GROUP trans_count BY $2;
min_trans_count= FOREACH min_trans GENERATE group,MIN(trans_count.numTrans) as minTrans;
trans_join= join min_trans_count BY minTrans,trans_count BY numTrans;
customers= LOAD '/user/hadoop/project1/Customers.txt' USING PigStorage(',') AS (CustID:int, name:chararray, age:int, country_code:int, salary:float);
clean2= FOREACH customers GENERATE CustID,name;
join_result= join clean2 BY CustID,trans_join BY CustID;
finaloutput= FOREACH join_result GENERATE name,minTrans;
STORE finaloutput INTO '/user/hadoop/Query1_output';
