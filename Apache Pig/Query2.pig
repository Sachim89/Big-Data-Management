transactions= LOAD '/user/hadoop/input/Transactions.txt' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
clean1= FOREACH transactions GENERATE CustID,TransTotal,TransNumItems;
trans_group= GROUP clean1 BY CustID;
trans_count= FOREACH trans_group GENERATE group,COUNT(clean1), SUM(clean1.TransTotal),MIN(clean1.TransNumItems);
customers= LOAD '/user/hadoop/input/Customers.txt' USING PigStorage(',') AS (CustID:int, name:chararray, age:int, country_code:int, salary:float);
clean2= FOREACH customers GENERATE CustID,name,salary;
join_result= join clean2 BY CustID,trans_count BY group using 'replicated';
finaloutput= FOREACH join_result GENERATE $0,$1,$2,$4,$5,$6;
STORE finaloutput INTO '/user/hadoop/Query2_output';
