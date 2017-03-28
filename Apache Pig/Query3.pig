customers= LOAD '/user/hadoop/input/Customers.txt' USING PigStorage(',') AS (CustID:int, name:chararray, age:int, country_code:int, salary:float);
clean1= FOREACH customers GENERATE country_code;
code_group= GROUP clean1 BY country_code;
code_count= FOREACH code_group GENERATE group as country_code, COUNT(clean1) as noOfCust;
result= FILTER code_count BY (noOfCust>5000) OR (noOfCust<2000);
finaloutput= FOREACH result GENERATE country_code;
STORE finaloutput INTO '/user/hadoop/Query3_output';
