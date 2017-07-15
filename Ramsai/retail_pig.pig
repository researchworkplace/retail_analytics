set default_parallel 1

log1 = load '/home/ubuntu/project/Retail_Analysis/Log_Files/0.tsv' using PigStorage('\t');
log2 = load '/home/ubuntu/project/Retail_Analysis/Log_Files/1.tsv' using PigStorage('\t') ;
log3 = load '/home/ubuntu/project/Retail_Analysis/Log_Files/2.tsv' using PigStorage('\t') ;
log4 = load '/home/ubuntu/project/Retail_Analysis/Log_Files/3.tsv' using PigStorage('\t') ;
log5 = load '/home/ubuntu/project/Retail_Analysis/Log_Files/4.tsv' using PigStorage('\t') ;
log6 = load '/home/ubuntu/project/Retail_Analysis/Log_Files/5.tsv' using PigStorage('\t') ;  
log = UNION log1, log2, log3, log4, log5, log6 ;

temp1 = FOREACH log  GENERATE $1 as timestamp ,$7 as ipaddress,$12 as url ,$13 as swid,$49 as city,$50 as country,$52as state ;

STORE temp1 into '/home/ubuntu/project/Retail_Analysis/Log_Files/multi' using org.apache.pig.piggybank.storage.CSVExcelStorage (',','NO_MULTILINE');

STORE (foreach(group temp1 all) generate flatten($1)) into '/home/ubuntu/project/Retail_Analysis/Log_Files/onelog' using org.apache.pig.piggybank.storage.CSVExcelStorage (',','NO_MULTILINE');