PIG Commands:
1.      Pig //Launch Pig to process data
2.     movies = LOAD '/movies/movies.csv' USING PigStorage(',') as (id,name,year,rating,duration);
3.      DUMP movies;
4.     movies_greater_than_four = FILTER movies BY (float)rating>4.0; 
5.      DUMP movies_greater_than_four;
6.     store movies_greater_than_four into '/movies/movies_greater_than_four';
7.      movies_between_50_60 = FILTER movies by year>1950 and year<1960;
8.     movies_starting_with_A = FILTER movies by name matches 'A.*';
9.     movies_duration_2_hrs = FILTER movies by duration > 7200;
10.   movies_rating_3_4 = FILTER movies BY rating>3.0 and rating<4.0;
11.    DESCRIBE movies;
12.   ILLUSTRATE movies_duration_2_hrs;
13.   movie_duration = FOREACH movies GENERATE name, (double)(duration/60);
14.   grouped_by_year = group movies by year;
15.   count_by_year = FOREACH grouped_by_year GENERATE group, COUNT(movies);
16.   group_all = GROUP count_by_year ALL;
17.   sum_all = FOREACH group_all GENERATE SUM(count_by_year.$1);
18.   DUMP sum_all;
19.   desc_movies_by_year = ORDER movies BY year ASC;
20.  DUMP desc_movies_by_year;
21.   asc_movies_by_year = ORDER movies by year DESC;
22.  DUMP asc_movies_by_year;
23.  top_10_movies = LIMIT movies 10;
24.  DUMP top_10_movies;
25.  sample_10_percent = sample movies 0.1;
26.  dump sample_10_percent;
27.  sample_group_all = GROUP sample_10_percent ALL;
28.  sample_count = FOREACH sample_group_all GENERATE COUNT(sample_10_percent.$0);
29.  dump sample_count;



**************************************************************************************

crimedata data commands

1. project= LOAD 'crimedata.csv' USING PigStorage(',')as(caseno,dataofoccurrence,block,iucr,primarydescription,seconddescription,locationdescription,arrest,domestic,beat,ward,fbicd,xcoordinate,ycoordinate,latitude,longitude,location);
2. theaft_data = filter project by primarydescription matches 'DECEPTIVE PRACTICE';
3.  theft_data = filter project by primarydescription matches ‘THEFT’;
4. homicide_data = filter project by primarydescription matches ‘HOMICIDE’;
5. assault_data = filter project by primarydescription matches ‘ASSAULT’;
6. battery_data = filter project by primarydescription matches ‘BATTERY’;
7. stalking_data = filter project by primarydescription matches ‘STALKING’;
8. residence_data = filter project by  primarydescription  matches 'RESIDENCE';
9.automobile_data = filter project by seconddescription matches ‘AUTOMOBILE’;
10.  entry_data = filter project by seconddescription matches 'ATTEMPT FORCIBLE ENTRY';









****************************************************************************************

                       Example commands To Load Movies.txt File in Pig 
                                      This used for all testfiles


lines = LOAD 'qwqw.txt' AS (line:chararray);
lines = LOAD 'hdfs://localhost:54310/small/war_and_peace.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;
************************************************************************************************
                                     Example commands To Load NYSE.txt File in Pig 
                                     This used for all testfiles


dividends = load '/input/NYSE_dividends' as (exchange, symbol, date, dividend);
grouped   = group dividends by symbol;
avg       = foreach grouped generate group, AVG(dividends.dividend);
store avg into '/output/average_dividend';

*******************************************************************************************
                                     Example commands To Load NYSE.txt File in Pig 
                                     This used for all CSVFILES 


batting = load Batting.csv using PigStorage(',');
runs = FOREACH batting GENERATE $0 as playerID, $1 as year, $8 as run;
grp_data = GROUP runs by (year);
max_runs = FOREACH grp_data GENERATE group as grp, MAX(runs.run) as max_runs;
join_max_run = JOIN max_runs BY ($0, max_runs), runs by (year, run);
join_data = FOREACH join_max_run GENERATE $0 as year, $2 as playerID, $1 as runs;
dump join_data;


  *******************************************************************************                    
                                     Example commands To Load NYSE.txt File in Pig 
                                     This used for all XMLFILES 


REGISTER piggybank.jar // use this command for older pig versions
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();
A = LOAD '/tmp/sample.xml' using org.apache.pig.piggybank.storage.XMLLoader('rows (x:chararray);
B = FOREACH A GENERATE XPath(x, 'row/course_provider'), XPath(x, 'row/city');

Result below:
(1st All Around (English),Clarendon Hills)
(1st All Around (Polish),Clarendon Hills)
************************************************************************************
lines = LOAD 'wars.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;




STORE wordcount INTO 'sample/temp' USING org.elasticsearch.hadoop.pig.EsStorage('es.nodes = localhost','es.port=9200', 'es.nodes.wan.only=true'); 
		
		
		
		
		
STORE movies INTO 'movies1/alldata' USING org.elasticsearch.hadoop.pig.EsStorage('es.nodes = localhost','es.port=9200', 'es.nodes.wan.only=true'); 


crimedata = LOAD 'crimedata.csv' USING PigStorage(',')as(caseno:int,dataofoccurrence:int,block:int,iucr:int,primarydescription:chararray,seconddescription:chararray,locationdescription:chararray,arrest:chararray,domestic:chararray,beat:int,ward:int,fbicd:int,xcoordinate:int,ycoordinate:int,latitude:int,longitude:int,location:int);


STORE crime INTO 'crimedata/alldata' USING org.elasticsearch.hadoop.pig.EsStorage('es.nodes = localhost','es.port=9200', 'es.nodes.wan.only=true');

********************************************************************************************************************************

batting = load Batting.csv using PigStorage(',');
runs = FOREACH batting GENERATE $0 as playerID, $1 as year, (int) $8  as run;
grp_data = GROUP runs by (year);
max_runs = FOREACH grp_data GENERATE group as grp, MAX(runs.run) as max_runs;
join_max_run = JOIN max_runs BY ($0, max_runs), runs by (year, run);
join_data = FOREACH join_max_run GENERATE $0 as year, $2 as playerID, $1 as runs;
dump join_data;


STORE join_data INTO 'batting/temp' USING org.elasticsearch.hadoop.pig.EsStorage('es.nodes = localhost','es.port=9200', 'es.nodes.wan.only=true'); 

*************************************************************************************************************************************


REGISTER  /usr/local/pig/lib/piggybank.jar // use this command for older pig versions
A = LOAD 'sample.xml' using org.apache.pig.piggybank.storage.XMLLoader('record') as (x:chararray);
B = FOREACH A GENERATE XPath(x, 'row/course_provider'), XPath(x, 'row/city');

**************************************************************************************************************************************
