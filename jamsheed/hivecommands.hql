Internal tables:
 -- create internal table with few properties
create table internal_tweets_raw(
min int comment 'Minute',
text string comment 'Users Tweet',
statuscount int comment 'total status count',
location string comment 'location',
hour int,
url string,
friendscount int,
id bigint,
username string,
timezone string,
sec int,
description string,
name string,
month int,
created_at string,
year int,
day int)
comment 'Description of a table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ('creator'='ME', 'created_at'='2017-22-06 10:00:00');


 **************************************************************************
-- Describe table property
describe formatted internal_tweets_raw;

***************************************************************************


 
-- load data into table
load data local inpath '/home/ubuntu/hive_data/bbmas.csv' into table internal_tweets_raw;

***************************************************************************
 
-- drop table
drop table internal_tweets_raw;

****************************************************************************
 
 External tables:
 -- Create external table
create external table if not exists external_tweets_raw(
min int comment 'Minute',
text string comment 'Users Tweet',
statuscount int comment 'total status count',
location string comment 'location',
hour int,
url string,
friendscount int,
id bigint,
username string,
timezone string,
sec int,
description string,
name string,
month int,
created_at string,
year int,
day int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/external_tweets_raw'
;
*********************************************************************************


-- describe table property
describe formatted external_tweets_raw;


********************************************************************************

 
-- load data into external table
load data local inpath '/home/ubuntu/hive_data/bbmas.csv' into table external_tweets_raw;
load data local inpath '/home/ubuntu/hive_data/bbmas18may2015-19-18-02.csv' into table external_tweets_raw;



********************************************************************************
-- drop external table
drop table external_tweets_raw;

**********************************************************************************
How do we create skewed tables?
create table <T> (schema) skewed by (keys) on ('c1', 'c2') [STORED as DIRECTORIES];
Example :

********************************************************************************
-- describe table 
describe formatted skewed_tr;
create external table if not exists skewed_tr(
min int comment 'Minute',
text string comment 'Users Tweet',
statuscount int comment 'total status count',
location string comment 'location',
hour int,
url string,
friendscount int,
id bigint,
username string,
timezone string,
sec int,
description string,
name string,
month int,
created_at string,
year int,
day int) 
COMMENT 'Skewed table for testing purpose'
SKEWED BY (month) ON ('05')
STORED AS SEQUENCEFILE;

***************************************************************************************

-- Create view 
CREATE VIEW IF NOT EXISTS sample_view 
AS SELECT 
min,
text,
statuscount,
location ,
hour,
friendscount, 
id ,
timezone ,
name ,
month,
created_at, 
year,
day,
hashtag 
from tweets_raw;

************************************************************************************************

-- run view
select * from sample_view;

***************************************************************************************************
-- View with Left Outer join
CREATE VIEW IF NOT EXISTS sample_join
AS SELECT
id,
location,
name,
hashtag,
timezonemap.country
from sample_view simplet LEFT OUTER JOIN time_zone_map timezonemap ON simplet.timezone = timezonemap.time_zone;


*********************************************************************************************************
-- run view
select * from sample_view;

*******************************************************************************************************

load data local inpath '/home/ubuntu/hive_data/bbmas.csv' into table tweets_raw PARTITION (hashtag='bbmas');

load data local inpath '/home/ubuntu/hive_data/describeyourselfin3words20may.csv' into table tweets_raw PARTITION (hashtag='describeyourselfin3words');


*************************************************************************************

CREATE TABLE TIME_ZONE_MAP

 TIME_ZONE STRING,
COUNTRY STRING,
NOTES STRING
 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

load data local inpath '/home/ubuntu/hive_data/tsv/time_zone_map.tsv' into table time_zone_map;

***********************************************************************************************

CREATE TABLE DICTIONARY
(
TYPE STRING,
LENGTH INT,
WORD STRING,
POS STRING,
STEMMED STRING,
POLARITY STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

load data local inpath '/home/ubuntu/hive_data/tsv/dictionary.tsv' into table dictionary;

**************************************************************************************************

