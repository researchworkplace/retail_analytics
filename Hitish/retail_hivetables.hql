create database retail;

Use retail;

create table logs (time_stamp STRING, ipaddress STRING, url STRING, swid STRING, city STRING, country STRING ,state STRING) ROW FORMAT DELIMITED FIELDS TERMINATED by ',' stored as textfile tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/ubuntu/project/Retail_Analysis/Log_Files/onelog/part*' OVERWRITE INTO TABLE logs;



create table users (swid STRING, birth_dt STRING, gender_cd CHAR(1)) ROW FORMAT DELIMITED FIELDS TERMINATED by '\t' stored as textfile tblproperties ("skip.header.line.count"="1");

load data local inpath '/home/ubuntu/project/Retail_Analysis/users.tsv' overwrite into table users;

create table products (url STRING, category STRING) ROW FORMAT DELIMITED FIELDS TERMINATED by '\t' stored as textfile tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/ubuntu/project/Retail_Analysis/products.tsv' OVERWRITE INTO TABLE products;


create table loganalytics as select to_date(o.time_stamp) logdate, o.url, o.ipaddress, o.city, upper(o.state) state, o.country, p.category, CAST(datediff( from_unixtime( unix_timestamp() ), from_unixtime( unix_timestamp(u.birth_dt, 'dd-MMM-yy'))) / 365 AS INT) age, u.gender_cd from logs o inner join products p on o.url = p.url left outer join users u on o.swid = concat('{', u.swid , '}');



SET hive.cli.print.header=true;

insert overwrite local directory '/home/ubuntu/project/Retail_Analysis/staging' row format delimited fields terminated by ',' select * from loganalytics;
