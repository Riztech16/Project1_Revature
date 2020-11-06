####PROJECT 1 HIVE QUERIES#####

##########################################################################
#Question 1: Most Viewed english wikipedia article on October 20th 2020


CREATE DATABASE PAGE_VIEW_10_20_2020_DB;


USE PAGE_VIEW_10_20_2020_DB;


CREATE TABLE PAGE_VIEW_GENERAL_DATA
(LANGUAGE STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


LOAD DATA LOCAL INPATH '/home/syed/page_view_files/*' INTO TABLE PAGE_VIEW_GENERAL_DATA;

SELECT ARTICLE_NAME, SUM(ARTICLE_VIEWS) AS TOTAL_VIEWS_DESKTOP
FROM PAGE_VIEW_GENERAL_DATA
WHERE LANGUAGE='en'
GROUP BY ARTICLE_NAME
ORDER BY TOTAL_VIEWS_DESKTOP DESC
LIMIT 10;

#######################################################################

#######################################################################
#Question 2: Largest fraction of its readers follow an internal link to another wikipedia article 
#Dataset: March 11, 2020 - April 11, 2020
#Link: https://dumps.wikimedia.org/other/clickstream/2020-03/
#use clickstream-enwiki-2020-03.tsv.gz 


CREATE DATABASE CLICK_STREAM_311_411_DB;


USE CLICK_STREAM_311_411_DB;


CREATE TABLE CLICK_STREAM_GENERAL_DATA;
(REFERRER STRING, CURRENT_ARTICLE STRING, LINK_TYPE STRING, CURRENT_ARTICLE_VIEWS INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


LOAD DATA LOCAL INPATH '/home/syed/clickstream-enwiki-2020-03.tsv' INTO TABLE CLICK_STREAM_GENERAL_DATA;


CREATE TABLE CLICK_STREAM_ARTICLE_VIEWED_BOTH
(REFERRER STRING, ARTICLE_VIEWS_BOTH INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#sending external + internal link data
INSERT INTO TABLE CLICK_STREAM_ARTICLE_VIEWED_BOTH
SELECT REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA
GROUP BY REFERRER;

SELECT * FROM CLICK_STREAM_ARTICLE_VIEWED_BOTH 
LIMIT 5;


CREATE TABLE CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY 
(REFERRER STRING, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


INSERT INTO TABLE CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY  
SELECT REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA
WHERE LINK_TYPE='link'
GROUP BY REFERRER;

SELECT * FROM CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY  
LIMIT 5;

CREATE TABLE CLICK_STREAM_ARTICLE_VIEWED_FRACTIONALLY 
(REFERRER STRING, ARTICLE_VIEWS_BOTH INT, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


INSERT INTO TABLE CLICK_STREAM_ARTICLE_VIEWED_FRACTIONALLY 
SELECT B.REFERRER, B.ARTICLE_VIEWS_BOTH, I.ARTICLE_VIEWS_INTERNALLY 
FROM CLICK_STREAM_ARTICLE_VIEWED_BOTH B 
INNER JOIN CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY I
ON (B.REFERRER=I.REFERRER);

SELECT REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY, 
ROUND(((ARTICLE_VIEWS_INTERNALLY/ARTICLE_VIEWS_BOTH)*100),3) AS INTERNAL_FRACTIONAL_VIEW
FROM CLICK_STREAM_ARTICLE_VIEWED_FRACTIONALLY
WHERE ARTICLE_VIEWS_INTERNALLY > 1000000
GROUP BY REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY
ORDER BY INTERNAL_FRACTIONAL_VIEW DESC
LIMIT 20;

DROP TABLE CLICK_STREAM_ARTICLE_VIEWED_FRACTIONALLY;
DROP TABLE CLICK_STREAM_ARTICLE_VIEWED_BOTH;
DROP TABLE CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY; 
##################################################################

##################################################################
#Question 3:  The series of wikipedia articles, starting with Hotel California that
#keeps the largest fraction of its readers clicking on internal links
#same dataset as question 2


CREATE TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH
(CURRENT_ARTICLE STRING, REFERRER STRING, ARTICLE_VIEWS_BOTH INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


INSERT INTO CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH
SELECT CURRENT_ARTICLE, REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA 
WHERE REFERRER LIKE 'Hotel_California%'
GROUP BY CURRENT_ARTICLE, REFERRER;

SELECT * FROM CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH
LIMIT 5;


CREATE TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY 
(CURRENT_ARTICLE STRING, REFERRER STRING, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY 
SELECT CURRENT_ARTICLE, REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA 
WHERE REFERRER LIKE 'Hotel_California%' AND LINK_TYPE='link'
GROUP BY CURRENT_ARTICLE, REFERRER;

SELECT * FROM CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY
LIMIT 5;

CREATE TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_FRACTIONALLY 
(CURRENT_ARTICLE STRING, REFERRER STRING, ARTICLE_VIEWS_BOTH INT, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';


INSERT INTO TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_FRACTIONALLY 
SELECT  B.CURRENT_ARTICLE, B.REFERRER, B.ARTICLE_VIEWS_BOTH, I.ARTICLE_VIEWS_INTERNALLY 
FROM CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH B 
INNER JOIN CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY I
ON (B.REFERRER=I.REFERRER) AND (B.CURRENT_ARTICLE=I.CURRENT_ARTICLE);


SELECT CURRENT_ARTICLE, REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY, 
ROUND(((ARTICLE_VIEWS_INTERNALLY/ARTICLE_VIEWS_BOTH)*100),3) AS INTERNAL_FRACTIONAL_VIEW
FROM CLICK_STREAM_HC_ARTICLE_VIEWED_FRACTIONALLY
GROUP BY CURRENT_ARTICLE, REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY
ORDER BY ARTICLE_VIEWS_INTERNALLY DESC
LIMIT 20;

##########OPTIONAL
SELECT LINK_TYPE, COUNT(LINK_TYPE)
FROM CLICK_STREAM_GENERAL_DATA
WHERE REFERRER LIKE 'Hotel_California%'
GROUP BY LINK_TYPE;

DROP TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_FRACTIONALLY;
DROP TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH;
DROP TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY; 

####################################################################

####################################################################
#Question 4: Wikipedia artciles popular in US UK and Australia
# LINK: https://dumps.wikimedia.org/other/pageviews/2020/2020-01/
# Use Jan 3, 13, 26, 31
#US timings: 24:00, 1:00, 2:00, 3:00,
#AUS timings: 8:00, 9:00, 10:00, 11:00
#UK timings: 19:00, 20:00, 21:00, 22:00

CREATE DATABASE COUNTRY_BASED_VIEWS_DB;

USE COUNTRY_BASED_VIEWS_DB;

CREATE TABLE US_PAGE_VIEWS_GENERAL_DATA 
(DOMAIN STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/US_page_views_general_data/*' INTO TABLE US_page_views_general_data;

SELECT ARTICLE_NAME, SUM(ARTICLE_VIEWS)
FROM US_PAGE_VIEWS_GENERAL_DATA
WHERE 
DOMAIN ='en'
AND
ARTICLE_NAME != 'Main_Page' 
AND 
ARTICLE_NAME != '-' 
AND 
ARTICLE_NAME != 'Special:Search'
ORDER BY ARTICLE_VIEWS DESC
GROUP BY ARTICLE_NAME
LIMIT 5;

CREATE TABLE UK_PAGE_VIEWS_GENERAL_DATA 
(DOMAIN STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/UK_page_views_general_data/*' INTO TABLE UK_page_views_general_data;

SELECT ARTICLE_NAME, SUM(ARTICLE_VIEWS)
FROM UK_PAGE_VIEWS_GENERAL_DATA 
WHERE
DOMAIN='en'
AND 
ARTICLE_NAME != 'Main_Page'
AND 
ARTICLE_NAME != '-' 
AND 
ARTICLE_NAME != 'Special:Search'
ORDER BY ARTICLE_VIEWS DESC
GROUP BY ARTICLE_NAME
LIMIT 5;

CREATE TABLE AUS_PAGE_VIEWS_GENERAL_DATA 
(DOMAIN STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/AUS_page_views_general_data/*' INTO TABLE AUS_page_views_general_data;

SELECT ARTICLE_NAME, SUM(ARTICLE_VIEWS)
FROM AUS_PAGE_VIEWS_GENERAL_DATA 
WHERE 
DOMAIN='en'
AND
ARTICLE_NAME != 'Main_Page'
AND 
ARTICLE_NAME != '-' 
AND 
ARTICLE_NAME != 'Special:Search'
ORDER BY ARTICLE_VIEWS DESC
GROUP BY ARTICLE_NAME
LIMIT 5;
####################################################################

####################################################################
#Question 5: Average article views before offending edit is reversed.
# Dataset: 01/01/20-01/31/20

#LINK: https://dumps.wikimedia.org/other/mediawiki_history/2020-09/enwiki/
#USE 2020-09.enwiki.2020-01.tsv.bz2 for page revision

#LINK:https://dumps.wikimedia.org/other/clickstream/2020-01/        for click-stream monthly
#USE clickstream-enwiki-2020-01.tsv.gz 

#LINK: https://dumps.wikimedia.org/other/pageviews/2020/2020-01/   for page-views

#01/03: use pageviews-20200103-030000 3-12
#01/05: use pageviews-20200105-100000 10-11

#I put 1/08/20 - 1/10/20 in same folder
#01/08: use pageviews-20200108-180000 18-19
#01/09: use pageviews-20200109-040000 4-5
#01/10: use pageviews-20200110-220000  22-23

#01/06: use pageviews-20200106-130000 13-15

CREATE DATABASE REVISION_USER_HISTORY_PAGE_DB;

#use the database
USE REVISION_USER_HISTORY_PAGE_DB;

#creating general table to store large dataset
CREATE TABLE REVISION_USER_HISTORY_PAGE_GENERAL_DATA
(WIKI_DB STRING, EVENT_ENTTITY STRING, EVENT_TYPE STRING, EVENT_TIMESTAMP STRING,
EVENT_COMMENT STRING, EVENT_USER_ID BIGINT, EVENT_USER_TEXT_HISTORICAL STRING,
EVENT_USER_TEXT STRING, EVENT_USER_BLOCKS_HISTORICAL ARRAY<STRING>, EVENT_USER_BLOCKS ARRAY<STRING>,
EVENT_USER_GROUPS_HISTORICAL ARRAY<STRING>, EVENT_USER_GROUPS ARRAY<STRING>, EVENT_USER_IS_BOT_BY_HISTORICAL ARRAY<STRING>,
EVENT_USER_IS_BOT_BY ARRAY<STRING>, EVENT_USER_IS_CREATED_BY_SELF BOOLEAN, EVENT_USER_IS_CREATED_BY_SYSTEM BOOLEAN, EVENT_USER_IS_CREATED_BY_PEER BOOLEAN,
EVENT_USER_IS_ANONYMOUS BOOLEAN, EVENT_USER_REGISTRATION_TIMESTAMP STRING, EVENT_USER_CREATION_TIMESTAMP STRING, EVENT_USER_FIRST_EDIT_TIMESTAMP STRING,
EVENT_USER_REVISION_COUNT BIGINT, EVENT_USER_SECONDS_SINCE_PREVIOUS_REVISION BIGINT, PAGE_ID BIGINT, PAGE_TITLE_HISTORICAL STRING, PAGE_TITLE STRING,
PAGE_NAMESPACE_HISTORICAL INT,  PAGE_NAMESPACE_IS_CONTENT_HISTORICAL BOOLEAN, PAGE_NAMESPACE INT, PAGE_NAMESPACE_IS_CONTENT BOOLEAN, PAGE_IS_REDIRECT BOOLEAN, 
PAGE_IS_DELETED BOOLEAN, PAGE_CREATION_TIMESTAMP STRING, PAGE_FIRST_EDIT_TIMESTAMP STRING, PAGE_REVISION_COUNT BIGINT, PAGE_SECONDS_SINCE_PREVIOUS_REVISION BIGINT,
USER_ID BIGINT, USER_TEXT_HISTORICAL STRING, USER_TEXT STRING, USER_BLOCKS_HISTORICAL ARRAY<STRING>, USER_BLOCKS ARRAY<STRING>, USER_GROUPS_HISTORICAL ARRAY<STRING>, USER_GROUPS ARRAY<STRING>,
USER_IS_BOT_BY_HISTORICAL ARRAY<STRING>, USER_IS_BOT_BY ARRAY<STRING>, USER_IS_CREATED_BY_SELF BOOLEAN, USER_IS_CREATED_BY_SYSTEM BOOLEAN, USER_IS_CREATED_BY_PEER BOOLEAN, USER_IS_ANONYMOUS BOOLEAN,
USER_REGISTRATION_TIMESTAMP STRING, USER_CREATION_TIMESTAMP STRING, USER_FIRST_EDIT_TIMESTAMP STRING, REVISION_ID BIGINT, REVISION_PARENT_ID BIGINT, REVISION_MINOR_EDIT BOOLEAN, 
REVISION_DELETED_PARTS ARRAY<STRING>, REVISION_DELETED_PARTS_ARE_SUPPRESSED BOOLEAN, REVISION_TEXT_BYTES BIGINT, REVISION_TEXT_BYTES_DIFF BIGINT, REVISION_TEXT_SHA1 STRING, REVISION_CONTENT_MODEL STRING, REVISION_CONTENT_FORMAT STRING,
REVISION_IS_DELETED_BY_PAGE_DELETION BOOLEAN, REVISION_DELETED_BY_PAGE_DELETION_TIMESTAMP STRING, REVISION_IS_IDENTITY_REVERTED BOOLEAN, REVISION_FIRST_IDENTITY_REVERTING_REVISION_ID BIGINT,
REVISION_SECONDS_TO_IDENTITY_REVERT BIGINT, REVISION_IS_IDENTITY_REVERT BOOLEAN, REVISION_IS_FROM_BEFORE_PAGE_CREATION BOOLEAN, REVISION_TAGS ARRAY<STRING>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/syed/2020-09.enwiki.2020-01.tsv' INTO TABLE REVISION_USER_HISTORY_PAGE_GENERAL_DATA;

#creating simplified general table to store large dataset
CREATE TABLE REVISION_USER_HISTORY_PAGE_SIMPLIFIED_GENERAL_DATA
(PAGE_TITLE STRING, 
VANDALISM_TIMESTAMP STRING,
VANDALISM_UNIX_TIMESTAMP BIGINT,
PAGE_FIXED_TIMELAPSE BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO TABLE REVISION_USER_HISTORY_PAGE_SIMPLIFIED_GENERAL_DATA
SELECT PAGE_TITLE,
PAGE_FIRST_EDIT_TIMESTAMP,
UNIX_TIMESTAMP(PAGE_FIRST_EDIT_TIMESTAMP),
SUM(REVISION_SECONDS_TO_IDENTITY_REVERT)
FROM REVISION_USER_HISTORY_PAGE_GENERAL_DATA
WHERE REVISION_SECONDS_TO_IDENTITY_REVERT > 0 AND PAGE_FIRST_EDIT_TIMESTAMP !=''
GROUP BY PAGE_TITLE, PAGE_FIRST_EDIT_TIMESTAMP,
UNIX_TIMESTAMP(PAGE_FIRST_EDIT_TIMESTAMP);

CREATE TABLE PAGE_RESTORE_TIMESTAMP
(PAGE_TITLE STRING,
PAGE_FIXED_UNIX_TIMESTAMP BIGINT,
PAGE_FIXED_TIMESTAMP STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO PAGE_RESTORE_TIMESTAMP
SELECT PAGE_TITLE, (SUM(VANDALISM_UNIX_TIMESTAMP) + SUM(PAGE_FIXED_TIMELAPSE)), 
FROM_UNIXTIME((SUM(VANDALISM_UNIX_TIMESTAMP) + SUM(PAGE_FIXED_TIMELAPSE)))
FROM REVISION_USER_HISTORY_PAGE_SIMPLIFIED_GENERAL_DATA
GROUP BY PAGE_TITLE;

CREATE TABLE JANUARY_2020_VANDALISMS
(PAGE_TITLE STRING,
VANDALISM_TIMESTAMP STRING,
PAGE_FIXED_TIMESTAMP STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO JANUARY_2020_VANDALISMS
SELECT SGD.PAGE_TITLE, SGD.VANDALISM_TIMESTAMP, PRT.PAGE_FIXED_TIMESTAMP 
FROM REVISION_USER_HISTORY_PAGE_SIMPLIFIED_GENERAL_DATA AS SGD 
INNER JOIN PAGE_RESTORE_TIMESTAMP AS PRT
ON SGD.PAGE_TITLE = PRT.PAGE_TITLE
WHERE SGD.VANDALISM_UNIX_TIMESTAMP > 1577836800 
AND 
PRT.PAGE_FIXED_UNIX_TIMESTAMP BETWEEN 1577836800 AND 1580515200;

CREATE TABLE CLICK_STREAM_GENERAL_DATA
(REFERRER STRING, CURRENT_ARTICLE STRING, LINK_TYPE STRING, CURRENT_ARTICLE_VIEWS INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/syed/clickstream-enwiki-2020-01.tsv' INTO TABLE CLICK_STREAM_GENERAL_DATA;

CREATE TABLE CLICK_STREAM_VIEWS
(CURRENT_ARTICLE STRING, CURRENT_ARTICLE_VIEWS INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO TABLE CLICK_STREAM_VIEWS
SELECT CURRENT_ARTICLE, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA
GROUP BY CURRENT_ARTICLE;

CREATE TABLE ARTICLES_VANDALIZED
(PAGE_TITLE STRING,VANDALISM_TIMESTAMP STRING,
PAGE_FIXED_TIMESTAMP STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO ARTICLES_VANDALIZED
SELECT JV.PAGE_TITLE, JV.VANDALISM_TIMESTAMP,
JV.PAGE_FIXED_TIMESTAMP
FROM JANUARY_2020_VANDALISMS AS JV 
INNER JOIN 
CLICK_STREAM_VIEWS AS CSV
ON JV.PAGE_TITLE = CSV.CURRENT_ARTICLE
ORDER BY CSV.CURRENT_ARTICLE_VIEWS DESC
LIMIT 6;  

CREATE TABLE PAGE_VIEW_GENERAL_DATA_1_03
(LANGUAGE STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/page_views_03/*' INTO TABLE PAGE_VIEW_GENERAL_DATA_1_03;

CREATE TABLE PAGE_VIEW_GENERAL_DATA_1_05
(LANGUAGE STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/page_views_05/*' INTO TABLE PAGE_VIEW_GENERAL_DATA_1_05;

CREATE TABLE PAGE_VIEW_GENERAL_DATA_1_08_09_10
(LANGUAGE STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/page_views_8_9_10/*' INTO TABLE PAGE_VIEW_GENERAL_DATA_1_08_09_10;

CREATE TABLE PAGE_VIEW_GENERAL_DATA_1_06
(LANGUAGE STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/page_views_06/*' INTO TABLE PAGE_VIEW_GENERAL_DATA_1_06;

CREATE TABLE VIEWS_BEFORE_FIXED
(ARTICLE STRING, VIEWS_BEFORE_VANDALISM_REVERSED INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT INTO VIEWS_BEFORE_FIXED
SELECT PVGD.ARTICLE_NAME, SUM(PVGD.ARTICLE_VIEWS)
FROM PAGE_VIEW_GENERAL_DATA_1_03 AS PVGD 
INNER JOIN 
ARTICLES_VANDALIZED AS AC
ON PVGD.ARTICLE_NAME = AC.PAGE_TITLE
WHERE AC.PAGE_TITLE='Esmail_Ghaani'
GROUP BY PVGD.ARTICLE_NAME;

INSERT INTO VIEWS_BEFORE_FIXED
SELECT PVGD.ARTICLE_NAME, SUM(PVGD.ARTICLE_VIEWS)
FROM PAGE_VIEW_GENERAL_DATA_1_05 AS PVGD 
INNER JOIN 
ARTICLES_VANDALIZED AS AC
ON PVGD.ARTICLE_NAME = AC.PAGE_TITLE
WHERE AC.PAGE_TITLE='Dr._Romantic_2'
GROUP BY PVGD.ARTICLE_NAME;

INSERT INTO VIEWS_BEFORE_FIXED
SELECT PVGD.ARTICLE_NAME, SUM(PVGD.ARTICLE_VIEWS)
FROM PAGE_VIEW_GENERAL_DATA_1_08_09_10 AS PVGD 
INNER JOIN 
ARTICLES_VANDALIZED AS AC
ON PVGD.ARTICLE_NAME = AC.PAGE_TITLE
WHERE AC.PAGE_TITLE='Circles_(Mac_Miller_album)'
GROUP BY PVGD.ARTICLE_NAME;

INSERT INTO VIEWS_BEFORE_FIXED
SELECT PVGD.ARTICLE_NAME, SUM(PVGD.ARTICLE_VIEWS)
FROM PAGE_VIEW_GENERAL_DATA_1_06 AS PVGD 
INNER JOIN 
ARTICLES_VANDALIZED AS AC
ON PVGD.ARTICLE_NAME = AC.PAGE_TITLE
WHERE AC.PAGE_TITLE='Reynhard_Sinaga'
GROUP BY PVGD.ARTICLE_NAME;

SELECT AVG(VIEWS_BEFORE_VANDALISM_REVERSED) AS AVERAGE_VIEWS_BEFORE_OFFENDING_EDIT_IS_REVERSED
FROM VIEWS_BEFORE_FIXED;

DROP TABLE VIEWS_BEFORE_FIXED;
DROP TABLE ARTICLES_VANDALIZED;
DROP TABLE CLICK_STREAM_VIEWS;
DROP TABLE JANUARY_2020_VANDALISMS;
DROP TABLE PAGE_RESTORE_TIMESTAMP;
####################################################################################

####################################################################################
#Question 6: Which domain searched for coronavirus the most
# Date: 04/05/20
#LINK: https://dumps.wikimedia.org/other/pageviews/2020/2020-04/
#04/05 use pageviews-20200405-000000 0-23

CREATE DATABASE CORONAVIRUS_SEARCH_BY_COUNTRY_DB;

USE CORONAVIRUS_SEARCH_BY_COUNTRY_DB;

CREATE TABLE PAGE_VIEWS_GENERAL_DATA_405
(DOMAIN STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '    ';

LOAD DATA LOCAL INPATH '/home/syed/page_views_general_data_405/*' INTO TABLE PAGE_VIEWS_GENERAL_DATA_405;

SELECT DOMAIN, SUM(ARTICLE_VIEWS) AS AMOUNT_SEARCHED
FROM PAGE_VIEWS_GENERAL_DATA_405
WHERE ARTICLE_NAME LIKE '%coronavirus%'
GROUP BY DOMAIN
ORDER BY AMOUNT_SEARCHED DESC
LIMIT 15;