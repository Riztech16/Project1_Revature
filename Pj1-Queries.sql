####PROJECT 1 HIVE QUERIES#####

##########################################################################
#Question 1: Most Viewed english wikipedia article on October 20th 2020

#create database
CREATE DATABASE PAGE_VIEW_10_20_2020_DB;

#use the database
USE PAGE_VIEW_10_20_2020_DB;

#creating general table to store large dataset
CREATE TABLE PAGE_VIEW_GENERAL_DATA
(LANGUAGE STRING, ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#Loading data
LOAD DATA LOCAL INPATH '/home/syed/page_view_files/*' INTO TABLE PAGE_VIEW_GENERAL_DATA;

#creating separate table for english wikipedia articles
CREATE TABLE PAGE_VIEW_EN 
(ARTICLE_NAME STRING, ARTICLE_VIEWS INT, IS_SPIDER BOOLEAN)
PARTITIONED BY (LANGUAGE STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#sending data to be partitioned from general table
INSERT INTO TABLE PAGE_VIEW_EN PARTITION(LANGUAGE='en')
SELECT ARTICLE_NAME, ARTICLE_VIEWS, IS_SPIDER 
FROM PAGE_VIEW_GENERAL_DATA
WHERE LANGUAGE='en';

#Merging the duplicate and sorting from highest to lowest viewed
#article (Top 10) on desktop
SELECT ARTICLE_NAME, SUM(ARTICLE_VIEWS) AS VIEWS 
FROM PAGE_VIEW_EN
GROUP BY ARTICLE_NAME
ORDER BY VIEWS DESC
LIMIT 10;
#######################################################################

#######################################################################
#Question 2: Largest fraction of its readers follow an internal link to another wikipedia article 
#Dataset: March 11, 2020 - April 11, 2020

#create database
CREATE DATABASE CLICK_STREAM_311_411_DB;

#use the database
USE CLICK_STREAM_311_411_DB;

#creating general table to store large dataset
CREATE TABLE CLICK_STREAM_GENERAL_DATA;
(REFERRER STRING, CURRENT_ARTICLE STRING, LINK_TYPE STRING, CURRENT_ARTICLE_VIEWS INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#Loading data
LOAD DATA LOCAL INPATH '/home/syed/clickstream-enwiki-2020-03.tsv' INTO TABLE CLICK_STREAM_GENERAL_DATA;

#creating separate table wikipedia articles
CREATE TABLE CLICK_STREAM_ARTICLE_VIEWED_BOTH
(REFERRER STRING, ARTICLE_VIEWS_BOTH INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#sending external + internal link data
INSERT INTO TABLE CLICK_STREAM_ARTICLE_VIEWED_BOTH
SELECT REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA
GROUP BY REFERRER;

#creating separate table for internal link wikipedia articles
CREATE TABLE CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY 
(REFERRER STRING, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#sending internal link data
INSERT INTO TABLE CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY  
SELECT REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA
WHERE LINK_TYPE='link'
GROUP BY REFERRER;

#creating both and internal data merge table
CREATE TABLE CLICK_STREAM_ARTICLE_VIEWED_FRACTIONALLY 
(REFERRER STRING, ARTICLE_VIEWS_BOTH INT, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#inner join the two tables
INSERT INTO TABLE CLICK_STREAM_ARTICLE_VIEWED_FRACTIONALLY 
SELECT B.REFERRER, B.ARTICLE_VIEWS_BOTH, I.ARTICLE_VIEWS_INTERNALLY 
FROM CLICK_STREAM_ARTICLE_VIEWED_BOTH B 
INNER JOIN CLICK_STREAM_ARTICLE_VIEWED_INTERNALLY I
ON (B.REFERRER=I.REFERRER);

#Query to find the Largest fraction of its readers follow an 
#internal link to another wikipedia article
#Whose pages have been read 1000000 times.
SELECT REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY, 
ROUND(((ARTICLE_VIEWS_INTERNALLY/ARTICLE_VIEWS_BOTH)*100),3) AS INTERNAL_FRACTIONAL_VIEW
FROM CLICK_STREAM_ARTICLE_VIEWED_FRACTIONALLY
WHERE ARTICLE_VIEWS_INTERNALLY > 1000000
GROUP BY REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY
ORDER BY INTERNAL_FRACTIONAL_VIEW DESC
LIMIT 20;
##################################################################

##################################################################
#Question 3:  The series of wikipedia articles, starting with Hotel California that
#keeps the largest fraction of its readers clicking on internal links

#creating separate table for both link type that begin with hotel california 
CREATE TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH
(CURRENT_ARTICLE STRING, REFERRER STRING, ARTICLE_VIEWS_BOTH INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#sending data from CLICK_STREAM_GENERAL_DATA table
INSERT INTO CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH
SELECT CURRENT_ARTICLE, REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA 
WHERE REFERRER LIKE 'Hotel_California%'
GROUP BY CURRENT_ARTICLE, REFERRER;

#creating separate table for internal link that begin with hotel california
CREATE TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY 
(CURRENT_ARTICLE STRING, REFERRER STRING, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#sending internal link data
INSERT INTO CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY 
SELECT CURRENT_ARTICLE, REFERRER, SUM(CURRENT_ARTICLE_VIEWS)
FROM CLICK_STREAM_GENERAL_DATA 
WHERE REFERRER LIKE 'Hotel_California%' AND LINK_TYPE='link'
GROUP BY CURRENT_ARTICLE, REFERRER;

#creating both and internal data merge table
CREATE TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_FRACTIONALLY 
(CURRENT_ARTICLE STRING, REFERRER STRING, ARTICLE_VIEWS_BOTH INT, ARTICLE_VIEWS_INTERNALLY INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#inner join the two tables
INSERT INTO TABLE CLICK_STREAM_HC_ARTICLE_VIEWED_FRACTIONALLY 
SELECT  B.CURRENT_ARTICLE, B.REFERRER, B.ARTICLE_VIEWS_BOTH, I.ARTICLE_VIEWS_INTERNALLY 
FROM CLICK_STREAM_HC_ARTICLE_VIEWED_BOTH B 
INNER JOIN CLICK_STREAM_HC_ARTICLE_VIEWED_INTERNALLY I
ON (B.REFERRER=I.REFERRER) AND (B.CURRENT_ARTICLE=I.CURRENT_ARTICLE);

#Query to find the Largest fraction of its readers follow an 
#internal link to another wikipedia article
SELECT CURRENT_ARTICLE, REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY, 
ROUND(((ARTICLE_VIEWS_INTERNALLY/ARTICLE_VIEWS_BOTH)*100),3) AS INTERNAL_FRACTIONAL_VIEW
FROM CLICK_STREAM_HC_ARTICLE_VIEWED_FRACTIONALLY
GROUP BY CURRENT_ARTICLE, REFERRER, ARTICLE_VIEWS_BOTH, ARTICLE_VIEWS_INTERNALLY
ORDER BY ARTICLE_VIEWS_INTERNALLY DESC
LIMIT 20;
####################################################################

#Question 4: English wikipedia article that is relatively more popular in the UK. 
#Find the same for the US and Australia.

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