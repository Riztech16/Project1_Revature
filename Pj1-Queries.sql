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

#creating separate table for internal link wikipedia articles
CREATE TABLE CLICK_STREAM_INTERNAL_LINKS 
(REFERRER STRING, CURRENT_ARTICLE STRING, CURRENT_ARTICLE_VIEWS INT)
PARTITIONED BY (LINK_TYPE STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

#sending data to be partitioned from general table
INSERT INTO TABLE CLICK_STREAM_INTERNAL_LINKS PARTITION(LINK_TYPE='link')
SELECT REFERRER, CURRENT_ARTICLE, CURRENT_ARTICLE_VIEWS
FROM CLICK_STREAM_GENERAL_DATA
WHERE LINK_TYPE='link';

#Merging the duplicate and sorting from highest to lowest viewed
#article (Top 10) on desktop
SELECT REFERRER, SUM(CURRENT_ARTICLE_VIEWS) AS PAIR_OCCURRENCES 
FROM CLICK_STREAM_INTERNAL_LINKS
GROUP BY REFERRER
ORDER BY PAIR_OCCURRENCES DESC
LIMIT 10;
##################################################################

##################################################################
#Question 3:  The series of wikipedia articles, starting with Hotel California that
#keeps the largest fraction of its readers clicking on internal links

