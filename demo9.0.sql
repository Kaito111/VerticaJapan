--Smart Meter data from the Irish Energy public dataset capturing kw readings 
--every 15 minutes on thousands of residential and business meters 24 hrs a day
--http://www.ucd.ie/issda/data/commissionforenergyregulationcer/
--Weather data was also captured to correalte with kw readings


--SQL to load the data should you need to do so
/*
CREATE TABLE sm_consumption
(
    meterID int,
    dateUTC timestamp,
    value numeric(25,5)
);

CREATE TABLE sm_weather
(
    dateUTC timestamp,
    temperature numeric(25,5),
    humidity numeric(25,5)
);

CREATE TABLE sm_meters
(
    meterID int NOT NULL,
    residenceType int NOT NULL,
    latitude numeric(25,15) NOT NULL,
    longitude numeric(25,15) NOT NULL
);

CREATE TABLE sm_residences
(
    id int NOT NULL,
    description varchar(15) NOT NULL
);

copy sm_consumption FROM '/home/dbadmin/sm_consumption.csv' delimiter ',';
copy sm_weather FROM '/home/dbadmin/sm_weather.csv' delimiter ',';
copy sm_meters FROM '/home/dbadmin/sm_meters.csv' delimiter ',';
copy sm_residences FROM '/home/dbadmin/sm_residences.csv' delimiter ',';
*/


--system info
select version();
select * from nodes;


------------------------------------------
------------------------------------------
--data exploration
------------------------------------------
------------------------------------------


--view the data
select * from sm_consumption limit 10;
select * from sm_weather limit 10;
select * from sm_meters limit 10;
select * from sm_residences;


--table count
select count(*) from sm_consumption;


------------------------------------------
------------------------------------------
--Feature Creation
--flag outliers with DETECT_OUTLIERS
------------------------------------------
------------------------------------------


--use robust zscore with threshold of 3
drop table if exists sm_outliers cascade;
SELECT DETECT_OUTLIERS('sm_outliers', 'sm_consumption', 'value', 'robust_zscore' 
        USING PARAMETERS outlier_threshold=3.0, key_columns='meterid, dateUTC');
        

--view results
select * from sm_outliers limit 10;


--flag the outliers
--if the value is not in sm_outliers then it is not an outlier
--use left outer join and check for null


--sequence for R UDX model scoring later on
drop sequence if exists seq;
CREATE SEQUENCE seq;

drop table if exists sm_consumption_outliers;
create table sm_consumption_outliers as
        select nextval('seq') as id, c.*, case when o.value is null then 0 else 1 end as highusage
        from sm_consumption c left outer join sm_outliers o on c.meterid=o.meterid and c.dateUTC=o.dateUTC;


--view results
select * from sm_consumption_outliers order by meterid, dateUTC limit 20;


--clean up
drop view if exists sm_outliers;


------------------------------------------
------------------------------------------
--Feature Creation
--meter location_id with KMEANS
------------------------------------------
------------------------------------------
--create clusters of meterids based on kmeans distances 
--allows for new meter locations to be added and assigned a location id


--create kmeans model and view of results
drop model if exists sm_kmeans;
select kmeans('sm_kmeans', 'sm_meters', 'latitude, longitude', 6);


--look at results
select summarize_model('sm_kmeans');


--use apply_kmeans to score on a table
drop table if exists sm_meters_location;
CREATE TABLE sm_meters_location AS
        SELECT meterid, residenceType, latitude, longitude,
        APPLY_KMEANS(latitude, longitude USING PARAMETERS model_name='sm_kmeans') AS locationid
        FROM sm_meters;


--view results
select * from sm_meters_location limit 20;


------------------------------------------
------------------------------------------
--Feature Creation
--fill in the weather gaps with GFI
------------------------------------------
------------------------------------------


--look at date intervals
select distinct date_part('minute', dateUTC) from sm_consumption;
select distinct date_part('minute', dateUTC) from sm_weather;


--view weather gaps
select distinct cdate, wdate, temperature, humidity 
        from 
        (SELECT c.meterid, c.dateUTC as cdate, w.dateUTC as wdate, w.temperature, w.humidity, c.value
        FROM  sm_consumption c left outer join sm_weather w on c.dateUTC = w.dateUTC order by cdate) a 
        order by 1 limit 10;


--fill weather gaps  with linear GFI
drop table if exists sm_weather_fill;
create table sm_weather_fill as 
SELECT ts as dateUTC, 
        TS_FIRST_VALUE(temperature, 'LINEAR') temperature, 
        TS_FIRST_VALUE(humidity, 'LINEAR') humidity 
        FROM sm_weather
        TIMESERIES ts AS '15 minutes' OVER (ORDER BY dateUTC);
   
   
--view weather gaps again
select distinct cdate, wdate, temperature, humidity 
        from 
        (SELECT c.meterid, c.dateUTC as cdate, w.dateUTC as wdate, w.temperature, w.humidity, c.value
        FROM  sm_consumption c left outer join sm_weather_fill w on c.dateUTC = w.dateUTC order by cdate) a 
        order by 1 limit 10;
        

------------------------------------------
------------------------------------------
--Create flat table
--Create dummy variables
------------------------------------------
------------------------------------------


--flat table, all cols together
drop table if exists sm_flat_pre;
create table sm_flat_pre as
        select  c.id, c.meterid, r.description as metertype, l.latitude, l.longitude, 
                l.locationid::varchar, dayofweek(c.dateUTC)::varchar as 'DOW',                 
                
                case when month(c.dateUTC) >= 3 and month(c.dateUTC) <= 5 then 'Spring' 
                     when month(c.dateUTC) >= 6 and month(c.dateUTC) <= 8 then 'Summer' 
                     when month(c.dateUTC) >= 9 and month(c.dateUTC) <= 11 then 'Fall' 
                     else 'Winter' end as 'Season',                
                
                case when hour(c.dateUTC) >= 6 and hour(c.dateUTC) <= 11 then 'Morning'
                        when hour(c.dateUTC) >= 12 and hour(c.dateUTC) <= 17 then 'Afternoon'
                        when hour(c.dateUTC) >= 18 and hour(c.dateUTC) <= 23 then 'Evening' 
                        else 'Night' end as 'TOD',                
                
                w.temperature, w.humidity, c.highusage, c.highusage::varchar as highusage_char, c.value,
                
                case when random() < 0.3 then 'test' else 'train' end as part
                
        from sm_consumption_outliers c 
                inner join sm_meters_location l on c.meterid = l.meterid 
		inner join sm_residences r on l.residenceType = r.id
                inner join sm_weather_fill w on c.dateUTC = w.dateUTC;
                
 
--normalize humidity and temperature
drop model if exists sm_normfit;
SELECT NORMALIZE_FIT('sm_normfit', 'sm_flat_pre', 'humidity, temperature', 'zscore');

select summarize_model('sm_normfit');


--one hot encoding fit
drop model if exists sm_ohe;
SELECT ONE_HOT_ENCODER_FIT ('sm_ohe','sm_flat_pre','metertype, locationid, DOW, Season, TOD');

select summarize_model('sm_ohe');


--one hot encoding and normalization in one step
drop table if exists sm_flat;
create table sm_flat as
select APPLY_ONE_HOT_ENCODER(* USING PARAMETERS model_name='sm_ohe')
FROM 
(SELECT APPLY_NORMALIZE (* USING PARAMETERS model_name = 'sm_normfit') FROM sm_flat_pre) a;


--rename encoded columns
ALTER TABLE sm_flat
    RENAME COLUMN metertype_1 TO multi_family;
ALTER TABLE sm_flat
    RENAME COLUMN metertype_2 TO single_family;
ALTER TABLE sm_flat
    RENAME COLUMN locationid_1 TO loc1;
ALTER TABLE sm_flat
    RENAME COLUMN locationid_2 TO loc2;
ALTER TABLE sm_flat
    RENAME COLUMN locationid_3 TO loc3;
ALTER TABLE sm_flat
    RENAME COLUMN locationid_4 TO loc4;
ALTER TABLE sm_flat
    RENAME COLUMN locationid_5 TO loc5;
ALTER TABLE sm_flat
    RENAME COLUMN dow_1 TO monday;
ALTER TABLE sm_flat
    RENAME COLUMN dow_2 TO tuesday;
ALTER TABLE sm_flat
    RENAME COLUMN dow_3 TO wednesday;
ALTER TABLE sm_flat
    RENAME COLUMN dow_4 TO thursday;
ALTER TABLE sm_flat
    RENAME COLUMN dow_5 TO friday;
ALTER TABLE sm_flat
    RENAME COLUMN dow_6 TO saturday;
ALTER TABLE sm_flat
    RENAME COLUMN season_1 TO spring;
ALTER TABLE sm_flat
    RENAME COLUMN season_2 TO summer;
ALTER TABLE sm_flat
    RENAME COLUMN season_3 TO winter;
    ALTER TABLE sm_flat
    RENAME COLUMN tod_1 TO evening;
ALTER TABLE sm_flat
    RENAME COLUMN tod_2 TO morning;
ALTER TABLE sm_flat
    RENAME COLUMN tod_3 TO night;


--extra step for OHE
--there is a bug which won't let dateUTC pass through
--will be fixed in future release
--use this workaround for now
drop table if exists sm_flat_tmp cascade;
create table sm_flat_tmp as select * from sm_flat;

drop table if exists sm_flat cascade;
create table sm_flat as select c.dateUTC, f.* from sm_flat_tmp f
inner join sm_consumption_outliers c on f.id = c.id;

drop table if exists sm_flat_tmp cascade;

------------------------------------------
------------------------------------------
--Predictive Modeling
------------------------------------------
------------------------------------------


--train and test
--we need a table or view with just the training data for modeling
--we can score it on sm_flat and then use where part='test'
--when we want to look at the test results only
drop table if exists sm_flat_train;
create table sm_flat_train as 
select * from sm_flat where part='train';


----------------------
--build all the models
----------------------


--linear regression
drop model if exists sm_linear;
select linear_reg('sm_linear', 'sm_flat_train', 'value', 
'multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
winter, Summer, spring, night, morning, Evening, temperature, humidity');


--SVM regression
drop model if exists sm_svm_reg;
select SVM_REGRESSOR('sm_svm_reg', 'sm_flat_train', 'value', 
'multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
winter, Summer, spring, night, morning, Evening, temperature, humidity');


--RF regression
drop model if exists sm_rf_reg;
select rf_regressor('sm_rf_reg', 'sm_flat_train', 'value', 
'metertype, locationid, DOW, Season, TOD, temperature, humidity');


--logistic regression
drop model if exists sm_logistic;
select logistic_reg('sm_logistic', 'sm_flat_train', 'highusage', 
'multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
winter, Summer, spring, night, morning, Evening, temperature, humidity');


--naive bayes
drop model if exists sm_nb;
select naive_bayes('sm_nb', 'sm_flat_train', 'highusage', 
'multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
winter, Summer, spring, night, morning, Evening, temperature, humidity');


--svm is a decision boundary optimizer
--does not produce probabilities so we must balance the data first
drop view if exists sm_flat_train_balanced;
select BALANCE ( 'sm_flat_train_balanced', 'sm_flat_train', 'highusage', 'over_sampling'
            USING PARAMETERS sampling_ratio=0.6 );
            

--new highusage rate
select avg(highusage) from sm_flat_train union
select avg(highusage) from sm_flat_train_balanced;


--SVM classification
drop model if exists sm_svm;
select svm_classifier('sm_svm', 'sm_flat_train_balanced', 'highusage', 
'multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
winter, Summer, spring, night, morning, Evening, temperature, humidity');


--RF classification
drop model if exists sm_rf;
select rf_classifier('sm_rf', 'sm_flat_train', 'highusage_char', 
'metertype, locationid, DOW, Season, TOD, temperature, humidity');


--take a look at the models created
select * from models;


--build a random forest classifier using R
--before running this you must install the random forest library on the instance of R that is running on
--every node in your Vertica cluster
--older versions of Vertica came with R installed automatically
--newer versions of Vertica may require you to install the Vertica-R-package manually (due to legal reasons)
--you can find vertica-R-package at https://my.vertica.com/
DROP library IF EXISTS rflib CASCADE;
CREATE library rflib AS '/home/dbadmin/R_UDX/randomforest/rf_udf.R' LANGUAGE 'R';
CREATE transform FUNCTION rf_build_udf AS LANGUAGE 'R' name 'rf_build_factory' library rflib;
CREATE transform FUNCTION rf_score_udf AS LANGUAGE 'R' name 'rf_score_factory' library rflib;


--run rf_build_udf
SELECT
rf_build_udf("highusage_char", "metertype", "locationid", "Season", "DOW", "TOD", "temperature", "humidity" 
using parameters append_date=1, model_name='my_rf_model', model_folder='/home/dbadmin')
over () 
FROM sm_flat_train;


---------------------------------------------------
--score all the models and save results in a table
---------------------------------------------------


--run rf_score_udf
--stores id, prediciton, and probability in a table
drop table if exists sm_pred_rfudx cascade;
create table sm_pred_rfudx as SELECT 
rf_score_udf("id", "highusage_char", "metertype", "locationid", "Season", "DOW", "TOD", "temperature", "humidity" 
using parameters model='/home/dbadmin/my_rf_model-2018-03-01-10-42-31.rda')
over () 
FROM sm_flat;


--build the results table for all the vertica ml models
--and join in the R UDF results
drop table if exists sm_flat_pred;
CREATE TABLE sm_flat_pred AS 
(SELECT a.*, 

        PREDICT_linear_REG(multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity 
                USING PARAMETERS model_name='sm_linear') as lin_reg_pred, 
                
        PREDICT_SVM_REGRESSOR(multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity 
                USING PARAMETERS model_name='sm_svm_reg') as svm_reg_pred,
                
        predict_rf_regressor(metertype, locationid, DOW, Season, TOD, temperature, humidity 
                USING PARAMETERS model_name='sm_rf_reg') as rf_reg_pred, 
                
        PREDICT_LOGISTIC_REG(multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity 
                USING PARAMETERS model_name='sm_logistic', type='probability') AS log_reg_prob, 
        
        PREDICT_LOGISTIC_REG(multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity  
                USING PARAMETERS model_name='sm_logistic', type = 'response') AS log_reg_pred,  
        
        PREDICT_LOGISTIC_REG(multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity  
                USING PARAMETERS model_name='sm_logistic', cutoff='0.15') AS log_reg_pred15  ,
                
        PREDICT_NAIVE_BAYES (multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity
                USING PARAMETERS model_name = 'sm_nb',type = 'probability', class='1')::float AS nb_prob, 
        
        PREDICT_NAIVE_BAYES (multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity
                USING PARAMETERS model_name = 'sm_nb',type = 'response') AS nb_pred, 
        
        case when PREDICT_NAIVE_BAYES (multi_family, single_family, loc1, loc2, loc3, loc4, loc5, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, 
                winter, Summer, spring, night, morning, Evening, temperature, humidity
                USING PARAMETERS model_name = 'sm_nb',type = 'probability', class='1')::float > 0.15 then 1 else 0 end AS nb_pred15,
                
        PREDICT_RF_CLASSIFIER (metertype, locationid, DOW, Season, TOD, temperature, humidity
                USING PARAMETERS model_name = 'sm_rf',type = 'probability', class='1')::float AS rf_class_prob, 
        
        PREDICT_RF_CLASSIFIER (metertype, locationid, DOW, Season, TOD, temperature, humidity
                USING PARAMETERS model_name = 'sm_rf',type = 'response') AS rf_class_pred, 
        
        case when PREDICT_RF_CLASSIFIER (metertype, locationid, DOW, Season, TOD, temperature, humidity
                USING PARAMETERS model_name = 'sm_rf',type = 'probability', class='1')::float > 0.15 then 1 else 0 end AS rf_class_pred15,
        
        case when b.pred = '0' then 1 - b.maxprob else b.maxprob end as r_rf_class_prob,
        case when b.pred = '1' or (b.pred = '0' and b.maxprob <= 0.85) then 1 else 0 end as r_rf_class_pred15
        
FROM sm_flat a inner join sm_pred_rfudx b on a.id = b.id);


--------------------
--model stats
--------------------


--model summaries
SELECT SUMMARIZE_MODEL('sm_linear');
SELECT SUMMARIZE_MODEL('sm_svm_reg');
SELECT SUMMARIZE_MODEL('sm_rf_reg');
SELECT SUMMARIZE_MODEL('sm_logistic');
SELECT SUMMARIZE_MODEL('sm_nb');
SELECT SUMMARIZE_MODEL('sm_svm');
SELECT SUMMARIZE_MODEL('sm_rf');


--save model stats in a table
drop table if exists sm_linear_sum;
create table sm_linear_sum as SELECT GET_MODEL_ATTRIBUTE 
(USING PARAMETERS model_name='sm_linear', attr_name = 'details');


--view model stats table
select * from sm_linear_sum;


---------------------
--model metrics
---------------------


--MSE
select MSE (value, lin_reg_pred) over() from sm_flat_pred where part = 'test';


--rsquare   
select corr(value, lin_reg_pred)^2 as r_square from sm_flat_pred where part = 'test';


--error rate
SELECT ERROR_RATE(obs, pred::int USING PARAMETERS num_classes=2) OVER() 
FROM (SELECT highusage AS obs, log_reg_pred15 AS pred FROM sm_flat_pred where part = 'test') a;


--ROC
SELECT ROC(obs::int, prob::float USING PARAMETERS num_bins=20) OVER() 
FROM (SELECT highusage AS obs, log_reg_prob as prob FROM sm_flat_pred where part='test') a;


--confusion matrix
SELECT CONFUSION_MATRIX(obs::int, pred::int USING PARAMETERS num_classes=2) OVER() 
FROM (SELECT highusage AS obs, log_reg_pred15 as pred FROM sm_flat_pred where part = 'test') AS prediction_output;


------------------------------------
--compare model AUC
-------------------------------------


--build an empty table
drop table if exists AUC_comp cascade;
CREATE TABLE AUC_comp
(
    model varchar(50),
    AUC float
);

--logistic
insert into AUC_comp
select 'logistic' as model, 
sum((true_positive_rate+prev_tpr)*(prev_fpr - false_positive_rate)/2) as AUC from 
(
        select lag(true_positive_rate) over (order by false_positive_rate desc) as prev_tpr, 
                lag(false_positive_rate) over (order by false_positive_rate desc) as prev_fpr, * from 
        (
                select false_positive_rate, avg(true_positive_rate) as true_positive_rate from
                (
                SELECT ROC(obs::int, prob::float USING PARAMETERS num_bins=1000) OVER() 
                FROM (SELECT highusage AS obs, log_reg_prob as prob FROM sm_flat_pred where part='test') AS prediction_output 
                ) q1 group by false_positive_rate  
        ) q2 
) q3;

--naive bayes
insert into AUC_comp
select 'nb' as model, 
sum((true_positive_rate+prev_tpr)*(prev_fpr - false_positive_rate)/2) as AUC from 
(
        select lag(true_positive_rate) over (order by false_positive_rate desc) as prev_tpr, 
                lag(false_positive_rate) over (order by false_positive_rate desc) as prev_fpr, * from 
        (
                select false_positive_rate, avg(true_positive_rate) as true_positive_rate from
                (
                SELECT ROC(obs::int, prob::float USING PARAMETERS num_bins=1000) OVER() 
                FROM (SELECT highusage AS obs, nb_prob as prob FROM sm_flat_pred where part='test') AS prediction_output 
                ) q1 group by false_positive_rate  
        ) q2 
) q3;

--random forest
insert into AUC_comp
select 'rf' as model, 
sum((true_positive_rate+prev_tpr)*(prev_fpr - false_positive_rate)/2) as AUC from 
(
        select lag(true_positive_rate) over (order by false_positive_rate desc) as prev_tpr, 
                lag(false_positive_rate) over (order by false_positive_rate desc) as prev_fpr, * from 
        (
                select false_positive_rate, avg(true_positive_rate) as true_positive_rate from
                (
                SELECT ROC(obs::int, prob::float USING PARAMETERS num_bins=1000) OVER() 
                FROM (SELECT highusage AS obs, rf_class_prob as prob FROM sm_flat_pred where part='test') AS prediction_output 
                ) q1 group by false_positive_rate  
        ) q2 
) q3;

--random forest from R
insert into AUC_comp
select 'rfudx' as model, 
sum((true_positive_rate+prev_tpr)*(prev_fpr - false_positive_rate)/2) as AUC from 
(
        select lag(true_positive_rate) over (order by false_positive_rate desc) as prev_tpr, 
                lag(false_positive_rate) over (order by false_positive_rate desc) as prev_fpr, * from 
        (
                select false_positive_rate, avg(true_positive_rate) as true_positive_rate from
                (
                SELECT ROC(obs::int, prob::float USING PARAMETERS num_bins=1000) OVER() 
                FROM (SELECT highusage AS obs, r_rf_class_prob as prob FROM sm_flat_pred where part='test') AS prediction_output 
                ) q1 group by false_positive_rate  
        ) q2 
) q3;


--compare all models
select * from AUC_comp order by AUC desc;


------------------------------------
--more model management
------------------------------------


--export models to file
SELECT EXPORT_MODELS ('/home/dbadmin/mlmodels', 'public.*')


--import models from file
--SELECT IMPORT_MODELS ('/home/dbadmin/mlmodels/*' USING PARAMETERS new_schema='public')


--upgrade models from a prior version
--one model
--SELECT UPGRADE_MODEL(USING PARAMETERS model_name = 'myLogisticRegModel');
--all models
--SELECT UPGRADE_MODEL();


--alter model metadata
--ALTER MODEL mymodel RENAME to mykmeansmodel;
--ALTER MODEL mykmeansmodel OWNER TO user1;
--ALTER MODEL mykmeansmodel SET SCHEMA public;
