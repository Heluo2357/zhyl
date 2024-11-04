DROP DATABASE IF EXISTS ads CASCADE;
CREATE DATABASE IF NOT EXISTS ads;
USE ads;
SHOW DATABASES;

// TODO: 各医院接诊次数
DROP TABLE IF EXISTS hospital_consultation;
CREATE EXTERNAL TABLE IF NOT EXISTS hospital_consultation
(
    `hospital`           STRING COMMENT '医院',
    `consultation_count` BIGINT COMMENT '接诊次数'

) COMMENT '各医院接诊次数'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/hospital_consultation';

select *
from hospital_consultation;

// TODO: 各街道接诊次数
DROP TABLE IF EXISTS street_consultation;
CREATE EXTERNAL TABLE IF NOT EXISTS street_consultation
(
    `street`             STRING COMMENT '街道',
    `consultation_count` BIGINT COMMENT '接诊数量'
) COMMENT '各街道接诊统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/street_consultation';

select *
from street_consultation;


// TODO 所有社区月患者数量
DROP TABLE IF EXISTS month_consultation;
CREATE EXTERNAL TABLE IF NOT EXISTS month_consultation
(
    `each_month`         STRING COMMENT '年月',
    `consultation_count` BIGINT COMMENT '患者数量'
) COMMENT '每月患者数量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/month_consultation';

select *
from month_consultation;

// TODO: 患者男女比例统计
DROP TABLE IF EXISTS grades_consultation;
CREATE EXTERNAL TABLE IF NOT EXISTS grades_consultation
(
    `gender`       STRING COMMENT '性别',
    `gender_count` BIGINT COMMENT '患者数量'
) COMMENT '患者男女比例统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/grades_consultation';

select *
from grades_consultation;


// TODO: 患者年龄阶段统计
DROP TABLE IF EXISTS age_grades_consultation;
CREATE EXTERNAL TABLE IF NOT EXISTS age_grades_consultation
(
    `age_group`        STRING COMMENT '年龄阶段',
    `age_gender_count` BIGINT COMMENT '患者数量'
) COMMENT '患者年龄阶段统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/age_grades_consultation';
select *
from age_grades_consultation;


// TODO: 总患者人数，医护人员人数，医院数量，科室数量， 日均就诊人数   --> 全社区患者人数
DROP TABLE IF EXISTS synthesize;
CREATE EXTERNAL TABLE IF NOT EXISTS synthesize
(
    patient_num                  BIGINT COMMENT '患者数量',
    doctor_num                   BIGINT COMMENT '医护人员数量',
    hospital_num                 BIGINT COMMENT '医院数量',
    department_num               BIGINT COMMENT '科室数量',
    bed_num                      BIGINT COMMENT '床位数量',
    avg_daily_consultation_count BIGINT COMMENT '日均就诊人数'
) COMMENT '汇总统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/synthesize';

select *
from synthesize;


// TODO: 所有医院满意度
DROP TABLE IF EXISTS all_hospital_review;
CREATE EXTERNAL TABLE IF NOT EXISTS all_hospital_review
(
    `satisfaction` BIGINT COMMENT '非常满意',
    `ordinary`     BIGINT COMMENT '一般',
    `discontent`   BIGINT COMMENT '不满意'
) COMMENT '所有医院满意度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/all_hospital_review';

select *
from all_hospital_review;


// TODO: 各医院好评次数
DROP TABLE IF EXISTS hospital_review;
CREATE EXTERNAL TABLE IF NOT EXISTS hospital_review
(
    `hospital`          STRING COMMENT '医院',
    `good_review_count` BIGINT COMMENT '好评数'

) COMMENT '各医院好评统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/hospital_review';
select *
from hospital_review;


// TODO:药品类使用次数统计
DROP TABLE IF EXISTS drug_class;
CREATE EXTERNAL TABLE IF NOT EXISTS drug_class
(
    `drug`             STRING COMMENT '药类',
    `drug_class_count` BIGINT COMMENT '药类使用次数'

) COMMENT '药品类使用次数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/drug_class';
select *
from drug_class;


// TODO :平均月就诊次数 总就诊次数
DROP TABLE IF EXISTS monthly_and_total_inquiry;
CREATE EXTERNAL TABLE IF NOT EXISTS monthly_and_total_inquiry
(
    `monthly_count` BIGINT COMMENT '月均就诊次数',
    `total_count`   BIGINT COMMENT '总就诊次数'
) COMMENT '平均月就诊次数 总就诊次数'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/monthly_and_total_inquiry';
select *
from monthly_and_total_inquiry;


// TODO:科室挂号次数，问诊成功次数
DROP TABLE IF EXISTS department_inquiry;
CREATE EXTERNAL TABLE IF NOT EXISTS department_inquiry
(
    `department`           STRING COMMENT '科室名',
    `registration_count`   BIGINT COMMENT '挂号次数',
    `department_suc_count` BIGINT COMMENT '完成就诊次数'
) COMMENT '科室就诊统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/department_inquiry';
select *
from department_inquiry;


--每个季节药品使用次数
DROP TABLE IF EXISTS drug_season;
CREATE EXTERNAL TABLE IF NOT EXISTS drug_season
(
    `season`     STRING COMMENT '季节',
    `drug`       STRING COMMENT '药品名',
    `drug_count` BIGINT COMMENT '药品使用次数'
) COMMENT '每个月药品使用'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ads.db/drug_season';

show tables;


