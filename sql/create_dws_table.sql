DROP DATABASE IF EXISTS dws CASCADE;
create database IF NOT EXISTS dws;

--医院每日问诊表
DROP TABLE IF EXISTS hospital_consultation_1d;
CREATE TABLE IF NOT EXISTS hospital_consultation_1d
(
    `hospital_id`        STRING COMMENT '医院ID',
    `hospital_name`      STRING COMMENT '医院名称',
    `consultation_count` BIGINT comment '问诊次数',
    `street`             STRING COMMENT '街道'
) COMMENT '医院每日问诊统计表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/hospital_consultation_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- nd表是最近n天的聚合统计
DROP TABLE IF EXISTS hospital_consultation_nd;
CREATE TABLE IF NOT EXISTS hospital_consultation_nd
(
    `hospital_id`        STRING COMMENT '医院ID',
    `hospital_name`      STRING COMMENT '医院名称',
    `consultation_count` BIGINT comment '问诊次数',
    `street`             STRING COMMENT '街道'
) COMMENT '医院n日问诊统计表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/hospital_consultation_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

----问诊患者统计表
use dws;
DROP TABLE IF EXISTS consult_patient_info;
CREATE TABLE IF NOT EXISTS consult_patient_info
(
    `id`                STRING COMMENT '问诊ID',
    `consultation_time` STRING COMMENT '问诊时间',
    `patient_id`        STRING COMMENT '患者ID',
    `patient_age`       STRING COMMENT '患者年龄',
    `patient_gender`    STRING COMMENT '患者性别',
    `age_group`         STRING COMMENT '年龄段：[0,2]婴儿期, [3,5]幼儿期, [6,11]小学阶段, [12,17]青少年期(中学阶段), [18-29]青年期, [30-59]中年期, [60-]老年期',
    `street`            STRING COMMENT '社区'
) COMMENT '问诊患者计表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/consult_patient_info'
    TBLPROPERTIES ('orc.compress' = 'snappy');

/**
  交易域问诊事务事实表 + dim患者表
 */

--医院评价每日
-- 基本信息表
DROP TABLE IF EXISTS hospital_review_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS hospital_review_1d
(
    `id`           STRING COMMENT '医院ID',
    `name`         STRING COMMENT '医院名称',
    `street`       STRING COMMENT '街道',
    `satisfaction` BIGINT COMMENT '非常满意',
    `ordinary`     BIGINT COMMENT '一般',
    `discontent`   BIGINT COMMENT '不满意'
) COMMENT '医院每天评价统计表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/hospital_review_1d/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


DROP TABLE IF EXISTS hospital_review_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS hospital_review_nd
(
    `id`           STRING COMMENT '医院ID',
    `name`         STRING COMMENT '医院名称',
    `street`       STRING COMMENT '街道',
    `satisfaction` BIGINT COMMENT '非常满意',
    `ordinary`     BIGINT COMMENT '一般',
    `discontent`   BIGINT COMMENT '不满意'
) COMMENT '医院历史至今评价统计表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/hospital_review_nd/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--科室挂号次数
DROP TABLE IF EXISTS department_registration;
CREATE TABLE IF NOT EXISTS department_registration
(
    `department`         STRING comment '科室',
    `consultation_count` BIGINT comment '问诊次数'
) COMMENT '科室每日挂号次数表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/department_registration'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 科室就诊完成次数
DROP TABLE IF EXISTS department_registration_suc;
CREATE TABLE IF NOT EXISTS department_registration_suc
(
    `department`             STRING comment '科室',
    `consultation_suc_count` BIGINT comment '问诊完成次数'
) COMMENT '科室就诊完成次数表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/department_registration_suc'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--药品类使用次数统计每天
DROP TABLE IF EXISTS drug_class_info_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS drug_class_info_1d
(
    `drug`             STRING COMMENT '药类',
    `drug_class_count` BIGINT COMMENT '药类使用次数'

) COMMENT '药品类使用次数统计'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/drug_class_info_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--药品类使用次数统计n天
DROP TABLE IF EXISTS drug_class_info_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS drug_class_info_nd
(
    `drug`             STRING COMMENT '药类',
    `drug_class_count` BIGINT COMMENT '药类使用次数'

) COMMENT '药品类n天使用次数统计'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/drug_class_info_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--每个月药品使用统计
DROP TABLE IF EXISTS drug_year_month;
CREATE EXTERNAL TABLE IF NOT EXISTS drug_year_month
(
    `drug`       STRING COMMENT '药品名',
    `year`       STRING COMMENT '年份',
    `month`      STRING COMMENT '月份',
    `drug_count` BIGINT COMMENT '药品使用次数'
) COMMENT '每个月药品使用记录'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws.db/drug_year_month'
    TBLPROPERTIES ('orc.compress' = 'snappy');



