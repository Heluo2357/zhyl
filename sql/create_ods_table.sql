drop database IF EXISTS ods CASCADE;
CREATE DATABASE IF NOT EXISTS ods;
use ods;
show databases;

--- 医生表（全量表）---
DROP TABLE IF EXISTS `doctor_full`;
CREATE EXTERNAL TABLE IF NOT EXISTS `doctor_full`
(
    `id`               STRING COMMENT '医生ID',
    `create_time`      STRING COMMENT '创建时间',
    `update_time`      STRING COMMENT '修改时间',
    `birthday`         STRING COMMENT '出生日期',
    `consultation_fee` DECIMAL(19, 2) COMMENT '就诊费用',
    `gender`           STRING COMMENT '性别：101.男 102.女',
    `password`         STRING COMMENT '密码',
    `role_id`          STRING COMMENT '角色权限',
    `specialty`        STRING COMMENT '专业：详情见字典表5xx条目',
    `title`            STRING COMMENT '职称：301. 医士 302. 医师 303. 主治医师 304. 副主任医师 305. 主任医师',
    `username`             STRING COMMENT '姓名',
    `hospital_id`      STRING COMMENT '所属医院'
) COMMENT '医生全量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/user/hive/warehouse/ods.db/doctor_full/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');


--- 医院表（全量表）---
DROP TABLE IF EXISTS `hospital_full`;
CREATE EXTERNAL TABLE IF NOT EXISTS `hospital_full`
(
    `id`              STRING COMMENT '医院ID',
    `create_time`     STRING COMMENT '创建时间',
    `update_time`     STRING COMMENT '修改时间',
    `address`         STRING COMMENT '地址',
    `alias`           STRING COMMENT '医院别名',
    `bed_num`         BIGINT COMMENT '病床数量',
    `city`            STRING COMMENT '市',
    `department_num`  BIGINT COMMENT '科室数量',
    `district`        STRING COMMENT '区县',
    `establish_time`  STRING COMMENT '建立时间',
    `health_care_num` BIGINT COMMENT '医护人数',
    `insurance`       STRING COMMENT '是否医保',
    `level`           STRING COMMENT '医院级别，一级甲等，二级甲等....',
    `name`            STRING COMMENT '医院名称',
    `province`        STRING COMMENT '省（直辖市）',
    `street`          STRING COMMENT '街道'
) COMMENT '医院表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/user/hive/warehouse/ods.db/hospital_full/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');


----------------药品表（全量表)------------------
DROP TABLE IF EXISTS `medicine_full`;
CREATE EXTERNAL TABLE IF NOT EXISTS `medicine_full`
(
    `id`            STRING COMMENT '药品ID',
    `create_time`   STRING COMMENT '创建时间',
    `update_time`   STRING COMMENT '修改时间',
    `approval_code` STRING COMMENT '药物批号',
    `dose_type`     STRING COMMENT '剂量',
    `name`          STRING COMMENT '药品名称',
    `name_en`       STRING COMMENT '英文名称',
    `price`         DECIMAL(19, 2) COMMENT '药品价格',
    `specs`         STRING COMMENT '规格',
    `trade_name`    STRING COMMENT '商品名'
) COMMENT '药品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/user/hive/warehouse/ods.db/medicine_full/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');


------患者表（全量表)-----
DROP TABLE IF EXISTS `patient_full`;
CREATE EXTERNAL TABLE IF NOT EXISTS `patient_full`
(
    `id`          STRING COMMENT '患者ID',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '修改时间',
    `birthday`    STRING COMMENT '出生日期',
    `gender`      STRING COMMENT '性别',
    `name`        STRING COMMENT '姓名',
    `user_id`     STRING COMMENT '所属用户'
) COMMENT '患者表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/user/hive/warehouse/ods.db/patient_full/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

-- select  count(*) from  patient_full group by id;

---------字典表（全量表)---------
DROP TABLE IF EXISTS `dict_full`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dict_full`
(
    `id`          STRING COMMENT '编码ID',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '修改时间',
    `value`       STRING
) COMMENT '字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/user/hive/warehouse/ods.db/dict_full/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');



---------用户表（全量表)--------
DROP TABLE IF EXISTS `user_full`;
CREATE EXTERNAL TABLE IF NOT EXISTS `user_full`
(
    `id`              STRING COMMENT '用户ID',
    `create_time`     STRING COMMENT '创建时间',
    `update_time`     STRING COMMENT '修改时间',
    `email`           STRING COMMENT '电邮',
    `hashed_password` STRING COMMENT '密码',
    `role_id`         STRING COMMENT '角色权限',
    `telephone`       STRING COMMENT '电话',
    `username`        STRING COMMENT '用户名'
) COMMENT '用户全量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/user/hive/warehouse/ods.db/user_full/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');



-----------就诊表（增量表）-----------
DROP TABLE IF EXISTS `consultation_inc`;
CREATE EXTERNAL TABLE IF NOT EXISTS `consultation_inc`
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT
           <id :STRING,
            create_time :STRING,
            update_time :STRING,
            consultation_fee :DECIMAL(16, 2),
            description :STRING,
            diagnosis :STRING,
            rating :STRING,
            review :STRING,
            status :STRING,
            doctor_id :STRING,
            patient_id :STRING,
            user_id :STRING
           > COMMENT '变更后数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '就诊表增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/ods.db/consultation_inc/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');



----------- 处方开单表（增量表）----------
DROP TABLE IF EXISTS `prescription_inc`;
CREATE EXTERNAL TABLE IF NOT EXISTS `prescription_inc`
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT
           <id :STRING,
            create_time :STRING,
            update_time :STRING,
            instruction :STRING,
            status :STRING,
            total_amount :DECIMAL(16, 2),
            consultation_id :STRING,
            doctor_id :STRING,
            patient_id :STRING> COMMENT '变更后数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '处方表增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/ods.db/prescription_inc/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');



-------------7.9 处方开单详情表（增量表）
DROP TABLE IF EXISTS `prescription_detail_inc`;
CREATE EXTERNAL TABLE IF NOT EXISTS `prescription_detail_inc`
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT
           <id :STRING,
            create_time :STRING,
            update_time :STRING,
            count :STRING,
            instruction :STRING,
            medicine_id :STRING,
            prescription_id :STRING> COMMENT '变更后数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '处方详情表增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/ods.db/prescription_detail_inc/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');




----------------支付表（增量表)
DROP TABLE IF EXISTS `payment_inc`;
CREATE EXTERNAL TABLE IF NOT EXISTS `payment_inc`
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT
           <id :STRING,
            create_time :STRING,
            update_time :STRING,
            payment_amount :DECIMAL(16, 2),
            status :STRING,
            consultation_id :STRING,
            prescription_id :STRING,
            user_id :STRING> COMMENT '变更后数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '支付表增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/ods.db/payment_inc/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');




----------医生表（增量表）
DROP TABLE IF EXISTS `doctor_inc`;
CREATE EXTERNAL TABLE IF NOT EXISTS `doctor_inc`
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT
           <id :STRING,
            create_time :STRING,
            update_time :STRING,
            birthday :STRING,
            consultation_fee :DECIMAL(16, 2),
            gender :STRING,
            password :STRING,
            role_id :STRING,
            specialty :STRING,
            title :STRING,
            username :STRING,
            hospital_id :STRING> COMMENT '变更后数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '医生增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/ods.db/doctor_inc/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');



--用户表（增量表）
DROP TABLE IF EXISTS `user_inc`;
CREATE EXTERNAL TABLE IF NOT EXISTS `user_inc`
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT
           <id :STRING,
            create_time :STRING,
            update_time :STRING,
            email :STRING,
            hashed_password :STRING,
            role_id :STRING,
            telephone :STRING,
            username :STRING> COMMENT '变更后数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/ods.db/user_inc/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');



-- 患者表（增量表）
DROP TABLE IF EXISTS `patient_inc`;
CREATE EXTERNAL TABLE IF NOT EXISTS `patient_inc`
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT
           <`id` : STRING,
            `create_time` : STRING,
            `update_time` : STRING,
            `birthday` : STRING,
            `gender` : STRING,
            `name` : STRING,
            `user_id` : STRING> COMMENT '变更后数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/user/hive/warehouse/ods.db/patient_inc/'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

SHOW TABLES;

