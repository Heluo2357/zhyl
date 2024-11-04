drop database if exists dim cascade;
CREATE DATABASE IF NOT EXISTS dim;

use dim;
show databases;

DROP TABLE IF EXISTS doctor_full;
CREATE EXTERNAL TABLE IF NOT EXISTS doctor_full
(
    `id`               STRING COMMENT '医生ID',
    `birthday`         STRING COMMENT '出生日期',
    `consultation_fee` DECIMAL(19, 2) COMMENT '就诊费用',
    `gender_code`      STRING COMMENT '性别编码：101.男 102.女',
    `gender`           STRING COMMENT '性别',
    `username`             STRING COMMENT '姓名',
    `specialty_code`   STRING COMMENT '专业编码：详情见字典表5xx条目',
    `specialty_name`   STRING COMMENT '专业名称',
    `title_code`       STRING COMMENT '职称编码：301. 医士 302. 医师 303. 主治医师 304. 副主任医师 305. 主任医师',
    `title_name`       STRING COMMENT '职称名称',
    `hospital_id`      STRING COMMENT '所属医院ID'
) COMMENT '医生维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dim.db/doctor_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



DROP TABLE IF EXISTS hospital_full;
CREATE EXTERNAL TABLE IF NOT EXISTS hospital_full
(
    `id`              STRING COMMENT '医院ID',
    `address`         STRING COMMENT '地址',
    `alias`           STRING COMMENT '医院别名',
    `bed_num`         BIGINT COMMENT '病床数量',
    `city`            STRING COMMENT '所在城市',
    `department_num`  BIGINT COMMENT '科室数量',
    `district`        STRING COMMENT '所属区县',
    `establish_time`  STRING COMMENT '建立时间',
    `health_care_num` BIGINT COMMENT '医护人数',
    `insurance`       STRING COMMENT '是否医保',
    `level`           STRING COMMENT '医院级别，一级甲等，二级甲等....',
    `name`            STRING COMMENT '医院名称',
    `province`        STRING COMMENT '所属省（直辖市）',
    `street`          STRING COMMENT '街道'
) COMMENT '医院维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dim.db/hospital_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



DROP TABLE IF EXISTS medicine_full;
CREATE EXTERNAL TABLE IF NOT EXISTS medicine_full
(
    `id`            STRING COMMENT '药品ID',
    `approval_code` STRING COMMENT '药物批号',
    `dose_type`     STRING COMMENT '剂量',
    `name`          STRING COMMENT '药品名称',
    `name_en`       STRING COMMENT '英文名称',
    `price`         DECIMAL(19, 2) COMMENT '药品价格',
    `specs`         STRING COMMENT '规格',
    `trade_name`    STRING COMMENT '商品名'
) COMMENT '药品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dim.db/medicine_full/'

    TBLPROPERTIES ('orc.compress' = 'snappy');



DROP TABLE IF EXISTS patient_full;
CREATE EXTERNAL TABLE IF NOT EXISTS patient_full
(
    `id`          STRING COMMENT '患者ID',
    `birthday`    STRING COMMENT '出生日期',
    `gender_code` STRING COMMENT '性别编码',
    `gender`      STRING COMMENT '性别',
    `name`        STRING COMMENT '姓名',
    `user_id`     STRING COMMENT '所属用户'
) COMMENT '患者维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dim.db/patient_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


DROP TABLE IF EXISTS user_full;
CREATE EXTERNAL TABLE IF NOT EXISTS user_full
(
    `id`        STRING COMMENT '用户ID',
    `email`     STRING COMMENT '电邮',
    `telephone` STRING COMMENT '电话',
    `username`  STRING COMMENT '用户名'
) COMMENT '用户维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dim.db/user_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
