drop database if exists dwd cascade;

create database if not exists  dwd;
use dwd;

show databases;

DROP TABLE IF EXISTS trade_consultation_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS trade_consultation_inc
(
    `id`                STRING COMMENT '问诊ID',
    `consultation_time` STRING comment '问诊时间',
    `consultation_fee`  decimal(16, 2) comment '问诊费用',
    `doctor_id`         STRING comment '医生id',
    `patient_id`        STRING comment '患者ID',
    `user_id`           STRING comment '用户id'
) COMMENT '交易域问诊事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/trade_consultation_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 业务数据
drop table if exists live_events;
create table if not exists live_events
(
    user_id      int comment '用户id',
    live_id      int comment '网页id',
    in_datetime  string comment '进入网页时间',
    out_datetime string comment '离开网页时间'
)
    comment '网页访问记录'
    LOCATION '/user/hive/warehouse/dwd.db/live_events/';

INSERT overwrite table live_events
VALUES (100, 1, '2024-10-27 19:00:00', '2024-10-27 19:28:00'),
       (100, 1, '2024-10-27 19:30:00', '2024-10-27 19:53:00'),
       (100, 2, '2024-10-27 21:01:00', '2024-10-27 22:00:00'),
       (101, 1, '2024-10-27 19:05:00', '2024-10-27 20:55:00'),
       (101, 2, '2024-10-27 21:05:00', '2024-10-27 21:58:00'),
       (102, 1, '2024-10-27 19:10:00', '2024-10-27 19:25:00'),
       (102, 2, '2024-10-27 19:55:00', '2024-10-27 21:00:00'),
       (102, 3, '2024-10-27 21:05:00', '2024-10-27 22:05:00'),
       (104, 1, '2024-10-27 19:00:00', '2024-10-27 20:59:00'),
       (104, 2, '2024-10-27 21:57:00', '2024-10-27 22:56:00'),
       (105, 2, '2024-10-27 19:10:00', '2024-10-27 19:18:00'),
       (106, 3, '2024-10-27 19:01:00', '2024-10-27 21:10:00');

drop table if exists page_view_events;
create table if not exists page_view_events
(
    user_id        int comment '用户id',
    page_id        string comment '页面id',
    view_timestamp bigint comment '访问时间戳'
)
    comment '页面访问记录'
    LOCATION '/user/hive/warehouse/dwd.db/page_view_events/';

insert overwrite table page_view_events
values (100, 'index', 1730020435),
       (100, 'medicine_search', 1730020446),
       (100, 'medicine_list', 1730020457),
       (100, 'index', 1730020541),
       (100, 'medicine_detail', 1730020552),
       (100, 'doctor', 1730020563),
       (101, 'index', 1730020435),
       (101, 'medicine_search', 1730020446),
       (101, 'medicine_list', 1730020457),
       (101, 'index', 1730020541),
       (101, 'medicine_detail', 1730020552),
       (101, 'doctor', 1730020563),
       (102, 'index', 1730020435),
       (102, 'medicine_search', 1730020446),
       (102, 'medicine_list', 1730020457),
       (103, 'index', 1730020541),
       (103, 'medicine_detail', 1730020552),
       (103, 'doctor', 1730020563);

drop table if exists login_events;
create table if not exists login_events
(
    user_id        int comment '用户id',
    login_datetime string comment '登录时间'
)
    comment '直播间访问记录'
    LOCATION '/user/hive/warehouse/dwd.db/login_events/';

INSERT overwrite table login_events
VALUES (100, '2024-10-22 19:00:00'),
       (100, '2024-10-22 19:30:00'),
       (100, '2024-10-23 21:01:00'),
       (100, '2024-10-24 11:01:00'),
       (101, '2024-10-22 19:05:00'),
       (101, '2024-10-22 21:05:00'),
       (101, '2024-10-24 21:05:00'),
       (101, '2024-10-26 15:05:00'),
       (101, '2024-10-27 19:05:00'),
       (102, '2024-10-22 19:55:00'),
       (102, '2024-10-22 21:05:00'),
       (102, '2024-10-23 21:57:00'),
       (102, '2024-10-24 19:10:00'),
       (104, '2024-10-25 21:57:00'),
       (104, '2024-10-23 22:57:00'),
       (105, '2024-10-22 10:01:00');

drop table if exists promotion_info;
create table promotion_info
(
    promotion_id string comment '优惠活动id',
    brand        string comment '优惠药品',
    start_date   string comment '优惠药品活动开始日期',
    end_date     string comment '优惠药品活动结束日期'
) comment '各药品活动周期表'
    LOCATION '/user/hive/warehouse/dwd.db/promotion_info/';

insert overwrite table promotion_info
values (1, '板蓝根', '2024-06-05', '2024-06-09'),
       (2, '板蓝根', '2024-06-11', '2024-06-21'),
       (3, '999感冒灵', '2024-06-05', '2024-06-15'),
       (4, '999感冒灵', '2024-06-09', '2024-06-21'),
       (5, '阿莫西林', '2024-06-05', '2024-06-21'),
       (6, '阿莫西林', '2024-06-09', '2024-06-15'),
       (6, '阿莫西林', '2024-06-09', '2024-06-15'),
       (7, '阿莫西林', '2024-06-17', '2024-06-26'),
       (8, '头孢', '2024-06-05', '2024-06-26'),
       (9, '头孢', '2024-06-09', '2024-06-15'),
       (10, '头孢', '2024-06-17', '2024-06-21');
-----------------------------------------------

-----------------------------
--事务型事实表每个分区存放当日发生的事实记录
--事务型事实表和相关的mysql表一般采用增量同步
--增量首日同步的是mysql表中所有的数据，此时表中包含有历史每天的问诊记录，所以需要根据事实发送的时间放入不同的分区中

--- 首日
-- 每日数据加载
--问诊对表的数据影响：插入数据

-------------------------------
--需要是同步的表中所有的数据，包含了历史每天的支付记录，需要根据支付成功的日期放入不同分区中
--需要ods.consultation_inc 所有诊断记录 。payment_inc 支付成功所有记录
DROP TABLE IF EXISTS trade_consultation_pay_suc_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS trade_consultation_pay_suc_inc
(
    `id`                        STRING COMMENT '问诊ID',
    `consultation_pay_suc_time` STRING comment '诊金支付成功时间',
    `consultation_fee`          decimal(16, 2) comment '问诊费用',
    `doctor_id`                 STRING comment '医生ID',
    `patient_id`                STRING comment '患者ID',
    `user_id`                   STRING comment '用户ID'
) COMMENT '交易域问诊支付成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/trade_consultation_pay_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


------------------------
DROP TABLE IF EXISTS trade_prescription_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS trade_prescription_inc
(
    `id`                STRING COMMENT '处方明细ID',
    `prescription_time` STRING COMMENT '处方开具时间',
    `count`             BIGINT COMMENT '剂量',
    `medicine_id`       STRING COMMENT '药品ID',
    `prescription_id`   STRING COMMENT '处方ID',
    `total_amount`      DECIMAL(16, 2) COMMENT '处方总金额',
    `consultation_id`   STRING COMMENT '问诊ID',
    `doctor_id`         STRING COMMENT '医生ID',
    `patient_id`        STRING COMMENT '患者ID'
) COMMENT '交易域处方开单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/trade_prescription_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



-------
--每日支付成功对prescription-inc的影响，变更数据status=203
DROP TABLE IF EXISTS trade_prescription_pay_suc_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS trade_prescription_pay_suc_inc
(
    `id`                        STRING COMMENT '处方明细ID',
    `prescription_pay_suc_time` STRING COMMENT '处方支付成功时间',
    `count`                     BIGINT COMMENT '剂量',
    `medicine_id`               STRING COMMENT '药品ID',
    `prescription_id`           STRING COMMENT '处方ID',
    `total_amount`              DECIMAL(16, 2) COMMENT '处方总金额',
    `consultation_id`           STRING COMMENT '问诊ID',
    `doctor_id`                 STRING COMMENT '医生ID',
    `patient_id`                STRING COMMENT '患者ID'
) COMMENT '交易域处方开单支付成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/trade_prescription_pay_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


DROP TABLE IF EXISTS doctor_register_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS doctor_register_inc
(
    `id`               STRING COMMENT '医生ID',
    `register_time`    STRING COMMENT '注册时间',
    `birthday`         STRING COMMENT '出生日期',
    `consultation_fee` DECIMAL(19, 2) COMMENT '就诊费用',
    `gender_code`      STRING COMMENT '性别编码：101.男 102.女',
    `gender`           STRING COMMENT '性别',
    `username`             STRING COMMENT '姓名',
    `specialty_code`   STRING COMMENT '专业编码：详情见字典表5xx条目',
    `specialty_name`   STRING COMMENT '专业名称',
    `title_code`       STRING COMMENT '职称编码：301. 医士 302. 医师 303. 主治医师 304. 副主任医师 305. 主任医师',
    `title_name`       STRING COMMENT '职称名称',
    `hospital_id`      STRING COMMENT '所属医院'
) COMMENT '医生域医生注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/doctor_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");



DROP TABLE IF EXISTS user_register_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS user_register_inc
(
    `id`            STRING COMMENT '用户ID',
    `register_time` STRING COMMENT '注册日期',
    `email`         STRING COMMENT '邮箱地址',
    `telephone`     STRING COMMENT '手机号',
    `username`      STRING COMMENT '用户名'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");



DROP TABLE IF EXISTS user_patient_add_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS user_patient_add_inc
(
    `id`          STRING COMMENT '患者ID',
    `add_time`    STRING COMMENT '登记时间',
    `birthday`    STRING COMMENT '生日',
    `gender_code` STRING COMMENT '性别编码',
    `gender`      STRING COMMENT '性别',
    `name`        STRING COMMENT '姓名',
    `user_id`     STRING COMMENT '所属用户ID'
) COMMENT '用户域患者登记事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/user_patient_add_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');


DROP TABLE IF EXISTS interaction_review_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS interaction_review_inc
(
    `id`          STRING COMMENT '问诊ID',
    `review_time` STRING COMMENT '评价时间',
    `rating`      STRING COMMENT '评分',
    `doctor_id`   STRING COMMENT '医生ID',
    `patient_id`  STRING COMMENT '病人ID',
    `user_id`     STRING COMMENT '用户ID'
) COMMENT '互动域用户评价事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dwd.db/interaction_review_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');





