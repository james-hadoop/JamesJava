CREATE TABLE `txkd_dc_hive_lineage_rel` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `full_src_field` varchar(200) NOT NULL COMMENT '完全源表字段名称',
  `full_des_field` varchar(200) NOT NULL COMMENT '完全目标表字段名称',
  `rel` varchar(500) DEFAULT NULL COMMENT '转换关系',
  `src_field` varchar(100) NOT NULL COMMENT '源表字段名称',
  `des_field` varchar(100) NOT NULL COMMENT '目标表字段名称',
  `src_table` varchar(100) NOT NULL COMMENT '源表名称',
  `des_table` varchar(100) NOT NULL COMMENT '目标表名称',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `sql_format` text NOT NULL COMMENT '格式化sql',
  `hive_lineage_log_id` int(11) NOT NULL COMMENT 'hive血缘关系日志id',
  `ext` varchar(200) DEFAULT NULL COMMENT '扩展字段',
  KEY `idx_id` (`id`),
  KEY `idx_hive_lineage_log_id` (`hive_lineage_log_id`),
  KEY `idx_src_field` (`src_field`),
  KEY `idx_des_field` (`des_field`),
  KEY `idx_src_table` (`src_table`),
  KEY `idx_des_table` (`des_table`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hive血缘关系点边关系';


CREATE TABLE `txkd_dc_hive_lineage_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `lineage_str` text NOT NULL,
  `update_time` datetime NOT NULL COMMENT '更新时间',
  KEY `idx_id` (`id`),
  KEY `index_update_time` (`update_time`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='hive血缘关系日志';


CREATE TABLE `txkd_dc_hive_sql` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sql_str` text NOT NULL,
  `update_time` datetime NOT NULL COMMENT '更新时间',
  KEY `idx_id` (`id`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=MyISAM CHARSET=utf8 COMMENT='hive sql';
SET @@global.sql_mode= '';

CREATE TABLE `txkd_dc_hive_sql_focus` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sql_str` text NOT NULL,
  KEY `idx_id` (`id`)
) ENGINE=MyISAM CHARSET=utf8 COMMENT='hive sql';

select count(1) cnt from txkd_dc_hive_sql;

insert into txkd_dc_hive_sql_focus
select null,sql_str from
(select str,max(sql_str) sql_str from
(select left(sql_str,50) str ,sql_str from txkd_dc_hive_sql) t group by str) tt;


select sql_str from txkd_dc_hive_sql_focus where (sql_str not like '%values%' and sql_str not like '%VALUES%' and sql_str not like '%with%' and sql_str not like '%WITH%' and
sql_str not like '%avonxu%');

select sql_str from txkd_dc_hive_sql_focus where (sql_str not like '%values%' and sql_str not like '%VALUES%' and sql_str not like '%with%' and sql_str not like '%WITH%' and
sql_str not like '%avonxu%' and sql_str not like '%hongyi%' and sql_str not like '%hdfs%' and sql_str not like '%--%') order by sql_str;

select sql_str from txkd_dc_hive_sql_focus where sql_str like '%kandian_mid_video_cinfo_d%' and sql_str like '%kandian_video_medium_full_info_d%';

select sql_str from txkd_dc_hive_sql_focus where sql_str like '%INSERT OVERWRITE TABLE t_dwa_kd_video_hudong_ald_layer_1d_d%';

CREATE TABLE `txkd_dc_hive_sql_with_ddl` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sql_str` text NOT NULL,
  `ddl` text not null,
  `update_time` datetime NOT NULL COMMENT '更新时间',
  KEY `idx_id` (`id`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


CREATE TABLE `txkd_dc_hive_sql_focus_clean` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sql_str` text NOT NULL,
  KEY `idx_id` (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='hive sql';

insert into txkd_dc_hive_sql_focus_clean
select id,sql_str from txkd_dc_hive_sql_focus where (sql_str not like '%values%' and sql_str not like '%VALUES%' and sql_str not like '%with%' and sql_str not like '%WITH%' and
sql_str not like '%avonxu%' and sql_str not like '%hongyi%' and sql_str not like '%hdfs%' and sql_str not like '%--%') order by sql_str;

CREATE TABLE `txkd_dc_hive_sql_with_table_names` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sql_str` text NOT NULL,
  `table_names` text NOT NULL,
  `update_time` datetime NOT NULL COMMENT '更新时间',
  KEY `idx_id` (`id`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

select count(1) cnt from txkd_dc_hive_sql_with_table_names;

CREATE TABLE `txkd_dc_hive_sql_table_names_with_ddl` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `	` text NOT NULL,
  `ddl` text NOT NULL,
  `update_time` datetime NOT NULL COMMENT '更新时间',
  KEY `idx_id` (`id`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;








