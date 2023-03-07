-- create database locator;
CREATE TABLE `scheduler_config` (
	`scheduler_url` varchar(128) NOT NULL UNIQUE ,
    `area_id` varchar(64) DEFAULT NULL ,
    `weight` int(5)  DEFAULT '0' ,
    `access_token` varchar(256) DEFAULT NULL ,
	PRIMARY KEY (`scheduler_url`)
  ) ENGINE=InnoDB COMMENT='locator scheduler config';

CREATE TABLE `node_info` (
	`node_id` varchar(128) NOT NULL UNIQUE ,
    `scheduler_url` varchar(128) DEFAULT NULL ,
    `area_id` varchar(64) DEFAULT NULL ,
    `online` TINYINT  DEFAULT '0'  ,
	PRIMARY KEY (`node_id`)
  ) ENGINE=InnoDB COMMENT='locator node info';

-- CREATE USER 'user01'@'localhost' IDENTIFIED BY 'new_password';
-- GRANT ALL ON locator.* TO 'user01'@'localhost';