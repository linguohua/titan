-- create database locator;
CREATE TABLE `scheduler_config` (
	  `scheduler_url` varchar(128) NOT NULL UNIQUE ,
    `area_id` varchar(64) DEFAULT NULL ,
    `weight` int(5)  DEFAULT '0' ,
    `access_token` varchar(256) DEFAULT NULL ,
	PRIMARY KEY (`scheduler_url`)
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='locator scheduler config';

CREATE TABLE `device` (
	  `device_id` varchar(128) NOT NULL UNIQUE ,
    `scheduler_url` varchar(128) DEFAULT NULL ,
    `area_id` varchar(64) DEFAULT NULL ,
    `online` TINYINT  DEFAULT '0'  ,
	PRIMARY KEY (`device_id`)
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='locator device info';

-- CREATE USER 'new_user'@'localhost' IDENTIFIED BY 'new_password';
-- GRANT ALL ON locator.* TO 'user01'@'localhost';
