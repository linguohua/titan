create database locator;
CREATE TABLE `config` (
    `id` int(20) unsigned NOT NULL AUTO_INCREMENT,
	`scheduler_url` varchar(128) NOT NULL UNIQUE ,
    `area_id` varchar(64) DEFAULT NULL ,
    `weight` int(5)  DEFAULT '0' ,
	PRIMARY KEY (`id`)
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='location config';

CREATE TABLE `device` (
    `id` int(20) unsigned NOT NULL AUTO_INCREMENT,
	`device_id` varchar(128) NOT NULL UNIQUE ,
    `scheduler_url` varchar(128) DEFAULT NULL ,
    `area_id` varchar(64) DEFAULT NULL ,
    `online` TINYINT  DEFAULT '0'  ,
	PRIMARY KEY (`id`)
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='device info';


-- GRANT ALL ON my_db.* TO 'new_user'@'localhost';