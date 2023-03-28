CREATE TABLE `node_info` (
	`node_id` varchar(128) NOT NULL UNIQUE ,
    `online_time` int DEFAULT '0',
    `profit` float DEFAULT '0',
    `download_traffic` float DEFAULT '0',
    `upload_traffic` float DEFAULT '0',
    `download_blocks` bigint DEFAULT '0' ,
    `last_time` datetime DEFAULT CURRENT_TIMESTAMP ,
    `quitted` BOOLEAN DEFAULT '0' ,
	`port_mapping` varchar(8) DEFAULT '' ,
    `mac_location` varchar(32) DEFAULT '',
    `product_type` varchar(32) DEFAULT '',
    `cpu_cores` int DEFAULT '0',
    `memory` float DEFAULT '0',
    `node_name` varchar(64) DEFAULT '' ,
    `latitude` float DEFAULT '0',
    `longitude` float DEFAULT '0',
    `disk_type` varchar(64) DEFAULT '',
    `io_system` varchar(64) DEFAULT '',
    `system_version` varchar(32) DEFAULT '',
    `nat_type` varchar(32) DEFAULT '',  
    `disk_space` float DEFAULT '0',
    `bandwidth_up` float DEFAULT '0',
    `bandwidth_down` float DEFAULT '0',
    `blocks` bigint DEFAULT '0' ,
    `disk_usage` float DEFAULT '0', 
    `scheduler_sid` varchar(128) NOT NULL,
	PRIMARY KEY (`node_id`)
) ENGINE=InnoDB COMMENT='node info';

CREATE TABLE `validate_result` (
    `round_id` varchar(128) NOT NULL,
    `node_id` varchar(128) NOT NULL,
    `validator_id` varchar(128) NOT NULL,
    `block_number` bigint DEFAULT '0' ,
    `status` tinyint DEFAULT '0',
    `duration` bigint DEFAULT '0' ,
    `bandwidth` float DEFAULT '0',
	`cid` varchar(128) DEFAULT '',
    `start_time` datetime DEFAULT CURRENT_TIMESTAMP,
    `end_time` datetime DEFAULT NULL,
    UNIQUE KEY (`round_id`,`node_id`)
) ENGINE=InnoDB COMMENT='validate result info';

CREATE TABLE `block_download_info` (
    `id` varchar(64) NOT NULL UNIQUE,
    `block_cid` varchar(128) NOT NULL,
    `node_id` varchar(128) NOT NULL,
    `carfile_cid` varchar(128) NOT NULL,
    `block_size` int(20) DEFAULT '0',
    `speed` int(20)  DEFAULT '0' ,
    `reward` int(20) DEFAULT '0',
    `status` TINYINT  DEFAULT '0' ,
    `failed_reason` varchar(128) DEFAULT '' ,
    `client_ip` varchar(18) NOT NULL,
    `created_time` datetime DEFAULT CURRENT_TIMESTAMP,
    `complete_time` datetime,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='block download information';

CREATE TABLE `node_register_info` (
	`node_id` varchar(128) NOT NULL UNIQUE,
	`public_key` varchar(1024) DEFAULT '' ,
    `create_time` varchar(64) DEFAULT '' ,
	`node_type` varchar(64) DEFAULT '' ,
	PRIMARY KEY (`node_id`)
) ENGINE=InnoDB COMMENT='node register info';

CREATE TABLE `replica_info` (
	`hash` varchar(128) NOT NULL,
    `status` TINYINT  DEFAULT '0' ,
    `node_id` varchar(128) NOT NULL ,
    `done_size` BIGINT DEFAULT '0' ,
    `is_candidate` BOOLEAN,
	`end_time` datetime DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY (`hash`,`node_id`)
) ENGINE=InnoDB COMMENT='replica info';

CREATE TABLE `asset_record` (
	`hash` varchar(128) NOT NULL UNIQUE,
	`cid` varchar(128) NOT NULL UNIQUE,
    `total_size` BIGINT  DEFAULT '0' ,
    `total_blocks` int  DEFAULT '0' ,
	`edge_replicas` TINYINT DEFAULT '0' ,
	`candidate_replicas` TINYINT DEFAULT '0' ,
	`state` varchar(128) NOT NULL DEFAULT '',
    `expiration` datetime NOT NULL,
    `created_time` datetime DEFAULT CURRENT_TIMESTAMP,
	`end_time` datetime DEFAULT CURRENT_TIMESTAMP,
    `scheduler_sid` varchar(128) NOT NULL,
	PRIMARY KEY (`hash`)
) ENGINE=InnoDB COMMENT='asset record';

CREATE TABLE `edge_update_info` (
	`node_type` int NOT NULL UNIQUE,
	`app_name` varchar(64) NOT NULL,
    `version`  varchar(32) NOT NULL,
    `hash` varchar(128) NOT NULL,
	`download_url` varchar(128) NOT NULL,
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`node_type`)
) ENGINE=InnoDB COMMENT='edge update info';

CREATE TABLE `validators` (
    `node_id` varchar(128) NOT NULL,
    `scheduler_sid` varchar(128) NOT NULL,
    PRIMARY KEY (`node_id`)
) ENGINE=InnoDB COMMENT='validators';
