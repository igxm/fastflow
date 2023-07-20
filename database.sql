CREATE DATABASE `fastflow` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;

CREATE TABLE `dag` (
   `id` varchar(255) not null,
   `created_at` bigint(20),
   `updated_at` bigint(20),
   `name` varchar(64) NOT NULL ,
   `desc` varchar(255) NOT NULL,
   `cron` varchar(100) DEFAULT NULL,
   `vars` json,
   `status` varchar(255) DEFAULT NULL,
   `tasks` json,
   PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
 

CREATE TABLE `dag_instance` (
   `id` varchar(255) not null,
   `created_at` bigint(20),
   `updated_at` bigint(20),
   `dag_id` varchar(64) NOT NULL ,
   `trigger` varchar(255) NOT NULL,
   `worker` varchar(100) DEFAULT NULL,
   `vars` json,
   `status` varchar(255) DEFAULT NULL,
   `share_data` json,
   `reason` varchar(255) DEFAULT null,
   `cmd` json,
   PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
 
CREATE TABLE `task_instance` (
   `id` varchar(255) not null,
   `created_at` bigint(20),
   `updated_at` bigint(20),
   `task_id` varchar(255) NOT NULL ,
   `dag_ins_id` varchar(255) NOT NULL,
   `name` varchar(255) DEFAULT NULL,
   `depend_on` varchar(255) DEFAULT NULL,
   `action_name` varchar(255) DEFAULT NULL,
   `timeout_secs` int(10) DEFAULT 0,
   `params` json,
   `traces` json,
   `status` varchar(255) DEFAULT null,
   `reason` varchar(255) DEFAULT null,
   `pre_checks` json,
   PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
 


CREATE TABLE `mutex` (
	`id` varchar(255) not null,
	`expired_at` timestamp,
	`identity` varchar(255) NOT NULL,
	PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;



CREATE table `heartbeat` (
	`id` varchar(255) not null,
	`updated_at` timestamp,
	PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;



CREATE table election (
	`id` varchar(255) not null,
	`worker_key` varchar(255),
	`updated_at` timestamp,
	PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

