if (!$MYSQL_MYROCKS_HOTBACKUP)
{
  --skip Requires external myrocks_hotbackup tool (not supplied by the MySQL 8.0 source branch)
}

--let $rpl_server_count= 2
--let $rpl_topology= none
--source include/rpl/init.inc
--source include/rpl/default_connections.inc

connection server_1;
create database db1;

create table db1.t1 (
  `id` int(10) not null auto_increment,
  `k` int(10),
  `data` varchar(2048),
  primary key (`id`),
  key (`k`)
) engine=rocksdb;

