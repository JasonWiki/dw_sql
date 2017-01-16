# ************************************************************
# Sequel Pro SQL dump
# Version 4499
#
# http://www.sequelpro.com/
# https://github.com/sequelpro/sequelpro
#
# Host: server.techshow.club (MySQL 5.6.21)
# Database: dw_explorer
# Generation Time: 2016-12-27 11:42:42 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table query_doc
# ------------------------------------------------------------

DROP TABLE IF EXISTS `query_doc`;

CREATE TABLE `query_doc` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `parent_id` bigint(20) NOT NULL,
  `is_folder` tinyint(1) NOT NULL,
  `filename` varchar(255) NOT NULL,
  `content` text NOT NULL,
  `is_deleted` tinyint(1) NOT NULL,
  `created` datetime NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `user_parent` (`user_id`,`parent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table query_stmt
# ------------------------------------------------------------

DROP TABLE IF EXISTS `query_stmt`;

CREATE TABLE `query_stmt` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `task_id` bigint(20) NOT NULL,
  `stmt` text NOT NULL,
  `status` tinyint(4) NOT NULL,
  `progress` tinyint(4) NOT NULL,
  `duration` int(11) NOT NULL,
  `created` datetime NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table query_task
# ------------------------------------------------------------

DROP TABLE IF EXISTS `query_task`;

CREATE TABLE `query_task` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `queries` text NOT NULL,
  `status` tinyint(4) NOT NULL,
  `progress` tinyint(4) NOT NULL,
  `duration` int(11) NOT NULL,
  `created` datetime NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table user
# ------------------------------------------------------------

DROP TABLE IF EXISTS `user`;

CREATE TABLE `user` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL,
  `truename` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `role` int(11) NOT NULL DEFAULT '0',
  `created` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `user` WRITE;
/*!40000 ALTER TABLE `user` DISABLE KEYS */;

INSERT INTO `user` (`id`, `username`, `truename`, `email`, `role`, `created`)
VALUES
	(50,'lrgoodboy','刘锐','ray@angejia.com',2,'2015-04-23 23:18:38'),
	(51,'jennyqin','覃贞妮','52587158@qq.com',0,'2015-04-24 10:07:39'),
	(52,'zzheric','张政和','zzheric@gmail.com',0,'2015-04-24 10:08:41'),
	(53,'pensz','waiting','waiting',0,'2015-04-24 10:14:30'),
	(54,'siyuanye','叶思远','siyuanye@angejia.com',0,'2015-04-24 13:58:04'),
	(55,'cyndiWade','张炜林','zhanglin492103904@qq.com',2,'2015-05-12 11:09:20'),
	(57,'storm12358','zjq','dlutzjq@gmail.com',2,'2015-05-12 11:09:20'),
	(58,'grjelf','grjelf','grjelf@163.com',0,'2015-05-19 00:01:00'),
	(59,'zyystudio','stanley','stanley@angejia.com',0,'2015-06-06 13:17:39'),
	(60,'woodwang','王以林','yilinwang@angejia.com',0,'2015-06-24 10:36:40'),
	(61,'rotkang','康珊伟','yvigmmwfn@163.com',0,'2015-07-07 11:25:40'),
	(63,'shuangyanluo','罗双燕','sukishuangyan@126.com',0,'2015-08-07 10:39:54'),
	(64,'zhiwenjiang','江志文','zhiwenjiang@angejia.com',2,'2015-08-21 11:54:25'),
	(65,'xiaojunyuan','袁小军','xiaojunyuan@angejia.com',0,'2015-08-27 11:54:25'),
	(66,'JasonWiki','Jason','waiting',2,'2015-11-18 10:56:37'),
	(68,'yima','平川','waiting',0,'2015-12-29 11:38:29'),
	(69,'bi-angejia','waiting','waiting',0,'2016-01-04 11:43:19'),
	(70,'songsiya','思雅','waiting',2,'2016-03-10 16:18:28'),
	(71,'mayanyuAngejia','马衍宇','waiting',0,'2016-03-10 16:25:22'),
	(72,'lidongmei','李冬梅','lidongmei@angejia.com',2,'2016-03-16 10:48:29'),
	(73,'xiawenrong','夏文荣','xiawenrong@angejia.com',2,'2016-04-06 11:54:59'),
	(74,'RobberPhex','波波','waiting',2,'2016-04-22 15:15:02'),
	(75,'fojouse','waiting','waiting',0,'2016-04-28 13:58:11'),
	(77,'libo295890307','李博','libo@angejia.com',0,'2016-05-20 11:28:22'),
	(78,'linzhouzhi','林周治','waiting',0,'2016-08-09 13:59:45'),
	(79,'huazhiqiang','华志强','waiting',2,'2016-08-15 13:27:23'),
	(80,'liulonghua','刘龙华','liulonghua@angejia.com',2,'2016-09-12 15:16:54'),
	(81,'luomixiang','王强','wangqiang1@angejia.com',0,'2016-10-13 15:16:54'),
	(82,'jiangzhiwen','waiting','waiting',0,'2016-11-22 20:17:51');

/*!40000 ALTER TABLE `user` ENABLE KEYS */;
UNLOCK TABLES;



/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
