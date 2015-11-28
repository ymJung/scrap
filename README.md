# scrap

python 개인 공부

날짜별로 비교

'content', 'CREATE TABLE `content` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `title` varchar(512) DEFAULT NULL,
  `contentData` longtext,
  `authorId` bigint(20) NOT NULL,
  `date` datetime DEFAULT NULL,
  `createdAt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8 COMMENT=''content data'''

'comment', 'CREATE TABLE `comment` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `contentId` bigint(20) NOT NULL,
  `authorId` bigint(20) NOT NULL,
  `commentData` varchar(1024) DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `createdAt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `author` (`authorId`),
  KEY `content` (`contentId`),
  CONSTRAINT `author` FOREIGN KEY (`authorId`) REFERENCES `author` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `content` FOREIGN KEY (`contentId`) REFERENCES `content` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=65 DEFAULT CHARSET=utf8 COMMENT=''comment '''

'author', 'CREATE TABLE `author` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `population` int(11) DEFAULT ''0'',
  `believe` int(11) DEFAULT ''0'',
  `good` int(11) DEFAULT ''0'',
  `bad` int(11) DEFAULT ''0'',
  `createdAt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=64 DEFAULT CHARSET=utf8'


CREATE TABLE `word` (
   `id` bigint(20) NOT NULL AUTO_INCREMENT,
   `word` varchar(50) NOT NULL,
   `good` int(11) DEFAULT '0',
   `bad` int(11) DEFAULT '0',
   PRIMARY KEY (`id`),
   UNIQUE KEY `word_UNIQUE` (`word`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `delimiter` (
   `id` bigint(20) NOT NULL AUTO_INCREMENT,
   `delimiter` varchar(10) NOT NULL,
   `good` int(11) DEFAULT '0',
   `bad` int(11) DEFAULT '0',
   PRIMARY KEY (`id`),
   UNIQUE KEY `delimiter_UNIQUE` (`delimiter`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8