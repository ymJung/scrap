# scrap

python 개인 공부

날짜별로 비교

'content', 'CREATE TABLE `content` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `title` varchar(512) DEFAULT NULL,
  `contentData` varchar(4096) DEFAULT NULL,
  `authorId` bigint(20) NOT NULL,
  `date` datetime DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT=''content data'''

'author', 'CREATE TABLE `author` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `population` int(11) DEFAULT ''0'',
  `believe` int(11) DEFAULT ''0'',
  `good` int(11) DEFAULT ''0'',
  `bad` int(11) DEFAULT ''0'',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8'

'comment', 'CREATE TABLE `comment` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `authorId` bigint(20) NOT NULL,
  `commentData` varchar(1024) DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `contentId` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `author` (`authorId`),
  KEY `content` (`contentId`),
  CONSTRAINT `content` FOREIGN KEY (`contentId`) REFERENCES `content` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `author` FOREIGN KEY (`authorId`) REFERENCES `author` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT=''comment '''
