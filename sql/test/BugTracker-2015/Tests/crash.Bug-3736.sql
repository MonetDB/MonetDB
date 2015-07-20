

CREATE TABLE open_auctions (
	  id int NOT NULL AUTO_INCREMENT,
	  open_auction_id varchar(255) NOT NULL,
	  initial double NOT NULL,
	  reserve double NOT NULL,
	  aktuell double NOT NULL,
	  privacy varchar(255) NOT NULL,
	  itemref varchar(255) NOT NULL,
	  seller varchar(255) NOT NULL,
	  quantity double NOT NULL,
	  type varchar(255) NOT NULL,
	  start varchar(255) NOT NULL,
	  ende varchar(255) NOT NULL,
	  PRIMARY KEY (id)
);

INSERT INTO "open_auctions" ("id", "open_auction_id", "initial", "reserve", "aktuell", "privacy", "itemref", "seller", "quantity", "type", "start", "ende") VALUES
(1, 'open_auction0', 210.62, 1540.75, 263.12, 'No', 'item0', 'person11', 1, 'Regular', '02/27/1998', '03/09/1999'),
(2, 'open_auction1', 69.64, 398.65, 168.64, '', 'item2', 'person10', 1, 'Featured', '06/14/1998', '02/27/1999'),
(3, 'open_auction2', 13.9, 0, 16.9, 'No', 'item3', 'person11', 1, 'Featured', '07/16/2000', '10/22/2000'),
(4, 'open_auction3', 17.12, 0, 179.12, '', 'item5', 'person13', 1, 'Featured', '02/01/2001', '07/22/1999'),
(5, 'open_auction4', 38.21, 116.91, 219.71, 'No', 'item7', 'person11', 4, 'Regular, Dutch', '08/05/2000', '01/09/1999'),
(6, 'open_auction5', 75.95, 174.84, 116.45, 'No', 'item9', 'person7', 1, 'Featured', '01/27/2001', '10/08/1999'),
(7, 'open_auction6', 67.27, 0, 77.77, 'Yes', 'item10', 'person14', 1, 'Featured', '12/15/2001', '07/13/1999'),
(8, 'open_auction7', 35.53, 133.36, 73.03, 'Yes', 'item12', 'person16', 1, 'Regular', '04/06/2001', '04/11/2000'),
(9, 'open_auction8', 92.54, 0, 101.54, '', 'item13', 'person12', 1, 'Regular', '10/20/1998', '02/04/2001'),
(10, 'open_auction9', 9.88, 0, 137.38, '', 'item14', 'person10', 1, 'Featured', '06/16/1998', '12/05/2000'),
(11, 'open_auction10', 86.28, 0, 239.28, 'Yes', 'item16', 'person8', 1, 'Featured', '06/26/2000', '02/04/1998'),
(12, 'open_auction11', 4.12, 24.53, 38.62, '', 'item17', 'person12', 1, 'Regular', '11/03/1999', '03/02/2000');

CREATE TABLE bidder (
	  id int NOT NULL AUTO_INCREMENT,
	  open_auction_id varchar(255) NOT NULL,
	  date varchar(255) NOT NULL,
	  time varchar(255) NOT NULL,
	  personref varchar(255) NOT NULL,
	  increase double NOT NULL,
	  PRIMARY KEY (id)
);
--
-- Daten für Tabelle "bidder"
--

INSERT INTO "bidder" ("id", "open_auction_id", "date", "time", "personref", "increase") VALUES
(1, 'open_auction0', '06/13/2001', '13:16:15', 'person0', 18),
(2, 'open_auction0', '09/18/2000', '11:29:44', 'person23', 12),
(3, 'open_auction0', '01/07/1998', '10:23:59', 'person14', 18),
(4, 'open_auction0', '07/10/2001', '14:00:39', 'person16', 4.5),
(5, 'open_auction1', '11/12/1998', '11:23:38', 'person20', 4.5),
(6, 'open_auction1', '10/02/2000', '22:48:00', 'person4', 15),
(7, 'open_auction1', '12/04/1998', '22:29:38', 'person23', 1.5),
(8, 'open_auction1', '06/22/1999', '12:43:47', 'person19', 15),
(9, 'open_auction1', '12/02/2001', '13:38:51', 'person15', 45),
(10, 'open_auction1', '11/12/2001', '04:50:27', 'person9', 6),
(11, 'open_auction1', '05/21/2001', '08:02:16', 'person5', 12),
(12, 'open_auction2', '12/04/2000', '14:03:16', 'person5', 3),
(13, 'open_auction3', '09/03/2000', '22:45:30', 'person8', 10.5),
(14, 'open_auction3', '08/18/1998', '17:13:40', 'person22', 25.5),
(15, 'open_auction3', '06/05/1998', '11:57:56', 'person19', 16.5),
(16, 'open_auction3', '09/20/1999', '00:39:18', 'person5', 46.5),
(17, 'open_auction3', '10/03/2001', '01:08:59', 'person23', 58.5),
(18, 'open_auction3', '02/17/2001', '07:13:32', 'person20', 4.5),
(19, 'open_auction4', '10/19/1998', '09:00:31', 'person7', 33),
(20, 'open_auction4', '03/11/2001', '01:59:02', 'person8', 9),
(21, 'open_auction4', '04/16/2001', '23:37:09', 'person8', 6),
(22, 'open_auction4', '03/06/1999', '12:19:57', 'person5', 4.5),
(23, 'open_auction4', '02/01/2000', '09:37:51', 'person21', 18),
(24, 'open_auction4', '01/09/1999', '19:31:44', 'person1', 31.5),
(25, 'open_auction4', '11/11/2001', '05:24:08', 'person16', 27),
(26, 'open_auction4', '11/19/1998', '16:16:17', 'person10', 7.5),
(27, 'open_auction4', '04/05/1999', '00:37:15', 'person17', 7.5),
(28, 'open_auction4', '09/14/1999', '12:00:43', 'person12', 37.5),
(29, 'open_auction5', '07/07/2000', '08:53:00', 'person15', 6),
(30, 'open_auction5', '08/06/2001', '10:16:15', 'person13', 4.5),
(31, 'open_auction5', '08/23/1999', '08:26:06', 'person17', 30),
(32, 'open_auction6', '01/23/2000', '17:14:42', 'person1', 10.5),
(33, 'open_auction7', '10/14/1999', '14:39:18', 'person16', 27),
(34, 'open_auction7', '05/19/1999', '23:51:16', 'person14', 9),
(35, 'open_auction7', '03/27/1999', '19:14:39', 'person23', 1.5),
(36, 'open_auction8', '04/26/2001', '00:41:04', 'person12', 9),
(37, 'open_auction9', '04/03/1999', '19:09:46', 'person22', 18),
(38, 'open_auction9', '01/21/1999', '08:14:44', 'person19', 19.5),
(39, 'open_auction9', '11/24/1999', '02:12:12', 'person11', 12),
(40, 'open_auction9', '01/07/2001', '05:33:55', 'person7', 9),
(41, 'open_auction9', '07/28/2000', '00:57:52', 'person16', 16.5),
(42, 'open_auction9', '01/28/2000', '20:24:02', 'person23', 1.5),
(43, 'open_auction9', '05/13/2001', '02:45:46', 'person13', 1.5),
(44, 'open_auction9', '12/06/2000', '15:18:07', 'person21', 24),
(45, 'open_auction9', '08/16/1998', '09:27:27', 'person14', 3),
(46, 'open_auction9', '06/18/2001', '19:57:53', 'person7', 3),
(47, 'open_auction9', '12/13/2001', '09:01:23', 'person2', 12),
(48, 'open_auction9', '03/18/2001', '08:59:02', 'person13', 7.5),
(49, 'open_auction10', '08/14/1998', '16:33:46', 'person23', 33),
(50, 'open_auction10', '03/24/1998', '11:45:48', 'person3', 1.5),
(51, 'open_auction10', '10/08/1998', '06:20:35', 'person1', 45),
(52, 'open_auction10', '01/12/1998', '21:43:02', 'person3', 15),
(53, 'open_auction10', '03/02/2001', '20:20:33', 'person0', 9),
(54, 'open_auction10', '03/01/2000', '10:08:07', 'person23', 10.5),
(55, 'open_auction10', '07/19/2001', '00:50:29', 'person6', 9),
(56, 'open_auction10', '10/04/2001', '00:58:25', 'person4', 3),
(57, 'open_auction10', '11/27/1998', '00:15:23', 'person21', 27),
(58, 'open_auction11', '05/16/1998', '15:08:01', 'person1', 15),
(59, 'open_auction11', '05/08/2000', '06:44:20', 'person3', 4.5),
(60, 'open_auction11', '10/22/2001', '15:34:49', 'person4', 15);

Select b.* FROM open_auctions o, b bidder WHERE (select b3.INCREASE from bidder b3 where b3.id = (select min (b3a.id) from bidder b3a where b3a.open_auction_id = o.open_auction_id)) * 2 <= (Select b2.INCREASE from bidder b2 where b2.id = (SELECT MAX (b2a.id) from bidder b2a where b2a.open_auction_id = o.open_auction_id)) AND o.open_auction_id = b.open_auction_id order by date, time;

plan Select b.* FROM open_auctions o, bidder b WHERE (select b3.INCREASE from bidder b3 where b3.id = (select min (b3a.id) from bidder b3a where b3a.open_auction_id = o.open_auction_id)) * 2 <= (Select b2.INCREASE from bidder b2 where b2.id = (SELECT MAX (b2a.id) from bidder b2a where b2a.open_auction_id = o.open_auction_id)) AND o.open_auction_id = b.open_auction_id;
Select b.* FROM open_auctions o, bidder b WHERE (select b3.INCREASE from bidder b3 where b3.id = (select min (b3a.id) from bidder b3a where b3a.open_auction_id = o.open_auction_id)) * 2 <= (Select b2.INCREASE from bidder b2 where b2.id = (SELECT MAX (b2a.id) from bidder b2a where b2a.open_auction_id = o.open_auction_id)) AND o.open_auction_id = b.open_auction_id order by date, time;

drop table bidder;
drop table open_auctions;
