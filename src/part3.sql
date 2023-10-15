-- Active: 1696963787123@@127.0.0.1@5432@info21
-- task1
CREATE
OR REPLACE FUNCTION fnc_part3_task1() RETURNS TABLE (
    "Peer1" VARCHAR,
    "Peer2" VARCHAR,
    "PointsAmount" INTEGER
) AS $$ BEGIN RETURN QUERY
SELECT
    t1.checkingpeer AS Peer1,
    t1.checkedpeer AS Peer2,
    COALESCE(t1.pointamount, 0) - COALESCE(t2.pointamount, 0) AS "PointsAmount"
FROM
    TransferredPoints AS t1 FULL
    JOIN TransferredPoints AS t2 ON t1.checkingpeer = t2.checkedpeer
    AND t2.checkingpeer = t1.checkedpeer
WHERE
    t1.id > t2.id
    OR t2.id IS NULL;

END;

$$ LANGUAGE plpgsql;
SELECT * FROM fnc_part3_task1();


-- task 2

--  Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
-- В таблицу включать только задания, успешно прошедшие проверку (определять по таблице Checks). 
-- Одна задача может быть успешно выполнена несколько раз. В таком случае в таблицу включать все успешные проверки.



CREATE
OR REPLACE FUNCTION fnc_part3_task2() RETURNS TABLE (
    "Peer" VARCHAR,
    "Task" VARCHAR,
    "XP" NUMERIC
) AS $$ BEGIN RETURN QUERY
select checks.peer as "Peer",checks.task as "Task",xp.xpamount as "XP" from xp Join checks on  checks.id = xp."Check"
join p2p on p2p."Check" = xp."Check"
join verter on verter."Check" = xp."Check"
where p2p."State" = 'Success' AND verter."State" = 'Success';
END;
$$ LANGUAGE plpgsql;

drop Function fnc_part3_task2;

select * from fnc_part3_task2();

-- task 3

-- Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
-- Параметры функции: день, например 12.05.2022. 
-- Функция возвращает только список пиров.


CREATE
OR REPLACE FUNCTION fnc_part3_task3(cur_date DATE) RETURNS TABLE (
    "Peer" VARCHAR
) AS $$ BEGIN RETURN QUERY
SELECT tt.peer From timetracking as tt where tt."Date" = cur_date and state = 2 GROUP BY Peer HAVING count(tt.peer) = 1;
END;
$$ LANGUAGE plpgsql;

SELECT * from fnc_part3_task3('2023-10-10');



-- task 4


select * from TransferredPoints;


-- task 6
-- 6) Определить самое часто проверяемое задание за каждый день
-- При одинаковом количестве проверок каких-то заданий в определенный день, вывести их все. 
-- Формат вывода: день, название задания

CREATE OR REPLACE PROCEDURE fnc_part3_task6(IN result REFCURSOR = 'pr_result_part3_task6')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
WITH tb1 as (
select checks.task,count(*),checks."date" from p2p join checks on checks.id = p2p."Check"
where p2p."State" = 'Start'
group by checks."date",checks.task
),
tb2 as (
    SELECT MAX(count) as max_count,date from tb1 GROUP BY date order by date
)
SELECT  tb1.date as Date, tb1.task as Task from tb1 join tb2 on tb1.date = tb2.date
WHERE tb1.count = tb2.max_count;
END;
$$ 

drop Procedure fnc_part3_task6;


BEGIN;
    CALL fnc_part3_task6();
    FETCH ALL FROM "pr_result_part3_task6";
END;

--task 7

-- Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
-- Параметры процедуры: название блока, например "CPP". 
-- Результат вывести отсортированным по дате завершения. 
-- Формат вывода: ник пира, дата завершения блока (т.е. последнего выполненного задания из этого блока)

  CREATE OR REPLACE PROCEDURE fnc_part3_task7(task_cur Varchar(16),IN result REFCURSOR = 'pr_result_part3_task7')
LANGUAGE plpgsql    
AS $$
BEGIN
IF task_cur = 'C' THEN
 	OPEN result FOR
    WITH tb1 as (
    select checks.peer,checks.task,count(checks.task) from p2p join checks on checks.id = p2p."Check" join verter on verter."Check" = p2p."Check"
    where p2p."State" = 'Success' AND verter."State" = 'Success' and  REGEXP_LIKE(checks.task,task_cur) GROUP BY checks.peer,checks.task
    ),
    tb2 as (
    select peer,count(task) from tb1 GROUP BY peer),
    tb3 as ( 
    select checks.peer,MAX(checks.date) as max_date from checks join p2p on p2p."Check" = checks.id WHERE checks.task = 'C7_3DViewer_v1.0' AND p2p."State" = 'Success' GROUP BY checks.peer
    )
    select tb2.peer,tb3.max_date as date from tb2 join tb3 on tb3.peer = tb2.peer where count >= 6 order by date desc;
ELSEIF task_cur = 'CPP' THEN
 	OPEN result FOR
    WITH tb1 as (
    select checks.peer,checks.task,count(checks.task) from p2p join checks on checks.id = p2p."Check" where p2p."State" = 'Success' and  REGEXP_LIKE(checks.task,'CPP') GROUP BY checks.peer,checks.task
    ),
    tb2 as (
    select peer,count(task) from tb1 GROUP BY peer),
    tb3 as ( 
    select checks.peer,MAX(checks.date) as max_date from checks join p2p on p2p."Check" = checks.id WHERE checks.task = 'CPP4_3DViewer_v2.0' AND p2p."State" = 'Success' GROUP BY checks.peer
    )
    select tb2.peer,tb3.max_date as date from tb2 join tb3 on tb3.peer = tb2.peer where count >= 4 order by date desc;
ELSEIF task_cur = 'DO' THEN
 OPEN result FOR
    WITH tb1 as (
    select checks.peer,checks.task,count(checks.task) from p2p join checks on checks.id = p2p."Check" where p2p."State" = 'Success' and  REGEXP_LIKE(checks.task,'DO') GROUP BY checks.peer,checks.task
    ),
    tb2 as (
    select peer,count(task) from tb1 GROUP BY peer),
    tb3 as ( 
    select checks.peer,MAX(checks.date) as max_date from checks join p2p on p2p."Check" = checks.id WHERE checks.task = 'DO6_CICD' AND p2p."State" = 'Success' GROUP BY checks.peer
    )
    select tb2.peer,tb3.max_date as date from tb2 join tb3 on tb3.peer = tb2.peer where count >= 5 order by date desc;
ELSE 
 RAISE  NOTICE 'Incorrect Input!';
END IF;
END;
$$

drop Procedure fnc_part3_task7;

BEGIN;
    CALL fnc_part3_task7('C');
    FETCH ALL FROM "pr_result_part3_task7";
END;





-- select 
-- select * from p2p join checks on checks.id =p2p."Check" join verter on verter."Check" = p2p."Check" where checks.task = 'DO1_Linux'


-- select * from p2p join checks on checks.id = p2p."Check"

-- call insert_p2p('antoinco','jeratta','CPP4_3DViewer_v2.0','Start','17:00:00');
-- call insert_p2p('antoinco','jeratta','CPP4_3DViewer_v2.0','Success','17:44:00');

-- call insert_verter('antoinco','CPP3_SmartCalc_v2.0','Start','17:45:00');
-- call insert_verter('antoinco','CPP3_SmartCalc_v2.0','Success','17:50:00');


-- select * from verter join checks on checks.id = verter."Check";

-- delete from verter where id = 64;
-- select * from xp;

-- insert into xp VALUES((SELECT MAX(ID) FROM xp) + 1,46,750);

-- task 8
-- Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира, проверяться у которого рекомендует наибольшее число друзей. 
-- Формат вывода: ник пира, ник найденного проверяющего

 CREATE OR REPLACE PROCEDURE fnc_part3_task8(IN result REFCURSOR = 'pr_result_part3_task8')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
with tb1 as (
select friends.peer1,rc.recommendedpeer,count(*) from friends  join recommendations as rc on friends.peer2 = rc.peer
where peer1 != rc.recommendedpeer
GROUP BY peer1,recommendedpeer
order by peer1),
tb2 as (
select tb1.peer1,MAX(count) as max_ from tb1 GROUP BY tb1.peer1)
select tb1.peer1 as Peer,recommendedpeer from tb1 join tb2 on tb1.peer1 = tb2.peer1 AND tb1.count = tb2.max_;
END;
$$

drop Procedure fnc_part3_task8;

BEGIN;
    CALL fnc_part3_task8();
    FETCH ALL FROM "pr_result_part3_task8";
END;

-- task 9


--  Определить процент пиров, которые:

-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному


 CREATE OR REPLACE PROCEDURE fnc_part3_task9(task1 VARCHAR(16), task2 VARCHAR(16),IN result REFCURSOR = 'pr_result_part3_task9')
LANGUAGE plpgsql    
AS $$
DECLARE
count_all_peers INT = (SELECT count(*) from peers);
count_peers_task1 INT = (select count(*) from 
(select DISTINCT checks.peer from p2p join checks on checks.id = p2p."Check" where p2p."State" = 'Start' 
AND REGEXP_LIKE (checks.task,(select concat (task1,'[0-7]'))) group by checks.peer));
count_peers_task2 INT = (select count(*) from 
(select DISTINCT checks.peer from p2p join checks on checks.id = p2p."Check" where p2p."State" = 'Start' 
AND REGEXP_LIKE (checks.task,(select concat (task2,'[0-7]'))) group by checks.peer));
count_peers_both INT = (with tb1 as (
select DISTINCT checks.peer from p2p join checks on checks.id = p2p."Check" where p2p."State" = 'Start' 
AND REGEXP_LIKE (checks.task,(select concat (task1,'[0-7]'))) group by checks.peer),
tb2 as (
select DISTINCT checks.peer from p2p join checks on checks.id = p2p."Check" where p2p."State" = 'Start' 
AND REGEXP_LIKE (checks.task,(select concat (task2,'[0-7]')))  group by checks.peer)
select count(*) from tb1 join tb2 on tb1.peer = tb2.peer);
count_peers_not_started INT = count_all_peers - count_peers_task1 - count_peers_task2 + count_peers_both;
BEGIN
OPEN result FOR
select count_peers_task1 as StartedBlock1,count_peers_task2 as StartedBlock2,count_peers_both as StartedBothBlocks, count_peers_not_started as DidntStartAnyBlock;
END;
$$

drop Procedure fnc_part3_task9;

BEGIN;
    CALL fnc_part3_task9('C','DO');
    FETCH ALL FROM "pr_result_part3_task9";
END;


--- task10
--Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения


CREATE OR REPLACE PROCEDURE fnc_part3_task10(IN result REFCURSOR = 'pr_result_part3_task10')
LANGUAGE plpgsql    
AS $$
DECLARE
count_peers_started INT = (select count(*) from (
select DISTINCT peers.nickname from p2p join checks on checks.id = p2p."Check" FULL join verter on verter."Check" = checks.id
join peers on checks.peer = peers.nickname and date_part('day', checks.date) = date_part('day', peers.birthday)
and date_part('month', checks.date) = date_part('month', peers.birthday)
where p2p."State" = 'Start'));
count_failed_peers INT = (select count(*) from (
select DISTINCT peers.nickname from p2p join checks on checks.id = p2p."Check" FULL join verter on verter."Check" = checks.id
join peers on checks.peer = peers.nickname and date_part('day', checks.date) = date_part('day', peers.birthday)
and date_part('month', checks.date) = date_part('month', peers.birthday)
where (p2p."State" = 'Failure' OR verter."State" = 'Failure')));
count_success_peers INT = count_peers_started - count_failed_peers;
BEGIN
OPEN result FOR
Select ROUND(CAST (((count_success_peers::NUMERIC) * 100 / (count_peers_started::NUMERIC)) as NUMERIC),0) as SuccessfulChecks,
ROUND(CAST (((count_failed_peers::NUMERIC) * 100 / (count_peers_started::NUMERIC)) as NUMERIC),0) as UnsuccessfulChecks;
END;
$$

drop Procedure fnc_part3_task10;

BEGIN;
    CALL fnc_part3_task10();
    FETCH ALL FROM "pr_result_part3_task10";
END;



-- call insert_p2p('heshi','jeratta','C1_SimpleBashUtils','Start','12:30:00');
-- call insert_p2p('heshi','jeratta','C1_SimpleBashUtils','Success','13:00:00');

-- call insert_verter('heshi','C1_SimpleBashUtils','Start','13:31:00');
-- call insert_verter('heshi','C1_SimpleBashUtils','Failure','13:36:00');
-- select * from p2p join checks on checks.id = p2p."Check"  LIMIT 120;

-- select * from checks;

-- select * from verter join checks on checks.id = verter."Check";


-- delete from verter where verter."Check" = 55;

-- update checks
-- set date = '2023-04-20'
-- where id = 59;

-- update peers
-- set birthday = '2005-09-13'
-- where nickname = 'dimas'


-- select * from peers;

-- insert into xp VALUES((SELECT MAX(ID) FROM xp) + 1,58,250);

-- select * from TimeTracking;

-- select * from xp;
-- insert into TimeTracking VALUES ((select max(id) from TimeTracking) + 1,'heshi','2023-04-20','13:20:00',2);

-- task 11
--  Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
-- Параметры процедуры: названия заданий 1, 2 и 3. 
-- Формат вывода: список пиров


CREATE OR REPLACE PROCEDURE fnc_part3_task10(task1 VARCHAR(255), task2 VARCHAR(255), task3 VARCHAR(255),IN result REFCURSOR = 'pr_result_part3_task14')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
WITH tb1 as (
select checks.peer  from p2p join verter on verter."Check" = p2p."Check" join checks on checks.id = p2p."Check"
where verter."State" = 'Success' AND p2p."State" = 'Success' and checks.task = task1),
tb2 as (
(select  checks.peer from p2p join verter on verter."Check" = p2p."Check" join checks on checks.id = p2p."Check"
where verter."State" = 'Success' AND p2p."State" = 'Success' and checks.task = task2)),
tb3 as (
    (select  checks.peer from p2p join verter on verter."Check" = p2p."Check" join checks on checks.id = p2p."Check"
where verter."State" = 'Success' AND p2p."State" = 'Success' and checks.task = task3))
select tb1.peer from tb1  inner join tb2 on tb2.peer = tb1.peer LEFT join tb3 on tb1.peer = tb3.peer WHERE tb3.peer is NULL;
END;
$$ 


BEGIN;
    CALL fnc_part3_task10('C1_SimpleBashUtils','C2_s21_string+','C7_3DViewer_v1.0');
    FETCH ALL FROM "pr_result_part3_task14";
END;



--- task 12
--  Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
-- То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей. 
-- Формат вывода: название задачи, количество предшествующих



CREATE OR REPLACE PROCEDURE fnc_part3_task11(IN result REFCURSOR = 'pr_result_part3_task11')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
WITH RECURSIVE tt AS (
   SELECT task.title, 0 as "count"  from task where task.parenttask is null
  UNION 
    SELECT task.title, "count" + 1  from task join tt on task.parenttask = tt.title
)
SELECT * FROM tt;
END;
$$ 


BEGIN;
    CALL fnc_part3_task11();
    FETCH ALL FROM "pr_result_part3_task11";
END;

--task13
--- Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок N. 
-- Временем проверки считать время начала P2P этапа. 
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных. 
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального. 
-- Формат вывода: список дней



select * from p2p join checks on checks.id = p2p."Check" join xp on xp."Check" = p2p."Check" join verter on verter."Check" = p2p."Check"


select * from xp


-- task 14 Определить пира с наибольшим количеством XP
-- Формат вывода: ник пира, количество XP

CREATE OR REPLACE PROCEDURE fnc_part3_task14(IN result REFCURSOR = 'pr_result_part3_task14')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
select peer as "Peer",sum(xpamount) as "XP" from xp join checks on checks.id = xp."Check" GROUP BY checks.peer
ORDER BY sum(xp.xpamount) DESC LIMIT 1;
END;
$$ 

BEGIN;
    CALL fnc_part3_task14();
    FETCH ALL FROM "pr_result_part3_task14";
END;

drop Procedure fnc_part3_task14;


-- task 15 
-- 15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
-- Параметры процедуры: время, количество раз N. 
-- Формат вывода: список пиров

-- Считаю только самый вход в кампус в течение дня


CREATE OR REPLACE PROCEDURE fnc_part3_task15(date_cur TIME WITHOUT TIME ZONE,N INT, IN result REFCURSOR = 'pr_result_part3_task15')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
with tb1 as (
select peer,"Date" from timetracking  where "state" = 1  and "Time" < date_cur GROUP BY peer,"Date")
select peer from tb1 GROUP BY peer having count(*) >= N;
END;
$$ 

BEGIN;
    CALL fnc_part3_task15('20:00:00',3);
    FETCH ALL FROM "pr_result_part3_task15";
END;

drop Procedure fnc_part3_task15;


-- task 16 

-- 16) Определить пиров, выходивших за последние N дней из кампуса больше M раз
-- Параметры процедуры: количество дней N, количество раз M. 
-- Формат вывода: список пиров

-- Считаю абсолютно все выходы из кампуса, даже в течение дня за исключением последнего выхода за день.

CREATE OR REPLACE PROCEDURE fnc_part3_task16(N INT ,M INT, IN result REFCURSOR = 'pr_result_part3_task16')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
with tb1 as (
select peer,"Date",count("Date") - 1 as count_ from timetracking where state = 2 AND "Date" > (select date(NOW()) - N) 
GROUP BY peer,"Date")
Select peer from tb1 GROUP BY peer having sum(count_) > M;
END;
$$ 

drop Procedure fnc_part3_task16;

BEGIN;
    CALL fnc_part3_task16(10,0);
    FETCH ALL FROM "pr_result_part3_task16";
END;


-- select * from timetracking where "Date" = '2023-10-11'

--  insert into TimeTracking VALUES ((select max(id) from TimeTracking) + 1,'antoinco','2023-10-11','14:53:00',2);


-- 17) Определить для каждого месяца процент ранних входов
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус за всё время (будем называть это общим числом входов). 
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус раньше 12:00 за всё время (будем называть это числом ранних входов). 
-- Для каждого месяца посчитать процент ранних входов в кампус относительно общего числа входов. 
-- Формат вывода: месяц, процент ранних входов


CREATE OR REPLACE PROCEDURE fnc_part3_task17( IN result REFCURSOR = 'pr_result_part3_task17')
LANGUAGE plpgsql    
AS $$
BEGIN
OPEN result FOR
with tb1 as (
select peer,TO_CHAR("Date", 'Month') AS "Month",count(*) from timetracking join peers on peers.nickname = timetracking.peer where date_part('month', timetracking."Date") = date_part('month',peers.birthday) 
and "state" = 1 group by peer,"Date"),
tb1_res as (
select tb1."Month",sum(count) as count_ from tb1 GROUP BY tb1."Month"),
tb2 as (
select  peer,TO_CHAR("Date", 'Month') AS "Month",count(*) from timetracking join peers on peers.nickname = timetracking.peer where date_part('month', timetracking."Date") = date_part('month',peers.birthday) 
and "state" = 1 and "Time" < '12:00:00' group by peer,"Date"),
tb2_res as (
select tb2."Month",sum(count) as count_early from tb2 GROUP BY tb2."Month")
select tb1_res."Month",ROUND(CAST (((tb2_res.count_early::NUMERIC) * 100 / (tb1_res.count_::NUMERIC)) as NUMERIC),0) from tb1_res join tb2_res on tb1_res."Month" = tb2_res."Month";
END;
$$ 

BEGIN;
    CALL fnc_part3_task17();
    FETCH ALL FROM "pr_result_part3_task17";
END;

drop Procedure fnc_part3_task16;



-- insert into TimeTracking VALUES ((select max(id) from TimeTracking) + 1,'masha','2023-07-14','18:40:00',2);

-- select * from peers;

-- select * from timetracking 

-- select *  from timetracking join peers on peers.nickname = timetracking.peer where date_part('month', timetracking."Date") = date_part('month',peers.birthday) 
-- and "state" = 1 

-- with tb1 as (
-- select peer,TO_CHAR("Date", 'Month') AS "Month",count(*) from timetracking join peers on peers.nickname = timetracking.peer where date_part('month', timetracking."Date") = date_part('month',peers.birthday) 
-- and "state" = 1 group by peer,"Date") 
-- select tb1."Month",sum(count) from tb1 GROUP BY tb1."Month" 
