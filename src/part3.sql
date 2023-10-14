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

drop Procedure fnc_part3_task9;

BEGIN;
    CALL fnc_part3_task8();
    FETCH ALL FROM "pr_result_part3_task8";
END;


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

