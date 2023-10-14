--Проверяет, что количество проект доступен для сдачи, что нету запущенного Start

CREATE OR REPLACE PROCEDURE insert_p2p(
    peer_cur VARCHAR(64),
    peer_checked varchar(64),
    task_cur varchar(64),
    state status,
    time_p2p TIME WITHOUT TIME ZONE
)
LANGUAGE plpgsql    
AS $$
Declare 
p2p_access_to_check INT = 0; 

p2p_check_no_repeet INT = (select count(checks.id) from p2p join checks on checks.id = p2p."Check"
where checkingpeer =  peer_checked and checks.peer = peer_cur and checks.task = task_cur);

checks_new_id INT = 0;
BEGIN
    IF (task_cur = 'C1_SimpleBashUtils') THEN p2p_access_to_check = 1;
    ELSE 
    p2p_access_to_check = (SELECT count(*) FROM P2P JOIN checks ON p2p."Check" = checks.id
where checks.peer = peer_cur AND checks.task = (select parenttask from task where task.title = task_cur)
and p2p."State" = 'Success' 
GROUP BY p2p."State");
    END IF;

    IF p2p_access_to_check > 0  THEN
        IF state = 'Start' THEN
            IF (p2p_check_no_repeet % 2 = 0) THEN
                INSERT INTO Checks (ID,Peer, Task,Date)
                VALUES ((SELECT MAX(ID) FROM checks) + 1,peer_cur, task_cur,CURRENT_DATE) RETURNING ID INTO checks_new_id;
               INSERT INTO P2P (ID,"Check",CheckingPeer, "State", "Time")
                VALUES ((SELECT MAX(ID) FROM P2P) + 1,checks_new_id, peer_checked, state, time_p2p);
            ELSE RAISE NOTICE 'This "%" task "%" peer and "%" checking peer in the "Start" status is already exist!',
                          task_cur, peer_cur, peer_checked;
            END IF;
        ELSE 
            IF (p2p_check_no_repeet % 2 = 1) THEN 
            IF (time_p2p > (select "Time" From p2p where p2p."Check" = (select checks.id from p2p join checks on checks.id = p2p."Check"
                where checkingpeer =  peer_checked and checks.peer = peer_cur and checks.task = task_cur
                Group BY checks.id
                HAVING count (*) % 2 != 0))) THEN
                INSERT INTO P2P (ID,"Check", CheckingPeer, "State", "Time")
                VALUES ((SELECT MAX(ID) FROM P2P) + 1,(select checks.id from p2p join checks on checks.id = p2p."Check"
                where checkingpeer =  peer_checked and checks.peer = peer_cur and checks.task = task_cur
                Group BY checks.id
                HAVING count (*) % 2 != 0), peer_checked, state,time_p2p);
                ELSE
                RAISE NOTICE 'This "%" task "%" peer and "%" checking peer was not started!', task_cur, peer_cur, peer_checked;
                END IF;
            END IF;
        END IF;
    ELSE RAISE NOTICE 'This "%" task not available  for "%" peer', task_cur, peer_cur;
    END IF;
END;
$$;



select * from p2p;
call insert_p2p('antoinco','gradyzan','C1_SimpleBashUtils','Failure','10:50:00');

call insert_p2p('gradyzan','heshi','DO6_CICD','Start','19:40:00');
call insert_p2p('gradyzan','heshi','DO6_CICD','Success','20:30:00');



call insert_p2p('yura','kristlee','C1_SimpleBashUtils','Start','11:50:00');
call insert_p2p('yura','kristlee','C1_SimpleBashUtils','Success','12:50:00');


call insert_p2p('antoinco','kristlee','CPP1_s21_matrix+','Success','12:50:00');

call insert_p2p('misha','gradyzan','C1_SimpleBashUtils','Failure','17:50:00');

select "Time" From p2p where p2p."Check" = (select checks.id from p2p join checks on checks.id = p2p."Check"
                where checkingpeer =  'gradyzan' and checks.peer = 'misha' and checks.task = 'C1_SimpleBashUtils'
                Group BY checks.id
                HAVING count (*) % 2 != 0)

INSERT into p2p VALUES (49,1,'gradyzan','Start','1:55:11');



(select checks.id from p2p join checks on checks.id = p2p."Check"
            where checkingpeer =  'gradyzan' and checks.peer = 'antoinco' and checks.task = 'C1_SimpleBashUtils'
            Group BY checks.id
            HAVING count (*) % 2 != 0)

select * from checks;

select * from p2p JOIN checks on checks.id = p2p."Check";


drop Procedure insert_p2p;


(SELECT count(*) FROM P2P JOIN checks ON p2p."Check" = checks.id
where checks.peer = 'antoinco' AND ( checks.task = (select parenttask from task where task.title = 'C1_SimpleBashUtils') or checks.task = 'C1_SimpleBashUtils')
and p2p."State" = 'Success' 
GROUP BY p2p."State");

(select count(checks.id) from p2p join checks on checks.id = p2p."Check"
where checkingpeer =  'gradyzan' and checks.peer = 'antoinco' and checks.task = 'C1_SimpleBashUtils'
GROUP BY checks.ID);


select  * from CURRENT_DATE;



------- PART 2


-- Написать процедуру добавления проверки Verter'ом
-- Параметры: ник проверяемого, название задания, статус проверки Verter'ом, время. 



CREATE OR REPLACE PROCEDURE insert_verter(
    peer_checked varchar(64),
    task_cur varchar(64),
    state status,
    time_verter TIME WITHOUT TIME ZONE
)
LANGUAGE plpgsql    
AS $$
Declare 
-- разрешение на проверку, проверяет что проект success
verter_access_to_check INT = (select count(*) from p2p join checks on checks.id = p2p."Check" where checks.task = task_cur
 and checks.peer = peer_checked AND p2p."State" = 'Success'
GROUP BY checks.peer);

-- находит id сhecks, по времени самое ближайшее, которое указано в входных параметрах
id_checks_cur INT = (Select  tb."Check" FROM (select p2p."Check",p2p."Time" from p2p join checks 
on checks.id = p2p."Check" where checks.task = task_cur
and checks.peer = peer_checked AND p2p."State" = 'Success') as tb
WHERE (time_verter > tb."Time")
ORDER BY tb."Time" DESC
LIMIT 1);
-- проверяем что нет репитов
verter_check_no_repeet INT = (select count(*) from Verter where Verter."Check" = id_checks_cur GROUP BY Verter."Check");

BEGIN
    IF state = 'Start' THEN
        IF verter_access_to_check > 0 THEN
            IF ( (verter_check_no_repeet IS NULL)) THEN 
            INSERT INTO Verter (ID,"Check","State","Time") VALUES ((SELECT MAX(ID) FROM Verter) + 1,
            id_checks_cur,state,time_verter);
            ELSE 
            RAISE NOTICE 'This "%" task "%" peer proccesing by Vecter "Start" status is already exist! OR Inccorect time!',
                            task_cur, peer_checked;
            END IF;
        ELSE
        RAISE NOTICE 'This "%" task by "%" not available for checking by Vecter', task_cur, peer_checked;
        END IF;
    ELSE 
    IF  verter_access_to_check > 0 THEN 
        IF  (verter_check_no_repeet = 1) THEN
            IF (time_verter > (SELECT "Time" FROM verter where "Check" = id_checks_cur AND "State" = 'Start')) THEN
            INSERT INTO Verter (ID,"Check","State","Time") VALUES ((SELECT MAX(ID) FROM Verter) + 1,
            id_checks_cur,state,time_verter);
            ELSE
             RAISE NOTICE 'This "%" task "%" peer checking by Verter Inccorect time', task_cur, peer_checked;
            END IF;
            ELSE
            RAISE NOTICE 'This "%" task "%" peer checking by Verter was not started!
            OR Inccorect time OR task checking by verter already!', task_cur, peer_checked;
            END IF;
    ELSE 
    RAISE NOTICE 'This "%" task by "%" not available for checking by Vecter', task_cur, peer_checked;
    END IF;
    end IF;

END;
$$;


call insert_verter('antoinco','C7_3DViewer_v1.0','Failure','20:00:00');
call insert_verter('yura','C4_s21_decimal','Start','17:00:00');


call insert_verter('antoinco','CPP1_s21_matrix+','Start','13:00:00');

call insert_verter('gradyzan','DO2_LinuxNetwork','Start','11:05:00');

call insert_verter('gradyzan','DO2_LinuxNetwork','Success','13:21:00');
call insert_verter('antoinco','CPP1_s21_matrix+','Success','13:26:00');



select * from verter;


drop Procedure insert_verter;

delete from verter where id = 38;



-- Первое время перед проверкой
Select  tb."Check",tb."Time" FROM (select p2p."Check",p2p."Time" from p2p join checks 
on checks.id = p2p."Check" where checks.task = 'C1_SimpleBashUtils' 
and checks.peer = 'antoinco' AND p2p."State" = 'Success') as tb
WHERE ('17:21:20' > tb."Time")
ORDER BY tb."Time" DESC
LIMIT 1


select count(*) from Verter where "Check" = 17 GROUP BY "Check"


select * from p2p join checks on p2p."Check" = checks.id


--- PART 3

CREATE OR REPLACE FUNCTION fnc_trg_person_update_point_trans()
RETURNS trigger AS
$$
DECLARE
check_in INT = (SELECT PointAmount FROM TransferredPoints WHERE TransferredPoints.CheckingPeer = NEW.CheckingPeer AND CheckedPeer = (SELECT Peer FROM checks WHERE checks.id = NEW."Check"));
BEGIN
IF (check_in IS NULL) THEN
    INSERT INTO TransferredPoints (ID,CheckingPeer, CheckedPeer, PointAmount)
    VALUES ((SELECT MAX(ID) FROM TransferredPoints) + 1,NEW.CheckingPeer, (SELECT Peer FROM checks WHERE checks.id = New."Check"), 1);
    ELSE 
    UPDATE TransferredPoints SET PointAmount = PointAmount + 1 
    WHERE TransferredPoints.CheckingPeer = NEW.CheckingPeer 
    AND CheckedPeer = (SELECT Peer FROM checks WHERE checks.id = New."Check");
    END IF;
RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';
CREATE TRIGGER trg_person_update_audit
  AFTER INSERT
  ON P2P
  FOR EACH ROW
  WHEN (NEW."State" = 'Start')
  EXECUTE PROCEDURE fnc_trg_person_update_point_trans();

  select * from transferredpoints;

select * from p2p join checks on p2p."Check" = checks.id

drop Function fnc_trg_person_update_point_trans;

DROP TRIGGER trg_person_update_audit ON p2p;



call insert_p2p('jeratta','gradyzan','C1_SimpleBashUtils','Start','12:50:00');



--- PART 4


CREATE OR REPLACE FUNCTION fnc_trg_update_xp_table()
RETURNS trigger AS
$$
DECLARE
xp_access_checks_id int = (SELECT p2p."Check" from p2p where p2p."Check" = NEW."Check" AND p2p."State" = 'Success');
max_xp int = (select task.maxxp from task join checks on checks.task = task.title where (checks.id = NEW."Check"));
xp_access_verter_id int = (SELECT verter.id from verter where verter."Check" = NEW."Check" AND verter."State" = 'Failure');
BEGIN
    IF max_xp < NEW.XPAmount OR NEW.XPAmount < 0 THEN
    RAISE  NOTICE 'XPAmount more than MaxXP!';
    RETURN NULL;
    ELSEIF xp_access_checks_id IS NULL THEN
    RAISE  NOTICE 'P2P Is Failure!';
    RETURN NULL;
    ELSEIF xp_access_verter_id is NOT NULL THEN
    RAISE  NOTICE 'Vecter Is Failure!';
    RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END;
$$
LANGUAGE 'plpgsql';
CREATE TRIGGER trg_table_before_update
  BEFORE INSERT
  ON XP
  FOR EACH ROW
  EXECUTE PROCEDURE fnc_trg_update_xp_table();

drop FUNCTION fnc_trg_update_xp_table;
drop TRIGGER trg_table_before_update on xp;
select * from verter;
select * from p2p JOIN checks on checks.id = p2p."Check";

select * from xp;
INSERT INTO XP VALUES (16,30,100);
