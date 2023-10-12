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


call insert_p2p('yura','kristlee','C1_SimpleBashUtils','Start','11:50:00');
call insert_p2p('yura','kristlee','C1_SimpleBashUtils','Success','12:50:00');

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
