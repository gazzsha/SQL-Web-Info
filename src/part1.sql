-- Active: 1696963787123@@127.0.0.1@5432@info21

DROP table IF EXISTS Friends;
DROP table IF EXISTS peers;
drop table if exists task;

drop table if exists checks;

DROP table IF EXISTS Recommendations;
drop table if exists TransferredPoints;

drop table if exists TimeTracking;

drop table if exists XP; 

drop table if exists Verter;

drop table if exists P2P;

CREATE TABLE IF NOT EXISTS Peers (
    Nickname VARCHAR(255) NOT NULL PRIMARY KEY,
    Birthday DATE NOT NULL
);

CREATE TYPE  status as ENUM ('Start','Success','Failure');
CREATE Table if NOT exists TASK ( 
    Title VARCHAR(255) PRIMARY KEY,
    ParentTask VARCHAR(255) DEFAULT NULL,
    MaxXP NUMERIC NOT NULL,
    CONSTRAINT fk_task_title FOREIGN KEY (ParentTask) REFERENCES Task(Title),
    CONSTRAINT fk_task_check_title CHECK (Title != ParentTask)
);


create table if not exists Checks ( 
    ID SERIAL PRIMARY KEY,
    Peer VARCHAR(255) NOT NULL,
    Task VARCHAR(255) NOT NULL,
    Date DATE NOT NULL,
    CONSTRAINT fk_checks_peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    CONSTRAINT fk_checks_task FOREIGN KEY (Task) REFERENCES Task(title)
);
CREATE TABLE IF NOT EXISTS Friends (
    "ID" SERIAL PRIMARY KEY,
    Peer1 VARCHAR(255) NOT NULL,
    Peer2 VARCHAR(255) NOT NULL,
    CONSTRAINT fk_friend_1 FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
    CONSTRAINT fk_friend_2 FOREIGN KEY (Peer2) REFERENCES Peers (Nickname),
    CONSTRAINT unique_check_friend CHECK (Peer1 != Peer2)
);

CREATE TABLE IF NOT EXISTS Recommendations (
    ID SERIAL PRIMARY KEY,
    Peer VARCHAR(255) NOT NULL,
    RecommendedPeer VARCHAR(255) NOT NULL,
    CONSTRAINT fk_recommend_peer_1 FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_recommend_peer_2 FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname),
    CONSTRAINT unique_check_reommend_peers CHECK (Peer != RecommendedPeer)
);

create table if not exists TransferredPoints (
    ID SERIAL PRIMARY KEY,
    CheckingPeer VARCHAR(255) NOT NULL,
    CheckedPeer VARCHAR(255) NOT NULL,
    PointAmount INT,
    CONSTRAINT fk_transfer_peer_1 FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
    CONSTRAINT fk_transfer_peer_2 FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname),
    CONSTRAINT unique_check_transfer_peers CHECK (CheckingPeer != CheckedPeer)
);

create table if not exists TimeTracking (
    ID SERIAL PRIMARY KEY,
    Peer VARCHAR(255) NOT NULL,
    "Date" Date not NULL,
    "Time" TIME WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIME,
    State int not null,
    CONSTRAINT range_check_state CHECK (State in (1,2)),
    CONSTRAINT fk_time_tracking_peer FOREIGN KEY (Peer) REFERENCES Peers (Nickname)
);

CREATE Table if not exists XP (
    ID SERIAL PRIMARY KEY,
    "Check" INT NOT NULL,
    XPAmount NUMERIC NOT NULL,
    CONSTRAINT fk_xp_check FOREIGN KEY ("Check") REFERENCES Checks(ID)
)

CREATE Table if not exists Verter (
    ID SERIAL PRIMARY KEY,
    "Check" INT NOT NULL,
    "State" Status NOT NULL,
    "Time" TIME NOT NULL,
    CONSTRAINT fk_verter_check FOREIGN KEY ("Check") REFERENCES Checks(ID)
)


create table if not exists P2P ( 
    ID SERIAL PRIMARY KEY,
    "Check" INT NOT NULL,
    CheckingPeer VARCHAR(255) NOT NULL,
    "State" Status NOT NULL,
    "Time" TIME NOT NULL,
    CONSTRAINT fk_p2p_check FOREIGN KEY ("Check") REFERENCES Checks(ID),
    CONSTRAINT fk_p2p_checkingpeer FOREIGN KEY (CheckingPeer) REFERENCES Peers(nickname) 
);


CREATE OR REPLACE PROCEDURE import_table(
    name_table VARCHAR(64),
    path_table varchar(255),
    delim varchar(1)
)
LANGUAGE plpgsql    
AS $$
BEGIN
    EXECUTE format(
    'COPY %1$s FROM %2$L WITH DELIMITER ''%3$s'' CSV HEADER',
     name_table,
    path_table,
    delim
);
END;
$$;



CREATE OR REPLACE PROCEDURE export_table(
    name_table VARCHAR(64),
    path_table varchar(255),
    delim varchar(1)
)
LANGUAGE plpgsql    
AS $$
BEGIN
    EXECUTE format(
    'COPY %1$s TO %2$L WITH DELIMITER ''%3$s'' CSV HEADER',
     name_table,
    path_table,
    delim
);
END;
$$;


--drop Procedure import_table;

call import_table('Peers','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_peers.csv',',');
call import_table('Recommendations','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_recommended.csv',',');
call import_table('Friends','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_friends.csv',',');

call import_table('timetracking','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_timetrack.csv',',');

call import_table('Task','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_task.csv',',');

call import_table('Checks','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_checks.csv',',');

call import_table('P2P','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_p2p.csv',',');


call import_table('TransferredPoints','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_point_tras.csv',',');



call import_table('Verter','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_vecter.csv',',');

call import_table('XP','D:/school_21_projects/SQL2_Info21_v1.0-1/src/import/data_xp.csv',',');

call export_table('Peers','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_peers.csv',',');
call export_table('Checks','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_checks.csv',',');

call export_table('p2p','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_p2p.csv',',');

call export_table('Recommendations','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_recommended.csv',',');
call export_table('Task','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/task.csv',',');
call export_table('TimeTracking','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_timetrack.csv',',');
call export_table('Verter','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_verter.csv',',');

call export_table('xp','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_xp.csv',',');

call export_table('friends','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_friends.csv',',');

call export_table('TransferredPoints','D:/school_21_projects/SQL2_Info21_v1.0-1/src/export/data_point_tras.csv',',');











-- select * from peers;
-- select * from Recommendations;

-- select * from friends;

-- drop table timetracking;
-- insert into timetracking VALUES (1,'antoinco','2020-10-10','10:23:32',1);

-- select * from TimeTracking;

-- select * from checks;

-- select * from p2p;

-- select * from Verter;

-- select * from xp;

-- TRUNCATE TABLE peers CASCADE;
-- TRUNCATE TABLE Recommendations CASCADE;

--TRUNCATE TABLE TransferredPoints CASCADE;
-- TRUNCATE TABLE p2p CASCADE;


-- select * from peers;


-- select * from timetracking where peer = 'yura'


-- show data_directory;