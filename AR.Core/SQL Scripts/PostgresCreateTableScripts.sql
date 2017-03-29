--Create global list of all flights
DROP TABLE IF EXISTS public.Logs;



-- Level (Warning, Verbose, Error)
-- Module (code)
-- Message (meaningful message)
-- Version code build version
-- 
CREATE TABLE public.Logs(
    id SERIAL primary key,
    level varchar(20) NOT NULL,
    Module varchar(100) NOT NULL,
    Message varchar(500) NOT NULL,
    Version varchar(10) NOT NULL,
    date_added timestamp default NOW()
);
