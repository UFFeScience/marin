-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS main_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create the table
CREATE TABLE main_keyspace.logs_wordcount (
   word text PRIMARY KEY, 
   count int   
);