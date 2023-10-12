Create database if not exists LibrairieEv3Eq1;

 

Use LibrairieEv3Eq1;
Create table livresev3eq1(nolivre int, tritrelivre string, auteurs array<string>, livreOriginal struct<langue:string, titre:string>)
row format delimited
fields terminated by ','
collection items terminated by '#'
map keys terminated by ':';
load data local inpath '/tmp/Librairie/livresPartie3AS.txt' overwrite into table livresev3eq1;