DROP TABLE IF EXISTS students;

CREATE TABLE students (
	student_ID varchar (30) NOT NULL,
	first_Name varchar (30) NOT NULL,
	middle_Name varchar (30),
	last_Name varchar (30) NOT NULL,
	email varchar (100) NOT NULL,
	course_Name varchar (30) NOT NULL,
	course_Description varchar (255) NOT NULL,
	marks real NOT NULL
);

-- 1
insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266200','Ahmed','Al','Abbad','alabba200cs.uregina.ca','CS100','Introduction to Computers',90.5);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266200','Ahmed','Al','Abbad','alabba200cs.uregina.ca','CS110','Programming and Problem Solving',85);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266200','Ahmed','Al','Abbad','alabba200@cs.uregina.ca','CS210','Data Structures and Abstractions',90);

-- 2

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266202','Qaswar','','Rai','ali202@cs.uregina.ca','CS100','Introduction to Computers',72.5);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266202','Qaswar','','Rai','ali202@cs.uregina.ca','CS110','Programming and Problem Solving',59.2);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266202','Qaswar','','Rai','ali202@cs.uregina.ca','CS215','Web Oriented Programming',82.6);

-- 3

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266203','Nicholas','','Phongsavath','phosan203@cs.uregina.ca','CS100','Introduction to Computers',100);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266203','Nicholas','','Phongsavath','phosan203@cs.uregina.ca','CS215','Web Oriented Programming',100);

-- 4

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266204','Nikki','','Pawlowski','pawlow204@cs.uregina.ca','CS100','Introduction to Computers',65);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266204','Nikki','','Pawlowski','pawlow204@cs.uregina.ca','CS210','Data Structures and Abstractions',92.5);

-- 5

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266205','Nathan','','Ellis','elli205@cs.uregina.ca','CS110','Programming and Problem Solving',92.5);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266205','Nathan','','Ellis','elli205@cs.uregina.ca','CS210','Data Structures and Abstractions',70.2);

-- 6

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266206','John','','Erick','eric20j206@cs.uregina.ca','CS100','Introduction to Computers',86);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266206','John','','Erick','eric20j206@cs.uregina.ca','CS110','Programming and Problem Solving',87);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266206','John','','Erick','eric20j206@cs.uregina.ca','CS210','Data Structures and Abstractions',81);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266206','John','','Erick','eric20j206@cs.uregina.ca','CS215','Web Oriented Programming',83);

-- 7

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266207','Jody-Lee','Van','DerVelden','vanervj207@cs.uregina.ca','CS100','Introduction to Computers',59);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266207','Jody-Lee','Van','DerVelden','vanervj207@cs.uregina.ca','CS110','Programming and Problem Solving',50.2);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266207','Jody-Lee','Van','DerVelden','vanervj207@cs.uregina.ca','CS210','Data Structures and Abstractions',67);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266207','Jody-Lee','Van','DerVelden','vanervj207@cs.uregina.ca','CS215','Web Oriented Programming',85.6);

-- 8

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266208','Sabrina ','','Tram','tra200s208@cs.uregina.ca','CS100','Introduction to Computers',70);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266208','Sabrina ','','Tram','tra200s208@cs.uregina.ca','CS110','Programming and Problem Solving',80);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266208','Sabrina ','','Tram','tra200s208@cs.uregina.ca','CS210','Data Structures and Abstractions',88.5);

-- 9

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266209','Ke','','Tao','taoke209@cs.uregina.ca','CS110','Programming and Problem Solving',66.5);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266209','Ke','','Tao','taoke209@cs.uregina.ca','CS210','Data Structures and Abstractions',77.5);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266209','Ke','','Tao','taoke209@cs.uregina.ca','CS215','Data Structures and Abstractions',86.5);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266209','Ke','','Tao','taoke209@cs.uregina.ca','CS000','00000000000000000000000',0);

-- 10

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266210','Yupeng','Xu','','x210@cs.uregina.ca','CS100','Introduction to Computers',89);

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266210','Yupeng','Xu','','x210@cs.uregina.ca','CS210','Data Structures and Abstractions',92);

-- 11

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266211','Chidinma','','Ukabam','ukaba211@cs.uregina.ca','CS100','Introduction to Computers',99);

-- 12

insert into students (student_ID, first_Name, middle_Name, last_Name, email, course_Name, course_Description, marks) values ('200266212','Mandeep','','Ghotra','gho212@cs.uregina.ca','CS215','Web Oriented Programming',98);
