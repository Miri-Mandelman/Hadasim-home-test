DROP TABLE Base;
DROP TABLE FamilyTree;

--Exercise1 - Building the Family-Tree:

--Creatin the base-table to work on:
CREATE TABLE IF NOT EXISTS Base(
	Person_Id varchar(9) NOT NULL PRIMARY KEY,
	Person_Name varchar(255),
	Family_Name varchar(255),
	Gender varchar(6),
	Father_Id varchar(9),
	Mother_Id varchar(9),
	Spouse_Id varchar(9)
);

--Inserting sample values:
INSERT INTO Base
VALUES
(111, 'son', 'a','Male' ,112, 113, 222),
(112, 'father', 'a', 'Male', 002, 003, 113),
(113, 'moter', 'a', 'Female', 332, 333, NULL),
(114, 'do', 'a', 'Female', 112, 113, 444);

--Creating the Family-Tree table:
--note: Allthough it is costomary to use only Eanglish, I used the Hebrew terms for the connection type,
--		this is for maintaining the diffrent between 'בו זוג' & 'בת זוג' which in Eanglish both are 'spouse'.
/*CREATE TYPE  connection_type_enum AS ENUM (
    'אב',
    'אם',
    'אח',
    'אחות',
    'בן',
    'בת',
    'בן זוג',
    'בת זוג'
);*/

CREATE TABLE IF NOT EXISTS FamilyTree(
	Person_Id varchar(9) NOT NULL,
	Relative_Id varchar(9) NOT NULL,
	Connection_Type connection_type_enum,
	PRIMARY KEY(Person_id, Relative_id)
);

--Inserting the 'אב' connection type:
INSERT INTO FamilyTree
SELECT Person_Id, Father_Id, 'אב'
FROM Base
WHERE Father_Id IS NOT NULL;

--Inserting the 'אם' connection type:
INSERT INTO FamilyTree
SELECT Person_Id, Mother_Id, 'אם'
FROM Base
WHERE Mother_Id IS NOT NULL;

--Inserting the 'אח' connection type:
INSERT INTO FamilyTree 
SELECT b1.Person_Id, b2.Person_Id, 'אח'
FROM Base b1 JOIN Base b2 ON (b1.Person_Id!= b2.Person_Id)
WHERE((b1.Father_Id IS NOT NULL AND b1.Father_Id = b2.Father_Id)
	  OR (b1.Mother_Id IS NOT NULL AND b1.Mother_Id = b2.Mother_Id))
	AND(b2.Gender ='Male');

--Inserting the 'אחות' connection type:
INSERT INTO FamilyTree 
SELECT b1.Person_Id, b2.Person_Id, 'אחות'
FROM Base b1 JOIN Base b2 ON (b1.Person_Id!= b2.Person_Id)
WHERE ((b1.Father_Id IS NOT NULL AND b1.Father_Id = b2.Father_Id)
	  OR (b1.Mother_Id IS NOT NULL AND b1.Mother_Id = b2.Mother_Id))
	AND(b2.Gender ='Female');

--Inserting the 'בן' connection type:
INSERT INTO FamilyTree 
SELECT b1.Person_Id, b2.Person_Id, 'בן'
FROM Base b1 JOIN Base b2 ON ((b1.Person_Id = b2.Father_Id)
	 OR (b1.Person_Id = b2.Mother_Id))
WHERE(b2.Gender ='Male');

--Inserting the 'בת' connection type:
INSERT INTO FamilyTree 
SELECT b1.Person_Id, b2.Person_Id, 'בת'
FROM Base b1 JOIN Base b2 ON ((b1.Person_Id = b2.Father_Id)
	 OR (b1.Person_Id = b2.Mother_Id))
WHERE(b2.Gender ='Female');

--Inserting the 'בן זוג' connection type:
--note:Based on the assumption that a couple can be onle Male & Female (Not including same-gender couples).
INSERT INTO FamilyTree
SELECT Person_Id, Spouse_Id,'בן זוג'
FROM Base
WHERE (Gender='Female') AND (Spouse_Id IS NOT NULL);

--Inserting the 'בת זוג' connection type:
INSERT INTO FamilyTree
SELECT Person_Id, Spouse_Id,'בת זוג'
FROM Base
WHERE (Spouse_Id IS NOT NULL) AND (Gender='Male');

--Exercise2 - Completing spouses:
--Completion of 'בן זוג'
INSERT INTO FamilyTree
SELECT b1.Person_id,(SELECT b2.Person_Id From Base b2
						WHERE b1.Person_Id=b2.Spouse_Id),'בן זוג'
FROM Base b1
WHERE (Spouse_Id IS NULL) AND (Gender='Female');

--Completion of 'בת זוג'
INSERT INTO FamilyTree
SELECT b1.Person_id,(SELECT b2.Person_Id From Base b2
						WHERE b1.Person_Id=b2.Spouse_Id),'בת זוג'
FROM Base b1
WHERE (Spouse_Id IS NULL) AND (Gender='Male');


Select* from FamilyTree;
