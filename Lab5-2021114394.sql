--DATABASE LAB#5
-- Lab#5-1
-- DEPARTMENT 테이블 생성
CREATE TABLE DEPARTMENT (
    DNAME VARCHAR(255) NOT NULL,
    DNUMBER INT PRIMARY KEY,
    MGR_SSN CHAR(9),
    MGR_START_DATE DATE,
    FOREIGN KEY (MGR_SSN) REFERENCES EMPLOYEE (SSN) ON DELETE SET NULL
);

-- EMPLOYEE 테이블 생성
CREATE TABLE EMPLOYEE (
    FNAME VARCHAR(255) NOT NULL,
    MINIT CHAR(1),
    LNAME VARCHAR(255) NOT NULL,
    SSN CHAR(9) PRIMARY KEY,
    BDATE DATE,
    ADDRESS VARCHAR(255),
    SEX CHAR(1),
    SALARY DECIMAL(10, 2),
    SUPER_SSN CHAR(9),
    DNO INT,
    FOREIGN KEY (DNO) REFERENCES DEPARTMENT (DNUMBER) ON DELETE SET NULL
);

-- PROJECT 테이블 생성
CREATE TABLE PROJECT (
    PNAME VARCHAR(255) NOT NULL,
    PNUMBER INT PRIMARY KEY,
    PLOCATION VARCHAR(255),
    DNUM INT,
    FOREIGN KEY (DNUM) REFERENCES DEPARTMENT (DNUMBER) ON DELETE SET NULL
);

-- DEPT_LOCATIONS 테이블 생성
CREATE TABLE DEPT_LOCATIONS (
    DNUMBER INT,
    DLOCATION VARCHAR(255),
    PRIMARY KEY (DNUMBER, DLOCATION),
    FOREIGN KEY (DNUMBER) REFERENCES DEPARTMENT (DNUMBER) ON DELETE CASCADE
);

-- WORKS_ON 테이블 생성
CREATE TABLE WORKS_ON (
    ESSN CHAR(9),
    PROJ_NUM INT,
    HOURS DECIMAL(5, 2) NOT NULL,
    PRIMARY KEY (ESSN, PROJ_NUM),
    FOREIGN KEY (ESSN) REFERENCES EMPLOYEE (SSN) ON DELETE CASCADE,
    FOREIGN KEY (PROJ_NUM) REFERENCES PROJECT (PNUMBER) ON DELETE CASCADE
);

-- DEPENDENT 테이블 생성
CREATE TABLE DEPENDENT (
    ESSN CHAR(9),
    DEPENDENT_NAME VARCHAR(255),
    SEX CHAR(1),
    BDATE DATE,
    RELATIONSHIP VARCHAR(255),
    PRIMARY KEY (ESSN, DEPENDENT_NAME),
    FOREIGN KEY (ESSN) REFERENCES EMPLOYEE (SSN) ON DELETE CASCADE
);

--Lab#5-2
--Insert The EMPLOYEE TABLE
INSERT INTO EMPLOYEE VALUES ('John', 'B', 'Smith', '123456789', TO_DATE('1965-01-09', 'yyyy-mm-dd'), '731 Fondren, Houston, TX', 'M', 30000, 333445555, 5);
INSERT INTO EMPLOYEE VALUES ('Franklin', 'T', 'Wong', '333445555', TO_DATE('1955-12-08', 'yyyy-mm-dd'), '638 Voss, Houston, TX', 'M', 40000, 888665555, 5);
INSERT INTO EMPLOYEE VALUES ('Alicia', 'J', 'Zelaya', '999887777', TO_DATE('1968-01-19', 'yyyy-mm-dd'), '3321 Castie, Spring, TX', 'F', 25000, 987654321, 4);
INSERT INTO EMPLOYEE VALUES ('Jennifer', 'S', 'Wallace', '987654321', TO_DATE('1941-06-20', 'yyyy-mm-dd'), '291 Berry, Bellarire, TX', 'F', 43000, 888665555, 4);
INSERT INTO EMPLOYEE VALUES ('Ramesh', 'K', 'Narayan', '666884444', TO_DATE('1962-09-15', 'yyyy-mm-dd'), '975 Fire Oak, Humble, TX', 'M', 38000, 333445555, 5);
INSERT INTO EMPLOYEE VALUES ('Joyce', 'A', 'English', '453453453', TO_DATE('1972-07-31', 'yyyy-mm-dd'), '5631 Rice, Houston, TX', 'F', 25000, 333445555, 5);
INSERT INTO EMPLOYEE VALUES ('Ahmad', 'V', 'Jabbar', '987987987', TO_DATE('1969-03-29', 'yyyy-mm-dd'), '980 Dallas, Houston, TX', 'M', 25000, 987654321, 4);
INSERT INTO EMPLOYEE VALUES ('James', 'E', 'Borg', '888665555', TO_DATE('1937-11-10', 'yyyy-mm-dd'), '450 Stone, Houston, TX', 'M', 55000, NULL, 1);


--Insert The DEPARTMENT TABLE
INSERT INTO DEPARTMENT VALUES ('Research', 5, '333445555', TO_DATE('1988-05-22', 'yyyy-mm-dd'));
INSERT INTO DEPARTMENT VALUES ('Administration', 4, '987654321', TO_DATE('1995-01-01', 'yyyy-mm-dd'));
INSERT INTO DEPARTMENT VALUES ('Headquarters', 1, '888665555', TO_DATE('1981-06-19', 'yyyy-mm-dd'));

--Insert The PROJECT TABLE
INSERT INTO PROJECT VALUES('ProductX', 1, 'Bellarie', 5);
INSERT INTO PROJECT VALUES('ProductY', 2, 'Sugariand, 5);
INSERT INTO PROJECT VALUES('ProductZ', 3, 'Houston', 5);
INSERT INTO PROJECT VALUES('Computerization', 10, 'Stafford', 4);
INSERT INTO PROJECT VALUES('Reorganization', 20, 'Houston', 1);
INSERT INTO PROJECT VALUES('Newbenefits', 30, 'Stafford', 4);

--Insert The DEPENDENT TABLE
INSERT INTO DEPENDENT VALUES(333445555, 'Alice', 'F', TO_DATE('1986-04-05', 'yyyy-mm-dd'), 'Daughter');
INSERT INTO DEPENDENT VALUES(333445555, 'Theodore', 'M', TO_DATE('1983-10-25', 'yyyy-mm-dd'), 'Son');
INSERT INTO DEPENDENT VALUES(333445555, 'Joy', 'F', TO_DATE('1958-05-03', 'yyyy-mm-dd'), 'Spouse');
INSERT INTO DEPENDENT VALUES(987654321, 'Abner', 'M', TO_DATE('1942-02-28', 'yyyy-mm-dd'), 'Spouse');
INSERT INTO DEPENDENT VALUES(123456789, 'Michael', 'M', TO_DATE('1988-01-04', 'yyyy-mm-dd'), 'Son');
INSERT INTO DEPENDENT VALUES(123456789, 'Alice', 'F', TO_DATE('1988-12-30', 'yyyy-mm-dd'), 'Daughter');
INSERT INTO DEPENDENT VALUES(123456789, 'Elizabeth', 'F', TO_DATE('1967-05-05', 'yyyy-mm-dd'), 'Spouse');


--Insert The DEPT_LOCATIONS TABLE
INSERT INTO DEPT-LOCATION VALUES(1, 'Houston');
INSERT INTO DEPT-LOCATION VALUES(4, 'Stafford');
INSERT INTO DEPT-LOCATION VALUES(5, 'Bellaire');
INSERT INTO DEPT-LOCATION VALUES(5, 'Sugarland');
INSERT INTO DEPT-LOCATION VALUES(5, 'Houston');

--Insert The WORKS_ON TABLE
INSERT INTO WORKS_ON VALUES('123456789', 1, 32.5);
INSERT INTO WORKS_ON VALUES('123456789', 2, 7.5);
INSERT INTO WORKS_ON VALUES('666884444', 3, 40.0);
INSERT INTO WORKS_ON VALUES('453453453', 1, 20.0);
INSERT INTO WORKS_ON VALUES('453453453', 2, 20.0);
INSERT INTO WORKS_ON VALUES('333445555', 2, 10.0);
INSERT INTO WORKS_ON VALUES('333445555', 3, 10.0);
INSERT INTO WORKS_ON VALUES('333445555', 10, 10.0);
INSERT INTO WORKS_ON VALUES('333445555', 20, 10.0);
INSERT INTO WORKS_ON VALUES('999887777', 30, 30.0);
INSERT INTO WORKS_ON VALUES('999887777', 10, 10.0);
INSERT INTO WORKS_ON VALUES('987987987', 10, 10.0);
INSERT INTO WORKS_ON VALUES('987987987', 30, 35.0);
INSERT INTO WORKS_ON VALUES('987654321', 30, 5.0);
INSERT INTO WORKS_ON VALUES('987654321', 20, 15.0);
INSERT INTO WORKS_ON VALUES('888665555', 20, NULL);

--Lab#5-3
--Q1) 부서 5에서 급여가 10,000 이상이고 'ProductZ' 프로젝트에서 작업하는 모든 직원의 이름, 성, 급여를 내림차순으로 정렬하여 검색

SELECT FNAME, LNAME, SALARY
FROM EMPLOYEE
INNER JOIN WORKS_ON ON EMPLOYEE.SSN = WORKS_ON.ESSN
INNER JOIN PROJECT ON WORKS_ON.PROJ_NUM = PROJECT.PNUMBER
WHERE EMPLOYEE.DNO = 5
  AND SALARY >= 10000
  AND PROJECT.PNAME = 'ProductZ'
ORDER BY SALARY DESC;
 
--Q2) "Houston"에서 근무하고 관리자의 SSN이 '987654321'인 모든 직원의 부서 이름, SSN, 이름 및 성을 나열
SELECT DEPARTMENT.DNAME, EMPLOYEE.SSN, EMPLOYEE.FNAME, EMPLOYEE.LNAME
FROM EMPLOYEE
INNER JOIN DEPARTMENT ON EMPLOYEE.DNO = DEPARTMENT.DNUMBER
WHERE EMPLOYEE.ADDRESS = 'Houston'
  AND EMPLOYEE.SUPER_SSN = '987654321';

--Q3) "NewBenefits" 프로젝트에서 2시간 이상 근무하는 모든 직원의 이름, 성, 근무 시간을 오름차순으로 정렬하여 검색
SELECT FNAME, LNAME, HOURS
FROM EMPLOYEE
INNER JOIN WORKS_ON ON EMPLOYEE.SSN = WORKS_ON.ESSN
INNER JOIN PROJECT ON WORKS_ON.PROJ_NUM = PROJECT.PNUMBER
WHERE PROJECT.PNAME = 'NewBenefits' AND HOURS >= 2
ORDER BY HOURS ASC;

--Q4) 'Product'로 시작하는 프로젝트에서 10시간 이상 근무하는 직원의 부서 이름, 이름 및 성을 나열하되, 중복 행을 제거
SELECT DISTINCT DEPARTMENT.DNAME, EMPLOYEE.FNAME, EMPLOYEE.LNAME
FROM EMPLOYEE
INNER JOIN WORKS_ON ON EMPLOYEE.SSN = WORKS_ON.ESSN
INNER JOIN PROJECT ON WORKS_ON.PROJ_NUM = PROJECT.PNUMBER
WHERE PROJECT.PNAME LIKE 'Product%' AND WORKS_ON.HOURS >= 10
ORDER BY LNAME ASC;

--Q5) 관리자의 SSN이 '888665555'인 감독의 부서 이름, 성, 이름 및 배우자 이름을 나열
SELECT DEPARTMENT.DNAME, EMPLOYEE.LNAME, EMPLOYEE.FNAME, SUPERVISOR.DEPENDENT_NAME AS SPOUSE_NAME
FROM EMPLOYEE
INNER JOIN DEPENDENT AS SUPERVISOR ON EMPLOYEE.SSN = SUPERVISOR.ESSN
INNER JOIN EMPLOYEE ON SUPERVISOR.RELATIONSHIP = 'Spouse'
WHERE EMPLOYEE.SSN = '888665555';

--Lab#5-4
--D1) 'Essn'이 '333445555'이고 'Relationship'이 'Son'인 DEPENDENT 튜플을 삭제하고 삭제된 튜플의 수를 확인
-- D1) Delete the DEPENDENT tuples associated with Essn = '333445555' and Relationship = 'Son'
DELETE FROM DEPENDENT WHERE ESSN = '333445555' AND RELATIONSHIP = 'Son';

-- 확인을 위해 삭제된 튜플의 수를 반환
SELECT COUNT(*) AS DELETED_ROWS FROM DEPENDENT WHERE ESSN = '33344555' AND RELATIONSHIP = 'Son';

--D2) 'Dnumber'가 4인 DEPARTMENT 튜플을 삭제하고 어떤 결과가 나타나는지 확인
-- D2) Delete the DEPARTMENT tuples with Dnumber = 4
DELETE FROM DEPARTMENT WHERE DNUMBER = 4;

-- 확인을 위해 어떤 결과가 나타나는지 확인
SELECT * FROM DEPARTMENT WHERE DNUMBER = 4;

--D3) 'Pno'가 10이고 'Hours'가 30 이상인 WORKS_ON 튜플을 삭제하고 삭제가 성공했는지 확인
-- D3) Delete the WORKS_ON tuples with Pno = 10 and Hours >= 30
DELETE FROM WORKS_ON WHERE PROJ_NUM = 10 AND HOURS >= 30;

-- 확인을 위해 삭제가 성공했는지 확인
SELECT * FROM WORKS_ON WHERE PROJ_NUM = 10 AND HOURS >= 30;

--Lab#5-5
-- U1) Update to 'Daegu' the Plocation of the PROJECT tuples with Dnum = 5
UPDATE PROJECT SET PLOCATION = 'Daegu' WHERE DNUM = 5;

-- 변경 사항 확인
SELECT * FROM PROJECT WHERE DNUM = 5;

-- U2) Update the super_ssn attribute of the EMPLOYEE tuple with Dno= 4 to '112233445'
UPDATE EMPLOYEE SET SUPER_SSN = '112233445' WHERE DNO = 4;

-- 변경 사항 확인
SELECT * FROM EMPLOYEE WHERE DNO = 4;

-- U3) Update the Dname attribute of the DEPARTMENT tuple with Dnumber = 1 to 'Research'
UPDATE DEPARTMENT SET DNAME = 'Research' WHERE DNUMBER = 1;

-- 변경 사항 확인
SELECT * FROM DEPARTMENT WHERE DNUMBER = 1;


