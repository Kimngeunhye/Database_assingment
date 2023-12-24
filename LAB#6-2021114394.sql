Lab#6-1 : Aggregates
1) For each department whose average employee salary between
20000 (inclusive) and 40000 (inclusive), retrieve the department
name and the number of employees working for that department and
sort the results by the number of employees in ascending order.

A1.
SELECT DEPARTMENT.DNAME, COUNT(EMPLOYEE.SSN) AS NUM_OF_EMPLOYEES
FROM DEPARTMENT
INSERT JOIN EMPLOYEE ON DEPARTMENT.DNUMBER = EMPLOYEE.DNO
GROUP BY DEPARTMENT.DNAME
HAVING AVG(EMPLOYEE.SALARY) BETWEEN 20000 AND 40000
ORDER BY NUM_OF_EMPLOYEES ASC;

2) For each department, retrieve its department number, department name, and the number of male employees making fewer than 40000in that department and sort the result by the department name in
descending order.

A2.
SELECT DEPARTMENT.DNUMBER, DEPARTMENT.DNAME, COUNT(EMPLOYEE.SSN) AS NUM_OF_MALE_EMPLOYEES
FROM DEPARTMENT
INNER JOIN EMPLOYEE ON DEPARTMENT.DNUMBER = EMPLOYEE.DNO
WHERE EMPLOYEE.SALARY < 40000 AND EMPLOYEE.SEX = 'M'
GROUP BY DEPARTMENT.DNUMBER, DEPARTMENT.DNAME
ORDER BY DEPARTMENT.DNAME DESC;

LAB#6-2 : NOT EXISTS
A1.
SELECT E.Fname, E.Lname, E.Salary
FROM EMPLOYEE E
WHERE E.SEX = 'F'
NOT EXISTS (
	SELECT P.PROJ_NUM
	FROM PROJECT P
	WHERE P.DNUM = 4
	AND P.PROJ_NUM NOT IN (
		SELECT W.PROJ_NUM
		FROM WORKS_ON W
		WHERE W.ESSN = E.SSN
	)
)
ORDER BY E.SALARY ASC;

A2.
SELECT D.DNUMBER, D.NAME, E.FNAME, E.LNAME
FROM DEPARTMENT D
INNER JOIN EMPLOYEE E ON D.MGR_SSN = E.SSN
WHERE NOT EXISTS(
	SELECT *
	FROM DEPENDENT DEP
	WHERE DEP.ESSN = E.SSN
);


Lab#6-3
A1. 
SELECT E.LANME, E.FNAME, E.SALARY
FROM EMPLOYEE E
WHERE E.DNO = (SELECT DNO FROM EMPLOYEE WHERE SALARY = (SELECT MAX(SALARY) FROM EMPLOYEE));

A2.
SELCET E.FNAME, E.LNAME
FROM EMPLOYEE E
WHERE E.SSN IN (
	SELECT E2.SUPER_SSN
	FROM EMPLOYEE E2
	WHERE E2.SSN IN(
		SELECT E3.SUPER_SSN
		FROM EMPLOYEE E3
		WHERE E3.SSN = '987654321'
	)
);

A3.
SELECT E.FNAME, E.LNAME, E.ADDRESS, E.SALARY
FROM EMPLOYEE E
WHERE E.SALARY >= 3000 OR E.SALARY > (SELECT AVG(SALARY) FROM EMPLOYEE);

Lab#6-4
A1.
CREATE VIEW DEPT_SUMMARY (D, C, Total_s, Avg_s, Mn_s, MX_s) AS
SELECT Dno, COUNT(*) as nemps, SUM(Salary), ROUND(AVG(Salary), 1), MIN(Salary)
FROM EMPLOYEE
GROUP BY Dno
ORDER BY nemps;

A2.
DEPT_SUMMARY에서 "D"값이 5인 행을 반환한다.

A3. 
DEPT_SUMMARY뷰에서 Avg_S 열 값이 30,000에서 40,000 사이인 행의 "D," "C," 및 "Avg_S" 열을 반환하고,  각 부서에 대한 집계 정보를 제공하므로, "Avg_S"는 해당 부서의 평균 급여를 반환한다.
따라서, 평균 급여가 30,000에서 40,000사이인 부서의 정보를 반환할 것이다.

Lab#6-5
A1.
-- 트리거 생성
CREATE OR REPLACE TRIGGER SALARY_VIOLATION
BEFORE INSERT OR UPDATE OF Salary ON EMPLOYEE
FOR EACH ROW
BEGIN
    IF :NEW.Salary > 100000 THEN
        dbms_output.put_line('Employee ' || :NEW.Ssn || ' has a salary exceeding $100,000');
    END IF;
END;
/










































