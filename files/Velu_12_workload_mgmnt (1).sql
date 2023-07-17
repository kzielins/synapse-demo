CREATE USER [krzychzet@gmail.com] FROM EXTERNAL PROVIDER;
SELECT * FROM sys.database_role_members;
select * from sys.database_principals;
SELECT
	r.[name]     AS [Role]
	, m.[name]   AS [Member]
	, m.Create_date AS [Created Date]
	, m.modify_Date AS [Modified Date]
FROM
	sys.database_role_members rm
	JOIN sys.database_principals AS r ON rm.[role_principal_id] = r.[principal_id]
	JOIN sys.database_principals AS m ON rm.[member_principal_id] = m.[principal_id]
WHERE
	r.[type_desc] = 'DATABASE_ROLE';



-- Drop objects if they exist
IF EXISTS (SELECT * FROM sys.workload_management_workload_classifiers WHERE [name] = 'HeavyLoaderKZ')
BEGIN
    DROP WORKLOAD CLASSIFIER HeavyLoaderKZ
END;

IF EXISTS (SELECT * FROM sys.workload_management_workload_groups WHERE name = 'BigDataLoadKZ')
BEGIN
    DROP WORKLOAD GROUP BigDataLoadKZ
END;

--Create workload group
CREATE WORKLOAD GROUP BigDataLoadKZ WITH
  (
      MIN_PERCENTAGE_RESOURCE = 50, -- integer value 
      REQUEST_MIN_RESOURCE_GRANT_PERCENT = 25, --  (guaranteed min 4 concurrency)
      CAP_PERCENTAGE_RESOURCE = 100
  );

-- Create workload classifier   that assigns the user to the BigDataLoad workload group.
CREATE WORKLOAD Classifier HeavyLoader_Krzychzet WITH
(
    Workload_Group ='BigDataLoad',
    MemberName='krzychzet@gmail.com',
    IMPORTANCE = HIGH
);


-------

-- Drop objects if they exist
IF EXISTS (SELECT * FROM sys.workload_management_workload_classifiers WHERE [name] = 'HeavyLoader')
BEGIN
    DROP WORKLOAD CLASSIFIER HeavyLoader
END;

IF EXISTS (SELECT * FROM sys.workload_management_workload_groups WHERE name = 'BigDataLoad')
BEGIN
    DROP WORKLOAD GROUP BigDataLoad
END;

--Create workload group
CREATE WORKLOAD GROUP BigDataLoad WITH
  (
      MIN_PERCENTAGE_RESOURCE = 50, -- integer value
      REQUEST_MIN_RESOURCE_GRANT_PERCENT = 25, --  (guaranteed min 4 concurrency)
      CAP_PERCENTAGE_RESOURCE = 100
  );

-- Create workload classifier  that assigns the asa.sql.import01 user to the BigDataLoad workload group.
CREATE WORKLOAD Classifier HeavyLoader WITH
(
    Workload_Group ='BigDataLoad',
    MemberName='asa.sql.import01',
    IMPORTANCE = HIGH
);

-- View classifiers
SELECT * FROM sys.workload_management_workload_classifiers