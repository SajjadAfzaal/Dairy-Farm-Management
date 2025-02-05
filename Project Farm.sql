create database farm_project
use farm_project

--All Component entities of Dairy Farm
-- Supplier Table 
CREATE TABLE supplier (
    supplierID INT IDENTITY(1,1) PRIMARY KEY,
    fullName NVARCHAR(100) NOT NULL,
    phoneNumber NVARCHAR(50),
    email NVARCHAR(100),
    completeAddress NVARCHAR(200)
);

-- Feed Table 
CREATE TABLE feed (
    feedID INT IDENTITY(1,1) PRIMARY KEY,
    feedType NVARCHAR(100) NOT NULL,
);


--Job Class Table
CREATE TABLE jobClass (
    jobClassID INT IDENTITY(1,1) PRIMARY KEY, -- Unique identifier for job class
    jobTitle NVARCHAR(100) NOT NULL UNIQUE,  -- e.g., Vet, Staff, Admin
    baseSalary DECIMAL(10, 2) NOT NULL       -- Default base salary for this role
);

-- Employee Table
CREATE TABLE employee (
    employeeID INT IDENTITY(1,1) PRIMARY KEY,
    fullName NVARCHAR(100) NOT NULL,
    jobClassID INT NOT NULL,                 -- Links to jobClass table
    phoneNumber NVARCHAR(50),
    hiringDate DATE NOT NULL,
    FOREIGN KEY (jobClassID) REFERENCES jobClass(jobClassID) ON DELETE CASCADE
);



-- Shed Table
CREATE TABLE shed (
    shedID INT IDENTITY(1,1) PRIMARY KEY,
    capacity INT NOT NULL,
);


-- Cow Table
CREATE TABLE cow (
    cowID INT IDENTITY(1,1) PRIMARY KEY,
    breed NVARCHAR(100) NOT NULL,
    age INT NOT NULL,
    healthStatus NVARCHAR(200),
);

-- Distributor Table
CREATE TABLE distributor (
    distributorID INT IDENTITY(1,1) PRIMARY KEY,
    fullName NVARCHAR(100) NOT NULL,
    phoneNumber NVARCHAR(50),
    email NVARCHAR(100),
    completeAddress NVARCHAR(200),
    distributionArea NVARCHAR(100) --Area of Distribution
);




/*
drop table supplier
drop table feed
drop table jobClass
drop table employee
drop table cow
drop table shed
drop table distributor
*/

--Transactional Entities
-- Feed Purchase Transactions Table
CREATE TABLE purchaseFeed (
    purchaseID INT IDENTITY(1,1) PRIMARY KEY,
    feedID INT NOT NULL,
    supplierID INT NOT NULL,
    quantity DECIMAL(10, 2) NOT NULL,
    cost DECIMAL(10, 2) NOT NULL,
    purchaseDate DATE NOT NULL,
    FOREIGN KEY (feedID) REFERENCES feed(feedID) ON DELETE CASCADE,
    FOREIGN KEY (supplierID) REFERENCES supplier(supplierID) ON DELETE CASCADE
);



-- Payroll Table
CREATE TABLE payroll (
    payrollID INT IDENTITY(1,1) PRIMARY KEY,
    employeeID INT NOT NULL,
    salaryMonth DATE NOT NULL,
    salary DECIMAL(10, 2) NOT NULL,          -- Actual salary (can differ from base salary for bonuses/deductions)
    bonus DECIMAL(10, 2) DEFAULT 0,          -- Additional payments
    FOREIGN KEY (employeeID) REFERENCES employee(employeeID) ON DELETE CASCADE
);



-- Employee-Shed Assignment Table
CREATE TABLE employeeShed (
    employeeID INT NOT NULL,
    shedID INT NOT NULL,
    assignmentDate DATE DEFAULT GETDATE(), -- Auto date
	PRIMARY KEY(employeeID, shedID),
    FOREIGN KEY (employeeID) REFERENCES employee(employeeID) ON DELETE CASCADE,
    FOREIGN KEY (shedID) REFERENCES shed(shedID) ON DELETE CASCADE
);



-- Shed Assignment for Cows
CREATE TABLE cowShed (
    assignmentID INT IDENTITY(1,1) ,
    cowID INT NOT NULL,
    shedID INT NOT NULL,
    startDate DATE NOT NULL,  -- Start and ending dates for record
    endDate DATE,
    FOREIGN KEY (cowID) REFERENCES cow(cowID) ON DELETE CASCADE,
    FOREIGN KEY (shedID) REFERENCES shed(shedID) ON DELETE CASCADE,
	PRIMARY KEY(assignmentID, cowID)
);


-- Milk Production Table
CREATE TABLE milkProduction (
    productionID INT IDENTITY(1,1) PRIMARY KEY,
    shedID INT NOT NULL,
    productionDate DATE NOT NULL,
    quantity DECIMAL(10, 2) NOT NULL,
    quality NVARCHAR(100) NOT NULL,  -- Comments about fat contents in milk
    FOREIGN KEY (shedID) REFERENCES shed(shedID) ON DELETE CASCADE
);



-- Milk Separation Table with actualAvailableQuantity to track real available quantity
CREATE TABLE milkSeparation (
    separationID INT IDENTITY(1,1) PRIMARY KEY,
    productType NVARCHAR(50) CHECK (productType IN ('Skimmed Milk', 'Cream Milk')), 
    quantity DECIMAL(10, 2) NOT NULL,  -- Original quantity
    actualAvailableQuantity DECIMAL(10, 2) NOT NULL,  -- Real available quantity for distribution
    wastageQuantity DECIMAL(10, 2) DEFAULT 0, 
    separationDate DATE NOT NULL
);




-- Distribution Table
CREATE TABLE distributionProcess (
    distributionID INT IDENTITY(1,1) PRIMARY KEY,
    separationID INT NOT NULL,
    distributorID INT NOT NULL,
    quantity DECIMAL(10, 2) NOT NULL,  --what about the product type and how to manage (Ans: Trigger validation Qty)
    distributionDate DATE NOT NULL,
    FOREIGN KEY (separationID) REFERENCES milkSeparation(separationID) ON DELETE CASCADE,
    FOREIGN KEY (distributorID) REFERENCES distributor(distributorID) ON DELETE CASCADE
);



-- Feed Consumption Table Shed wise
CREATE TABLE feedConsumption (
    feedConsumptionID INT IDENTITY(1,1) PRIMARY KEY,
    feedID INT NOT NULL,
    feedingDate DATE NOT NULL,
    quantity DECIMAL(10, 2) NOT NULL,
    shedID INT NOT NULL,
    FOREIGN KEY (feedID) REFERENCES feed(feedID) ON DELETE CASCADE,
    FOREIGN KEY (shedID) REFERENCES shed(shedID) ON DELETE CASCADE
);

-- Component Data
select  * from jobClass
select  * from supplier
select  * from shed
select  * from feed
select  * from employee
select  * from distributor
select  * from cow

-- Transactional Data
select  * from purchaseFeed
select  * from payroll
select * from employeeShed
select * from cowShed
select * from milkProduction
select * from milkSeparation
select  * from distributionProcess
select * from feedConsumption



/*
drop table purchaseFeed
drop table payroll
drop table employeeShed
drop table cowShed
drop table milkProduction
drop table milkSeparation
drop table distributionProcess
truncate table feedConsumption
*/





--A U D I T		T A B L E	 
create table history(
tableName varchar(100),
recordID varchar(100),
operation varchar(100),
oldValues nvarchar(max),
updatedBy varchar(200) default system_user,
updatedAt datetime default getdate());



--Trigger to manage update and delete in distributionProcess
CREATE TRIGGER trg_after_update_delete_on_distributionProcess
ON distributionProcess
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Audit for UPDATE operation
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO history (tableName, recordID, operation, oldValues)
        SELECT 
            'distributionProcess' AS tableName,
            d.distributionID AS recordID,
			'UPDATE' as operation,
            'distributionID = ' + CAST(d.distributionID AS NVARCHAR) + ', ' +
            'separationID = ' + CAST(d.separationID AS NVARCHAR) + ', ' +
            'distributorID = ' + CAST(d.distributorID AS NVARCHAR) + ', ' +
            'quantity = ' + CAST(d.quantity AS NVARCHAR) + ', ' +
            'distributionDate = ' + CONVERT(NVARCHAR, d.distributionDate, 120) AS oldValues
        FROM deleted as d;
    END

    -- Audit for DELETE operation
    ELSE IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
    BEGIN
        INSERT INTO history (tableName, recordID, operation , oldValues)
        SELECT 
            'distributionProcess' AS tableName,
            d.distributionID AS recordID,
			'DELETE' as operation,
            'distributionID = ' + CAST(d.distributionID AS NVARCHAR) + ', ' +
            'separationID = ' + CAST(d.separationID AS NVARCHAR) + ', ' +
            'distributorID = ' + CAST(d.distributorID AS NVARCHAR) + ', ' +
            'quantity = ' + CAST(d.quantity AS NVARCHAR) + ', ' +
            'distributionDate = ' + CONVERT(NVARCHAR, d.distributionDate, 120) AS oldValues
        FROM deleted as d;
    END
END;


--Trigger to manage update and delete in Payrolls
CREATE TRIGGER trg_after_update_delete_on_payroll
ON payroll
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Audit for UPDATE operation
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO history (tableName, recordID, operation, oldValues)
        SELECT 
            'payroll' AS tableName,
            d.payrollID AS recordID,
			'UPDATE' as operation,
            'payrollID = ' + CAST(d.payrollID AS NVARCHAR) + ', ' +
            'employeeID = ' + CAST(d.employeeID AS NVARCHAR) + ', ' +
            'salaryMonth = ' + CONVERT(NVARCHAR, d.salaryMonth, 120) + ', ' +
            'salary = ' + CAST(d.salary AS NVARCHAR) + ', ' +
            'bonus = ' + CAST(d.bonus AS NVARCHAR) AS oldValues
        FROM deleted AS d;
    END

    -- Audit for DELETE operation
    ELSE IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
    BEGIN
        INSERT INTO history (tableName, recordID, operation, oldValues)
        SELECT 
            'payroll' AS tableName,
            d.payrollID AS recordID,
			'DELETE' as operation,
            'payrollID = ' + CAST(d.payrollID AS NVARCHAR) + ', ' +
            'employeeID = ' + CAST(d.employeeID AS NVARCHAR) + ', ' +
            'salaryMonth = ' + CONVERT(NVARCHAR, d.salaryMonth, 120) + ', ' +
            'salary = ' + CAST(d.salary AS NVARCHAR) + ', ' +
            'bonus = ' + CAST(d.bonus AS NVARCHAR) AS oldValues
        FROM deleted AS d;
    END
END;



--Trigger to manage update and delete in Purchase Feed
CREATE TRIGGER trg_after_update_delete_on_purchaseFeed
ON purchaseFeed
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Audit for UPDATE operation
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
    BEGIN
        INSERT INTO history (tableName, recordID, operation, oldValues)
        SELECT 
            'purchaseFeed' AS tableName,
            d.purchaseID AS recordID,
			'UPDATE' as operation,
            'purchaseID = ' + CAST(d.purchaseID AS NVARCHAR) + ', ' +
            'feedID = ' + CAST(d.feedID AS NVARCHAR) + ', ' +
            'supplierID = ' + CAST(d.supplierID AS NVARCHAR) + ', ' +
            'quantity = ' + CAST(d.quantity AS NVARCHAR) + ', ' +
            'cost = ' + CAST(d.cost AS NVARCHAR) + ', ' +
            'purchaseDate = ' + CONVERT(NVARCHAR, d.purchaseDate, 120) AS oldValues
        FROM deleted AS d;
    END

    -- Audit for DELETE operation
    ELSE IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
    BEGIN
        INSERT INTO history (tableName, recordID, operation, oldValues)
        SELECT 
            'purchaseFeed' AS tableName,
            d.purchaseID AS recordID,
			'DELETE' as operation,
            'purchaseID = ' + CAST(d.purchaseID AS NVARCHAR) + ', ' +
            'feedID = ' + CAST(d.feedID AS NVARCHAR) + ', ' +
            'supplierID = ' + CAST(d.supplierID AS NVARCHAR) + ', ' +
            'quantity = ' + CAST(d.quantity AS NVARCHAR) + ', ' +
            'cost = ' + CAST(d.cost AS NVARCHAR) + ', ' +
            'purchaseDate = ' + CONVERT(NVARCHAR, d.purchaseDate, 120) AS oldValues
        FROM deleted AS d;
    END
END;












-- I N D E X E S

--Used for supplier-feed-specific queries and joins.
CREATE NONCLUSTERED INDEX IX_PurchaseFeed_Supplier_Feed ON purchaseFeed (supplierID, feedID);

--This index optimizes payroll queries for specific employees and months.
CREATE NONCLUSTERED INDEX IX_Payroll_EmployeeID_SalaryMonth ON payroll (employeeID, salaryMonth);

--Used for filtering or sorting by production date, common in daily reports.
CREATE NONCLUSTERED INDEX IX_MilkProduction_ProductionDate ON milkProduction (productionDate);

--Filters by milk product types (Cream Milk, Skimmed Milk).
CREATE NONCLUSTERED INDEX IX_MilkSeparation_ProductType ON milkSeparation (productType);

--Used for date-based queries on feed consumption.
CREATE NONCLUSTERED INDEX IX_FeedConsumption_FeedingDate ON feedConsumption (feedingDate);

--Tracks changes for specific tables in chronological order.
CREATE NONCLUSTERED INDEX IX_Audit_TableName_UpdatedAt ON AuditTable (tableName, updatedAt);




--T R I G G E R S

-- Trigger to Validate Distribution Quantity

CREATE TRIGGER trg_after_distribution_insert
ON distributionProcess
AFTER INSERT
AS
BEGIN
    -- Declare variables for validation and updates
    DECLARE @separationID INT;
    DECLARE @distributedQuantity DECIMAL(10, 2);
    DECLARE @productType NVARCHAR(50);
    DECLARE @availableQuantity DECIMAL(10, 2);

    -- Get values from the inserted row
    SELECT @separationID = separationID, @distributedQuantity = quantity
    FROM inserted;

    -- Get the product type (Cream Milk / Skimmed Milk) and available quantity from the milkSeparation table
    SELECT @productType = productType, @availableQuantity = actualAvailableQuantity
    FROM milkSeparation
    WHERE separationID = @separationID;

    -- Validate if the distributed quantity exceeds the available quantity
    IF @distributedQuantity > @availableQuantity
    BEGIN
        -- If the distributed quantity exceeds the available quantity, raise an error
        RAISERROR('Distributed quantity exceeds available quantity in milk separation table.', 16, 1);
        ROLLBACK TRANSACTION;  -- Rollback the insert operation in the distributionProcess table
    END
    ELSE
    BEGIN
        -- If validation passes, update the milkSeparation table to reduce the available quantity
        UPDATE milkSeparation
        SET actualAvailableQuantity = actualAvailableQuantity - @distributedQuantity
        WHERE separationID = @separationID;
    END
END;





-- S T O C K S	 M A N A G E M E N T
CREATE TABLE stocksFeed (
    feedID INT PRIMARY KEY,
    stockIn DECIMAL(10, 2) NOT NULL DEFAULT 0, -- Total feed purchased
    stockOut DECIMAL(10, 2) NOT NULL DEFAULT 0, -- Total feed consumed
    totalStock AS (stockIn - stockOut), -- Calculated stock available
    FOREIGN KEY (feedID) REFERENCES feed(feedID) ON DELETE CASCADE
);

select * from stocksFeed


-- Triggers for stocks
CREATE TRIGGER trg_updateStockIn_onPurchase
ON purchaseFeed
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    -- Update stockIn in stocksFeed table
    UPDATE stocksFeed
    SET stockIn = stockIn + i.quantity
    FROM stocksFeed s
    INNER JOIN inserted i ON s.feedID = i.feedID;

    -- Insert new feedID into stocksFeed if not exists
    INSERT INTO stocksFeed (feedID, stockIn, stockOut)
    SELECT i.feedID, i.quantity, 0
    FROM inserted i
    WHERE NOT EXISTS (
        SELECT 1 FROM stocksFeed s WHERE s.feedID = i.feedID
    );
END;


CREATE TRIGGER trg_updateStockOut_onConsumption
ON feedConsumption
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    -- Update stockOut in stocksFeed table
    UPDATE stocksFeed
    SET stockOut = stockOut + i.quantity
    FROM stocksFeed s
    INNER JOIN inserted i ON s.feedID = i.feedID;

    -- Insert new feedID into stocksFeed if not exists
    INSERT INTO stocksFeed (feedID, stockIn, stockOut)
    SELECT i.feedID, 0, i.quantity
    FROM inserted i
    WHERE NOT EXISTS (
        SELECT 1 FROM stocksFeed s WHERE s.feedID = i.feedID
    );
END;





-- S T O R E D		P R O C E D U R E S

--Feed Consumption ('INSERT', 'UPDATE', or 'DELETE')
create procedure ManageFeedConsumption
	@Action NVARCHAR(10),
	@feedConsumptionID int = null,
    @feedID INT = NULL,
    @feedingDate DATE = NULL,
    @quantity DECIMAL(10,2) = NULL,
    @shedID INT = NULL

as
begin
-- Insertion
if @Action = 'INSERT'
begin
insert into feedConsumption(feedID, feedingDate, quantity, shedID)
values (@feedID, @feedingDate, @quantity, @shedID);
end

-- update
else if @Action = 'UPDATE'
begin
if @feedConsumptionID is null
begin
raiserror('FeedConsumptionID is required for update',16,1);
return;
end
update feedConsumption
set
feedID = @feedID,
feedingDate = @feedingDate,
shedID = @shedID,
quantity = @quantity
where
feedConsumptionID = @feedConsumptionID

end

-- Delete 
else if @Action = 'DELETE' 
begin
if @feedConsumptionID is null
begin
raiserror('FeedConsumptionID is required for delete opertaion',16,1);
return;
end
delete feedConsumption
where feedConsumptionID = @feedConsumptionID

end
-- Invalid Action
else
begin
RAISERROR ('Invalid Action. Use INSERT, UPDATE, or DELETE.', 16, 1);
end

end



-- Milk Production ('INSERT', 'UPDATE', or 'DELETE')
CREATE PROCEDURE ManageMilkProduction
    @Action NVARCHAR(10), 
    @productionID INT = NULL,  
    @shedID INT = NULL,        
    @productionDate DATE = NULL, 
    @quantity DECIMAL(10, 2) = NULL, 
    @quality NVARCHAR(100) = NULL   
AS
BEGIN
    SET NOCOUNT ON;

    -- Insert Operation
    IF @Action = 'INSERT'
    BEGIN
        INSERT INTO milkProduction (shedID, productionDate, quantity, quality)
        VALUES (@shedID, @productionDate, @quantity, @quality);
    END

    -- Update Operation
    ELSE IF @Action = 'UPDATE'
    BEGIN
        IF @productionID IS NULL
        BEGIN
            RAISERROR ('productionID is required for UPDATE operation.', 16, 1);
            RETURN;
        END

        UPDATE milkProduction
        SET 
            shedID = @shedID,
            productionDate = @productionDate,
            quantity = @quantity,
            quality = @quality
        WHERE productionID = @productionID;
    END

    -- Delete Operation
    ELSE IF @Action = 'DELETE'
    BEGIN
        IF @productionID IS NULL
        BEGIN
            RAISERROR ('productionID is required for DELETE operation.', 16, 1);
            RETURN;
        END

        DELETE FROM milkProduction
        WHERE productionID = @productionID;
    END

    -- Invalid Action
    ELSE
    BEGIN
        RAISERROR ('Invalid Action. Use INSERT, UPDATE, or DELETE.', 16, 1);
    END
END



--Milk Distribution ('INSERT', 'UPDATE', or 'DELETE')
create procedure ManageDistribution
@Action nvarchar(20),
@distributionID int = null,
@separationID int = null,
@distributorID int = null,
@quantity int = nulll,
@distributionDate Date = null
as
begin
-- insert operation
if @Action = 'INSERT'
begin
insert into distributionProcess(separationID, distributorID, quantity, distributionDate)
values(@separationID, @distributorID, @quantity, @distributionDate);
end

-- update operation
else if @Action = 'UPDATE'
begin
if @distributionID = null
begin
raiserror('DistributionID is required for update ',16,1);
return;
end

update distributionProcess
set

separationID = @separationID,
distributorID = @distributorID,
quantity = @quantity,
distributionDate = @distributionDate
where distributionID = @distributionID

end

-- delete operation
else if @Action = 'DELETE'
begin
if @distributionID is null
begin
raiserror('DistributionID is required for deletion',16,1);
return;
end
delete distributionProcess
where distributionID = @distributionID
end

-- invalid action
else
begin
RAISERROR ('Invalid Action. Use INSERT, UPDATE, or DELETE.', 16, 1);
end


end







--Execution
EXEC ManageMilkProduction 'INSERT', NULL, 1, '2024-12-22', 500.25, 'High Fat Content';

exec ManageDistribution 'INSERT', null, 100, 500, '2024-12-22';

exec ManageFeedConsumption 'DELETE', 3652;

exec ManageFeedConsumption 'INSERT', null, 4, '2022-01-01', 2700.00, 2;






-- S Q L	R E P O R T S

-- 1. Stored Procedures/Functions

-- Generate supplier-wise feed purchase reports
CREATE PROCEDURE sp_SupplierFeedReport
    @supplierID INT
AS
BEGIN
    SELECT 
        pf.purchaseID,
        s.fullName AS SupplierName,
        f.feedType,
        pf.quantity,
        pf.cost,
        pf.purchaseDate
    FROM 
        purchaseFeed as pf
    JOIN 
        supplier as s ON pf.supplierID = s.supplierID
    JOIN 
        feed as f ON pf.feedID = f.feedID
    WHERE 
        pf.supplierID = @supplierID;
END;






-- Calculate employee payroll summaries for a specific month
CREATE PROCEDURE sp_EmployeePayrollSummary
    @salaryMonth DATE
AS
BEGIN
    SELECT 
        e.fullName AS EmployeeName,
        jc.jobTitle,
        p.salaryMonth,
        p.salary,
        p.bonus
    FROM 
        payroll as p
    JOIN 
        employee as e ON p.employeeID = e.employeeID
    JOIN 
        jobClass as jc ON e.jobClassID = jc.jobClassID
    WHERE 
        FORMAT(p.salaryMonth, 'yyyy-MM') = FORMAT(@salaryMonth, 'yyyy-MM');
END;


-- To add master details in payroll each month
CREATE PROCEDURE InsertPayrollForMonth
    @salaryMonth DATE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO payroll (employeeID, salaryMonth, salary, bonus)
    SELECT 
        e.employeeID,
        @salaryMonth AS salaryMonth,
        jc.baseSalary AS salary,
        0 AS bonus
    FROM 
        employee AS e
    JOIN 
        jobClass AS jc ON e.jobClassID = jc.jobClassID
    WHERE 
        NOT EXISTS (
            SELECT 1 
            FROM payroll AS p
            WHERE p.employeeID = e.employeeID AND p.salaryMonth = @salaryMonth
        );
END;

-- To get purchase history cost and quantity wise over the period of time
CREATE PROCEDURE GetFeedPurchaseHistory
    @startDate DATE,
    @endDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    SELECT  
        SUM(pf.quantity) AS total_quantity,
        f.feedType,
        SUM(pf.cost) AS totalCost
    FROM 
        purchaseFeed AS pf
    JOIN 
        feed AS f ON f.feedID = pf.feedID
    WHERE 
        pf.purchaseDate BETWEEN @startDate AND @endDate
    GROUP BY 
        f.feedType;
END;

-- Execution
exec sp_SupplierFeedReport 2

exec sp_EmployeePayrollSummary @salaryMonth ='2022-03-01'

exec InsertPayrollForMonth '2024-04-01';

exec GetFeedPurchaseHistory '2022-01-01', '2024-12-31';



-- 2. Complex Views

-- View: Display monthly milk production details, grouped by shed
CREATE VIEW MonthlyMilkProduction AS
SELECT 
    shedID,
    FORMAT(productionDate, 'yyyy-MM') AS ProductionMonth,
    SUM(quantity) AS TotalMilkProduced
FROM 
    milkProduction
GROUP BY 
    shedID, FORMAT(productionDate, 'yyyy-MM');

drop view MonthlyMilkProduction

 

-- View: Employee assignments showing shed details and durations
CREATE VIEW EmployeeShedAssignments AS
SELECT 
    e.fullName AS EmployeeName,
    s.shedID,
    es.assignmentDate
FROM 
    employeeShed as es
JOIN 
    employee as e ON es.employeeID = e.employeeID
JOIN 
    shed as s ON es.shedID = s.shedID;



-- View: Consolidated feed consumption reports, with feed type and quantity breakdown
CREATE VIEW FeedConsumptionSummary AS
SELECT 
    fc.shedID,
    f.feedType,
    SUM(fc.quantity) AS TotalFeedConsumed
FROM 
    feedConsumption as fc
JOIN 
    feed as f ON fc.feedID = f.feedID
GROUP BY 
    fc.shedID, f.feedType;



--View for cowshed
CREATE VIEW CowShedView AS
SELECT 
    c.cowID,
    c.breed,
    c.age,
    c.healthStatus,
    cs.shedID,
    cs.startDate,
    cs.endDate
FROM 
    cow as c
JOIN 
    cowShed as cs
ON 
    c.cowID = cs.cowID;

-- view to see total payrolls per jobclass
CREATE VIEW JobClassPayrollSummaryView AS
SELECT 
    jc.jobTitle,
    jc.baseSalary,
    COUNT(p.payrollID) AS TotalPayrolls,
    SUM(p.salary) AS TotalSalaries,
    SUM(p.bonus) AS TotalBonuses
FROM 
    jobClass as jc
LEFT JOIN 
    employee as e
ON 
    jc.jobClassID = e.jobClassID
LEFT JOIN 
    payroll p
ON 
    e.employeeID = p.employeeID
GROUP BY 
    jc.jobTitle, jc.baseSalary;

	
-- view for employees without any payroll
CREATE VIEW EmployeesWithoutPayrollView AS
SELECT 
    e.employeeID,
    e.fullName,
    e.hiringDate,
    jc.jobTitle
FROM 
    employee as e
JOIN 
    jobClass as jc
ON 
    e.jobClassID = jc.jobClassID
WHERE 
    NOT EXISTS (
        SELECT 1 
        FROM payroll p 
        WHERE p.employeeID = e.employeeID
    );

 -- view to see the latest month payroll data
CREATE VIEW CurrentEmployeePayrollView AS
SELECT 
    e.employeeID,
    e.fullName,
    jc.jobTitle,
    p.salaryMonth,
    p.salary,
    p.bonus
FROM 
    employee as e
JOIN 
    jobClass as jc
ON 
    e.jobClassID = jc.jobClassID
JOIN 
    payroll p
ON 
    e.employeeID = p.employeeID
WHERE 
    p.salaryMonth = (SELECT MAX(salaryMonth) FROM payroll WHERE employeeID = e.employeeID);


-- to view each product type distributed to each distributor till last distribution date
CREATE VIEW vw_MilkDistributionSummary AS
SELECT 
    ms.productType,
    dp.distributorID,
    SUM(dp.quantity) AS totalDistributedQuantity,
    MAX(dp.distributionDate) AS lastDistributionDate
FROM distributionProcess as dp
JOIN milkSeparation as ms
    ON dp.separationID = ms.separationID
GROUP BY ms.productType, dp.distributorID;



-- view to see the complete distribution details
CREATE VIEW vw_DistributionDetails AS
SELECT 
    dp.distributionID,
    dp.separationID,
    ms.productType,
    dp.distributorID,
    dp.quantity AS distributedQuantity,
    dp.distributionDate,
    ms.actualAvailableQuantity AS remainingQuantity
FROM distributionProcess as dp
JOIN milkSeparation as ms
    ON dp.separationID = ms.separationID;

--This view provides detailed daily feed consumption records by shed and type.
CREATE VIEW DailyFeedConsumptionView AS
SELECT 
    fc.feedingDate,
    fc.shedID,
    f.feedType,
    fc.quantity AS QuantityConsumed
FROM 
    feedConsumption fc
JOIN 
    feed f
ON 
    fc.feedID = f.feedID



--This view lists the details of feed purchases from each supplier.
CREATE VIEW SupplierFeedPurchaseView AS
SELECT 
    pf.supplierID,
    f.feedType,
    pf.quantity,
    pf.cost,
    pf.purchaseDate
FROM 
    purchaseFeed as pf
JOIN 
    feed as f
ON 
    pf.feedID = f.feedID;



-- This view lists all employees who are not currently assigned to any shed.
CREATE VIEW EmployeesWithoutShedView AS
SELECT 
    e.employeeID,
    e.fullName,
    jc.jobTitle
FROM 
    employee as e
LEFT JOIN 
    employeeShed as es ON e.employeeID = es.employeeID
JOIN 
    jobClass as jc ON e.jobClassID = jc.jobClassID
WHERE 
    es.shedID IS NULL;




--This view shows employees assigned to each shed, along with their job titles and assignment dates.
CREATE VIEW EmployeeShedDetailsView AS
SELECT 
    e.employeeID,
    e.fullName,
    jc.jobTitle,
    es.shedID,
    es.assignmentDate
FROM 
    employeeShed as es
JOIN 
    employee as e ON es.employeeID = e.employeeID
JOIN 
    jobClass as jc ON e.jobClassID = jc.jobClassID;



-- Execution
-- 1. Monthly Milk Production View
SELECT * 
FROM MonthlyMilkProduction
ORDER BY ProductionMonth ASC, TotalMilkProduced DESC;

-- 2. Employee Shed Assignments View
SELECT * 
FROM EmployeeShedAssignments;

-- 3. Feed Consumption Summary View
SELECT * 
FROM FeedConsumptionSummary;

-- 4. Cow Shed View
SELECT * 
FROM CowShedView;

-- 5. Job Class Payroll Summary View
SELECT * 
FROM JobClassPayrollSummaryView;

-- 6. Employees Without Payroll View
SELECT * 
FROM EmployeesWithoutPayrollView;

-- 7. Current Employee Payroll View
SELECT * 
FROM CurrentEmployeePayrollView;

-- 8. Milk Distribution Summary View
SELECT * 
FROM vw_MilkDistributionSummary;

-- 9. Complete Distribution Details View
SELECT * 
FROM vw_DistributionDetails;

-- 10. Daily Feed Consumption View
SELECT * 
FROM DailyFeedConsumptionView;

-- 11. Supplier Feed Purchase View
SELECT * 
FROM SupplierFeedPurchaseView;

-- 12. Employees Without Shed View
SELECT * 
FROM EmployeesWithoutShedView;

-- 13. Employee Shed Details View
SELECT * 
FROM EmployeeShedDetailsView;






-- 3. Materialized Views (Using Tables)

-- Materialized View: Total milk production and wastage across all sheds
CREATE TABLE mv_TotalMilkProduction (
    shedID INT,
    TotalProduction DECIMAL(10, 2),
    TotalWastage DECIMAL(10, 2),
    LastUpdated DATE
);

-- Populate Materialized View
INSERT INTO mv_TotalMilkProduction
SELECT 
    s.shedID,
    SUM(mp.quantity) AS TotalProduction,
    SUM(ms.wastageQuantity) AS TotalWastage,
    GETDATE() AS LastUpdated
FROM 
    milkProduction as mp
JOIN 
    shed as s ON mp.shedID = s.shedID
LEFT JOIN 
    milkSeparation ms ON mp.productionID = ms.separationID
GROUP BY 
    s.shedID;




-- Materialized View: Distributor-wise milk distribution summary
CREATE TABLE mv_DistributorMilkDistribution (
    distributorID INT,
    TotalDistributed DECIMAL(10, 2),
    LastUpdated DATE
);

-- Populate Materialized View
INSERT INTO mv_DistributorMilkDistribution
SELECT 
    dp.distributorID,
    SUM(dp.quantity) AS TotalDistributed,
    GETDATE() AS LastUpdated
FROM 
    distributionProcess as dp
GROUP BY 
    dp.distributorID;




-- Materialized View: Feed consumption trends
-- Which shed consumed which feed type the most
CREATE TABLE mv_FeedConsumptionTrends (
    shedID INT,
    feedID INT,
    TotalConsumed DECIMAL(10, 2),
    LastUpdated DATE
);

-- Populate Materialized View
INSERT INTO mv_FeedConsumptionTrends
SELECT 
    fc.shedID,
    fc.feedID,
    SUM(fc.quantity) AS TotalConsumed,
    GETDATE() AS LastUpdated
FROM 
    feedConsumption as fc
GROUP BY 
    fc.shedID, fc.feedID



-- Execution

-- Total Milk Production and Wastage
SELECT * 
FROM mv_TotalMilkProduction
ORDER BY TotalProduction DESC;

-- Distributor-wise Milk Distribution Summary
SELECT * 
FROM mv_DistributorMilkDistribution
ORDER BY distributorID;

-- Feed Consumption Trends
SELECT * 
FROM mv_FeedConsumptionTrends
ORDER BY shedID ASC, TotalConsumed DESC;


-- A N A L Y T I C A L	  R E P O R T S

-- 1. Feed Usage and Cost Analysis
/*This report tracks the total feed consumption and associated costs for each feed type 
and shed. */  

SELECT 
    f.feedType,
    s.shedID,
    SUM(fc.quantity) AS TotalQuantityConsumed,
    SUM(fc.quantity * pf.cost / pf.quantity) AS TotalCost
FROM 
    feedConsumption as fc
JOIN 
    feed as f ON fc.feedID = f.feedID
JOIN 
    shed as s ON fc.shedID = s.shedID
JOIN 
    purchaseFeed pf ON f.feedID = pf.feedID
GROUP BY 
    f.feedType, s.shedID
ORDER BY 
    f.feedType, s.shedID;



-- 2. Employee Payroll Analysis by Job Class
/*This report analyzes employee payroll data based on job classifications, including 
salaries and bonuses.*/

SELECT 
    jc.jobTitle,
    COUNT(e.employeeID) AS TotalEmployees,
    SUM(p.salary) AS TotalSalariesPaid,
    SUM(p.bonus) AS TotalBonusesPaid,
    AVG(p.salary) AS AverageSalary,
    MAX(p.salary) AS HighestSalary,
    MIN(p.salary) AS LowestSalary
FROM 
    jobClass jc
LEFT JOIN 
    employee e ON jc.jobClassID = e.jobClassID
LEFT JOIN 
    payroll p ON e.employeeID = p.employeeID
GROUP BY 
    jc.jobTitle
ORDER BY 
    TotalSalariesPaid DESC;





-- 3. Distribution Efficiency by Product Type
/*This report assesses the distribution efficiency of milk products by calculating the 
quantity distributed and wastage.*/

SELECT 
    ms.productType,
    d.distributionArea,
    COUNT(dp.distributionID) AS TotalDistributions,
    SUM(dp.quantity) AS TotalQuantityDistributed,
    SUM(ms.wastageQuantity) AS TotalWastage,
    (SUM(dp.quantity) * 100.0 / SUM(ms.quantity)) AS DistributionEfficiency
FROM 
    distributionProcess dp
JOIN 
    milkSeparation ms ON dp.separationID = ms.separationID
JOIN 
    distributor d ON dp.distributorID = d.distributorID
GROUP BY 
    ms.productType, d.distributionArea
ORDER BY 
    DistributionEfficiency DESC;




-- 4. Supplier Feed Delivery and Quality Report
/*This report evaluates feed delivery performance by suppliers, including total feed 
delivered and cost analysis.*/

SELECT 
    s.fullName AS SupplierName,
    f.feedType,
    SUM(pf.quantity) AS TotalFeedDelivered,
    SUM(pf.cost) AS TotalCost,
    AVG(pf.cost / pf.quantity) AS AverageCostPerUnit,
    MIN(pf.purchaseDate) AS FirstDeliveryDate,
    MAX(pf.purchaseDate) AS LastDeliveryDate
FROM 
    supplier s
JOIN 
    purchaseFeed pf ON s.supplierID = pf.supplierID
JOIN 
    feed f ON pf.feedID = f.feedID
GROUP BY 
    s.fullName, f.feedType
ORDER BY 
    TotalFeedDelivered DESC;








-- S U B Q U E R I E S

--1. Find Employees by Job Title
--Write a query to retrieve the names of all employees working in a job class 
--with a base salary greater than 50,000.
select 
e.fullName,
jc.jobtitle
from employee as e
join jobClass as jc on e.jobClassID = jc.jobClassID
where jc.baseSalary > 50000


--2. Most Expensive Feed Purchase
--Find the details of the feed purchase (purchase ID, feed type, and supplier name)
--with the highest cost
select (select feedType from feed where feed.feedID = purchaseFeed.feedID) as feedType,
* from purchaseFeed
--where cost = (select top 1 cost from purchaseFeed order by cost desc)
where cost = (SELECT MAX(cost) FROM purchaseFeed)




--3. Average Age of Cows in Sheds
--Write a query to calculate the average age of cows assigned to each shed.
select shedID,
avg(age) as avgAge
from cow
join  cowshed on cow.cowID = cowShed.cowID
group by shedID


--4. Employees Assigned to More Than One Shed
--List the employees (name and phone number) assigned to more than one shed.

select (select fullName from employee 
where employee.employeeID = employeeShed.employeeID) as emp_Name,
(select phoneNumber from employee 
where employee.employeeID = employeeShed.employeeID) as phoneNumber,
* from employeeShed
where shedID >(select count(shedID) from employeeShed)

--Another Method
SELECT 
    E.fullName, 
    E.phoneNumber
FROM 
    employee E
JOIN 
    employeeShed ES ON E.employeeID = ES.employeeID
GROUP BY 
    E.employeeID, E.fullName, E.phoneNumber
HAVING 
    COUNT(ES.shedID) > 1;




--5. Shed with Highest Milk Production
--Retrieve the shed ID and production date of the day with the highest milk production quantity.

select productionDate, shedID, quantity 
from milkProduction
where quantity = (SELECT MAX(quantity) FROM milkProduction)




--6. Distributors in a Specific Area
--List the names and phone numbers of all distributors operating in the same 
--area as the distributor with the ID 3.
select count(fullName),
distributionArea from distributor
group by distributionArea

select d.fullName,
d.phoneNumber
from distributor as d
where d.distributionArea = (select distributionArea
from distributor
where distributorID = 3)





--7. Underutilized Feed
--Find feed types that have been purchased but never consumed in any shed.
select (select feedType from feed where feed.feedID = feedConsumption.feedID) as feedType
from feedConsumption
where feedConsumptionID is null

select f.feedType
from feed as f
where 
not exists (select 1
from feedConsumption as fc
where f.feedID = fc.feedID)



--8. Total Salary Expenditure
--Calculate the total salary expenditure (including bonuses) 
--for each employee using subqueries.
create function totalExpenditure()
returns table
as
return (select sum(salary) as totalPaidSalary,
employeeID
from payroll
group by employeeID);
--order by totalPaidSalary desc);

select * from totalExpenditure() order by totalPaidSalary desc




--9. Employee Assigned Longest in Shed
--Find the employee name and shed ID for the longest employee-shed assignment duration.
select employeeID,
(select fullName from employee
where employee.employeeID = employeeShed.employeeID)as empName,
shedID
from employeeShed
where assignmentDate =(select top 1 assignmentDate from employeeShed
order by assignmentDate)
-- For testing above query
update employeeShed
set assignmentDate = '2021-12-01' where employeeID = 1

--Another Method
create function findOldestEmployee()
returns table
as return(select e.fullName,
es.shedID,
es.assignmentDate
from employeeShed as es
join employee as e on e.employeeID = es.employeeID
where assignmentDate = (select MIN(assignmentDate) from employeeShed));

select * from findOldestEmployee()


--10. Cows Never Assigned to a Shed
--List all cows (ID and breed) that have never been assigned to any shed.
select (select cowID from cow where cow.cowID = cowShed.cowID ) as cowID , 
(select breed from cow where cow.cowID = cowShed.cowID ) as breed 
from cowShed
where assignmentID is null

-- Another Way
SELECT C.cowID, C.breed
FROM cow C
WHERE NOT EXISTS (
    SELECT 1 
    FROM cowShed CS 
    WHERE C.cowID = CS.cowID
);


