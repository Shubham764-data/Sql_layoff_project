SELECT 
    *
FROM
    layoffs;

CREATE TABLE laoff_stage LIKE layoffs;
#/ Insert Data into the table using the Statement/#

Insert laoff_stage
Select * from layoffs;

SELECT 
    *
FROM
    laoff_stage;

#/Check the duplicate from the table using ROW_Number/#

Select *,
Row_number() over
(Partition By company, location, industry, total_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from laoff_stage;

/* As we applied partition by on columns just to check the duplicacy of every record in each columns. Now we
use CTE(Common Table Expression just to check number of duplicates */

With duplicate_cte as
(
Select *,
Row_number() over
(Partition By company, location, industry, total_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from laoff_stage
) Select * from
duplicate_cte
where row_num > 1; 

/*Now we make sure we can delete the duplicate data from the table*/

With duplicate_cte as
(
Select *,
Row_number() over
(Partition By company, location, industry, total_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from laoff_stage
) Delete from
duplicate_cte
where row_num > 1; 


/*We cannot delete the duplicates by applying this fucntion because it shows an error that the Target table
duplicate_cte of the DELETE is not updatable.
So we have to make change in the table and add the row_num as the new column and delete the data where row_num are over 2*/

Alter table laoff_stage add column row_num int;
SELECT 
    *
FROM
    laoff_stage;

CREATE TABLE laoff_stage1 LIKE laoff_stage;

SELECT 
    *
FROM
    laoff_stage1;

Insert laoff_stage1
Select `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
Row_number() over
(Partition By company, location, industry, total_laid_off, 
'date', stage, country, funds_raised_millions) 
as row_num
from laoff_stage;

SELECT 
    *
FROM
    laoff_stage1;


/*We add the new column row_num by adding new table with inserting all the data using the above function
and check that all the data in the table is corrected and we can now delete the duplicates who has
row_number over 2*/

DELETE FROM laoff_stage1 
WHERE
    row_num = 2;

SELECT 
    *
FROM
    laoff_stage1
WHERE
    row_num = 1;

-- Standardizing the data--

SELECT DISTINCT
    (Company)
FROM
    laoff_stage1;

-- We Trim the space from the complete coloumn and update the Company column

SELECT DISTINCT
    (RTRIM(Company))
FROM
    laoff_stage1;

UPDATE laoff_stage1 
SET 
    company = TRIM(Company);

-- Now we check the Industry coloumn what we can make changes in the Industry Coloumn

SELECT DISTINCT
    (industry)
FROM
    laoff_stage1;

-- Now I see that crpto is appearing thrice so we should check it and update it

SELECT 
    *
FROM
    laoff_stage1
WHERE
    industry LIKE '%crypto%';

UPDATE laoff_stage1 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE '%crypto%';

-- Now we see the country coloumn and if there is any issue we fix it

SELECT DISTINCT
    (country)
FROM
    laoff_stage1;

-- So I see the country united states two times as one is written with dot at the end

SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM Country)
FROM
    laoff_stage1;

UPDATE laoff_stage1 
SET 
    country = TRIM(TRAILING '.' FROM Country)
WHERE
    country LIKE 'United States%';

-- As now i saw my date coloumn it is in text so we need to change that coloumn in the date format


SELECT 
    date, STR_TO_DATE(date, '%m/%d/%Y')
FROM
    laoff_stage1;

UPDATE laoff_stage1 
SET 
    date = STR_TO_DATE(date, '%m/%d/%Y');

-- Set the date column to date format

Alter table laoff_stage1
Modify column date date;

SELECT 
    *
FROM
    laoff_stage1;

-- Now we check the Null values in the table

SELECT 
    *
FROM
    laoff_stage1
WHERE
    industry IS NULL OR total_laid_off = '';

UPDATE laoff_stage1 
SET 
    total_laid_off = NULL
WHERE
    total_laid_off = '';

SELECT 
    t1.industry, t2.industry
FROM
    laoff_stage1 t1
        JOIN
    laoff_stage1 t2 ON t1.company = t2.company
        AND t1.industry = t2.industry
WHERE
    (t1.industry IS NULL
        OR t1.industry = ''
        AND t2.industry IS NOT NULL);
 
 -- Check the Null values
 
SELECT 
    *
FROM
    laoff_stage1
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;
 
 -- Delete the data where we see the Null values 
 
DELETE FROM laoff_stage1 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
    
    
SELECT 
    *
FROM
    laoff_stage1;
    
-- Drop the row_num column from the table, now we don't need this column 
    
    Alter table laoff_stage1
    Drop column row_num;
    
    
    
    Select country, sum(total_laid_off)
    from laoff_stage1
    group by country
    order by 2 Desc;
    
    
    Select Year(date), sum(total_laid_off)
    from laoff_stage1
    group by Year(date)
    order by 1 Desc;
    
    
    Select Trim(substring(date, 1,7)) as MONTH, Sum(total_laid_off) as total_off
    from laoff_stage1
    where substring(date, 1,7) is not null
    Group by Month
    Order by 1 Desc;
    
    
    With Rolling_total as 
    (Select Trim(substring(date, 1,7)) as MONTH, Sum(total_laid_off) as total_off
    from laoff_stage1
    where substring(date, 1,7) is not null
    Group by Month
    Order by 1 Desc
    )
    Select Month, total_off,
    sum(total_off) over (Order By Month)
    from Rolling_total;
    
    
    With company_year (company, years, total_laid_off) AS
    (
    Select company, year (date), sum(total_laid_off)
    from laoff_stage1
    Group by company, Year(date)
    )
    Select *,
    Dense_rank() over(Partition by years order by total_laid_off Desc) as Ranking
    from company_year
    where years is not null
    order by Ranking ASC;