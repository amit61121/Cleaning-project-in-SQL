-- data cleaning
-- import all the data from csv with table data import wizard

select *
from layoffs;

-- what need to do:
-- 1. Remove Duplicates
-- 2. Standardize the Data - issues like spelling
-- 3. Null Values or blank values
-- 4. Remove Any Columns or rows

-- Making a copy for work
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;


-- 1. Remove Duplicates
-- create row_number for unique value
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs;

-- Search for duplicate values
with duplicate_cte as 
(
select *,
row_number() over(
partition by company,location , industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = "Casper";

-- create new table for delete duplicate
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2
where row_num > 1;

-- insert all the data from layoffs_staging to the new table
insert into layoffs_staging2
select *,
row_number() over(
partition by company,location , industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs;

-- delete duplicate
delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;


-- 2. Standardize the Data
select company, (trim(company))
from layoffs_staging2;

-- Change unnecessary space
update layoffs_staging2
set company = trim(company);

-- check misspelling in industry
select distinct industry
from layoffs_staging2
;
-- change misspelling in Crypto
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- check misspelling in country
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

-- change misspelling in United States
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';


-- change Date format from string to date
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2
;
-- change Date
update layoffs_staging2
set  `date` = str_to_date(`date`, '%m/%d/%Y');

-- change Date format
alter table layoffs_staging2
modify column `date` date;


-- 3. Null Values or blank values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- update blank to null
update layoffs_staging2
set industry = null
where industry = "";


select *
from layoffs_staging2
where industry is null 
or industry = "";

select *
from layoffs_staging2
where company like "Bally%";

-- check if can Combining the empty and non-empty industry columns
select t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = "")
and t2.industry is not null;

-- Combining the empty and non-empty industry columns
update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

-- 4. Remove Any Columns or rows

-- delete null rows in laid off
delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

-- drop row_num column
alter table layoffs_staging2
drop column row_num;





























