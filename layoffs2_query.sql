
-- DATA CLEANING & EXPLORATORY DATA ANALYSIS

-- ----------------------------------------------------------------------------------------------------
-- Data Cleaning

-- Steps
-- 1. Populate/replace the blanks and the nulls where possible.
-- 2. Remove duplicates.
-- 3. Standardize the data. 
-- 4. Remove columns or rows that are irrelevant.

SELECT * 
FROM layoffs;

-- Create a duplicate working table to avoid tampering with the original data

CREATE TABLE layoffs1
LIKE layoffs;

SELECT * 
FROM layoffs1;

INSERT INTO layoffs1
SELECT *
FROM layoffs;

-- 1. Populate/replace the blanks and the nulls where possible.

SELECT * 
FROM layoffs1
WHERE company LIKE 'Bally%';

UPDATE layoffs1
SET industry = 'Entertainment'
WHERE company LIKE 'Bally%';

UPDATE layoffs1
SET industry = NULL
WHERE industry = '';

SELECT lay1.industry, lay2.industry
FROM layoffs1 lay1
JOIN layoffs1 lay2
	ON lay1.company = lay2.company
WHERE lay1.industry IS NULL
AND lay2.industry IS NOT NULL;

UPDATE layoffs1 lay1
JOIN layoffs1 lay2
	ON lay1.company = lay2.company
SET lay1.industry = lay2.industry
WHERE lay1.industry IS NULL AND lay2.industry IS NOT NULL;

SELECT * 
FROM layoffs1;

-- 2. Remove duplicates.

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_id
FROM layoffs1;

WITH cte_duplicate AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_id
FROM layoffs1
)
SELECT *
FROM cte_duplicate
WHERE row_id >= 2;

CREATE TABLE `layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_id` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_id
FROM layoffs1;

SELECT *
FROM layoffs2;

DELETE
FROM layoffs2
WHERE row_id > 1;

SELECT *
FROM layoffs2
WHERE row_id > 1;

-- 3. Standardize the data. 

SELECT *
FROM layoffs2;

SELECT company, TRIM(company)
FROM layoffs2;

UPDATE layoffs2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs2
ORDER BY 1;

UPDATE layoffs2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs2
ORDER BY 1;

UPDATE layoffs2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs2;

UPDATE layoffs2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs2
MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs2
MODIFY COLUMN percentage_laid_off FLOAT;

-- 4. Remove columns or rows that are irrelevant.

ALTER TABLE layoffs2
DROP COLUMN row_id;

SELECT *
FROM layoffs2
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;

DELETE
FROM layoffs2
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;

SELECT stage, COUNT(stage)
FROM layoffs2
GROUP BY stage;

-- ---------------------------------------------------------------------------------------------------

-- ---------------------------------------------------------------------------------------------------
 
 -- Exploratory Data Analysis
 
SELECT *
FROM layoffs2;

-- The total layoff by company.
SELECT company, SUM(total_laid_off) AS total_layoff
FROM layoffs2
GROUP BY company
ORDER BY 2 DESC;

-- The total layoff by industry.
SELECT industry, SUM(total_laid_off) AS total_layoff
FROM layoffs2
GROUP BY industry
ORDER BY 2 DESC;

-- The total layoff by stage.
SELECT stage, SUM(total_laid_off) AS total_layoff
FROM layoffs2
GROUP BY stage
ORDER BY 2 DESC;

-- The total layoff by country.
SELECT country,  COUNT(DISTINCT(company)) AS number_of_companies, SUM(total_laid_off) AS total_layoff 
FROM layoffs2
GROUP BY country
ORDER BY 3 DESC; 

-- The total funds raised by company in millions.
SELECT DISTINCT(company), funds_raised_millions
FROM layoffs2
GROUP BY company, funds_raised_millions
ORDER BY 2 DESC;

-- The total funds raised by company in millions yearly compared to their layoffs.
SELECT DISTINCT(company), funds_raised_millions, YEAR(`date`), SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY company, funds_raised_millions, YEAR(`date`)
ORDER BY 2 DESC;

-- Total yearly layoffs.
SELECT YEAR(`date`), SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Total yearly layoffs by number of months.
SELECT YEAR(`date`), COUNT(DISTINCT(DATE_FORMAT(`date`, '%m'))) AS number_of_months, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Total monthly layoffs.
SELECT DATE_FORMAT(`date`, '%Y') AS year, DATE_FORMAT(`date`, '%m') AS month, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
GROUP BY year, month
ORDER BY 3 DESC;

-- Layoff year range.
SELECT MIN(`date`), MAX(`date`)
FROM layoffs2;

-- Minimum and maximum layoffs.
SELECT MIN(total_laid_off), MAX(total_laid_off)
FROM layoffs2;

-- Total layoffs.
SELECT SUM(total_laid_off)
FROM layoffs2;

-- Average percentage layoffs.
SELECT company, ROUND(AVG(percentage_laid_off),2) AS avg_percent_laid_off
FROM layoffs2
GROUP BY company
ORDER BY 2 DESC;

-- The companies with 100% layoff compared to their funding.
SELECT DISTINCT(company), percentage_laid_off, funds_raised_millions, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
WHERE percentage_laid_off = 1
GROUP BY company, percentage_laid_off, funds_raised_millions
ORDER BY 3 DESC;

-- Top 5 monthly total layoffs by company.
WITH monthly_cte AS (
SELECT company, DATE_FORMAT(`date`, '%Y-%m') AS yearmonth, SUM(total_laid_off) AS total_layoff
FROM layoffs2
GROUP BY company, yearmonth
ORDER BY 3 DESC
),
monthly_ranking_cte AS (
SELECT *,
DENSE_RANK() OVER(PARTITION BY yearmonth ORDER BY total_layoff DESC) AS ranking
FROM monthly_cte
WHERE yearmonth IS NOT NULL
)
SELECT *
FROM monthly_ranking_cte
WHERE ranking <= 5;

-- Yearly rolling total of layoffs by company.
SELECT DISTINCT company, DATE_FORMAT(`date`, '%Y') AS year, 
SUM(total_laid_off) OVER(PARTITION BY company, DATE_FORMAT(`date`, '%Y')  ORDER BY DATE_FORMAT(`date`, '%Y')) AS yearly_total_layoff
FROM layoffs2
ORDER BY 3 DESC;

-- Yearly and monthly cumulative layoffs ascendingly.
WITH rolling_total_cte AS (
SELECT DATE_FORMAT(`date`, '%Y-%m') AS month, SUM(total_laid_off) AS total_layoffs
FROM layoffs2
WHERE DATE_FORMAT(`date`, '%Y-%m') IS NOT NULL
GROUP BY month
ORDER BY 1 ASC
)
SELECT month, total_layoffs, SUM(total_layoffs) OVER(ORDER BY month, total_layoffs) AS rolling_total
FROM rolling_total_cte;

-- Top 5 highest yearly company layoffs
WITH years_cte AS (
SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_layoff
FROM layoffs2
GROUP BY company, years
),
company_ranking_cte AS (
SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_layoff DESC) AS ranking
FROM years_cte
WHERE years IS NOT NULL
)
SELECT *
FROM company_ranking_cte
WHERE ranking <= 5;

-- -----------------------------------------------------------------------------------------------------------

