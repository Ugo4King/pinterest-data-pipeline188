-- Find the most popular Pinterest category people post to based on their country.
SELECT
  country,
  category,
  COUNT(*) AS category_count
FROM 
  full_data_set
GROUP BY
  country,
  category
ORDER BY
  category_count DESC;

-- Find how many posts each category had between 2018 and 2022.

SELECT 
  category, 
  YEAR(geo_timestamp) AS year,
  COUNT(*) AS category_count
FROM full_data_set
WHERE YEAR(geo_timestamp) BETWEEN 2018 AND 2022
GROUP BY
  category,
  YEAR(geo_timestamp)
ORDER BY
  category_count;



SELECT country, poster_name, follower_count
FROM (
    SELECT country, poster_name, follower_count,
           ROW_NUMBER() OVER (PARTITION BY country ORDER BY follower_count DESC) AS row_num
    FROM full_data_set
) AS ranked_data
WHERE row_num = 1
ORDER BY follower_count DESC;


SELECT 
    country,
    MAX(follower_count) AS max_follower_count
FROM full_data_set
GROUP BY 
    country
ORDER BY
  max_follower_count DESC
LIMIT 1;


-- What is the most popular category people post to based on the following age groups:

SELECT 
  age_group,
  category,
  category_count
FROM (
  SELECT 
    CASE 
      WHEN age BETWEEN 18 AND 24 THEN '18-24'
      WHEN age BETWEEN 25 AND 35 THEN '25-35'
      WHEN age BETWEEN 36 AND 50 THEN '36-50'
      ELSE '+50'
    END AS age_group,
    category,
    COUNT(*) AS category_count,
    ROW_NUMBER() OVER (PARTITION BY 
      CASE 
        WHEN age BETWEEN 18 AND 24 THEN '18-24'
        WHEN age BETWEEN 25 AND 35 THEN '25-35'
        WHEN age BETWEEN 36 AND 50 THEN '36-50'
        ELSE '+50'
      END 
      ORDER BY COUNT(*) DESC) AS rn
  FROM full_data_set
  GROUP BY 
    CASE 
      WHEN age BETWEEN 18 AND 24 THEN '18-24'
      WHEN age BETWEEN 25 AND 35 THEN '25-35'
      WHEN age BETWEEN 36 AND 50 THEN '36-50'
      ELSE '+50'
    END,
    category
) AS ranked_data
WHERE rn = 1
ORDER BY category_count DESC;



-- What is the median follower count for users in the following age groups:

SELECT 
  CASE 
    WHEN age BETWEEN 18 AND 24 THEN '18-24'
    WHEN age BETWEEN 25 AND 35 THEN '25-35'
    WHEN age BETWEEN 36 AND 50 THEN '36-50'
    ELSE '+50'
  END AS age_group,
  percentile_approx(follower_count, 0.5) AS median_follower_count
FROM full_data_set
GROUP BY age_group
ORDER BY median_follower_count DESC;


-- Find how many users have joined between 2015 and 2020.

SELECT  
  YEAR(geo_timestamp) AS post_year,
  COUNT(user_name) AS number_users_joined
FROM full_data_set
WHERE YEAR(geo_timestamp) BETWEEN 2015 AND 2020
GROUP BY
  YEAR(geo_timestamp)
ORDER BY
  number_users_joined DESC;

-- Find the median follower count of users have joined between 2015 and 2020.

SELECT
    YEAR(geo_timestamp) AS post_year,
    percentile_approx(follower_count, 0.5) AS median_follower_count
FROM full_data_set
WHERE YEAR(geo_timestamp) BETWEEN 2015 AND 2020
GROUP BY
  YEAR(geo_timestamp)
ORDER BY
  median_follower_count DESC;

--Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.

SELECT 
  CASE 
    WHEN age BETWEEN 18 AND 24 THEN '18-24'
    WHEN age BETWEEN 25 AND 35 THEN '25-35'
    WHEN age BETWEEN 36 AND 50 THEN '36-50'
    ELSE '+50'
  END AS age_group,
  YEAR(geo_timestamp) AS post_year,
  percentile_approx(follower_count, 0.5) AS median_follower_count
FROM full_data_set
WHERE YEAR(geo_timestamp) BETWEEN 2015 AND 2020
GROUP BY CASE 
    WHEN age BETWEEN 18 AND 24 THEN '18-24'
    WHEN age BETWEEN 25 AND 35 THEN '25-35'
    WHEN age BETWEEN 36 AND 50 THEN '36-50'
    ELSE '+50'
  END,
  YEAR(geo_timestamp)  
ORDER BY median_follower_count DESC;

