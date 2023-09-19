USE mavenfuzzyfactory;

# Let's run some basic  queries to understand the data --

/*
Analyze Website Sessions Table
PRIMARY KEY IS website_session_id
*/
SELECT *
FROM website_sessions
LIMIT 100;

SELECT 
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT website_session_id) AS sessions,
    DATEDIFF(MAX(created_at),MIN(created_at)) AS life_days_of_data,
    ROUND(DATEDIFF(MAX(created_at),MIN(created_at)) / 365,0) AS life_years_of_data,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT is_repeat_session ) n_unique_repteat_sessions, # 0 or 1
    COUNT(DISTINCT utm_source) AS n_unique_utm_source, # gsearch, bsearch, socialbook
    COUNT(DISTINCT utm_campaign) AS n_unique_utm_campaign, # brand, nonbrand, pilot, desktop_targeted
    COUNT(DISTINCT utm_content) AS n_unique_utm_content, # g_ad_1, b_ad_1, g_ad_2, b_ad_2, social_ad_1, social_ad_2
    COUNT(DISTINCT device_type) AS n_unique_device_type, # mobile, desktop
    COUNT(DISTINCT http_referer) AS n_unique_http_referer # https://www.gsearch.com, https://www.bsearch.com, https://www.socialbook.com
FROM website_sessions;

# count null values for each column
SELECT
	COUNT(CASE WHEN website_session_id IS NULL THEN 1 ELSE NULL END) AS website_session_id ,
    COUNT(CASE WHEN created_at IS NULL THEN 1 ELSE NULL END) AS created_at ,
    COUNT(CASE WHEN user_id IS NULL THEN 1 ELSE NULL END) AS user_id ,
    COUNT(CASE WHEN is_repeat_session IS NULL THEN 1 ELSE NULL END) AS is_repeat_session ,
    COUNT(CASE WHEN utm_source IS NULL THEN 1 ELSE NULL END) AS utm_source ,
    COUNT(CASE WHEN utm_campaign IS NULL THEN 1 ELSE NULL END) AS utm_campaign ,
    COUNT(CASE WHEN device_type IS NULL THEN 1 ELSE NULL END) AS device_type ,
    COUNT(CASE WHEN http_referer IS NULL THEN 1 ELSE NULL END) AS http_referer 
FROM
	website_sessions;

# create procedure take input(column_name) to aggregate some matrices 

DROP PROCEDURE matrices;
DELIMITER $$
CREATE PROCEDURE matrices(IN columnName VARCHAR(20))
BEGIN
	SELECT
         CASE
			WHEN columnName = 'is_repeat_session' THEN is_repeat_session 
            WHEN columnName = 'utm_source' THEN utm_source
            WHEN columnName = 'utm_campaign' THEN utm_campaign
            WHEN columnName = 'utm_content' THEN utm_content
            WHEN columnName = 'device_type' THEN device_type
            WHEN columnName = 'http_referer' THEN http_referer
            ELSE 'NOT EXIST'
         END AS columnName,
		 COUNT(DISTINCT website_session_id) AS total_sessions,
         COUNT(DISTINCT user_id) AS total_users
	FROM 
		website_sessions
	GROUP BY 1
    ORDER BY total_sessions , total_users DESC;
END $$
DELIMITER ;

# You can now put any column that you want to group by it
CALL matrices('is_repeat_session');
CALL matrices('utm_source');
CALL matrices('utm_campaign');
CALL matrices('utm_content');
CALL matrices('device_type');
CALL matrices('http_referer');


#  create procedure take input(date) to aggregate trands --

DROP PROCEDURE DateMatrices;
DELIMITER $$
CREATE PROCEDURE DateMatrices(IN matrice VARCHAR(20))
BEGIN
	SELECT
          CASE
			WHEN matrice = 'year' THEN sub.year 
            WHEN matrice = 'month' THEN sub.month
            WHEN matrice = 'quarter' THEN sub.quarter
            WHEN matrice = 'week' THEN sub.week
            WHEN matrice = 'hour' THEN sub.hour
            WHEN matrice = 'minute' THEN sub.minute
            ELSE 'NOT EXIST'
         END AS DateMatric,
          COUNT(DISTINCT sub.website_session_id) AS total_sessions,
		  COUNT(DISTINCT sub.user_id) AS total_users
		  FROM
          (
           SELECT
				YEAR(created_at) AS year,
				CONCAT(YEAR(created_at),' & ',MONTH(created_at)) AS month ,
				CONCAT(YEAR(created_at),' & ',QUARTER(created_at)) AS quarter,
				MIN(DATE(created_at)) AS week ,
				CONCAT(YEAR(created_at),' & ',MONTH(created_at),' & ', DAYNAME(created_at),' & ',HOUR(created_at)) AS hour,
				CONCAT(YEAR(created_at),' & ',MONTH(created_at),' & ', DAYNAME(created_at),' & ',HOUR(created_at),' & ',MINUTE(created_at)) AS minute,
		         website_session_id,
		         user_id
		   FROM
			    website_sessions
		   GROUP BY website_session_id,user_id
           ) AS sub

	GROUP BY 1;
END $$
DELIMITER ;

# Now, You can now put any date matrice that you want to group by it
CALL DateMatrices('year');
CALL DateMatrices('month');
CALL DateMatrices('quarter');
CALL DateMatrices('week');
CALL DateMatrices('hour');
CALL DateMatrices('minute');

# Monthly Trands by average of the sessions for each utm source
SELECT 
     sub.yr,
     sub.mo,
	 ROUND(AVG(CASE WHEN sub.utm_source = 'bsearch' THEN sub.sessions ELSE NULL END),2) AS ave_sessions_bsearch,
     ROUND(AVG(CASE WHEN sub.utm_source = 'gsearch' THEN sub.sessions ELSE NULL END),2) AS ave_sessions_gsearch, -- socialbook
     ROUND(AVG(CASE WHEN sub.utm_source = 'socialbook' THEN sub.sessions ELSE NULL END),2) AS ave_sessions_socialbook,
     ROUND(AVG(CASE WHEN sub.utm_source IS NULL THEN sub.sessions ELSE NULL END),2) AS ave_sessions_free
FROM
(
SELECT
    DATE(created_at) AS date,
	YEAR(created_at) AS yr,
    MONTH(created_at) AS mo,
    utm_source,
    COUNT(*) AS sessions
FROM
	website_sessions
GROUP BY 1,2,3,4
) AS sub
GROUP BY 1,2
ORDER BY 1,2;


# Quarterly Trands by average of the sessions for each utm source
SELECT 
     sub.yr,
     sub.qr,
	 ROUND(AVG(CASE WHEN sub.utm_source = 'bsearch' THEN sub.sessions ELSE NULL END),2) AS ave_sessions_bsearch,
     ROUND(AVG(CASE WHEN sub.utm_source = 'gsearch' THEN sub.sessions ELSE NULL END),2) AS ave_sessions_gsearch, -- socialbook
     ROUND(AVG(CASE WHEN sub.utm_source = 'socialbook' THEN sub.sessions ELSE NULL END),2) AS ave_sessions_socialbook,
     ROUND(AVG(CASE WHEN sub.utm_source IS NULL THEN sub.sessions ELSE NULL END),2) AS ave_sessions_free
FROM
(
SELECT
    DATE(created_at) AS date,
	YEAR(created_at) AS yr,
    QUARTER(created_at) AS qr,
    utm_source,
    COUNT(*) AS sessions
FROM
	website_sessions
GROUP BY 1,2,3,4
) AS sub
GROUP BY 1,2
ORDER BY 1,2;

# How many time the user repeate the sessions
SELECT
	is_repeat_session,
    COUNT(*) AS sessions,
    COUNT(DISTINCT user_id ) AS users
FROM
	website_sessions
GROUP BY 1;

# Identify users who have repeated sessions and life value for each one
SET @max_date = (SELECT MAX(created_at) FROM  website_sessions);
SELECT
	user_id,
    COUNT(CASE WHEN is_repeat_session = 1 THEN website_session_id ELSE NULL END) AS repeat_session,
    DATEDIFF(@max_date ,MAX(created_at)) AS days_from_last_session,
    DATEDIFF(MAX(created_at),MIN(created_at)) AS user_life_value_days,
    ROUND(DATEDIFF(MAX(created_at),MIN(created_at)) / 30,0) AS user_life_value_months,
    COUNT(CASE WHEN is_repeat_session = 1 THEN website_session_id ELSE NULL END) / DATEDIFF(MAX(created_at),MIN(created_at)) AS avg_session_per_day,
    COUNT(CASE WHEN is_repeat_session = 1 THEN website_session_id ELSE NULL END) / ROUND(DATEDIFF(MAX(created_at),MIN(created_at)) / 30,0) AS avg_session_per_month
FROM
	website_sessions
GROUP BY 1
HAVING repeat_session <> 0
ORDER BY user_life_value_days DESC;
-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Analyze Orders Table
PRIMARY KEY IS order_id
FOREIGN KEY IS website_session_id REFERENCE to website_sessions table (website_session_id)
FOREIGN KEY IS product_id REFERENCE to products table (product_id)
*/
SELECT *
FROM orders
LIMIT 100;

# Describe of the data
SELECT 
    COUNT(*) AS orders,
    DATEDIFF(MAX(created_at),MIN(created_at)) AS life_days_of_data,
    ROUND(DATEDIFF(MAX(created_at),MIN(created_at)) / 365,0) AS life_years_of_data,
    COUNT(DISTINCT user_id) AS users,
    COUNT(DISTINCT primary_product_id ) n_unique_product, # 1,2,3,4
    COUNT(DISTINCT items_purchased) AS n_unique_items_purchased, # 1,2
    SUM(price_usd) AS total_price, 
    MIN(price_usd) AS min_price,
    MAX(price_usd) AS max_price,
    ROUND(AVG(price_usd),2) AS ave_price,
    ROUND(STD(price_usd),2) AS std_price,
    SUM(cogs_usd) AS total_cogs, 
    MIN(cogs_usd) AS min_cogs,
    MAX(cogs_usd) AS max_cogs,
    ROUND(AVG(cogs_usd),2) AS ave_cogs,
    ROUND(STD(cogs_usd),2) AS std_cogs,
    ROUND(SUM(cogs_usd) / COUNT(DISTINCT order_id),2) AS aov,
    ROUND(SUM(price_usd) - SUM(cogs_usd),2) AS margin,
    ROUND((SUM(price_usd) - SUM(cogs_usd)) / SUM(price_usd) ,2) AS profit_margin
FROM orders;

# count null values for each column
SELECT
	COUNT(CASE WHEN order_id IS NULL THEN 1 ELSE NULL END) AS order_id ,
    COUNT(CASE WHEN created_at IS NULL THEN 1 ELSE NULL END) AS created_at ,
    COUNT(CASE WHEN website_session_id IS NULL THEN 1 ELSE NULL END) AS website_session_id ,
    COUNT(CASE WHEN user_id IS NULL THEN 1 ELSE NULL END) AS user_id ,
    COUNT(CASE WHEN primary_product_id IS NULL THEN 1 ELSE NULL END) AS primary_product_id ,
    COUNT(CASE WHEN items_purchased IS NULL THEN 1 ELSE NULL END) AS items_purchased ,
    COUNT(CASE WHEN price_usd IS NULL THEN 1 ELSE NULL END) AS price_usd ,
    COUNT(CASE WHEN cogs_usd IS NULL THEN 1 ELSE NULL END) AS cogs_usd 
FROM
	orders;
    
-- create procedure take input(column_name) to aggregate some matrices --
-- Join session table with orders table
DROP PROCEDURE session_and_orders;
DELIMITER $$
CREATE PROCEDURE session_and_orders(IN columnName VARCHAR(20))
BEGIN
	SELECT
         CASE
			WHEN columnName = 'is_repeat_session' THEN w.is_repeat_session 
            WHEN columnName = 'utm_source' THEN w.utm_source
            WHEN columnName = 'utm_campaign' THEN w.utm_campaign
            WHEN columnName = 'utm_content' THEN w.utm_content
            WHEN columnName = 'device_type' THEN w.device_type
            WHEN columnName = 'http_referer' THEN w.http_referer
            WHEN columnName = 'primary_product_id' THEN o.primary_product_id
            WHEN columnName = 'item_purchased' THEN o.items_purchased
            ELSE 'NOT EXIST'
         END AS columnName,
		 COUNT(DISTINCT w.website_session_id) AS total_sessions,
         COUNT(DISTINCT w.user_id) AS total_users,
         COUNT(DISTINCT o.order_id) AS total_orders,
         SUM(o.price_usd) AS revenue,
         SUM(o.cogs_usd) AS total_cogs,
         AVG(o.price_usd) AS avg_price,
         ROUND(SUM(o.price_usd) - SUM(o.cogs_usd),2) AS margin,
         ROUND((SUM(o.price_usd) - SUM(o.cogs_usd)) / SUM(o.price_usd) ,4) AS profit_margin,
         ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT w.website_session_id),2) AS convertion_rate,
         ROUND(SUM(o.price_usd) / COUNT(DISTINCT o.order_id),2) AS avg_life_value
	FROM 
		website_sessions w
    LEFT JOIN
		orders o
	ON w.website_session_id = o.website_session_id
	GROUP BY 1
    ORDER BY total_sessions , total_users DESC;
END $$
DELIMITER ;

CALL session_and_orders('device_type');


--  create procedure take input(date) to aggregate trands --

DROP PROCEDURE date_session_orders;
DELIMITER $$
CREATE PROCEDURE date_session_orders(IN matrice VARCHAR(20))
BEGIN
	SELECT
          CASE
			WHEN matrice = 'year' THEN sub.year 
            WHEN matrice = 'month' THEN sub.month
            WHEN matrice = 'quarter' THEN sub.quarter
            WHEN matrice = 'week' THEN sub.week
            WHEN matrice = 'hour' THEN sub.hour
            WHEN matrice = 'minute' THEN sub.minute
            ELSE 'NOT EXIST'
         END AS DateMatric,
		 COUNT(DISTINCT website_session_id) AS total_sessions,
         COUNT(DISTINCT user_id) AS total_users,
         COUNT(DISTINCT order_id) AS total_orders,
         SUM(price_usd) AS revenue,
         SUM(cogs_usd) AS total_cogs,
         ROUND(AVG(price_usd),2) AS avg_price,
         ROUND(SUM(price_usd) - SUM(cogs_usd),2) AS margin,
         ROUND((SUM(price_usd) - SUM(cogs_usd)) / SUM(price_usd) ,4) AS profit_margin,
         ROUND(COUNT(DISTINCT order_id) / COUNT(DISTINCT website_session_id),2) AS convertion_rate,
         ROUND(SUM(price_usd) / COUNT(DISTINCT order_id),2) AS avg_life_value
		  FROM
          (
           SELECT
				YEAR(w.created_at) AS year,
				CONCAT(YEAR(w.created_at),' & ',MONTH(w.created_at)) AS month ,
				CONCAT(YEAR(w.created_at),' & ',QUARTER(w.created_at)) AS quarter,
				MIN(DATE(w.created_at)) AS week ,
				CONCAT(YEAR(w.created_at),' & ',MONTH(w.created_at),' & ', DAYNAME(w.created_at),' & ',HOUR(w.created_at)) AS hour,
				CONCAT(YEAR(w.created_at),' & ',MONTH(w.created_at),' & ', DAYNAME(w.created_at),' & ',HOUR(w.created_at),' & ',MINUTE(w.created_at)) AS minute,
		         w.website_session_id,
		         w.user_id,
                 o.order_id,
                 o.price_usd,
                 o.cogs_usd
		   FROM
			    website_sessions w
		   LEFT JOIN
				orders o
			ON w.website_session_id = o.website_session_id
		   GROUP BY w.website_session_id,w.user_id
           ) AS sub

	GROUP BY 1;
END $$
DELIMITER ;

# Now, You can now put any date matrice that you want to group by it
CALL date_session_orders('year');
CALL date_session_orders('month');
CALL date_session_orders('quarter');
CALL date_session_orders('week');
CALL date_session_orders('hour');
CALL date_session_orders('minute');

-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Analyze website pageviews Table
PRIMARY KEY IS website_pageview_id
FOREIGN KEY IS website_session_id REFERENCE to website_sessions table (website_session_id)
*/
SELECT *
FROM website_pageviews
LIMIT 100;

# Describe Numbers of the tabel
SELECT 
	  COUNT(DISTINCT website_pageview_id) AS n_clicks,
      COUNT(DISTINCT website_session_id) AS sessions,
      COUNT(DISTINCT pageview_url) AS nunique_pagview_url
FROM
	website_pageviews;

# Number of clicks is the same of number of sessions for each page view
SELECT 
	pageview_url,
    COUNT(DISTINCT website_pageview_id) AS n_clicks,
	COUNT(DISTINCT website_session_id) AS sessions
FROM 
	website_pageviews
GROUP BY 1
ORDER BY 2 DESC;
	

# count null values for each column
SELECT
	COUNT(CASE WHEN website_pageview_id IS NULL THEN 1 ELSE NULL END) AS website_pageview_id ,
    COUNT(CASE WHEN created_at IS NULL THEN 1 ELSE NULL END) AS created_at ,
    COUNT(CASE WHEN website_session_id IS NULL THEN 1 ELSE NULL END) AS website_session_id ,
    COUNT(CASE WHEN pageview_url IS NULL THEN 1 ELSE NULL END) AS pageview_url
FROM
	website_pageviews;

# bounce rate
WITH cte_bounce AS(
SELECT
	COUNT(bounce.website_session_id) AS bounce_sessions
FROM
(
	SELECT 
		website_session_id,
		COUNT(DISTINCT website_pageview_id) AS count
	FROM
		website_pageviews
	GROUP BY 1
	HAVING count = 1
) AS bounce)
SELECT
	 COUNT(DISTINCT website_pageviews.website_session_id) AS sessions ,
     cte_bounce.bounce_sessions ,
     CONCAT(ROUND(cte_bounce.bounce_sessions / COUNT(DISTINCT website_pageviews.website_session_id) * 100,2),'%') AS bounce_rate
FROM 
	website_pageviews
JOIN
    cte_bounce;
## 44% of people who enter the website go out from the home page without doing anything ##

# Bounce rate of products and product page
SELECT
    # of sessions
    COUNT(DISTINCT website_session_id) AS sessions,
    # of bounce sessions and bounce rate by the product page
	SUM(product_bounce) AS product_bounce_sessions,
    CONCAT(ROUND(SUM(product_bounce) / COUNT(DISTINCT website_session_id) * 100,2),'%') AS product_bounce_rate,
    # of bounce sessions and bounce rate by the original mrfuzzy page
    SUM(mrfuzzy_bounce) as mrfuzzy_bounce_sessions,
    CONCAT(ROUND(SUM(mrfuzzy_bounce) / COUNT(DISTINCT website_session_id) * 100,2),'%') AS mrfuzzy_bounce_rate,
    # of bounce sessions and bounce rate by the forever love bear page
    SUM(lovebear_bounce) as lovebear_bounce_sessions,
    CONCAT(ROUND(SUM(lovebear_bounce) / COUNT(DISTINCT website_session_id) * 100,2),'%') AS lovebear_bounce_rate,
    # of bounce sessions and bounce rate by the birthday sugar panda page
    SUM(sugarpanda_bounce) assugarpanda_bounce_sessions,
    CONCAT(ROUND(SUM(sugarpanda_bounce) / COUNT(DISTINCT website_session_id) * 100,2),'%') AS sugarpanda_bounce_rate,
    # of bounce sessions and bounce rate by the hudson river mini bear page
    SUM(hudsonbear_bounce) as hudsonbear_bounce_sessions,
    CONCAT(ROUND(SUM(hudsonbear_bounce) / COUNT(DISTINCT website_session_id) * 100,2),'%') AS hudsonbear_bounce_rate
FROM
(
	SELECT 
		website_session_id,
		COUNT(CASE WHEN pageview_url = '/products' AND website_pageview_id = max_pageview_id THEN 1 ELSE NULL END) AS product_bounce,
        COUNT(CASE WHEN pageview_url = '/the-original-mr-fuzzy' AND website_pageview_id = max_pageview_id THEN 1 ELSE NULL END) AS mrfuzzy_bounce,
        COUNT(CASE WHEN pageview_url = '/the-forever-love-bear' AND website_pageview_id = max_pageview_id THEN 1 ELSE NULL END)  AS lovebear_bounce,
        COUNT(CASE WHEN pageview_url = '/the-birthday-sugar-panda' AND website_pageview_id = max_pageview_id THEN 1 ELSE NULL END)  AS sugarpanda_bounce,
        COUNT(CASE WHEN pageview_url = '/the-hudson-river-mini-bear' AND website_pageview_id = max_pageview_id THEN 1 ELSE NULL END)  AS hudsonbear_bounce
FROM
(
	SELECT 
		*,
		MAX(website_pageview_id) OVER(PARTITION BY website_session_id) AS max_pageview_id
	FROM
		website_pageviews
) AS max_page
GROUP BY 1
) AS final;
## 10% of people who made it to the product page did not choose any of the products ##
## 20% of people who choose the-original-mr-fuzzy product but haven't bought it ##
## 2.5% of people who choose the-forever-love-bear but haven't bought it ##
## 2.2% of people who choose the-birthday-sugar-panda product but haven't bought it ##
## 0.2% of people who choose the-hudson-river-mini-bear product but haven't bought it ##



SELECT
      COUNT(CASE WHEN pageview_url = '/home' THEN 1 ELSE NULL END) AS home_sessions,
	  COUNT(CASE WHEN pageview_url = '/products' THEN 1 ELSE NULL END) AS clicks_through_product,
      CONCAT(ROUND(COUNT(CASE WHEN pageview_url = '/products' THEN 1 ELSE NULL END)/COUNT(CASE WHEN pageview_url = '/home' THEN 1 ELSE NULL END) * 100,2),'%') AS pct_session_to_product,
      COUNT(CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE NULL END) AS clicks_through_mrfuzzy,
      CONCAT(ROUND(COUNT(CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE NULL END)/COUNT(CASE WHEN pageview_url = '/products' THEN 1 ELSE NULL END) * 100,2),'%') AS pct_product_to_mrfuzzy,
      COUNT(CASE WHEN pageview_url = '/the-forever-love-bear' THEN 1 ELSE NULL END) AS clicks_through_lovebear,
      CONCAT(ROUND(COUNT(CASE WHEN pageview_url = '/the-forever-love-bear' THEN 1 ELSE NULL END)/COUNT(CASE WHEN pageview_url = '/products' THEN 1 ELSE NULL END) * 100,2),'%') AS pct_product_to_lovebear,
      COUNT(CASE WHEN pageview_url = '/cart' THEN 1 ELSE NULL END) AS clicks_through_cart,
      CONCAT(ROUND(COUNT(CASE WHEN pageview_url = '/cart' THEN 1 ELSE NULL END)/
		(COUNT(CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE NULL END) + COUNT(CASE WHEN pageview_url = '/the-forever-love-bear' THEN 1 ELSE NULL END)) ,2),'%')AS pct_mrfuzzylovebear_cart,
      COUNT(CASE WHEN pageview_url = '/shipping' THEN 1 ELSE NULL END) AS clicks_through_shipping,
      CONCAT(ROUND(COUNT(CASE WHEN pageview_url = '/shipping' THEN 1 ELSE NULL END)/COUNT(CASE WHEN pageview_url = '/cart' THEN 1 ELSE NULL END),2),'%') AS pct_cart_to_billing,
      COUNT(CASE WHEN pageview_url = '/billing' THEN 1 ELSE NULL END) AS clicks_through_billing,
      CONCAT(ROUND(COUNT(CASE WHEN pageview_url = '/billing' THEN 1 ELSE NULL END)/COUNT(CASE WHEN pageview_url = '/shipping' THEN 1 ELSE NULL END),2),'%') AS pct_billing_to_shipping
FROM
	website_pageviews;
## 190% of people who are coming from the home page are hitting and going through the product page AND -90% of people didn't go to the product page ##
## 62% of people who are coming from the product page are hitting and going through the original mr fuzzy page AND 38% of people didn't go to the the original mr fuzzy page ##
## 10% of people who are coming from the product page are hitting and going through the forever love bear page AND 90% of people didn't go to the forever love bear page ##
## 0.5% of people who are coming from the original mr fuzzy or the forever love bear page are hitting and going through the cart page AND 99.5% of people didn't go to the cart page ##
## 0.7% of people who are coming from the cart page are hitting and going through the billing page AND 99.2% of people didn't go to the billing page ##
## 0.06% of people who are coming from the billing page are hitting and going through the shopping page AND 99.94% of people didn't go to the shopping page ##


# The page and Next pages

DELIMITER $$
CREATE PROCEDURE next_pages(IN pageview VARCHAR(30))
BEGIN
	SELECT
	COUNT(*) AS count
	FROM 
	(
		SELECT 
			website_session_id,
			website_pageview_id as first_page
		FROM
			website_pageviews
		WHERE pageview_url = pageview
		
	) AS test
	JOIN
		website_pageviews
	ON website_pageviews.website_session_id = test.website_session_id
	AND website_pageviews.website_pageview_id >= test.first_page;
END $$
DELIMITER ;
CALL next_pages('/home'); # 30% of people who are hitting the home page and move forward to the next pages (clicks = 355925)
CALL next_pages('/products'); # 60% of people who are hitting the product page and move forward to the next pages (clicks = 715253)
CALL next_pages('/the-original-mr-fuzzy');# 28% of people who are hitting the original mrfuzzy page and move forward to the next pages (clicks = 342530)
CALL next_pages('/the-forever-love-bear');# 0.5% of people who are hitting the forever lovebear page and move forward to the next pages (clicks = 62791)
CALL next_pages('/the-birthday-sugar-panda');# 0.3% of people who are hitting the birthday sugar panda page and move forward to the next pages (clicks = 342530)
CALL next_pages('/the-hudson-river-mini-bear');# 0.05% of people who are hitting the hudson river mini bear page and move forward to the next pages (clicks = 6957)
CALL next_pages('/billing');# 0.04% of people who are hitting the billing page and move forward to the next pages (clicks = 5237)
CALL next_pages('/cart');# 20% of people who are hitting the cart page and move forward to the next pages (clicks = 243808)
CALL next_pages('/shipping');# 12% of people who are hitting the shipping page and move forward to the next pages (clicks = 148855)


# Orders and Converation rate by Pages
DROP PROCEDURE pages_orders;
DELIMITER //
CREATE PROCEDURE pages_orders(IN groupby VARCHAR(20))
BEGIN
SELECT 
	CASE 
		WHEN groupby = 'utm_source' THEN w.utm_source
        WHEN groupby = 'utm_campaign' THEN w.utm_campaign
        WHEN groupby = 'utm_content' THEN w.utm_content
        WHEN groupby = 'device_type' THEN w.device_type
        WHEN groupby = 'http_referer' THEN w.http_referer
		WHEN groupby = 'is_repeat_session' THEN w.is_repeat_session
        ELSE 'ERORR'
	END AS ColumnName,
    COUNT(CASE WHEN wp.pageview_url = '/home' THEN 1 ELSE NULL END) AS nclick_home,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/home' THEN o.order_id ELSE NULL END) AS order_home,
	CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/home' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/home' THEN 1 ELSE NULL END)*100,2),'%')AS home_conv_rate,
    COUNT(CASE WHEN wp.pageview_url = '/products' THEN 1 ELSE NULL END) AS nclick_product,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/products' THEN o.order_id ELSE NULL END) AS order_product,
    CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/products' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/products' THEN 1 ELSE NULL END)*100,2),'%')AS product_conv_rate,
    COUNT(CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE NULL END) AS nclick_mrfuzzy,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN o.order_id ELSE NULL END) AS order_mrfuzzy,
    CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE NULL END)*100,2),'%')AS mrfuzzy_conv_rate,
    COUNT(CASE WHEN wp.pageview_url = '/the-forever-love-bear' THEN 1 ELSE NULL END) AS nclick_lovebear,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/the-forever-love-bear' THEN o.order_id ELSE NULL END) AS order_lovebear,
    CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/the-forever-love-bear' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/the-forever-love-bear' THEN 1 ELSE NULL END)*100,2),'%')AS lovebear_conv_rate,
    COUNT(CASE WHEN wp.pageview_url = '/the-hudson-river-mini-bear' THEN 1 ELSE NULL END) AS nclick_minibear,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/the-hudson-river-mini-bear' THEN o.order_id ELSE NULL END) AS order_minibear,
	CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/the-hudson-river-mini-bear' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/the-hudson-river-mini-bear' THEN 1 ELSE NULL END)*100,2),'%')AS minibear_conv_rate,
    COUNT(CASE WHEN wp.pageview_url = '/billing' THEN 1 ELSE NULL END) AS nclick_billing,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/billing' THEN o.order_id ELSE NULL END) AS order_billing,
    CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/billing' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/billing' THEN 1 ELSE NULL END)*100,2),'%')AS billing_conv_rate,
    COUNT(CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE NULL END) AS nclick_cart,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/cart' THEN o.order_id ELSE NULL END) AS order_cart,
    CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/cart' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/cart' THEN 1 ELSE NULL END)*100,2),'%')AS cart_conv_rate,
    COUNT(CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE NULL END) AS nclick_shipping,
    COUNT(DISTINCT CASE WHEN wp.pageview_url = '/shipping' THEN o.order_id ELSE NULL END) AS order_shipping,
    CONCAT(ROUND(COUNT(DISTINCT CASE WHEN wp.pageview_url = '/shipping' THEN o.order_id ELSE NULL END) / COUNT(CASE WHEN wp.pageview_url = '/shipping' THEN 1 ELSE NULL END)*100,2),'%')AS shipping_conv_rate
FROM 
	website_sessions w
LEFT JOIN
	website_pageviews wp
ON w.website_session_id = wp.website_pageview_id
LEFT JOIN
	orders o
ON w.website_session_id = o.website_session_id
GROUP BY 1;
END //
DELIMITER ;
/*
 You could choose any of the follwoing column to group by 
'utm_source' ,'utm_campaign','utm_content','device_type','http_referer','is_repeat_session'
*/
CALL pages_orders('is_repeat_session');

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
products table
PRIMARY KEY IS product_id
-------------------------
order items table
PRIMARY KEY IS order_item_id
FOREIGN KEY IS order_id REFERENCE to orders(order_id)
FOREIGN KEY IS product_id REFERENCE to orders(primary_product_id)
*/
SELECT *
FROM products;

SELECT *
FROM order_items;

# Describe order items
SELECT
	COUNT(DISTINCT order_item_id) AS order_items,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT product_id) AS products,
    COUNT(DISTINCT is_primary_item) AS nunique
FROM order_items;

# total order for each product and item
SELECT 
	products.product_name,
    CASE 
		WHEN order_items.is_primary_item = 1 THEN 'bought as the first product'
        WHEN order_items.is_primary_item = 0 THEN 'bought as the second product'
	ELSE 'OUTCH..'
    END AS itmes,
    COUNT(DISTINCT orders.order_id) AS orders
FROM
	orders
LEFT JOIN order_items
ON orders.order_id = order_items.order_id
LEFT JOIN
products
ON products.product_id = order_items.product_id
GROUP BY 1,2;
    


# Cross selling products
SELECT 
	orders.primary_product_id,
    CONCAT(products.product_name ,' ',(order_items.product_id))AS cross_sell_product,
    COUNT(DISTINCT orders.order_id) AS orders,
    SUM(orders.price_usd) AS revenue,
    ROUND(SUM(orders.price_usd) / COUNT(DISTINCT orders.order_id),2) AS avg_order_value,
    CONCAT(ROUND((SUM(orders.price_usd) - SUM(orders.cogs_usd))/SUM(orders.price_usd) * 100,2),'%') AS profit_margin
FROM 
orders
LEFT JOIN 
order_items
ON orders.order_id = order_items.order_id
AND order_items.is_primary_item = 0
LEFT JOIN
products
ON products.product_id = order_items.product_id 
GROUP BY 1,2;

-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
order item refunds table
PRIMARY KEY IS order_item_refund_id
FOREIGN KEY IS order_item_id REFERENCE TO order_items(order_item_id)
FOREIGN KEY IS order_id REFERENCE TO orders(order_id)
*/
SELECT *
FROM order_item_refunds;

# DESCRIBE DATA
SELECT 
	COUNT(DISTINCT order_item_refund_id) AS count,
    COUNT(DISTINCT order_item_id) AS nunique_order_item,
    COUNT(DISTINCT order_id) AS nunique_orders,
    SUM(refund_amount_usd) AS total_refund,
    ROUND(AVG(refund_amount_usd),2)AS avg_refund,
    MIN(refund_amount_usd) AS min_refund,
    MAX(refund_amount_usd) AS max_refund,
    ROUND(STD(refund_amount_usd),2) AS std_refund
FROM
	order_item_refunds;

# 1731 order has refunds amount
SELECT 
	order_items.order_id,
    order_items.order_item_id,
    order_items.price_usd AS price_paid_usd,
    order_items.created_at,
    order_item_refunds.order_item_refund_id,
    order_item_refunds.refund_amount_usd,
    order_item_refunds.created_at,
    DATEDIFF(order_item_refunds.created_at , order_items.created_at) AS orders_from_refund_days
FROM
	order_items
JOIN order_item_refunds
ON order_items.order_item_id = order_item_refunds.order_item_id
ORDER BY orders_from_refund_days DESC;

# most frequent value
SELECT 
	days.orders_from_refund_days,
	COUNT(*) AS most_freq
FROM
(
SELECT
    DATEDIFF(order_item_refunds.created_at , order_items.created_at) AS orders_from_refund_days
FROM
	order_items
JOIN order_item_refunds
ON order_items.order_item_id = order_item_refunds.order_item_id
ORDER BY orders_from_refund_days DESC
) AS days
GROUP BY 1
ORDER BY 2 DESC;
## most frequent days is 14 and min one is 2 ##

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
# USER ANALYSIS
select *
from website_sessions;

select *
from website_pageviews;

# About users
SELECT 
	COUNT(DISTINCT orders.user_id) AS user_have_orders,
    ROUND(COUNT(DISTINCT orders.user_id) / COUNT(DISTINCT website_sessions.user_id),2) AS conv_users,
    COUNT(CASE WHEN orders.user_id  IS NULL THEN 1 ELSE NULL END) user_havenot_orders,
    ROUND(COUNT(CASE WHEN orders.user_id  IS NULL THEN 1 ELSE NULL END) / COUNT(*),2) pct_user_havenot_orders,
    COUNT(DISTINCT website_sessions.user_id) AS user_have_sessions ,
    COUNT(*) AS all_sessions
FROM
	website_sessions
LEFT JOIN 
	orders
ON website_sessions.website_session_id = orders.website_session_id;

# repeat_sessions
SELECT
	sub2.users AS repeat_sessions,
    COUNT(*) AS users
FROM
(
SELECT
	user_id,
	COUNT(*) AS users
	FROM
		(
		SELECT 
			user_id
		FROM
			website_sessions
		) AS sub
		GROUP BY 1
) AS sub2
GROUP BY 1;

## users who repeat their session for one time is 343,048 ##
## users who repeat their session for two time is 37,386 ##
## users who repeat their session for three time is 485 ##
## users who repeat their session for four time is 13,399 ##


# First select users who have repeated sessions 
DROP TEMPORARY TABLE repeate_users;
CREATE TEMPORARY TABLE repeate_users
SELECT
    CASE 
		WHEN repeate_users.is_repeat_session = 0 AND website_session_id <> max THEN user_id 
        WHEN repeate_users.is_repeat_session = 1 AND website_session_id <= max THEN user_id 
        ELSE NULL
	END AS repeate_users,
    created_at
FROM
(
SELECT 
	*,
    max(website_session_id) OVER(PARTITION BY user_id) as max
FROM website_sessions
) AS repeate_users;

# life time value of repeated users
SELECT
	repeate_users,
    ROUND(DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) / 30) AS life_value_month,
    ROUND(DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) / 7) AS life_value_week,
    DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) AS life_value_days,
    DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) * 24 AS life_value_hour,
    (DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) * 24) * 60 AS life_value_minute,
    # sessions
    ROUND(COUNT(DISTINCT website_sessions.website_session_id) / ROUND(DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) / 30),2) AS avg_session_per_month,
    ROUND(COUNT(DISTINCT website_sessions.website_session_id) / ROUND(DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) / 7),2) AS avg_session_per_week,
    ROUND(COUNT(DISTINCT website_sessions.website_session_id) / DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)),2)AS avg_session_per_day,
    # orders
    ROUND(COUNT(DISTINCT orders.order_id) / ROUND(DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) / 30),2) AS avg_order_per_month,
    ROUND(COUNT(DISTINCT orders.order_id) / ROUND(DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) / 7),2) AS avg_order_per_week,
    ROUND(COUNT(DISTINCT orders.order_id) / DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)),2)AS avg_order_per_day,
    ROUND(COUNT(DISTINCT orders.order_id) / DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) * 24,2)AS avg_order_per_hour,
    ROUND(COUNT(DISTINCT orders.order_id) / (DATEDIFF(MAX(repeate_users.created_at) , MIN(repeate_users.created_at)) * 24) * 60 ,2)AS avg_order_per_minute
FROM
	repeate_users
JOIN 
	website_sessions
ON repeate_users.repeate_users = website_sessions.user_id
LEFT JOIN 
	orders
ON website_sessions.website_session_id = orders.website_session_id
WHERE repeate_users IS NOT NULL # To Select only repeat users 
GROUP BY 1
ORDER BY 2 DESC;

# Which's most device type that users use
SELECT 
	device_type,	
	COUNT(DISTINCT user_id) AS users
FROM 
	website_sessions
GROUP BY 1;

select * from website_sessions;


-- --------------------------------------------------------------------------------------------------------------------------------------------------------------
# TRENDS ANALYSIS
# Monthly user churn
SELECT 
    YEAR(website_sessions.created_at) AS yr,
	MONTH(website_sessions.created_at) AS mo,
	COUNT(DISTINCT website_sessions.user_id) AS users,
    ROUND((COUNT(DISTINCT website_sessions.user_id) - LAG(COUNT(DISTINCT website_sessions.user_id)) OVER())/COUNT(DISTINCT website_sessions.user_id),2) AS users_growth,
    ROUND((LAG(COUNT(DISTINCT website_sessions.user_id)) OVER() - COUNT(DISTINCT website_sessions.user_id))/LAG(COUNT(DISTINCT website_sessions.user_id)) OVER(),2) AS users_churn
FROM 
website_sessions
GROUP BY 1 ,2
ORDER BY 1 ,2;


# Quartly trends
SELECT 
    YEAR(website_sessions.created_at) AS yr,
	QUARTER(website_sessions.created_at) AS qr,
	COUNT(DISTINCT website_sessions.website_session_id) AS sessions,
    SUM(price_usd) AS revenue,
    COUNT(DISTINCT orders.order_id) AS orders,
    ROUND((SUM(price_usd) - SUM(cogs_usd)) / SUM(price_usd),2) AS profit_margin,
    ROUND((COUNT(DISTINCT orders.order_id) - LAG(COUNT(DISTINCT orders.order_id)) OVER())/COUNT(DISTINCT orders.order_id),2) AS order_growth,
    ROUND((SUM(price_usd) - LAG(SUM(price_usd)) OVER())/SUM(price_usd),2) AS revenue_growth,
    ROUND(COUNT(DISTINCT orders.order_id)/ COUNT(DISTINCT website_sessions.website_session_id),2)AS conv_rate,
    ROUND(SUM(price_usd) / COUNT(DISTINCT orders.order_id),2) AS avg_order_value,
    ROUND(SUM(price_usd) / COUNT(DISTINCT website_sessions.website_session_id),2) AS avg_session_value
FROM 
website_sessions
LEFT JOIN 
orders
ON website_sessions.website_session_id = orders.website_session_id
GROUP BY 1 ,2;

# average monthly orders
SELECT
	sub.yr,
    sub.mo,
    AVG(sub.orders) AS avg_orders
FROM
(
	SELECT
        DATE(created_at) AS date,
		YEAR(created_at) AS yr,
		MONTH(created_at) AS mo,
		COUNT(DISTINCT order_id) AS orders
	FROM
		orders
	GROUP BY
		1,2,3
) AS sub
GROUP BY 1,2;



# average hourly orders
SELECT
	sub.dy,
    sub.hr,
    AVG(sub.orders) AS avg_orders
FROM
(
	SELECT
        DATE(created_at) AS date,
		YEAR(created_at) AS yr,
		MONTH(created_at) AS mo,
        DAY(created_at) AS dy,
        HOUR(created_at) AS hr,
		COUNT(DISTINCT order_id) AS orders
	FROM
		orders
	GROUP BY
		1,2,3,4,5
) AS sub
GROUP BY 1,2;


# Monthly trends of sessions by product pages
SELECT
    YEAR(created_at) AS yr,
    MONTH(created_at) AS mo,
    COUNT(DISTINCT CASE WHEN pageview_url = '/the-birthday-sugar-panda' THEN website_session_id ELSE NULL END) AS sugarpanda_sessions, 
    SUM(CASE WHEN pageview_url = '/the-birthday-sugar-panda' THEN price_usd ELSE NULL END)/COUNT(DISTINCT CASE WHEN pageview_url = '/the-birthday-sugar-panda' THEN website_session_id ELSE NULL END)AS sugarpanda_avg_session_value,
    COUNT(DISTINCT CASE WHEN pageview_url = '/the-forever-love-bear' THEN website_session_id ELSE NULL END) AS lovebear_sessions, 
	SUM(CASE WHEN pageview_url = '/the-forever-love-bear' THEN price_usd ELSE NULL END)/COUNT(DISTINCT CASE WHEN pageview_url = '/the-forever-love-bear' THEN website_session_id ELSE NULL END)AS lovebear_avg_session_value,
    COUNT(DISTINCT CASE WHEN pageview_url = '/the-hudson-river-mini-bear' THEN website_session_id ELSE NULL END) AS minibear_sessions, 
	SUM(CASE WHEN pageview_url = '/the-hudson-river-mini-bear' THEN price_usd ELSE NULL END)/COUNT(DISTINCT CASE WHEN pageview_url = '/the-hudson-river-mini-bear' THEN website_session_id ELSE NULL END)AS minibear_avg_session_value,
    COUNT(DISTINCT CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN website_session_id ELSE NULL END) AS mrfuzzy_sessions,
	SUM(CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN price_usd ELSE NULL END)/COUNT(DISTINCT CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN website_session_id ELSE NULL END)AS mrfuzzy_avg_session_value
FROM
( 
	SELECT 
		website_pageviews.website_session_id, 
		website_pageviews.created_at,
		website_pageviews.pageview_url ,
		orders.order_id, 
		orders.price_usd
	FROM website_pageviews 
		LEFT JOIN orders
			ON orders.website_session_id = website_pageviews.website_session_id
) AS pageviews_and_order_data
WHERE pageview_url IN ('/the-birthday-sugar-panda','/the-forever-love-bear','/the-hudson-river-mini-bear','/the-original-mr-fuzzy')
GROUP BY 1,2
ORDER BY 1,2;

