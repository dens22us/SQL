-----------Check if odd number or not odd (четные и нечетные)------
select *
from cinema
--where mod(id,2)<>0 and description <> 'boring' --Variant 1 Остаток от деления если не равен 0
where id % 2 <> 0 and description <> 'boring' -- Variant 2
order by rating desc


------------------------------Retention rate for unique cohort of users(acive in 2022-03-01) from  2022-03-01 to 2022-12-01 -------------
with cte as(
select distinct user_id 
from activity
where date(date_trunc('month', activity_date::date)) = '2022-03-01'
),cte2 as(
select date(date_trunc('month', activity_date::date)) as active_month, count(distinct a.user_id) as cnt
from activity a join cte on a.user_id = cte.user_id
group by 1
order by 1 
)
select active_month, 
(round(cnt::numeric/(select count(distinct user_id) from 
		activity where date(date_trunc('month', activity_date::date)) = '2022-03-01'),2)*100)::varchar||'%' as retention_rate
from cte2


-- Розділення речення  в стовбці на складові через розділюючий знак ‘ ; ‘ 
SELECT unnest(string_to_array(categories, ';')) AS category,
          review_count
   FROM yelp_business
------------------------------------------------------------------------------------------------------
/* Є наступні массиви чисел, з'єднати їх у один массив та найти медіану
 * '1,2,3,43,4,3,2,56,77,33,2,33,5'
 * '1,2,3,43,4,3,2,56,77,33,2,33,5'
 * '43,2,3,43,2,3,2,56,77,67,2,34,5'
 * '1,2,33,43,4,32,2,56,77,3,2,33,5'
 */
--CREATE TABLE unnest_test (numbers varchar)

insert into unnest_test values ('1,2,3,43,4,3,2,56,77,33,2,33,5');
insert into unnest_test values ('1,2,3,43,4,3,2,56,77,33,2,33,5');
insert into unnest_test values ('43,2,3,43,2,3,2,56,77,67,2,34,5');
insert into unnest_test values ('1,2,33,43,4,32,2,56,77,3,2,33,5');

with cte as(
	select unnest(string_to_array(numbers,',')) as numbers
	from unnest_test
), cte2 as (
	select numbers,
		row_number () over (order by numbers) as down,
		row_number () over (order by numbers desc) as up
	from cte
)
select round(avg(numbers::numeric))
from cte2
where down = round((select max(down) from cte2)::numeric/2) 
			or up = round((select max(up) from cte2)::numeric/2);


--------------------------Задачка складності HARD з використанням генерації--------------------------------------------------
/*
Google's marketing team is making a Superbowl commercial and needs a simple 
statistic to put on their TV ad: the median number of searches a person made last year.

However, at Google scale, querying the 2 trillion searches is too costly. Luckily,
 you have access to the summary table (generation) which tells you the number of searches made last year and 
 how many Google users fall into that bucket.

Write a query to report the median of searches made by a user. Round the median to one decimal point.
*/
create table generation (searches numeric, num_users numeric);

insert into generation values (2, 3); 
insert into generation values (4, 1); 
insert into generation values (6, 7); 
insert into generation values (7, 4); 
insert into generation values (9, 8);

with cte as (
	select searches
	from generation
	group by searches, GENERATE_SERIES(1, num_users)
), cte2 as(
	select searches,
		row_number() over (order by searches) as down,
		row_number() over (order by searches) as up,
		count(searches) over () as cnt
	from cte
)
select round(avg(searches),1)
from cte2
where down = round(cnt/2) or up = round(cnt/2)


---------------перетворення значеннь 1 колонці в колонці значень  ---------------------------------------------------------------------
with t as(
select user_id, event, date(date_trunc('day', cast((event_timestamp) as date))) as day_, 
date(date_trunc('month', cast((event_timestamp) as date))) as month_
from public.onboarding_funnel_product
), t2 as (
select count(day_) as count_, month_, event as event_
from t
group by 2,3
order by 2 asc, 1 desc
)
select month_, sum(case when event_ = 'registration' then count_ else 0 end) as registration,
		sum(case when event_ = 'email-verification' then count_ else 0 end) as "email-verification" ,
		sum(case when event_ = 'profile-completion' then count_ else 0 end) as "profile-completion",
		sum(case when event_ = 'setup-completion' then count_ else 0 end) as "setup-completion",
		sum(case when event_ = 'trial-start' then count_ else 0 end) as "trial-start" ,
		sum(case when event_ = 'first-payment' then count_ else 0 end) as  "first-payment"
from t2
group by 1
order by 1 asc

-------------------------------------------задачка з гонщиком--------------------------------------------
	 drop table "_records"  	
	   	
	   	create table _Records (RacerID serial primary key, time_in_sec float)
	
	insert into _Records (time_in_sec) values
	(123),
	(108),
	(144),
	(167),
	(113),
	(99)
	
with result_1 as (	
	select racerid,
		4000/avg(time_in_sec) over ()  as avg_speed,
		4000/time_in_sec as speed
	from _records1 r 
	)
	select racerid, speed
	from result_1
	where speed>=avg_speed
	
	select racerid,
	case when 4000/time_in_sec > 4000/ avg(time_in_sec) over () then  4000/time_in_sec end as speed
	from "_records"  
	
	select racerid, 4000/time_in_sec
	from _records 
	where 4000/time_in_sec > (select avg(4000/time_in_sec) from "_records" );
------------------------------------------------------------------------------
-----------------------	

--------візов функції для декодування кирилиці в URL--------------------------------------------------------------------------------------

 CREATE OR REPLACE FUNCTION pg_temp.decode_url_part(p character varying)
 RETURNS character varying
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
SELECT convert_from(CAST(E'\\x' || string_agg(CASE WHEN length(r.m[1]) = 1 THEN encode(convert_to(r.m[1], 'SQL_ASCII'), 'hex') ELSE substring(r.m[1] from 2 for 2) END, '') AS bytea), 'UTF8')
FROM regexp_matches($1, '%[0-9a-f][0-9a-f]|.', 'gi') AS r(m);
$function$
;

 select pg_temp.decode_url_part (substring(fabd.url_parameters, 'utm_campaign=([^&#$]+)')),
 		pg_temp.decode_url_part (substring(gabd.url_parameters, 'utm_campaign=([^&#$]+)'))
from facebook_ads_basic_daily fabd, google_ads_basic_daily gabd 

select url_parameters , lower(substring(url_parameters,49)), lower(substring(url_parameters,'utm_campaign=([^&#$]+)')) 
from facebook_ads_basic_daily

-----------------------HOME TASK 5----------------------------------------------------------
with total_t as (
select ad_date ,lower(substring(url_parameters,49)) as url ,fa.adset_name , fc.campaign_name, ------використав номер позиції---
       coalesce(spend,0) as "spend", coalesce(impressions,0) as "impressions", coalesce(reach,0) as "reach",
       coalesce(clicks,0) as "clicks", coalesce(leads,0) as "leads", coalesce(value,0) as "value"
from facebook_ads_basic_daily fabd join facebook_adset fa on fabd.adset_id = fa.adset_id 
                                   join facebook_campaign fc on fabd.campaign_id = fc.campaign_id                        
union 
select ad_date ,lower(substring(url_parameters,'utm_campaign=([^&#$]+)')) as url ,adset_name ,campaign_name , ----використав регулярне вираження (reg expression)---
       coalesce(spend,0) as "spend", coalesce(impressions,0) as "impressions", coalesce(reach,0) as "reach",
       coalesce(clicks,0) as "clicks", coalesce(leads,0) as "leads", coalesce(value,0) as "value"
from google_ads_basic_daily gabd 
)
  select ad_date ,campaign_name, case when (url = 'nan') then null else url end newurl ,sum(spend) as spend, sum(impressions) as impressions,
  		sum(reach) as reach, sum(clicks) as clicks, sum(leads) as leads, sum(value) as value,
  		case when sum(impressions) = 0 then null else sum(clicks)::float / sum (impressions) * 100 end CTR,
  		case when sum(spend) = 0 then null else sum(clicks)::float/ sum(Spend)  end CPC,
  		case when sum(spend) = 0 then null else sum(impressions)::float/ sum(Spend)  end CPM,
  		case when sum(spend) = 0 then null else (sum(value)::float - sum(spend)) / sum (spend)  end ROMI
  from total_t
  group by ad_date, campaign_name, newurl
  order by ad_date desc;


  -----------------------------Приклади вичітання дат----------------

select '2022-07-10', '2022-11-04', 
date_part('day', age(date('2022-11-04'),date('2022-07-10'))) as using_date_part, 
date('2022-11-04')-date('2022-07-10') as using_difference,
date_trunc('day',date('2022-11-04'))- date_trunc('day',date('2022-07-10')) as using_date_trunc

------------------------Home task 5 from google sheets---------------
---------------SQL query for Retention Rate----------------------------
select a.user_id, 
		date(date_trunc('month',activity_date::date)) as Active_month,
		min(date(date_trunc('month',activity_date::date))) over (partition by a.user_id) as First_active_month,
		round((date(date_trunc('month',activity_date::date))- min(date(date_trunc('month',activity_date::date))) 
                                                    over (partition by a.user_id))/30) as Activity_month_number
from public.activity a join public.active_users au  on a.user_id = au.user_id
-------------------------------------------------------------------------------

/* Знайдіть три найвищі зарплати для кожного відділу, але виведіть тільки ті відділи, 
 де кількість працівників зарплата яких вища за середню зарплату в цьому відділі, перевищує 3 особи.
 */
with cte as (
select job_title, salary,
row_number() over (partition by job_title order by salary desc) as rnk,
count(salary) over (partition by job_title) as count_
from public.ds_salaries p2
where salary >= (select avg(salary) from public.ds_salaries p1 where p1.job_title = p2.job_title )
)
select job_title, salary
from cte
where rnk <=3 and  count_>3
order by 1 asc, 2 desc 

--------------HARD TASK WITH USING ONLY WINDOWS FUNCTION-------------------------------------------------------------------------
/* INPUT    OUTPUT
 * Null			4
 * 4			4
 * Null			10
 * Null			10
 * Null			10
 * Null			10
 * Null			10
 * 10			10
 * Null			3
 * Null			3
 * Null			3
 * 3			3
 */
CREATE TABLE your_table (your_column numeric);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (4);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (10);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (NULL);
INSERT INTO your_table VALUES (3);

with cte_row as(
select your_column, row_number() over(partition by (select null)) as row_
from your_table
), cte2 as (
select your_column, row_, count(your_column) over (order by row_ desc) as gpr
from cte_row
)
select your_column, row_, gpr, sum(your_column) over (partition by gpr)
from cte2
order by row_

/*
Given a table of tweet data over a specified time period, calculate the 3-day 
rolling average of tweets for each user. Output the user ID, 
tweet date, and rolling averages rounded to 2 decimal places.
*/


SELECT user_id, tweet_date, 
round(avg(tweet_count) over (partition by user_id ORDER BY tweet_date ROWS BETWEEN 2 PRECEDING AND 0 FOLLOWING),2)
FROM tweets
order by user_id asc, tweet_date
