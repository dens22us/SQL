---------------------Retention rate of GA sample--------------------
with cte as (
select
distinct user_pseudo_id,
date(date_trunc(timestamp_micros(event_timestamp),week)) as active_week,
min(date(date_trunc(timestamp_micros(event_timestamp),week))) over (partition by user_pseudo_id) as first_active_week,
DATE_DIFF(date(date_trunc(timestamp_micros(event_timestamp),week)), min(date(date_trunc(timestamp_micros(event_timestamp),week))) over (partition by user_pseudo_id), week) as active_week_number
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
order by 1
)
select first_active_week,
    sum(case when active_week_number = 0 then 1 else 0 end) as m0,
    sum(case when active_week_number = 1 then 1 else 0 end) as m1,
    sum(case when active_week_number = 2 then 1 else 0 end) as m2,
    sum(case when active_week_number = 3 then 1 else 0 end) as m3,
    sum(case when active_week_number = 4 then 1 else 0 end) as m4,
    sum(case when active_week_number = 5 then 1 else 0 end) as m5,
    sum(case when active_week_number = 6 then 1 else 0 end) as m6,
    sum(case when active_week_number = 7 then 1 else 0 end) as m7,
    sum(case when active_week_number = 8 then 1 else 0 end) as m8,
    sum(case when active_week_number = 9 then 1 else 0 end) as m9,
    sum(case when active_week_number = 10 then 1 else 0 end) as m10,
    sum(case when active_week_number = 11 then 1 else 0 end) as m11,
    sum(case when active_week_number = 12 then 1 else 0 end) as m12,
    sum(case when active_week_number = 13 then 1 else 0 end) as m13
    from cte
    group by 1
    order by 1;



---------Do the 20% of best users made 80% of total revenue? (Paretto principle)-----
with cte as(
      select user_pseudo_id, sum(ecommerce.purchase_revenue_in_usd) as user_revenue
      from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
      where event_name = 'purchase'
      group by 1
), cte2 as(
      select user_pseudo_id, user_revenue,
            sum(user_revenue) over (order by user_revenue desc) as run_sum,
            row_number() over (order by user_revenue desc) as user_number,
            count(user_pseudo_id) over () as total_count, --4419
            sum(user_revenue) over () as total_sum --362165.0
      from cte
      order by 2 desc
)
      select round(run_sum*100/total_sum,2) as Must_be_80_percent --(but only 56,6%)
      from cte2
      where user_number = round(total_count/5);

----------Top 20 users by number of purchases-----method #1----
select
user_pseudo_id,
count(case when event_name = 'purchase' then 1 end) as purchase_
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1 
order by 2 desc
limit 20;
----------Top 20 users by by number of purchases-----method #2----
select
user_pseudo_id,
count(*) as purchases_count
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where event_name in ('purchase')
group by 1 
order by 2 desc
limit 20;
-------count of unique users-----
select count(distinct user_pseudo_id)
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;
-------count of event_name----
select  event_name,
count(*)
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 2 desc;
----------Top 20 users by total revenue----
select user_pseudo_id,
sum(ecommerce.purchase_revenue) as revenue_per_user,
count(*) as number_of_purchases
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where event_name in ('purchase')
group by 1 
order by 2 desc
limit 20;
------------------What exactly top buyer did buy?------
select *
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where user_pseudo_id = '81036435.1157569602' and event_name = 'purchase'
order by 2 desc
limit 20;

---- total revenue for each week-----
select date_trunc(date(timestamp_micros(event_timestamp)),week) as timestamp_,
sum(ecommerce.purchase_revenue) as total_revenue
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 1 asc;
------Number of buyers, total revenue, count of purshases sorted by countries-----
select geo.country,
sum(ecommerce.purchase_revenue) as total_revenue,
count(distinct user_pseudo_id||cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string)) as total_users,
sum(case when event_name = 'purchase' then 1 else 0 end) as number_of_purchases
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 2 desc;
-----------------Checking field 'session_engaged' for values-------------------
select user_pseudo_id||(select value.int_value from unnest (event_params) where key = 'ga_session_id') as unique_user,
sum(case when(select value.string_value from unnest (event_params) where key = 'session_engaged') = '1' then 1 else 0 end) as string_value,
sum(case when(select value.int_value from unnest (event_params) where key = 'session_engaged') = 1 then 1 else 0 end) as int_value,
sum(case when(select value.float_value from unnest (event_params) where key = 'session_engaged') = 1 then 1 else 0 end) as float_value,
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 2 desc;

-------------------How many page view and session start by user-----
Select 
    user_pseudo_id,
    sum(case when event_name = 'page_view' then 1 else null end) as page_view,
    sum(case when event_name = 'session_start' then 1 else null end) as session_start
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 3 desc
limit 10000;
--------find out user which did most purchases, and which is this % of total revenue ----
select
user_pseudo_id,
sum(quantity) as quantity_,
sum(ecommerce.purchase_revenue) as purchase_revenue_ ,
sum(quantity * price) as total_ruvenue,
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`, UNNEST(items)
where event_name = 'purchase'
group by 1
order by 4 desc
limit 10000;

-----------how many events---------('select_item','view_item','add_to_cart','purchase')---
select event_name,
count(*)
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where (_TABLE_SUFFIX BETWEEN '20200101' AND '20201230')
group by 1
order by 2 desc;

select
(select value.int_value from unnest (event_params) where key = 'ga_session_id') as session_id
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where (select value.int_value from unnest (event_params) where key = 'ga_session_id') is null;

-----------------Convertion rate per week----------Number_of_purchases/Number_of_unique_session(user)*100%----
select
date_trunc(date(timestamp_micros(event_timestamp)),week) as week,
count(distinct user_pseudo_id||(select value.int_value from unnest (event_params) where key = 'ga_session_id')) as count_unique_user,
sum(case when event_name = 'purchase' then 1 else 0 end) as number_of_purchase,
round(sum(case when event_name = 'purchase' then 1 else 0 end)/count(distinct user_pseudo_id||(select value.int_value from unnest (event_params) where key = 'ga_session_id')) * 100,1) as CVR
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 1;

-----------------------------------Average order value------AOV = Total Revenue / Total Number of Orders------
select 
round(sum(ecommerce.purchase_revenue)/count(*),2) as AOV
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where event_name= 'purchase';

------------comparison total AOV with week AOV -----
with week_ as (
select
date_trunc(date(timestamp_micros(event_timestamp)),week) as week,
sum(ecommerce.purchase_revenue)/sum(case when event_name = 'purchase' then 1 else 0 end) as week_AOV,
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 1
),
total_ as (
    select 
round(sum(ecommerce.purchase_revenue)/count(*),2) as AOV
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where event_name= 'purchase'
)
select 
week_.week, round(week_.week_AOV,2), total_.AOV
from week_ , total_
order by 1;
--------------------------Total users in a week, number of purchases every week, revenue per week, items count-----------
select
date_trunc(date(timestamp_micros(event_timestamp)),week) as week,
count(user_pseudo_id) as Total_users,
sum(case when event_name = 'purchase' then 1 else 0 end) as number_of_purchases,
sum(ecommerce.purchase_revenue) as revenue_per_week,
sum(quantity) as items_count
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`, unnest(items)
group by 1
order by 1;

----------------------------------------part 1-------------------------------------------------------
--Побудова воронки закритого типу за 2020 рік:
--Створити SQL-запит до публічного набору даних Google Analytics 4 (ga4_obfuscated_sample_ecommerce), який буде будувати воронку закритого --типу за 2020 рік для користувачів.
--Воронка повинна включати етапи: select_item, view_item, add_to_cart, purchase.
--Запит повинен дозволяти фільтрувати дані за назвою товару.
--Візуалізувати отримані дані у вигляді воронки Looker Studio.
select 
user_pseudo_id,
case when event_name = 'select_item' then 1 else 0 end as select_item ,
case when event_name = 'view_item' then 1 else 0 end as view_item ,
case when event_name = 'add_to_cart' then 1 else 0 end as add_to_cart,
case when event_name = 'purchase' then 1 else 0 end as purchase,
item_name,
event_name
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,unnest(items)
where (_TABLE_SUFFIX BETWEEN '20200101' AND '20201230')
and event_name in ('select_item','view_item','add_to_cart','purchase')
limit 10000;

---------------------------part 2--------------------------------------------------------------
--Звіт по многоканальній послідовності сесій до покупки за 2020 рік:
--Створити SQL-запит для отримання джерел/каналів для користувачів до здійснення покупки (подія purchase) у 2020 році.
--Результати повинні містити джерела/ канали та кількість користувачів, які здійснили покупку, для кожного.
--Візуалізувати отримані дані.
select
traffic_source.medium,
traffic_source.name,
traffic_source.`source`,
count(distinct user_pseudo_id) as number_of_purchases
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where (_TABLE_SUFFIX BETWEEN '20200101' AND '20201230')
and event_name in ('purchase')
group by 1,2,3;

---------------------------part 3--------------------------------------------------------------
--Звіт типу Time Lag:
--Розрахувати різницю в днях між першим заходом користувача на сайт і першою покупкою.
--Вивести дані в розрізі кожного користувача.
--Порахувати середню кількість днів, необхідних користувачам для здійснення першої покупки після першого заходу на сайт.
--Візуалізувати отримані дані.
with t3 as (
select
user_pseudo_id,
DATE_DIFF(timestamp_micros(case when event_name = 'purchase' then event_timestamp end),timestamp_micros(user_first_touch_timestamp),day) as day_diff
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
order by 2 desc
limit 10000
)
select
user_pseudo_id,
round(min(day_diff)) as difference,
round(avg(min(day_diff)) over ()) as avg_day_for_first_purchase,
from t3
group by 1
order by 2 desc;

------------------------Part 4----------------------------------------------------------
--Когортний аналіз - Weekly Retention:
--Створити SQL-запит для розрахунку щотижневого повернення користувачів.
--Когорти формуються за першим візитом користувача.
--Вивести дані в розрізі кожної когорти.
--Візуалізувати отримані дані.

select
user_pseudo_id,
date(date_trunc(timestamp_micros(event_timestamp),week)) as active_week,
min(date(date_trunc(timestamp_micros(event_timestamp),week))) over (partition by user_pseudo_id) as first_active_week,
DATE_DIFF(date(date_trunc(timestamp_micros(event_timestamp),week)), min(date(date_trunc(timestamp_micros(event_timestamp),week))) over (partition by user_pseudo_id), week) as active_week_number
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
limit 10000;

/*
Завдання 1. Підготовка даних для побудови звітів у BI системах

Створи запит для отримання таблиці з інформацією про події, користувачів та сесії в GA4. В результаті виконання запиту ми маємо отримати таблицю, що включатиме в себе такі поля:

event_timestamp - дата та час події (тип даних має бути timestamp).
user_pseudo_id - анонімний ідентифікатор користувача в GA4
session_id - ідентифікатор сесії подій в GA4
event_name - назва події
country - країна користувача сайту
device_category - категорія пристрою користувача сайту
source - джерело відвідування сайту
medium - medium відвідування сайту
campaign - назва кампанії відвідування сайту
Таблиця має включати лише дані за 2021 рік, та дані з таких подій:

Початок сесії на сайті
Перегляд товару
Додавання товару до корзини
Початок оформлення замовлення
Додавання інформації про доставку
Додавання платіжної інформації
Покупка
*/
--------------------------The 1st home task---------------

select timestamp_micros(event_timestamp) as timestamp_,
     user_pseudo_id, 
     PARSE_DATE('%Y%m%d', event_date),
  (select value.int_value from unnest (event_params) where key = 'ga_session_id') as session_id,
    event_name, geo.country, 
    device.category as device_category, 
    traffic_source.`source`, 
    traffic_source.medium,
    traffic_source.name as campaign_name
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where (_TABLE_SUFFIX BETWEEN '20210101' AND '20211230') 
  and event_name IN ('session_start','view_item','add_to_cart','add_payment_info','add_shipping_info','purchase')
limit 1000;

/*
Завдання 2. Розрахунок конверсій в розрізі дат та каналів трафіку
Створи запит для отримання таблиці з інформацією про конверсії від початку сесії до покупки. Результуюча таблиця має включати в себе такі поля:

event_date - дата старту сесії, що отримана з поля event_timestamp
source - джерело відвідування сайту
medium - medium відвідування сайту
campaign - назва кампанії відвідування сайту
user_sessions_count - кількість унікальних сесій в 
унікальних користувачів у відповідну дату та для відповідного каналу трафіку.
visit_to_cart - конверсія від початку сесії на сайті до додавання товару 
в корзину (у відповідну дату та для відповідного каналу трафіку)
visit_to_checkout - конверсія від початку сесії на сайті до спроби 
оформити замвовлення (у відповідну дату та для відповідного каналу трафіку)
Visit_to_purchase - конверсія від початку сесії на сайті до покупки 
(у відповідну дату та для відповідного каналу трафіку)
Примітка Зверни увагу, що різні користувачі можуть мати однакові 
ідентифікатори сесій. Тому щоб порахувати унікальні сесії унікальних користувачів, 
треба враховувати не тільки ідентифікатор сесії, а й ідентифікатор користувача.
*/
--------------------------The 2nd home task---------------
with temp_ as (
select date(timestamp_micros(event_timestamp)) as event_date,
      traffic_source.`source` as source_,
      traffic_source.medium as medium_,
      traffic_source.name as campaign_name,
      count(distinct(select value.int_value from unnest (event_params) where key = 'ga_session_id')) as user_sessions_count,
      count(distinct user_pseudo_id) as users_count,
      count( case when event_name = 'add_to_cart' then event_timestamp else null end) as visit_to_cart,
      sum(case when event_name ='add_payment_info' then 1 else 0 end) as visit_to_checkout,
      sum(case when event_name ='purchase' then 1 else 0 end) as Visit_to_purchase,
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where (_TABLE_SUFFIX BETWEEN '20210101' AND '20211230') 
group by 1,2,3,4
limit 1000
)
  Select event_date, source_, medium_, campaign_name, user_sessions_count,users_count, visit_to_cart,         
         visit_to_checkout, Visit_to_purchase,
         round(visit_to_cart/users_count * 100,2) as conv_cart,
         round(visit_to_checkout/users_count * 100,2) as conv_checkout,
         round(Visit_to_purchase/users_count * 100,2) as conv_purchase
from temp_;

/*
Завдання 3. Порівняння конверсії між різними посадковими сторінками
Для виконання цієї задачі тобі потрібно буде отримати page path (шлях до сторінки без  адреси домену 
та без параметрів посилання) з page_location в події початку сесії.

Для кожного унікального page page початку сесії порахуй такі метрики на основі даних за 2020 рік:

Кількість унікальних сесій в унікальних користувачів
Кількість покупок
Конверсія від початку сесії в покупку
Примітка Події старту сесії та покупки можуть мати різні url. 
Тому злити подію старту сесії з подією покупки можна за ідентифікатором користувача та ідентифікатором сесії.
*/

---------------------The 3rd home task-------------------
with t1 as (      --Витягуємо name з page path, та зливаємо сессії та юзерів в одное поле для розрахунку інікальності користувачів
     select regexp_extract((select value.string_value from unnest (event_params) where key = 'page_location'),r'\/([a-zA-Z0-9_-]+)\/?$')as page,
      --regexp_extract((select value.string_value from unnest (event_params) where key = 'page_location'),r'https?://[^/]+(/[^?#]*)?')as page2, 
     concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string)) as user_session_id1
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where (_TABLE_SUFFIX BETWEEN '20200101' AND '20201230')
      and event_name in ('session_start')
),
     t2 as (        --- Витягуємо покупки для унікальних користувачів
      select 
      user_pseudo_id || cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string) as user_session_id2,
      case when event_name = 'purchase' then 1 end as purchase_
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where (_TABLE_SUFFIX BETWEEN '20200101' AND '20201230')
      and event_name in ('purchase')
     )           --- Джоінім таблицю покупок з таблицею page path на підставі однакових унікальних користувачів та разраховуємо конверсію
     select t1.page,count(distinct t1.user_session_id1) as user_session_id, count(t2.purchase_) as purchase_, 
     round(count(t2.purchase_)/count(distinct t1.user_session_id1)*100,2) as conversion_
     from t1 left join t2 on t1.user_session_id1 = t2.user_session_id2
     group by 1
     order by 2 desc
     limit 5000;

/*
Завдання 4. Перевірка кореляції між залученістю користувачів та здійсненням покупок
Для кожної унікальної сесії визнач:

Чи був користувач залучений під час цієї сесії (якщо значення параметру session_engaged = ‘1’)
Загальний час активності користувача під час сесії (сума параметру engagement_time_msec з кожної події сесії)
Чи відбулася покупка під час сесії
Порахуй значення коефіцієнту кореляції:
між п.1 та п.3
*/
------------------the 4-th home task-------------------
with temp1 as(  -- Для кожного унікального користувача в унікальной сессії суммуємо початки сессеії та час сессій також покупки
select user_pseudo_id||cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string) as user_session,
        sum(coalesce((case when (select value.string_value from unnest(event_params) where key = 'session_engaged') is not null then 1 end),(case when (select value.int_value from unnest(event_params) where key = 'session_engaged') is not null then 1 end))) as s_engage,
        sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec')) as time_,
        sum(case when event_name = 'purchase' then 1 else 0 end) as purchase    
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
group by 1
order by 4 desc
limit 10000
), 
      temp2 as (    ---Розраховуємо коеффіцієнти корреляцій початки_сессії/покупки та час_сессій/покупки
select round(corr(s_engage,purchase),3) as correlation1,
      round(corr(time_,purchase),4) as correlation2
from temp1
)
select temp1.user_session, temp1.s_engage, temp1.purchase, temp2.correlation1,
      temp1.time_, temp1.purchase, temp2.correlation2
      from temp1, temp2;


    
