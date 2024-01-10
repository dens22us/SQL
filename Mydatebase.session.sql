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