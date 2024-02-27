with begin_ as (	
	select user_id, game_name, 
	date(date_trunc('month',payment_date)) as current_month, 
	sum(revenue_amount_usd) as revenue_amount
	from project.games_payments gp 
	group by 1,2,3
	),
all_data as (
	select current_month, user_id, game_name, revenue_amount,
	date(current_month - interval '1' month) as previous_calendar_month,
		date(current_month + interval '1' month) as next_calendar_month,
	lag(revenue_amount,1) over (partition by user_id order by current_month) as previous_revenue_amount,
	lag(current_month,1) over (partition by user_id order by current_month) as previous_month,
	lead(current_month,1) over (partition by user_id order by current_month) as next_month
	from begin_
	), 
All_revenue_metrics as (
	select
		current_month,
		user_id,
		game_name,
		revenue_amount,
		'MRR' as revenue_type
	from all_data
union all
	select
		current_month,
		user_id,
		game_name,
		revenue_amount,
		'New MRR' as revenue_type
		from all_data 
		where previous_month is null
union all
	select 
		next_calendar_month,
		user_id,
		game_name,
		-revenue_amount as revenue_amount ,
		'Churned revenue' as revenue_type
		from all_data 
		where next_month is null or next_month <> next_calendar_month
union all	
	select 
		current_month,
		user_id,
		game_name,
		revenue_amount as revenue_amount ,
		'back_from_churn_revenue' as revenue_type
		from all_data 
		where previous_month <> previous_calendar_month 
				and previous_month is not null
union all		
	select 
		current_month,
		user_id,
		game_name,
		(revenue_amount-previous_revenue_amount) as revenue_amount ,
		'Contraction MRR' as revenue_type
		from all_data 
		where previous_month = previous_calendar_month and revenue_amount < previous_revenue_amount
union all		
	select 
		current_month,
		user_id,
		game_name,
		(revenue_amount - previous_revenue_amount) as revenue_amount ,
		'Expansion MRR' as revenue_type
		from all_data 
		where previous_month = previous_calendar_month and revenue_amount > previous_revenue_amount
)
	select 
		current_month,
		a.user_id,
		a.game_name,
		revenue_amount ,
		revenue_type,
		gpu."language",
		gpu.has_older_device_model,
		gpu.age
	from All_revenue_metrics a left join project.games_paid_users gpu on a.user_id = gpu.user_id;
