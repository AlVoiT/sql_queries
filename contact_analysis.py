import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime


def ch_get_df(sql):
    cc_conn = cc_connect(host='localhost', user='***', password='***', database='***')
    with cc_conn:
        df = pd.read_sql(sql, cc_conn)
    return df


camp2 = ch_get_df("""
with qwe as (
	select 
		user_id ,
		toDate(created) as  date_pay,
		payment_sum 
	from
		eres.daily_extarnal_payments_unisender p 
	where
	    toDate(created) >= '2021-09-01'
	    and payment_sum  > 0
	order by 
		date_pay asc
	limit 1 by user_id),
qwe2 as (
    select 
        user_id,
        toDate(started_at) as str_date,
        1 as counter 
    from
        prod.campaign_all as ca
    where 
        campaign_type = 'email'
        and str_date >= '2021-09-01'
        and num_sent > 0),
qwe3 as (
    select 
        u.id as user_id,
        toDate(reg_time) as date_reg,
        date_pay,
        (IF (date_pay='1970-01-01',today(),date_pay) - date_reg) as delt,
        (If (delt <8,7,0)) as groups1,
        (If (delt <15,14,0)) as groups2,
        (If (delt >=15,15,0)) as groups3
    from
        prod.user as u
    left join
        qwe
    on 
        u.id = qwe.user_id
    where
        toDate(reg_time) >= '2021-09-01'
        and payment_sum != 0
    order by
        delt desc)
select 
    user_id, 
    groups1,
    groups2,
    groups3,
    COUNT(counter) 
from
    qwe3
left join 
    qwe2
on
    qwe3.user_id=qwe2.user_id
WHERE 
    str_date BETWEEN date_reg and date_pay
group by 
    user_id,
    groups1,
    groups2,
    groups3
""")

camp2 = camp2.groupby(['groups1','groups2','groups3']).mean().drop(['user_id',], axis = 1 )

df = ch_get_df("""
with qwe as (
	select 
		user_id ,
		toDate(created) as  date_pay,
		payment_sum 
	from
		eres.daily_extarnal_payments_unisender p 
	where
	    toDate(created) >= '2021-09-01'
	    and payment_sum  > 0
	order by 
		date_pay asc
	limit 1 by user_id),
qwe2 as (
    select 
        user_id,
        started_at as str_date
    from
        prod.campaign_all as ca
    where 
        campaign_type = 'email'
        and str_date >= '2021-09-01'
        and num_sent > 0)
select 
    user_id,
    str_date
from 
    qwe
left join 
    qwe2
on
    qwe.user_id=qwe2.user_id
order by 
    user_id asc,
    str_date asc
""")

df['diff'] = df.groupby(['user_id'])['str_date'].diff() #.fillna(timedelta(0))
df1 = df.groupby(['user_id']).sum().reset_index()
df2 = df.groupby(['user_id']).count().drop(['str_date'],axis=1).reset_index()
last = df1.merge(df2,
                    how='left',
                    left_on=['user_id'],
                    right_on=['user_id'])
last.rename(columns={'diff_x': 'sum', 'diff_y': 'count'}, inplace=True)
last['mean'] = last['sum'].divide(last['count']) #.fillna(timedelta(0))
last.describe()
    
    
    
    
 
