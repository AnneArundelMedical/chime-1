/*
File column names: date,day,census_hospitalized,census_icu,census_ventilated,region_name,population,market_share,relative_contact_rate,doubling_time,mitigation_date,mse
*/

--/*
drop table CovidPennModel;
--*/

/*
date,day,census_hospitalized,census_icu,census_ventilated,param_set_id,region_name,population,market_share,group_param_set_id,relative_contact_rate,mitigation_date,mse,run_date
*/

create table CovidPennModel (
  [date] date not null,
  [day] int not null,
  [census_hospitalized] real not null,
  [census_icu] real not null,
  [census_ventilated] real not null,
  [param_set_id] int not null,
  [region_name] varchar(60) not null,
  [population] int not null,
  [market_share] real not null,
  [group_param_set_id] int not null,
  [relative_contact_rate] real not null,
  [mitigation_date] date not null,
  hospitalized_rate real not null,
  [doubling_time] real not null,
  [mse] real not null,
  [run_date] date not null,
  hospitalized_days int not null,
  icu_rate real not null,
  icu_days int not null,
  ventilated_rate real not null,
  ventilated_days int not null,
  primary key ([run_date], [region_name], [relative_contact_rate]
  --/*
  , [doubling_time]
  --*/
  , [mitigation_date], [day]
  , hospitalized_rate, hospitalized_days, icu_rate, icu_days, ventilated_rate, ventilated_days
  )
);

/*
date,day,census_hospitalized,census_icu,census_ventilated,param_set_id,region_name,population,market_share,group_param_set_id,relative_contact_rate,
mitigation_date,hospitalized_rate,mse,run_date,hospitalized_days,icu_rate,icu_days,ventilated_rate,ventilated_days
*/

create index ix_cpm_psi on CovidPennModel ([param_set_id]);

bulk insert CovidPennModel
from 'D:\PennModelFit_Combined_2020-04-23_DT.csv'
with (firstrow=2, fieldterminator=',', rowterminator='\r\n')
;

select *
from (
select top 10 group_param_set_id, mse
from CovidPennModel
group by group_param_set_id, mse
order by mse asc
) g
join CovidPennModel m on m.group_param_set_id = g.group_param_set_id
order by m.mse, m.group_param_set_id, m.region_name, m.day

select m.mse, m.group_param_set_id, m.day
, sum(m.census_hospitalized) census_hospitalized
, max(m.mitigation_date) mitigation_date, max(m.[date]) [date]
from (
select top 10 group_param_set_id, mse
from CovidPennModel
group by group_param_set_id, mse
order by mse asc
) g
join CovidPennModel m on m.group_param_set_id = g.group_param_set_id
group by m.mse, m.group_param_set_id, m.day
order by m.mse, m.group_param_set_id, m.day

select group_param_set_id, count(distinct mse) distinct_mse
from CovidPennModel
group by group_param_set_id
having count(distinct mse) > 1
