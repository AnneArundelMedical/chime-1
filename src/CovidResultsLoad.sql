/*
File column names: date,day,census_hospitalized,census_icu,census_ventilated,region_name,population,market_share,relative_contact_rate,doubling_time,mitigation_date,mse
*/

use CovidModel;
go

--/*
drop table CovidPennModel;
-- truncate table CovidPennModel;
--*/

/*
date,day,census_hospitalized,census_icu,census_ventilated,param_set_id,region_name,population,market_share,group_param_set_id,relative_contact_rate,mitigation_date,mse,run_date
*/

create table CovidPennModel (
  [date] date not null,
  [day] int not null,
  susceptible real not null,
  infected real not null,
  recovered real not null,
  ever_infected real not null,
  ever_hospitalized real not null,
  hospitalized real not null,
  ever_icu real not null,
  icu real not null,
  ever_ventilated real not null,
  ventilated real not null,
  admits_hospitalized real not null,
  admits_icu real not null,
  admits_ventilated real not null,
  census_hospitalized real not null,
  census_icu real not null,
  census_ventilated real not null,
  param_set_id int not null,
  region_name varchar(60) not null,
  population int not null,
  market_share real not null,
  group_param_set_id int not null,

  policy_str varchar(max),
  policy_hash bigint not null,
  past_policy_str varchar(max),
  past_policy_hash bigint not null,
  future_policy_str varchar(max),
  future_policy_hash bigint not null,

  mitigation_date_1 date not null,
  relative_contact_rate_1 real not null,
  mitigation_date_2 date,
  relative_contact_rate_2 real,
  mitigation_date_3 date,
  relative_contact_rate_3 real,

  hospitalized_rate real not null,
  --doubling_time real not null,
  mse real not null,
  mse_icu real not null,
  mse_cum real not null,
  run_date date not null,
  end_date_days_back int not null,
  hospitalized_days int not null,
  icu_rate real not null,
  icu_days int not null,
  ventilated_rate real not null,
  ventilated_days int not null,
  current_hospitalized int not null,

  primary key (run_date, [day], region_name, param_set_id)

  -- mitigation_policy_hash
  /*
  , doubling_time
  --*/
  --, [mitigation_date]
  --, [day], end_date_days_back
  --, hospitalized_rate, hospitalized_days, icu_rate, icu_days, ventilated_rate, ventilated_days
  --)
) with (data_compression = page);

print 'Table created.';

/*

date day susceptible infected recovered ever_infected ever_hospitalized
hospitalized ever_icu icu ever_ventilated ventilated admits_hospitalized
admits_icu admits_ventilated census_hospitalized census_icu census_ventilated
param_set_id region_name population market_share group_param_set_id
relative_contact_rate mitigation_date hospitalized_rate mse mse_icu run_date
end_date_days_back hospitalized_days icu_rate icu_days ventilated_rate
ventilated_days current_hospitalized 

2020-03-13,-34,596418.1169857314,768.2639666494831,47.619047619047606,815.8830142685307,1.223824521402796,0.22382452140279618,0.2447649042805592,0.04476490428055921,0.17133543299639145,0.03133
5432996391466,0.22382452140279618,0.04476490428055921,0.031335432996391466,0.22382452140279618,0.04476490428055921,0.031335432996391466,1,Anne Arundel,597234,0.3,1,0.1,2020-04-05,0.005,128.83,38.03,2020-04-23,7,5,0.001,8,0.0007,8,61
2020-03-14,-33,596246.203681642,885.301273121125,102.49504523686782,987.7963183579927,1.4816944775369891,0.25786995613419306,0.29633889550739784,0.051573991226838645,0.20743722685517846,0.0361
0179385878701,0.25786995613419306,0.051573991226838645,0.03610179385878701,0.48169447753698924,0.09633889550739785,0.06743722685517847,1,Anne Arundel,597234,0.3,1,0.1,2020-04-05,0.005,128.83,38.03,2020-04-23,7,5,0.001,8,0.0007,8,61

*/

create index ix_cpm_psi on CovidPennModel ([param_set_id])
with (data_compression = page);

print 'Index created.';

bulk insert CovidPennModel
from 'D:\PennModelFit_Combined_2020-05-10_202005111247.csv'
with (tablock, firstrow=2, fieldterminator=',', rowterminator='\r\n')
;

print 'Bulk insert complete';

-- date,day,susceptible,infected,recovered,ever_infected,ever_hospitalized,hospitalized,ever_icu,icu,ever_ventilated,ventilated,admits_hospitalized,admits_icu,admits_ventilated,census_hospitalized,census_icu,census_ventilated,param_set_id,region_name,population,market_share,group_param_set_id,mitigation_policy_hash,mitigation_date_1,relative_contact_rate_1,mitigation_date_2,relative_contact_rate_2,mitigation_date_3,relative_contact_rate_3,hospitalized_rate,mse,mse_icu,mse_cum,run_date,end_date_days_back,hospitalized_days,icu_rate,icu_days,ventilated_rate,ventilated_days,current_hospitalized

print 'Beginning queries.';

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
, max(m.mitigation_date_1) mitigation_date_1
, max(m.mitigation_date_2) mitigation_date_2
, max(m.mitigation_date_3) mitigation_date_3
, max(m.[date]) [date]
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
