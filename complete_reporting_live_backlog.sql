/**********************************
First load all the user tables that I have uploaded so that the query runs faster. 
I have found doing this is much faster than joining on usertables. 
**********************************/

-- load the upsells table and only include the ones that have an start date in the past
-- this table is available (https://docs.google.com/spreadsheets/d/10wzAcpwksW1reaGA7bKBQDiLDLVrOlnKcvuk1JTqocI/edit)
with current_upsells as (
   select * from usertables.mc_upsells_csv where include = 1 
),
-- load backlog curve, this determines the backlog 
backlog_curve as (
   select * from usertables.mc_backlog_master_global_csv
),
-- country name and code groupings
country_code as(
   select * from usertables.mc_country_codes_csv),
-- team role and location data [MAKE SURE THIS IS UP TO DATE]
team_role as(
   select * from usertables.mc_team_role_csv),

-- load the sales_merchant_id end dates for backlog query and sales override activation date
special_dates as (
   select 
      stripe_parent_merchant_id as sales_merchant_id, 
      max(opportunity_expected_end_date) as backlog_end_date,
      max(opportunity_sales_override_activation_date) as adjusted_activation_date
   from sales.salesforce 
   where 
      opportunity_expected_end_date is not null  
      OR
      opportunity_sales_override_activation_date is not null
   group by 1
),

in_qtr_opportunities as (
   select 
      stripe_parent_merchant_id as sales_merchant_id
   from sales.salesforce 
   where 
      
)



/**********************************
Now calculate the non_upsell_processing and then the upsell processing in two steps so that we can see the contribution
of upsells to nPV. 
**********************************/

-- calculate processing volumes before upsells
non_upsell_processing as (
   select
      'weekly_processing' as data_type,
      capture_date,
      ap.sales_merchant_id as sales_merchant_id,
      'new biz' AS sales_category,
      nvl(adjusted_activation_date, sales_funnel__activation_date) as sales_activation_date,
      sales_funnel__activation_date as orig_activation, 
      sum(case 
         when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 and upsells.effective_activation_date::date is null then 1 * ap.npv_usd_fixed / 100 -- no upsell 99.9%
         when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 and datediff('d', upsells.effective_activation_date::date, capture_date) < 0 then 1 * ap.npv_usd_fixed / 100 -- upsell not started
         when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 and datediff('d', upsells.effective_activation_date::date, capture_date) >= 0 and datediff('d', upsells.effective_activation_date::date, capture_date) < 366 then (1 - pct_share_of_npv) * ap.npv_usd_fixed / 100 -- upsell and original sale overlap
         else 0 
      end) as first_year_sold_npv_usd_fx,
      sum( ap.npv_usd_fixed / 100) as total_npv
   from aggregates.payments ap
      join dim.merchants AS m ON ap.sales_merchant_id = m._id
      left join current_upsells as upsells ON upsells.sales_merchant_id = ap.sales_merchant_id
      left join special_dates as adj_start_date ON ap.sales_merchant_id = adj_start_date.sales_merchant_id -- include manual adjustment for start date
   where
      capture_date >= '2017-01-01'
   and 
                               capture_date < '2017-04-16' and

   m.sales__is_sold = true
   group by 1,2,3,4,5,6
),

upsell_processing as (
select
'weekly_processing' as data_type,
capture_date,
ap.sales_merchant_id as sales_merchant_id,
'upsell' AS sales_category,
upsells.effective_activation_date::date as sales_activation_date,
sales_funnel__activation_date as orig_activation,
sum(case 
   when datediff('d', upsells.effective_activation_date::date, capture_date) >= 0 and datediff('d', upsells.effective_activation_date::date, capture_date) < 366 then pct_share_of_npv * ap.npv_usd_fixed / 100 -- upsell and original sale overlap
   else 0 
end) as first_year_sold_npv_usd_fx,
sum( ap.npv_usd_fixed / 100) as total_npv

from aggregates.payments ap
JOIN dim.merchants AS m ON ap.sales_merchant_id = m._id
INNER JOIN current_upsells as upsells ON upsells.sales_merchant_id = ap.sales_merchant_id
where
capture_date >= '2017-01-01'
and 
                               capture_date < '2017-04-16' and
 
m.sales__is_sold = true

group by 1,2,3,4,5,6), 


/**********************************
Join the upsell and non-upsell tables so that we have a complete data set of 
all sold processing volumes
**********************************/


processing_volume as (
select * from non_upsell_processing
union
select * from upsell_processing ),

/********************************************
********************************************
CHANGE DATES BELOW TO REPORTING DATE (TWO PLACES)
We have to do this because if you use the max of the capture date and there are gaps in processing days
we will miscalculate the days_since_activation. We could use a dynamic CURRENT_DATE - 1 or something,
but this could be prown to errors also. 
********************************************
********************************************/
backlog_summary as (
select
sales_merchant_id,
sales_category, 
sales_activation_date,
datediff('day', pv.sales_activation_date, '2017-03-26') as days_since_activation,      -- UPDATE THIS TO THE LAST DAY OF PROCESSING
orig_activation,
first_year_npv,
first_year_npv/first_year_sold_cumulative_pct as first_year_est_npv
from
(select 
sales_merchant_id,
sales_category, 
sales_activation_date,
orig_activation,
sum(first_year_sold_npv_usd_fx) as first_year_npv
from processing_volume group by 1,2,3,4) pv 


inner join backlog_curve bc on bc.days_since_activation = datediff('day', pv.sales_activation_date, '2017-03-26')  -- HAVE TO SPECIFY THE DATE
where first_year_npv > 0),

daily_backlog as (select
dateadd('day', curve.days_since_activation, sales_activation_date) as fcst_date, 
sales_merchant_id, 
sales_category,
orig_activation,
sales_activation_date,
first_year_est_npv * first_year_sold_pct as backlog_npv
from backlog_summary bs 
cross join backlog_curve as curve
where (curve.days_since_activation between bs.days_since_activation and 365))


/************************************* 
Create formatted output adding in all the information that is useful for reporting 
user_level_table
**************************************/ 

select 
  'weekly_processing' as data_type,
  to_char(date_trunc('year', capture_date),'YYYY') as year,
  to_char(date_trunc('quarter', capture_date), 'YYYY-MM') as quarter,
  to_char(date_trunc('week', capture_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as finance_week,
  case when date_trunc('quarter', capture_date) = date_trunc('quarter', CURRENT_DATE) then 1 else 0 end as qtd, 
  case when date_trunc('week', capture_date + '1 day'::interval)::date - '1 day'::interval = date_trunc('week', dateadd('day',-3, CURRENT_DATE) + '1 day'::interval)::date - '1 day'::interval then 1 else 0 end as this_week, 
  cc.sales_region as region,
  cc.sfdc_country_name as country,
  '' as sales_channel,
  case
  -- 1. filter team type first
  --when sales_location = 'Hub' then 'Hub' 
  when role = 'NBA' then 'NBA'
  -- UK verticals
  when cc.sales_region = 'UK' and m.sales__industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and m.sales__industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and m.sales__industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate', 'On-Demand Services') then 'Services, Software & Content'
  when cc.sales_region = 'UK' and m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sales_region = 'UK' and m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  -- US/CA
  when cc.sfdc_country_name = 'United States' and m.sales__industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate', 'On-Demand Services') then 'Services'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry is null then 'No industry'
  when cc.sfdc_country_name = 'Canada' then 'CA'  
  -- SouthernEU
  when cc.sales_region = 'Southern EU' then cc.sfdc_country_name
  -- NorthernEU
  when cc.sales_region = 'Northern EU' and cc.sfdc_country_name in ('Germany','Austria','Switzerland') then 'DACH'
  when cc.sales_region = 'Northern EU' and cc.sfdc_country_name in ('Belgium','Netherlands','Luxembourg') then 'BENELUX'
  when cc.sales_region = 'Northern EU' and cc.sfdc_country_name in ('Norway', 'Finland', 'Sweden', 'Denmark', 'Iceland') then 'NORDICS'  
  -- AU/NZ
  when cc.sales_region = 'AU' then cc.sfdc_country_name
  -- SG
  when cc.sales_region = 'SG' then cc.sfdc_country_name
  when cc.sales_region = 'New Markets' then cc.sfdc_country_name
  -- IE
  when cc.sales_region = 'IE' then cc.sfdc_country_name
  else 'other'
end AS sub_region,  
case
  -- UK verticals
  when cc.sales_region = 'UK' and m.sales__industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and m.sales__industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and m.sales__industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate', 'On-Demand Services') then 'Services, Software & Content'
  when cc.sales_region = 'UK' and m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sales_region = 'UK' and m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  -- Standard verticals
  when m.sales__industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when m.sales__industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate', 'On-Demand Services')
  then 'Services'
  when m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when m.sales__industry is null then 'No industry'
  else 'other'
end
 AS vertical,  
  m.sales__owner as owner,
  usr.role as sales_role,
  usr.team AS sales_location,
  sales_merchant_id as sales_merchant_id,
  m.sales__name AS merchant_name,
  sales_category,
  sales_activation_date,
  case when datediff('d', sales_activation_date, capture_date) >= 0 and datediff('d', sales_activation_date, capture_date) < 91 then 1 else 0 end as ninety_day_live,
  case when datediff('d', sales_activation_date, capture_date) >= 0 and datediff('d', sales_activation_date, capture_date) < 366 then 1 else 0 end as first_year_sold,
  COALESCE(SUM(first_year_sold_npv_usd_fx), 0) AS npv_fixed_fx,
  case when orig_activation > '2016-12-31' then 1 else 0 end as newNPV,
  case when date_trunc('quarter', sales_activation_date) = date_trunc('quarter',current_date-5) and date_trunc('quarter', min(opportunity_created_date)) = date_trunc('quarter',current_date-5) then 1 else 0 end as in_quarter_opportunity

FROM processing_volume pv
JOIN dim.merchants AS m ON pv.sales_merchant_id = m._id
JOIN country_code as cc ON m.sales__merchant_country = cc.country_code
JOIN team_role as usr ON usr.sales_owner = m.sales__owner
LEFT JOIN sales.salesforce as sfdc ON sfdc.stripe_parent_merchant_id ON m._id

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,23



union

select 
  'weekly_backlog' as data_type,
  to_char(date_trunc('year', fcst_date),'YYYY') as year,
  to_char(date_trunc('quarter', fcst_date), 'YYYY-MM') as quarter,
  to_char(date_trunc('week', fcst_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as finance_week,
  0 as qtd, 
  0 as this_week, 
  cc.sales_region as region,
  cc.sfdc_country_name as country,
  '' as sales_channel,
  case
  -- 1. filter team type first
  --when sales_location = 'Hub' then 'Hub' 
  when role = 'NBA' then 'NBA'
  -- UK verticals
  when cc.sales_region = 'UK' and m.sales__industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and m.sales__industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and m.sales__industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate', 'On-Demand Services') then 'Services, Software & Content'
  when cc.sales_region = 'UK' and m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when cc.sales_region = 'UK' and m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'



  -- US/CA
  when cc.sfdc_country_name = 'United States' and m.sales__industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate', 'On-Demand Services') then 'Services'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sfdc_country_name = 'United States' and  m.sales__industry is null then 'No industry'
  when cc.sfdc_country_name = 'Canada' then 'CA'  
  -- SouthernEU
  when cc.sales_region = 'Southern EU' then cc.sfdc_country_name
  -- NorthernEU
  when cc.sales_region = 'Northern EU' and cc.country_code in ('DE','AT','CH') then 'DACH'
  when cc.sales_region = 'Northern EU' and cc.country_code in ('BE','NL','LU') then 'BENELUX'
  when cc.sales_region = 'Northern EU' and cc.country_code in ('NO', 'FI', 'SE', 'DK', 'IS') then 'NORDICS'  
  -- AU/NZ
  when cc.sales_region = 'AU' then cc.sfdc_country_name
  -- SG
  when cc.sales_region = 'SG' then cc.sfdc_country_name
  when cc.sales_region = 'New Markets' then cc.sfdc_country_name
  -- IE
  when cc.sales_region = 'IE' then cc.sfdc_country_name
  else 'other'
end AS sub_region,  
case
  -- UK verticals
  when cc.sales_region = 'UK' and m.sales__industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and m.sales__industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and m.sales__industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate', 'On-Demand Services') then 'Services, Software & Content'
  when cc.sales_region = 'UK' and m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when cc.sales_region = 'UK' and m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  -- Standard verticals
  when m.sales__industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when m.sales__industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate', 'On-Demand Services')
  then 'Services'
  when m.sales__industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when m.sales__industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when m.sales__industry is null then 'No industry'
  else 'other'
end
 AS vertical,  
  m.sales__owner as owner,
  usr.role as sales_role,
  usr.team AS sales_location,
  pv.sales_merchant_id as sales_merchant_id,
  m.sales__name AS merchant_name,
  sales_category,
  sales_activation_date,
  case 
     when backlog_end_date is not null and datediff('d', backlog_end_date, fcst_date) >= 0 then 0
     when datediff('d', sales_activation_date, fcst_date) >= 0 and datediff('d', sales_activation_date, fcst_date) < 91 then 1 
     else 0 end as ninety_day_live,
  
  case 
    when backlog_end_date is not null and datediff('d', backlog_end_date, fcst_date) >= 0 then 0
    when datediff('d', sales_activation_date, fcst_date) >= 0 and datediff('d', sales_activation_date, fcst_date) < 366 then 1 
    else 0 end as first_year_sold,

  COALESCE(SUM(backlog_npv), 0) AS npv_fixed_fx,
  case when orig_activation > '2016-12-31' and fcst_date <='2017-12-31' then 1 else 0 end as newNPV
  case when date_trunc('quarter', sales_activation_date) = date_trunc('quarter',current_date-5) and date_trunc('quarter', min(opportunity_created_date)) = date_trunc('quarter',current_date-5) then 1 else 0 end as in_quarter_opportunity
FROM daily_backlog pv
JOIN dim.merchants AS m ON pv.sales_merchant_id = m._id
JOIN country_code as cc ON m.sales__merchant_country = cc.country_code
JOIN team_role as usr ON usr.sales_owner = m.sales__owner
LEFT JOIN special_dates as early_ending_users ON early_ending_users.sales_merchant_id = pv.sales_merchant_id
LEFT JOIN sales.salesforce as sfdc ON sfdc.stripe_parent_merchant_id ON m._id

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22
