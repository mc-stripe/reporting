with country_code as(
   select * from usertables.mc_country_codes_csv),
-- team role and location data [MAKE SURE THIS IS UP TO DATE]
team_role as(
select * from usertables.mc_team_role_csv

),
processing as (
   select
      'weekly_processing' as data_type,
      capture_date,
      ap.sales_merchant_id as sales_merchant_id,
      'new biz' AS sales_category,
      nvl(sales_funnel__activation_date) as sales_activation_date,
      sales_funnel__activation_date as orig_activation, 
      sum(case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 * ap.npv / 100 else 0 end) as first_year_sold_npv_usd_fx,
      sum(case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 * ap.revenue__total / 100 else 0 end) as first_year_sold_revenue_usd_fx,
      sum(case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 * ap.costs__total / 100 else 0 end) as first_year_sold_costs_usd_fx,
      sum(case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 * ap.revenue__fx / 100 else 0 end) as first_year_sold_revenue_fx_usd_fx,
      sum(case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 * ap.revenue__disputes / 100 else 0 end) as first_year_sold_revenue_disputes_usd_fx,
      sum(case when datediff('d', sales_funnel__activation_date, capture_date) >= 0 and datediff('d', sales_funnel__activation_date, capture_date) < 366 then 1 * ap.revenue__other_api / 100 else 0 end) as first_year_sold_revenue_other_api_usd_fx
   from aggregates.margin ap
      join dim.merchants AS m ON ap.sales_merchant_id = m._id
   where
      capture_date >= '2016-04-01'
   and 
                               capture_date < '2017-06-11' and

   m.sales__is_sold = true
   group by 1,2,3,4,5,6
)

select 
  'weekly_processing' as data_type,
  to_char(date_trunc('year', capture_date),'YYYY') as year,
  to_char(date_trunc('quarter', capture_date), 'YYYY-MM') as quarter,
  to_char(date_trunc('month', capture_date),'YYYY-MM') as month,
  case when date_trunc('quarter', capture_date) = date_trunc('quarter', CURRENT_DATE) then 1 else 0 end as mtd, 
  0 as this_month, 
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
  when cc.sales_region = 'AU/NZ' then cc.sfdc_country_name
  -- SG
  when cc.sales_region = 'SG/HK' then m.sales__industry
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
  pv.sales_merchant_id as sales_merchant_id,
  m.sales__name AS merchant_name,
  sales_category,
  sales_activation_date,
  case when datediff('d', sales_activation_date, capture_date) >= 0 and datediff('d', sales_activation_date, capture_date) < 91 then 1 else 0 end as ninety_day_live,
  case when datediff('d', sales_activation_date, capture_date) >= 0 and datediff('d', sales_activation_date, capture_date) < 366 then 1 else 0 end as first_year_sold,
  COALESCE(SUM(first_year_sold_npv_usd_fx), 0) AS npv_fixed_fx,
  COALESCE(SUM(first_year_sold_revenue_usd_fx), 0) AS revenue_fixed_fx,
  COALESCE(SUM(first_year_sold_costs_usd_fx), 0) AS costs_fixed_fx,
  COALESCE(SUM(first_year_sold_revenue_fx_usd_fx), 0) AS revenue_fx_fixed_fx,
  COALESCE(SUM(first_year_sold_revenue_disputes_usd_fx), 0) AS revenue_disputes_fixed_fx,
  COALESCE(SUM(first_year_sold_revenue_other_api_usd_fx), 0) AS revenue_other_api_fixed_fx,
  opportunity_amount
  

FROM processing pv
JOIN dim.merchants AS m ON pv.sales_merchant_id = m._id
JOIN country_code as cc ON m.sales__merchant_country = cc.country_code
JOIN team_role as usr ON usr.sales_owner = m.sales__owner
JOIN sales.salesforce ss ON m.sales__sfdc_opportunity = ss.opportunity
where
capture_date >= '2017-01-01' 


GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,27
