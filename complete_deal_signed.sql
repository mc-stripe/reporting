-- change date at the bottom to report date

-- country detail mapping
with country_code as(
select * from usertables.mc_country_codes_csv),
-- team mapping
team_role as(
select * from usertables.mc_team_role_csv)

select
'deal_data' as data_type,
-- deal_signed stats
to_char(date_trunc('quarter', opportunity_close_date), 'YYYY-MM') as close_quarter,
to_char(date_trunc('week', opportunity_close_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as close_finance_week,
case when opportunity_stage in ('Live', 'Onboarding') then 1 else 0 end as closed,
-- pipeline created date
to_char(date_trunc('quarter', opportunity_created_date), 'YYYY-MM') as pipline_created_quarter,
to_char(date_trunc('week', opportunity_created_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as pipeline_created_finance_week,
-- expected go live dates
to_char(date_trunc('quarter', opportunity_expected_go_live_date), 'YYYY-MM') as expected_go_live_quarter,
to_char(date_trunc('week', opportunity_expected_go_live_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') as expected_go_live_finance_week,
to_char(opportunity_expected_go_live_date, 'YYYY-MM-DD') as opportunity_expected_go_live_date,
-- reference data
cc.sales_region as region,
cc.sfdc_country_name as country,
'' as sales_channel,
case
  -- 1. filter team type first
  --when sales_location = 'Hub' then 'Hub' 
  when role = 'NBA' then 'NBA'
  -- UK verticals
  when cc.sales_region = 'UK' and opportunity_industry in ('Ticketing & Events', 'Travel & Hosp') then 'Ticketing/Travel'
  when cc.sales_region = 'UK' and opportunity_industry in ('Financial') then 'Financial Services'
  when cc.sales_region = 'UK' and opportunity_industry in ('Healthcare', 'Professional Services', 'Other Services','B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)', 'Real Estate') then 'Services, Software & Content'
  when cc.sales_region = 'UK' and opportunity_industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sales_region = 'UK' and opportunity_industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  
  -- US/CA
  when cc.sfdc_country_name = 'United States' and opportunity_industry in ('B2B', 'B2C Software', 'Content', 'Other Software & Content', 'B2C (Software)', 'B2B (Software)') then 'Software & Content'
  when cc.sfdc_country_name = 'United States' and  opportunity_industry in ('Ticketing & Events', 'Financial', 'Healthcare', 'Professional Services', 'Other Services', 'Travel & Hosp', 'Real Estate') then 'Services'
  when cc.sfdc_country_name = 'United States' and  opportunity_industry in ('Government', 'EDU', 'Non-Profit', 'Utilities', 'Other Public Sector') then 'Public Sector'
  when cc.sfdc_country_name = 'United States' and  opportunity_industry in ('Fashion', 'Food & Bev', 'Manufacturing', 'Other Retail') then 'Retail'
  when cc.sfdc_country_name = 'United States' and  opportunity_industry is null then 'No industry'
  when cc.sfdc_country_name = 'Canada' then 'Canada'  
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
  opportunity_owner as owner,
  usr.role as sales_role,
  usr.team AS sales_location,
  'OPTI__' || opportunity as sales_merchant_id,
 opportunity_name as merchant_name, 
opportunity_stage,
opportunity_probability,
opportunity_amount,
opportunity_probability*opportunity_amount as  wgted_opportunity_amount,
case when to_char(date_trunc('week', opportunity_created_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') = '2017-02-26' then 1 else 0 end as created_this_week,
case when to_char(date_trunc('week', opportunity_close_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') = '2017-02-26'  then 1 else 0 end as closed_this_week,
case when to_char(date_trunc('week', opportunity_expected_go_live_date + '1 day'::interval)::date - '1 day'::interval,'YYYY-MM-DD') = '2017-02-26'  then 1 else 0 end as live_this_week


from sales.salesforce ss
JOIN country_code as cc ON ss.opportunity_merchant_country = cc.sfdc_country_name
JOIN team_role as usr ON usr.sales_owner = ss.opportunity_owner
WHERE
opportunity_stage <> 'Lost'
