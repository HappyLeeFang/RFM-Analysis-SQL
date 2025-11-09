Use snowflake_sample_data.tpch_sf1;
--Part 1 Customer Segmentation (RFM) 

--CTE 1: Customer Segmentation (RFM) 

with customer_base as (

            select
                c.c_custkey as customer_id, -- Unique customer identifier
                c.c_name as customer_name, 
                n.n_name as nation,
                min(o.o_orderdate) as first_order_date, --Oldest order date
                max(o.o_orderdate) as last_order_date, --Tracks customer lifecycle start for dynamic purpose (as if it is from now time)
                count(distinct o.o_orderkey) as total_orders, --I use COALESCE and NULLIFF to ensuring safe division
                sum(l.l_extendedprice * (1 - l.l_discount)) as total_revenue, -- Total revenue
                coalesce(
                    sum(l.l_extendedprice * (1 - l.l_discount)) / 
                        nullif(count(distinct o.o_orderkey), 0), 0
                         ) as avg_order_value -- Aggregation on customer level
            from snowflake_sample_data.tpch_sf1.customer as c
                inner join snowflake_sample_data.tpch_sf1.orders as o
                    on c.c_custkey = o.o_custkey -- Only customers with orders
                inner join snowflake_sample_data.tpch_sf1.lineitem as l
                    on o.o_orderkey = l.l_orderkey -- Only orders with line items
                inner join snowflake_sample_data.tpch_sf1.nation as n
                    on c.c_nationkey = n.n_nationkey -- Attach nation details
            group by
                c.c_custkey,
                c.c_name,
                n.n_name
),

-- CTE 2. Calculate RFM values for each customer
--I use DATEDIFF to measure how many days since each customer's last order, compared to the newest order in the dataset (defines Recency Value).

rfm_value as (

            select
                customer_id,
                customer_name,
                nation,
                first_order_date,
                last_order_date,
                total_revenue,
                total_orders,
                avg_order_value,
                datediff('day', last_order_date, max(last_order_date) over (
                )) as recency_value, -- Latest order day(s)
                total_orders as frequency_value,
                total_revenue as monetary_value
            from customer_base as cb
    ),
    
-- CTE 3: Assign RFM scores with "case when"
--I use percentile-based ranking (CUME_DIST) to handle data skew and ties
--Ensuring consistent segmentation across datasets and allowing dynamic identification of top X% VIP customers (e.g. top 5%)
--(The percentage can be adjusted here, which can be dynamic, I avoid using ROUND, because I want to keep the decimals for this case)
--Manual buckets CASE WHEN ensure that the scoring system stays consistent, even if the dataset changes

rfm_segmentation as (

            select
                customer_id,
                customer_name,
                nation,
                first_order_date,
                last_order_date,
                total_revenue,
                total_orders,
                avg_order_value,
                --R/F/M value below 
                recency_value, 
                frequency_value,
                monetary_value,
                --I use cume_dist()to calculate MONETARY PERCENTILE, it handles skewed spending behavior and adapts to different customer distributions
                cume_dist() over (
                    order by 
                        rfmv.monetary_value desc, 
                        rfmv.customer_id asc) 
                        as monetary_percentile,
                --I insert a VIP label on MONETARY VALUE (The percentage can be adjusted here, which can be dynamic)
                case 
                    when cume_dist() over (
                        order by 
                            rfmv.total_revenue desc, 
                            rfmv.customer_id asc) <= 0.05 then 'Yes'
                    else 'No' 
                end as mon_vip,
                -- More recent = Higher score, (5 = Best)
                case 
                    when rfmv.recency_value <= 30  then 5
                    when rfmv.recency_value <= 90  then 4
                    when rfmv.recency_value <= 180 then 3
                    when rfmv.recency_value <= 365 then 2
                    else 1 -- >365 days
                end as r_score, 
                --Frequency score (5 = Best)
                case
                    when rfmv.frequency_value >= 25 then 5
                    when rfmv.frequency_value between 17 and 24 then 4
                    when rfmv.frequency_value between 12 and 16 then 3
                    when rfmv.frequency_value between 9 and 11 then 2
                    when rfmv.frequency_value <= 8 then 1
                end as f_score,
                case
                    when monetary_percentile <= 0.5 then 5
                    when monetary_percentile between 0.5 and 0.15 then 4
                    when monetary_percentile between 0.15 and 0.43 then 3
                    when monetary_percentile between 0.43 and 0.71 then 2
                    when monetary_percentile between 0.71 and 1.00 then 1
                end as m_score
            from rfm_value as rfmv

),

-- CTE 4: RFM Label Construction
--This label makes it easier to group and analyze customers' behavior patterns at a glance

rfm_label as(

            select 
                customer_id,
                customer_name,
                nation,
                first_order_date,
                last_order_date,
                total_revenue,
                total_orders,
                avg_order_value,
                recency_value,
                frequency_value,
                monetary_value,
                monetary_percentile,
                mon_vip,
                r_score,
                f_score,
                m_score,
                concat(
                    cast(rfmseg.r_score as varchar),
                    cast(rfmseg.f_score as varchar),
                    cast(rfmseg.m_score as varchar)
                ) as rfm_label
            from rfm_segmentation as rfmseg
            
),

-- CTE 5: Final Customer Segmentation based on RFM scores and how I define them.

--1 Champions: Customers with the highest possible scores in all three metrics. They buy often, spend a lot, and purchased recently.
--2 Potential Customers: High scores (4–5) in all metrics, but not perfect '555'.
--3 Loyal Customers: Medium-high engagement. Consistent buyers who bring steady revenue.
--4 New Customers: Recent purchasers (high recency) but low frequency and monetary scores. They’ve just started buying.
--5 Big Spenders: Spend alot (High Monetary) but not necessarily frequent or recent.
--6 At Risk Customers: Used to buy frequently and spend well, but haven’t purchased recently (Low recency).
--7 Need Attentions: Moderate recency but low frequency and spend.
--8 About To Sleep: Low recency, frequency, and spend. Showing clear signs of disengagement.
--9 Dormant Customers: Lowest possible RFM scores.
--10 Fading Customers: Mixed or transitional patterns that don’t fit neatly into others.

customer_segmentation as(

            select
                customer_id,
                customer_name,
                nation,
                first_order_date,
                last_order_date,
                total_revenue,
                total_orders,
                avg_order_value,
                recency_value,
                frequency_value,
                monetary_value,
                monetary_percentile,
                r_score,
                f_score,
                m_score,
                rfm_label,
                case
                    when r_score = 5 and f_score = 5 and m_score = 5 then 'Champions' --1
                    when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Potential Champions' --2
                    when r_score >= 3 and f_score >= 3 and m_score >= 3 then 'Loyal Customers' --3
                    when r_score >= 4 and f_score <= 2 and m_score <= 2 then 'New Customers' --4
                    when m_score = 5 and r_score >= 2 and f_score >= 2 then 'Big Spenders' --5
                    when r_score <= 2 and f_score >= 3 and m_score >= 3 then 'At Risk Customers' --6
                    when r_score = 3 and f_score <= 2 and m_score <= 2 then 'Need Attention' --7
                    when r_score = 2 and f_score <= 2 and m_score <= 2 then 'About To Sleep' --8
                    when r_score = 1 and f_score = 1 and m_score = 1 then 'Dormant Customers' --9
                    else 'Fading Customers' --10
                end as customer_type
            from rfm_label

)

--Final Select: Top 10 rows of result set

select * 
from customer_segmentation
order by rfm_label desc
limit 10
;

-- Part 2 Calculate the total revenue generated by each segment

/*
-- 1. Potential Champions and Big Spenders show strong buying potential and are the most likely to become new Champions with the right engagement
-- By analyzing the products they purchases, we can personalize offers, highlight top repeat items and design loyalty campaigns that boost both spending and retention (e.g. upgrade incentives: Offer exclusive bundles, member only offers)

select
    customer_type,
    count(customer_id) as customer_count,
    -- Here I calculates the proportion of customers in each segment to show how much contributtion on each of them
    round(count(customer_id) * 1.0 / sum(count(customer_id))over (), 2) as segment_size_percent
from customer_segmentation
group by customer_type
order by customer_count desc
;

/*

/*

-- 2. Revenue is heavily concentrated in Potential Champions and Loyal Customers
-- We can strengthen these groups while re-engaging high value but declining segments, like Fading Customers, will have potential to drive both short-term gains and long-term stability

select 
    customer_type, 
    round(sum(monetary_value), 2) as segment_revenue, 
    round(sum(monetary_value) * 1.0 / sum(sum(monetary_value)) over (), 2) as segement_size_percent_on_revenue,
    -- Here I assigns a rank to each segment based on total revenue, highest revenue = rank 1
    rank() over (order by sum(monetary_value) desc) as revenue_seg_rank
from customer_segmentation
group by customer_type
order by segment_revenue desc
;

*/

/*

-- 3. Here, we can see that not all high spenders (e.g., those generating $6.8M) achieve a 555 score, as some contribute significant revenue but purchase less frequently
--By re-examining their recency and frequency values, we can better identify thesee valuable yet irregular buyers
--This refined approach ensures our RFM segmentation captures both consistent loyalty and true long-term value
--Additionally, analyzing their preferred products and purchase timing throughout the year can help uncover seasonal patterns and optimize personalized marketing strategies

select 
    customer_id, 
    customer_name, 
    nation,
    rfm_label, 
    total_revenue, 
    total_orders, 
    last_order_date,
from customer_segmentation
order by total_revenue desc
limit 5
;

*/

/*

-- 4. Germany shows a mature and consistent customer base, with strong spending habits and stable buying patterns that make it ideal for loyalty and upsell strategies
--However, Morocco's total revenue is high as well
--By reinforcing loyalty with Champions in Germany and building a closer erelationships with Morocco, we can help shift Potential Champions in Morocco to Champions

select
  nation,
  count(*) as champion_count,
  sum(monetary_value) as total_revenue__champions,
  round(
    avg(monetary_value), 2) 
        as avg_revenue_per_champion,
  -- I add rank nations based on number of champions, dense_rank avoids gaps in ranking
  dense_rank() over (order by count(*) desc) as nation_rank,
  -- I add a flag the nation with the highest number of champions as 'Top'
  case when dense_rank() over (order by count(*) desc) = 1 then 'Top' else '' end as top_nation_flag,
from customer_segmentation
where r_score = 5 and f_score = 5 and m_score = 5
group by nation
order by 
    nation_rank, 
    nation
;

*/


