**Part 1 – RFM Segmentation**
# RFM Analysis with SQL (Snowflake)

This project applies the RFM model (Recency, Frequency, Monetary) to segment customers and analyze behavioral patterns using Snowflake SQL.  
The goal is to identify valuable customer groups such as Champions, Loyal Customers, Big Spenders, and Dormant Customers, and provide insights for marketing and retention strategies.

---

## Project Overview
- Database: `snowflake_sample_data.tpch_sf1`
- Language: SQL (Snowflake)
- Focus: Customer segmentation, loyalty analysis, and revenue insights

This analysis demonstrates how to transform transactional data into meaningful customer insights using Common Table Expressions (CTEs), window functions, and conditional logic.

---

## Analysis Steps

### 1. Customer Base Construction
- Combined customer, orders, line items, and nation tables.
- Calculated total orders, total revenue, and average order value per customer.

### 2. Calculate RFM Values
- Recency: Days since last purchase  
- Frequency: Total number of unique orders  
- Monetary: Total revenue per customer  

Used `DATEDIFF` to calculate recency dynamically.

### 3. Assign RFM Scores
- Used `CUME_DIST()` to handle skewed spending distribution.
- Applied `CASE WHEN` logic to assign scores:
  - 5 = Best (most recent / frequent / high-spending)
  - 1 = Lowest (least recent / rare / low-spending)

### 4. Create RFM Labels
- Combined R, F, M scores into one label (e.g., 555 for Champions).
- Added a “VIP” tag for the top 5% spenders based on monetary value.

### 5. Segment Customers
Defined 10 customer types based on behavioral patterns:
| Segment | Description |
|----------|-------------|
| Champions | Recent, frequent, and high spenders |
| Potential Champions | Strong across most metrics |
| Loyal Customers | Consistent and steady buyers |
| New Customers | Recently joined, low frequency |
| Big Spenders | Spend a lot but not frequent |
| At Risk Customers | Used to buy often but inactive recently |
| Need Attention | Moderate recency, low activity |
| About To Sleep | Almost inactive |
| Dormant Customers | Fully inactive |
| Fading Customers | Transitional group |

---

---

## Part 1 Result Preview  

In Part 1, we built the full RFM segmentation model using Snowflake SQL.  
You can skip the previous preview query (`LIMIT 10`) and instead move directly into **Part 2 — Extended Revenue Analysis** for deeper insights.

➡️ **Next step:** Uncomment and run the queries in **Part 2** to explore:
1. Segment Size and Contribution  
2. Segment Revenue and Ranking  
3. Top 5 Revenue Customers  
4. Nations with Most Champions  

---


## Part 2 — Extended Revenue Analysis  

Once you have run **Part 1 – RFM Segmentation**, you can **uncomment** the following SQL queries to continue exploring deeper insights.  
Each query focuses on a different analytical perspective of customer and revenue performance.  
*(To run them, simply remove the `/*` and `*/` comment marks in your SQL editor.)*  

---

### Query 1 — Segment Size and Customer Contribution  
Calculates how many customers belong to each RFM segment and what percentage of total customers they represent.  
Use this to understand which customer groups are the largest and most influential.  

---

### Query 2 — Segment Revenue and Ranking  
Reveals how total revenue is distributed across segments and ranks them from highest to lowest.  
This helps identify which groups generate the most income and where to focus retention efforts.  

---

### Query 3 — Top 5 Revenue Customers  
Displays the top five customers by total revenue.  
Shows that not all high-revenue customers have perfect **555 scores** — some buy less often but spend heavily when they do.  
This highlights the importance of personalized engagement for irregular yet valuable buyers.  

---

### Query 4 — Nations with Most Champions  
Ranks nations by how many **Champion customers (R = 5, F = 5, M = 5)** they have,  
and shows each nation’s total and average Champion revenue.  
This reveals which markets demonstrate the strongest loyalty and spending patterns.  

---

💡 **Tip:**  
You can keep all of Part 2 commented (`/* ... */`) in your SQL file.  
When you’re ready to analyze specific insights, **uncomment one query at a time** and run it after executing **Part 1 – RFM Segmentation**.  

---


