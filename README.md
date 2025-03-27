# 📢 Marketing Campaigns

**View the Dashboard directly:** [Power BI](https://app.powerbi.com/view?r=eyJrIjoiMDIxYjFjOWYtN2ZhYS00OTMzLTk4YjYtZTNhNjRiZWMyN2UxIiwidCI6ImNiNDg0NDZlLTkwZTYtNGJmMS04MjViLTQwZTQ4ZmNjOWZmNiJ9)

## 📌 Objective
This project aims to evaluate the performance of marketing campaigns in 2021 through a **Marketing Campaign Performance Dashboard**. The dashboard helps businesses identify the **most effective campaigns** based on key performance metrics to optimize cost and maximum revenue

---

## 📂 Data Source  
The dataset was sourced from Kaggle and consists of a **table with 16 columns** containing marketing campaign performance indicators. The data includes information from **5 companies** running **5 campaign types**: *Display, Email, Influencer, Search, Social Media*  
#### **View the original dataset on Kaggle:** [Marketing Campaign Performance Dataset](https://www.kaggle.com/datasets/manishabhatt22/marketing-campaign-performance-dataset/data)

---

## ⚡ Approach
The analysis combines **SQL (Google BigQuery) and Power BI** for data processing and visualization:  

### **1. Building a [Logical Tree](https://github.com/ngthuylinh3003/Marketing_Campaigns_SQL/blob/b0a3d7ebc60aa432ae265c3c99e3438361c1f555/Logical_tree.png) :** 
- To define key factors for analysis & identify **necessary metrics** to calculate from the dataset

### **2. Data Cleaning using [SQL](https://github.com/ngthuylinh3003/Marketing_Campaigns_SQL/blob/b0a3d7ebc60aa432ae265c3c99e3438361c1f555/pj1_data_cleaning.sql) (Google BigQuery)**  
- **Renaming columns**, handling **NULL values**, and adjusting **data types**  
- **Enriching data** by creating new columns  before importing into Power BI

### **3. Building the Dashboard in Power BI**  
- **Upload cleaned data** and create a **Date Dimension table** for time-based analysis 
- **Add new columns** using **Conditional Columns** (ex: add new col of group age ranges for better segmentation)  
- **Create measures** for key performance indicators such as:  
  - **SUM** (e.g., total impressions, clicks, conversions)  
  - **AVG** (e.g., average ROI, CPC)  
  - **Growth Rate Calculation**  
- **Build visualizations**, including:  
  - **Cards**: Display **Engagement and Financial Metrics with the growth**  
  - **Line Charts**: Track key metrics over time 
  - **Scatter Plots**: Identify correlations between factors.  
  - **Funnel Charts**: Analyze contribution percentages at different stages.  
  - Others are **Heat Maps**, **Matrix Tables**,**Bar + Line Combo Charts**,**Pie Charts**

Additionally, **SQL queries** were created to handle **ad hoc requests** [Code](https://github.com/ngthuylinh3003/Marketing_Campaigns_SQL/blob/0250a41ce5a9d5b096ee2e5c1cfeb4693ba43c65/pj1_data_analysis.sql) [Output](https://docs.google.com/spreadsheets/d/1u8L-Up-JknGl1IGwAD_5Ja4pRiXFMx6UPjwpROBa6bY/edit?gid=2024676831#gid=2024676831), such as:  
- Identifying the **campaign with the highest revenue** 
- Analyzing the **performance of all five campaigns for a specific company**  
- The SQL code and output tables are available at this link

---

## 📊 Results  

### *Engagement Metrics*
- **Conversions and Click-Through Rate (CTR) are significantly low compared to impressions** (accounting for only **10% of total impressions**) 

### *Campaign Performance*  
- Among the **5 campaigns**, **Display offers the best balance between ROI and Cost**, with the **highest average score**
- **ROI and Average Score** of all campaigns remain relatively **consistent over time**  

### *Duration Effectiveness* 
- **CTR and Conversion rates are stable**, so the focus should be on **ROI and CPC**
- The **optimal duration for each campaign type**:  
  - **45 days**: Display, Email  
  - **30 days**: Influencer, Search  
  - **15 days**: Social Media  

### *Channel Performance*  
- **Facebook** is the most **cost-effective** channel, offering the **highest ROI**, especially in **Search Campaigns**  
- **Websites rank second** in terms of performance

### *Customer Segmentation*
- **ROI is similar across all customer segments**, but **Foodies have the highest Average Engagement Score**
- **Conversion rates are evenly distributed among genders across all five segments**
- Notes: The dataset only includes gender data for the **25-34 age group**

### *Location*  
- **Miami and Houston have the highest ROI**, but **Miami has a higher cost**  

### *Company Insights*  
- **No significant differences** in key performance indicators across the five companies
- Within each company, **campaign performance remains relatively stable**
- Some recommendations based on ROI:  
  - **Alpha Innovations & TechCorp** perform best with **Influencer campaigns**
  - **Data Solutions & Innovate Industries** see the highest ROI from **Display campaigns**
  - **NextGen Systems** benefits the most from **Search campaigns**

## *⚠️ Limitations* 
- The dataset that I have analyzed from Kaggle appears to have been **overly cleaned**, leading to **highly uniform metrics** such as CTR, Conversion Rate, and Average Score, so this lack of variation makes it **difficult to derive precise insights** and identify **true performance differences** across campaigns

---

📌 This analysis reflects my personal perspective and approach. **I’d love to hear your feedback and suggestions!**  
