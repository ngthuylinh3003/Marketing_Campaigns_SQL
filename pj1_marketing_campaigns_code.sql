-- TABLE 1: Clean and Enrich the Data


WITH change_name_1 AS (
  SELECT * EXCEPT (Campaign_Type,Channel_Used,Date)
    , Campaign_Type AS Campaigns
    , Channel_Used AS Channels
    , Date AS Started_day
  FROM vigilant-walker-371416.kaggle_cleansed.project1_marketing_campaign_2025
WHERE Campaign_ID IS NOT NULL ),
change_datatype_2 AS (
  SELECT * EXCEPT (Duration, Acquisition_Cost)
    , CAST(REPLACE(Duration," days","") AS int64) AS Duration_days -- Duration (days)
    , CAST (Acquisition_Cost AS numeric) AS Acquisition_Cost -- Unit: $
  FROM change_name_1),
--The remaining variables have reasonable data types.
--The data has no NULL values.


enrich_Target_Audience_3_1 AS (
-- Split the "Target Audience" column into two columns: "Gender" and "Age"
  SELECT *  
    , SPLIT(Target_Audience," ")[OFFSET(0)] AS Gender
    , SPLIT(Target_Audience," ")[OFFSET(1)] AS Age
  FROM change_datatype_2),


enrich_Target_Audience_3_2 AS (
  SELECT * EXCEPT(Gender,Age)
    , CASE WHEN (SPLIT(Target_Audience," ")[OFFSET(0)]) = "All" THEN "Both" ELSE (SPLIT(Target_Audience," ")[OFFSET(0)]) END AS Gender
    , CASE WHEN (SPLIT(Target_Audience," ")[OFFSET(1)]) = "Ages" THEN "All ages" ELSE (SPLIT(Target_Audience," ")[OFFSET(1)]) END AS Age
  FROM enrich_Target_Audience_3_1),
-- Now we have 2 columns Age and Gender


-- Add some collumns from the Date Column
enrich_Date_4 AS (
  SELECT *
    , CAST(EXTRACT (MONTH FROM Started_day) AS numeric) AS Month
    , FORMAT_DATE('%B',Started_day) AS Month_fullname
    , FORMAT_DATE('%b',Started_day) AS Mon_name
    , DATE_ADD(Started_day, INTERVAL Duration_days DAY) AS End_day
  FROM enrich_Target_Audience_3_2),


-- Segment the ROI into 3 groups (Poor, Good, Excellent)
enrich_ROI_5 AS (
  SELECT *,CASE
    WHEN ROI >= 2 AND ROI <= 4 THEN "Poor"
    WHEN ROI >4 AND ROI <=7 THEN "Good"
    WHEN ROI >7 AND ROI <=8 THEN "Excellent"
  END AS Performance_level
  FROM enrich_Date_4)


--Rearrange the collumns to have a logical view
SELECT
    Campaign_ID
    ,Company
    ,Campaigns
    ,Channels
    ,Acquisition_Cost
    ,ROI
    ,Performance_level
    ,Clicks
    ,Impressions
    ,CAST(Clicks*Conversion_Rate AS int64) AS Conversions
    ,Conversion_Rate
    ,Engagement_Score
    ,Customer_Segment
    ,Gender
    ,Age
    ,Location
    ,Started_day
    ,Duration_days
    ,End_day
    ,Month
    ,Month_fullname
    ,Mon_name
FROM enrich_ROI_5
ORDER BY Campaign_ID


-- The dataset has 200000 rows and 22 columns with no NULL values
-- The data was recorded in 2021


-- TABLE 2: Company vs Campaigns
-- There are five companies in the dataset, and each company has used all five campaigns.


WITH Campaign_company_1 AS (
  SELECT
    Company
    ,Campaigns
    ,COUNT(DISTINCT(Campaign_ID)) AS Campaign_Frequency -- The number of times each company has used each campaign.
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
  GROUP BY 1,2)


  SELECT Company
    , Campaigns
    , Campaign_Frequency
    , Avg_ROI
    , RANK() OVER(PARTITION BY Company ORDER BY Avg_ROI DESC) AS rank_company_ROI
    , RANK() OVER(ORDER BY Avg_ROI DESC) AS rank_ROI -- Engagement scores are ranked for all companies
    , Sum_of_Cost
    , RANK() OVER(ORDER BY Sum_of_Cost DESC) AS rank_Cost -- Acquisition Cost is ranked for all companies
    , Sum_of_Clicks
    , Sum_of_Impressions
    , Sum_of_Conversions
    , Avg_score
    , RANK() OVER(PARTITION BY Company ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate
  FROM Campaign_company_1
  ORDER BY Company,Avg_ROI DESC --Rank 1 represents the highest ROI, while Rank 5 represents the lowest cost


-- Notes:
-- For example, Alpha Innovations Company, when using the Influencer Campaign, has the highest ROI compared to other campaigns.
-- DataTech Solutions Company, using the Display Campaign, has the highest Avg_ROI (5.049), and its cost is also significantly low (18/25)
-- However, The conversion rate remains consistent across all records, regardless of company or campaign.
-- (?) This suggests that the dataset might have undergone preprocessing, potentially affecting its variability.
-- TABLE 3: Company (Overview)
WITH company_1 AS (
  SELECT
    Company
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
  GROUP BY 1)


  SELECT Company
    , Avg_ROI
    , RANK() OVER( ORDER BY Avg_ROI DESC) AS rank_ROI
    , Sum_of_Cost
    , RANK() OVER( ORDER BY Sum_of_Cost DESC) AS rank_Cost
    , Sum_of_Clicks
    , Sum_of_Impressions
    , Sum_of_Conversions
    , Avg_score
    , RANK() OVER(ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate
  FROM company_1
  ORDER BY Avg_ROI DESC --Rank 1 represents the highest ROI, while Rank 5 represents the lowest cost


--  Notes:
--  Although TechCorp Company has the highest ROI, the difference is marginal. It also has the largest cost and the lowest average score.
--  DataTech Solutions has the second-highest ROI, but the gap is minimal. Meanwhile, its cost remains relatively low compared to the other five companies (ranked 3rd out of 5).
--TABLE 4: Campaigns (Overview)


WITH Campaign_summary AS (
  SELECT
    Campaigns
    ,COUNT(DISTINCT(Campaign_ID)) AS Campaign_Frequency
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
  GROUP BY 1)


  SELECT Campaigns
    , Campaign_Frequency
    , Avg_ROI
    , RANK() OVER(ORDER BY Avg_ROI DESC) AS rank_ROI
    , Sum_of_Cost
    , RANK() OVER(ORDER BY Sum_of_Cost DESC) AS rank_Cost
    , Sum_of_Clicks
    , RANK() OVER(ORDER BY Sum_of_Clicks DESC) AS rank_Clicks
    , Sum_of_Impressions
    , RANK() OVER(ORDER BY Sum_of_Impressions DESC) AS rank_Impressions
    , Sum_of_Conversions
    , RANK() OVER(ORDER BY Sum_of_Conversions DESC) AS rank_Conversions
    , Avg_score
    , RANK() OVER(ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate
  FROM Campaign_summary
  ORDER BY Avg_ROI DESC


-- Notes:
-- The Influencer campaign is the most frequently used.
-- Although Influencer has the highest ROI, the difference is marginal. It also has the largest cost and the lowest average score.
-- There is no significant difference in conversion rate across campaigns in this analysis.




-- TABLE 5: Campaing Duration
-- The campaigns are divided into deployment groups of 15 days, 30 days, 45 days, and 60 days.
-- Determine the optimal campaign duration to achieve high ROI and low cost.


WITH campaign_duration_1 AS (
  SELECT Campaigns
    ,Duration_days
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
  GROUP BY 1,2),


  campaign_duration_2 AS (
  SELECT *
    , RANK() OVER(PARTITION BY Campaigns ORDER BY Avg_ROI DESC) AS rank_ROI
    , ROUND((Sum_of_Cost/Sum_of_Conversions),3) AS Cost_per_Conversion
    , ROUND(Sum_of_Conversions/SUM(Sum_of_Conversions) OVER (PARTITION BY Campaigns),4) AS Conversion_Share --Analyze the contribution ratio of each data entry to the total group.
  FROM campaign_duration_1
  ORDER BY 1,2)


-- Add column Rank Cost_Per_Conversion
  SELECT
     Campaigns
    , Duration_days
    , Avg_ROI
    , rank_ROI
    , Sum_of_Cost
    , RANK() OVER(PARTITION BY Campaigns ORDER BY Cost_per_Conversion ASC) AS rank_Cost
    , Sum_of_Clicks
    , Sum_of_Impressions
    , Sum_of_Conversions
    , Conversion_Share
    , Avg_score
    , Avg_Conversion_Rate


  FROM campaign_duration_2
  ORDER BY Campaigns, Duration_days


-- Notes:
-- For example, despite the minimal difference, the Display Campaign running for 45 days has the highest ROI but also the highest cost. We can consider the 60-day campaign instead, as it has the second-highest ROI with the lowest cost, making it a better option for maximizing profit while minimizing expenses.


-- TABLE 6: Campaign with Channel


WITH campaign_channel_1 AS (
  SELECT Campaigns
    ,Channels
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
  GROUP BY 1,2),




  campaign_channel_2 AS (
  SELECT *
    , RANK() OVER(PARTITION BY Campaigns ORDER BY Avg_ROI DESC) AS rank_ROI
    , ROUND((Sum_of_Cost/Sum_of_Conversions),3) AS Cost_per_Conversion
    , SUM(Sum_of_Conversions) OVER (PARTITION BY Campaigns) AS sum_conversion_campaigns
    , ROUND(Sum_of_Conversions/SUM(Sum_of_Conversions) OVER (PARTITION BY Campaigns),4) AS Conversion_Share
  FROM campaign_channel_1
  ORDER BY 1,2)


  SELECT
     Campaigns
    , Channels
    , Avg_ROI
    , rank_ROI
    , Sum_of_Cost
    , Cost_per_Conversion
    , RANK() OVER(PARTITION BY Campaigns ORDER BY Cost_per_Conversion ASC) AS rank_Cost
    , Sum_of_Clicks
    , Sum_of_Impressions
    , Sum_of_Conversions
    , sum_conversion_campaigns
    , Conversion_Share
    , Avg_score
    , Avg_Conversion_Rate




  FROM campaign_channel_2
  ORDER BY Campaigns, rank_ROI


-- Notes
-- For example, across six different channels, Display Campaigns run on Websites and Email have the highest ROI.
-- However, to minimize costs, running the campaign on Email could be a better option since it has a lower cost.
-- Due to the minimal difference, the campaign on Google Ads can also be considered, as it has the second-highest ROI but the lowest cost.


--TABLE 7: Channels (Overview)


WITH Channel_summary AS (
  SELECT
    Channels
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
  GROUP BY 1)


  SELECT Channels
    ,Avg_ROI
    , RANK() OVER(ORDER BY Avg_ROI DESC) AS rank_ROI
    , Sum_of_Cost
    , RANK() OVER(ORDER BY Sum_of_Cost DESC) AS rank_Cost
    , Sum_of_Clicks
    , RANK() OVER(ORDER BY Sum_of_Clicks DESC) AS rank_Clicks
    , Sum_of_Impressions
    , RANK() OVER(ORDER BY Sum_of_Impressions DESC) AS rank_Impressions
    , Sum_of_Conversions
    , RANK() OVER(ORDER BY Sum_of_Conversions DESC) AS rank_Conversions
    , Avg_score
    , RANK() OVER(ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate
  FROM Channel_summary
  ORDER BY Avg_ROI DESC


-- Notes:
-- Facebook is the most efficient channel, achieving the highest ROI with the lowest cost.
-- Websites have the second-highest ROI, with the second-lowest cost.
-- Email is the most expensive channel for running campaigns.


-- Table 8: Customer
-- There 5 five groups of Customers (Fashionistas,Foodies, Health & Wellness, Outdoor Adventurers, Tech Enthusiasts)


WITH customer_1 AS (
SELECT Customer_Segment
    , Campaigns
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
GROUP BY 1,2)


SELECT Customer_Segment
    ,Campaigns
    ,Avg_ROI
    ,RANK() OVER (PARTITION BY Customer_Segment ORDER BY Avg_ROI DESC) AS rank_customer_segment_ROI
    ,RANK() OVER (ORDER BY Avg_ROI DESC) AS rank_ROI
    ,Avg_score
    ,RANK() OVER (PARTITION BY Customer_Segment ORDER BY Avg_score DESC) AS rank_customer_segment_score
    ,RANK() OVER (ORDER BY Avg_score DESC) AS rank_score
    ,Avg_Conversion_Rate
    ,RANK() OVER (PARTITION BY Customer_Segment ORDER BY Avg_Conversion_Rate DESC) AS  Avg_Conversion_Rate
FROM customer_1
ORDER BY Customer_Segment, rank_score


-- Notes:
-- The differences in metrics across customer segments are minimal.
-- Tech Enthusiasts have the highest ROI (5.03) but the score just 13/25 (5.5).
-- For example, the Fashionistas segment achieves the highest ROI and avg_score when targeted through Email Campaigns compared to the other four campaigns.
-- Table 9: Location
-- There are 5 Location in this dataset: Chicago, House, Los Angeles, Miami, New York


WITH location_1 AS (
SELECT Location
    ,Campaigns
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
GROUP BY 1,2)


SELECT Location
    ,Campaigns
    ,Avg_ROI
    ,RANK() OVER (PARTITION BY Location ORDER BY Avg_ROI DESC) AS rank_company_ROI
    ,RANK() OVER (ORDER BY Avg_ROI DESC) AS rank_ROI
    ,Avg_score
    ,RANK() OVER (PARTITION BY Location ORDER BY Avg_score DESC) AS rank_company_score
    ,RANK() OVER (ORDER BY Avg_score DESC) AS rank_score
    ,Avg_Conversion_Rate
FROM location_1
ORDER BY Location , rank_score


-- Notes:  
-- Miami achieves the highest ROI and avg_score with Display Campaigns.  
-- Chicago has the highest ROI with Influencer Campaigns but the lowest Avg_ROI.
-- TABLE 10: Month


WITH time_trend_1 AS
(SELECT Month
, Company
, Campaigns
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
FROM vigilant-walker-371416.kaggle_cleansed.pj1_final
GROUP BY 1,2,3),


time_trend_2 AS --enrich
(SELECT *
,ROUND(Sum_of_Clicks/NULLIF(Sum_of_Impressions,0),3) AS click_through_rate
,ROUND(Sum_of_Cost/NULLIF(Sum_of_Conversions,0),3) AS cost_per_conversion
,RANK() OVER (PARTITION BY Month ORDER BY Avg_ROI DESC) AS rank_ROI
FROM time_trend_1
)
SELECT
Month
,Company
,Campaigns
,Sum_of_Cost
,Sum_of_Clicks
,Sum_of_Impressions
,click_through_rate
,Sum_of_Conversions
,cost_per_conversion
,Avg_score
,Avg_Conversion_Rate
,Avg_ROI
,rank_ROI
FROM time_trend_2
ORDER BY 1,2,3

