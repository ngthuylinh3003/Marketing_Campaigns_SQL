-- TABLE1: Clean and Enrich the Data


WITH change_name_1 AS (
  SELECT * EXCEPT (Campaign_Type,Channel_Used)
    , Campaign_Type AS Campaigns
    , Channel_Used AS Channels
    --, SPLIT(Target_Audience," ")[OFFSET(0)] AS Gender
    --, SPLIT(Target_Audience," ")[OFFSET(1)] AS Age
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
    , CAST(EXTRACT (MONTH FROM Date) AS numeric) AS Month
    , FORMAT_DATE('%B',Date) AS Month_fullname
    , FORMAT_DATE('%b',Date) AS Mon_name
    , FORMAT_DATE("%m-%d-%Y",Date) AS Started_day
    , FORMAT_DATE("%m-%d-%Y",DATE_ADD(Date, INTERVAL Duration_days DAY)) AS End_day
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
    ,CAST(Clicks*Conversion_Rate AS int64) AS Conversions -- Tạo thêm cột Conversion
    --,CAST(ROUND(Clicks * Conversion_Rate) AS INT64) AS Conversions
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






-- TABLE 2: Company vs Campaigns
-- Phân tích theo cả Company & Campaigns / Tương tự với Company & Channels
-- Vì có 5 công ty và 5 chiến dịch nên tổng cộng có 25 hàng
WITH Campaign_summary AS (
  SELECT
    Company
    ,Campaigns
    ,COUNT(DISTINCT(Campaign_ID)) AS Campaign_Frequency
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
  GROUP BY 1,2)
-- Xét về avg_score: min là 5.42 và max là 5.6 => không chênh lệch quá nhiều
  SELECT Company
    , Campaigns
    , Campaign_Frequency
    , Avg_ROI
    , RANK() OVER(PARTITION BY Company ORDER BY Avg_ROI DESC) AS rank_ROI
    , Sum_of_Cost
    , Sum_of_Clicks
    , Sum_of_Impressions
    , Sum_of_Conversions
    , Avg_score
    , RANK() OVER(PARTITION BY Company ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate
    , RANK() OVER(PARTITION BY Company ORDER BY Avg_Conversion_Rate DESC) AS rank_Conversion_Rate
  FROM Campaign_summary
  ORDER BY Company,Avg_ROI DESC --1 là ROI cao nhẩt
-- Nhận xét: Công ty TechCorp dùng chiến lược Influencer trong năm 2021 là 8148 lần (xếp thứ 11 trong tổng 25) và có ROI lớn nhất cụ thể là 40928.92
-- So với Avg_score nằm trong khoảng (5.42 và 5.6) thì nó là 5.51 (khá phù hợp)
-- TABLE 5: Company (Overview)
WITH Campaign_summary AS (
  SELECT
    Company
    ,COUNT(DISTINCT(Campaign_ID)) AS Campaign_Frequency
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
  GROUP BY 1)


  SELECT Company
    , Campaign_Frequency
    , Avg_ROI
    , RANK() OVER( ORDER BY Avg_ROI DESC) AS rank_ROI
    , Sum_of_Cost
    , Sum_of_Clicks
    , Sum_of_Impressions
    , Sum_of_Conversions
    , Avg_score
    , RANK() OVER(ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate
    , RANK() OVER(ORDER BY Avg_Conversion_Rate DESC) AS rank_Conversion_Rate
  FROM Campaign_summary
  ORDER BY Avg_ROI DESC --1 là ROI cao nhẩt
-- Nhận xét: Công ty TechCorp dùng chiến lược Influencer trong năm 2021 là 8148 lần (xếp thứ 11 trong tổng 25) và có ROI lớn nhất cụ thể là 40928.92
-- So với Avg_score nằm trong khoảng (5.42 và 5.6) thì nó là 5.51 (khá phù hợp)
--TABLE 6: Campaigns (Only)


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
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
  GROUP BY 1)


  SELECT Campaigns
    , Campaign_Frequency
    , Avg_ROI
    , RANK() OVER(ORDER BY Avg_ROI DESC) AS rank_ROI
    , Sum_of_Cost
    , Sum_of_Clicks
    , RANK() OVER(ORDER BY Sum_of_Clicks DESC) AS rank_Clicks
    , Sum_of_Impressions
    , RANK() OVER(ORDER BY Sum_of_Impressions DESC) AS rank_Impressions
    , Sum_of_Conversions
    , RANK() OVER(ORDER BY Sum_of_Conversions DESC) AS rank_Conversions
    , Avg_score
    , RANK() OVER(ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate -- Vì dataset sẵn phân bổ chỉ số conversion rate đồng đều nên không có sự khác biệt ở chỉ số này
  FROM Campaign_summary
  ORDER BY Avg_ROI DESC --1 là ROI cao nhẩt


-- TABLE 7: Campaing with Duration


WITH campaign_duration_1 AS (
  SELECT Campaigns
    ,Duration_days
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
   -- ,ROUND((Acquisition_Cost/Conversions),3) AS Cost_per_Conversion
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
  GROUP BY 1,2),


  campaign_duration_2 AS (
  SELECT *
    , RANK() OVER(PARTITION BY Campaigns ORDER BY Avg_ROI DESC) AS rank_ROI
    , ROUND((Sum_of_Cost/Sum_of_Conversions),3) AS Cost_per_Conversion
    , ROUND(Sum_of_Conversions/SUM(Sum_of_Conversions) OVER (PARTITION BY Campaigns),4) AS Conversion_Share --Phân tích tỷ lệ đóng góp của từng dòng dữ liệu so với tổng nhóm.
  FROM campaign_duration_1
  ORDER BY 1,2)
-- Thêm cột rank của Cost_Per_Conversion
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


-- TABLE 8: Campaign with Channel


WITH campaign_channel_1 AS (
  SELECT Campaigns
    ,Channels
    ,ROUND(SUM(Acquisition_Cost),2) AS Sum_of_Cost
   -- ,ROUND((Acquisition_Cost/Conversions),3) AS Cost_per_Conversion
    ,SUM(Clicks) AS Sum_of_Clicks
    ,SUM(Impressions) AS Sum_of_Impressions
    ,SUM(Conversions) AS Sum_of_Conversions
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
  GROUP BY 1,2),


  campaign_channel_2 AS (
  SELECT *
    , RANK() OVER(PARTITION BY Campaigns ORDER BY Avg_ROI DESC) AS rank_ROI
    , ROUND((Sum_of_Cost/Sum_of_Conversions),3) AS Cost_per_Conversion
    , SUM(Sum_of_Conversions) OVER (PARTITION BY Campaigns) AS sum_conversion_campaigns
    , ROUND(Sum_of_Conversions/SUM(Sum_of_Conversions) OVER (PARTITION BY Campaigns),4) AS Conversion_Share
  FROM campaign_channel_1
  ORDER BY 1,2)
-- Thêm cột rank của Cost_Per_Conversion
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


--TABLE 9: Channels (Only)


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
  FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
  GROUP BY 1)


  SELECT Channels
    ,Avg_ROI
    , RANK() OVER(ORDER BY Avg_ROI DESC) AS rank_ROI
    , Sum_of_Cost
    , Sum_of_Clicks
    , RANK() OVER(ORDER BY Sum_of_Clicks DESC) AS rank_Clicks
    , Sum_of_Impressions
    , RANK() OVER(ORDER BY Sum_of_Impressions DESC) AS rank_Impressions
    , Sum_of_Conversions
    , RANK() OVER(ORDER BY Sum_of_Conversions DESC) AS rank_Conversions
    , Avg_score
    , RANK() OVER(ORDER BY Avg_score DESC) AS rank_avg_score
    , Avg_Conversion_Rate -- Vì dataset sẵn phân bổ chỉ số conversion rate đồng đều nên không có sự khác biệt ở chỉ số này
  FROM Channel_summary
  ORDER BY Avg_ROI DESC --1 là ROI cao nhẩt


-- Table 10: Customer
-- CÓ 5 nhóm Customer_segment
WITH customer_1 AS (
SELECT Customer_Segment
    , Campaigns
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
GROUP BY 1,2)


SELECT Customer_Segment
    ,Campaigns
    ,Avg_score
    ,RANK() OVER (PARTITION BY Customer_Segment ORDER BY Avg_score DESC) AS rank_score
    ,Avg_Conversion_Rate
    ,RANK() OVER (PARTITION BY Customer_Segment ORDER BY Avg_Conversion_Rate DESC) AS  Avg_Conversion_Rate -- Phần này không quan tâm
    ,Avg_ROI
    ,RANK() OVER (PARTITION BY Customer_Segment ORDER BY Avg_ROI DESC) AS rank_ROI
FROM customer_1
ORDER BY Customer_Segment, rank_score
-- Table 11: Location
-- CÓ 5 nhóm Customer_segment
WITH location_1 AS (
SELECT Location
    , Campaigns
    ,ROUND(AVG(Engagement_Score),3) AS Avg_score
    ,ROUND(AVG(Conversion_Rate),3) AS Avg_Conversion_Rate
    ,ROUND(AVG(ROI),3) AS Avg_ROI
FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
GROUP BY 1,2)


SELECT Location
    ,Campaigns
    ,Avg_score
    ,RANK() OVER (PARTITION BY Location ORDER BY Avg_score DESC) AS rank_score
    ,Avg_Conversion_Rate
    ,RANK() OVER (PARTITION BY Location ORDER BY Avg_Conversion_Rate DESC) AS  Avg_Conversion_Rate -- Phần này không quan tâm
    ,Avg_ROI
    ,RANK() OVER (PARTITION BY Location ORDER BY Avg_ROI DESC) AS rank_ROI
FROM location_1
ORDER BY Location , rank_score
-- TABLE 12: Tim Series
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
FROM vigilant-walker-371416.kaggle_cleansed.pj1_table25_1
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


DONE
Table 1 (Fact Table):

