-- Data Cleaning & Enrichment
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