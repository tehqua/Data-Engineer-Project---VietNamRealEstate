# Data-Engineer-Project---VietNamRealEstate
Projcet about Crawling Data, ETL and visualization 

## Introduction 
In the context of rising real estate prices in Hanoi, gaining an overview of property prices in Hanoi in particular, and Vietnam in general, as well as detailed information, is crucial. To meet this demand, this project will build an automated system to aggregate and analyze job information in the data sector from the website batdongsan. By leveraging big data and modern analytical techniques, the system will provide an overall view of property prices, the number of properties currently for sale, and detailed analysis by region.
## Outline and Workflow 
**1.Data Collection**  
Lambda: Use Selenium and undetected-chromedriver to crawl data from real estate pages on batdongsan.com.vn.  
Storage: Raw data is stored in S3 in the Bronze zone as CSV files.  
**2.ETL Phase 1**  
Glue: The raw data is loaded into Glue for cleaning, removing unnecessary information, adding the price per m² column, and normalizing the format.  
Storage: The cleaned data is transferred to the Bronze zone in S3.  
**3.ETL Phase 2**  
SCD Type 1: Apply SCD Type 1 to update changes in the data.  
Storage: The data is stored in the Silver zone on S3.  
**4.ETL Phase 3**  
Glue: Clean and format the data in preparation for visualization.  
Storage: The data is saved in the Gold zone on S3.  
**5.Querying and Visualization**  
BI Tools - Tableau: Connect to the dataset and perform queries to create charts, statistics, and dashboards using Tableau.
![image](https://github.com/user-attachments/assets/24f87c08-2256-48ec-aef3-0d4a12c4716e)
## Report 
### 1. Overview of the Vietnam Real Estate Market:
Link: https://public.tableau.com/app/profile/eden.ichnobates/viz/ReportBDSVN/TngQuanTnhHnhBtngSn 
![image](https://github.com/user-attachments/assets/0e7da57d-a2ac-4056-85da-e24b251af1ec)
### 2. Overview of the Hanoi Real Estate Market:
Link: https://public.tableau.com/app/profile/eden.ichnobates/viz/ReportBDSHN/HNi
![image](https://github.com/user-attachments/assets/e3cee06a-0a03-478f-9f02-4756bfc76f7b)
