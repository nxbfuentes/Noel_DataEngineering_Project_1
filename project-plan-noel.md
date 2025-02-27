# Project plan

## Objective

A short statement about the objective of your project.
Example:

> The objective of our project is to build analytical datasets from the OpenSky Network API, capturing and processing aircraft movement data over the Greater Perth area to generate a weekly flight schedule."

[OpenSky Network API Documentation](https://openskynetwork.github.io/opensky-api/rest.html)

## Consumers


The users of our datasets are aviation enthusiasts, airport operations teams, and local government authorities. Aviation enthusiasts can use the data to track and identify different types of aircraft in the Greater Perth area. Airport operations teams can utilize the data to optimize scheduling and improve operational efficiency. Local government authorities can leverage the data for urban planning and noise management.

They want to access the data through interactive dashboards, downloadable reports, and real-time API endpoints to integrate with their existing systems.


## Questions

What questions are you trying to answer with your data? How will your data support your users?

1. Flight Tracking and Analysis:
Track Flights Over Time: Monitor how aircraft move between departure and arrival airports. You can analyze flight patterns and timings.

Flight Frequency: Determine the frequency of flights between specific airports within certain time intervals.

2. Airport Activity Analysis:
Busy Airports: Identify the busiest departure and arrival airports based on the number of flights.

Airport Performance: Compare the performance and usage of different airports.

3. Distance and Duration Analysis:
Distance Calculation: Calculate the horizontal and vertical distances covered by flights between departure and arrival points.

Flight Duration: Analyze the time taken for flights between their first seen and last seen timestamps.

4. Aircraft Activity:
Aircraft Utilization: Track the activity of specific aircraft identified by their icao24 addresses.

Flight Paths: Map the flight paths of different aircraft using their positions over time.

5. Callsign Analysis:
Flight Identification: Identify and track flights based on their callsigns.

Callsign Patterns: Analyze the patterns and usage of different callsigns.

Example:

> - What types of aircraft are most common in the Greater Perth area?
> - What times of day have the highest aircraft activity?
> - How does aircraft activity vary throughout the week?
> - Which airlines operate the most flights in the area?
> - How can airport operations be optimized based on aircraft activity data?

## Source datasets

Here’s how you can structure your answer based on the OpenSky Network API:  

| Source name | Source type | Source documentation |  
|-------------|-------------|----------------------|  
| **OpenSky Network API** | REST API | [OpenSky Network API Documentation](https://openskynetwork.github.io/opensky-api/rest.html) |  
| **Aircraft Database (for aircraft type lookup, e.g., ADS-B Exchange or FAA registry)** | External database (CSV/API) | Depends on chosen database (e.g., [ADS-B Exchange](https://www.adsbexchange.com/)) |  
| **Flight Schedule Data (optional, for airline/operator info)** | External dataset (CSV/API) | Varies by source (e.g., [OpenFlights](https://openflights.org/data.html)) |  

### **Update Frequency:**  
- **OpenSky Network API**: Updates every **5 seconds** for live tracking (historical data access is limited).  
- **Aircraft Database**: Update frequency varies depending on the provider (may require periodic syncs).  
- **Flight Schedule Data**: Typically updated **daily or weekly**, depending on the source.  

This setup ensures that you can track live aircraft movement and enrich your data with aircraft type and airline details from external sources. Would you like suggestions on automating data ingestion? 🚀

## Solution architecture

How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?

- What data extraction patterns are you going to be using?
- What data loading patterns are you going to be using?
- What data transformation patterns are you going to be performing?


This architecture describes how we will **extract, transform, and load (ETL)** aircraft tracking data from the **OpenSky Network API** into a **PostgreSQL database**, applying **data engineering best practices** learned in the bootcamp.  


## **1️⃣ Data Flow Overview**  
### **Step 1: Extraction (Source Data)**
- **Source:** OpenSky REST API  
- **Pattern:** **Incremental extraction** (hourly API calls)  
- **Method:** Python script with `requests`  
- **Storage:** Raw JSON stored in a PostgreSQL table (`raw_aircraft_data`)  

### **Step 2: Transformation (Processing & Cleaning)**
- **Pattern:** ELT (Extract-Load-Transform)  
- **Processing:**  
  - Convert timestamps to human-readable format  
  - Remove duplicate records (upsert mechanism)  
  - Normalize missing values (e.g., empty `callsign` fields)  
  - Add calculated fields (e.g., speed in km/h)  

### **Step 3: Loading (Serving & Analysis)**
- **Pattern:** **Upsert loading** (overwrite existing records)  
- **Destination:** PostgreSQL `aircraft_flight_data` table  
- **Optimization:** Partitioning by `date` and indexing for fast queries  


## **2️⃣ Solution Components & Services**  
| **Component**            | **Technology / Tool**            | **Purpose** |
|------------------------|---------------------------|---------------------------|
| **Data Extraction**     | Python (`requests`)        | Fetch aircraft data from OpenSky API |
| **Storage (Raw Data)**  | PostgreSQL (`raw_aircraft_data` table) | Store raw API response |
| **Data Transformation** | Pandas, SQLAlchemy        | Clean, enrich, and prepare data |
| **ETL Orchestration**   | Prefect / Airflow (Optional) | Automate scheduling & logging |
| **Data Loading**        | PostgreSQL (`aircraft_flight_data` table) | Store cleaned data for analysis |
| **Scheduling**         | `schedule` (Python) / Windows Task Scheduler | Run ETL pipeline every hour |
| **Logging**             | Python Logging + PostgreSQL | Track pipeline execution |
| **Version Control**     | Git / GitHub               | Code management & collaboration |
| **Deployment (Optional)** | Docker + AWS (ECS, RDS, S3) | Host the ETL pipeline in the cloud |

---

## **3️⃣ ETL Pipeline Automation**  
We need to ensure the pipeline **runs automatically every hour** without manual intervention.  

### **Local (Windows) Automation**
- Use `schedule` in Python + Windows Task Scheduler  
- Script runs hourly, fetching new data and storing it in PostgreSQL  

### **Cloud Deployment (Optional)**
- **Docker**: Package the ETL pipeline in a container  
- **AWS ECS + RDS**: Deploy ETL job as a scheduled task  
- **Prefect / Airflow**: Monitor & manage workflow  

---

## **4️⃣ Data Engineering Patterns Used**  
### **🔹 Extraction Patterns**
✅ **Incremental Extraction** → API only keeps 1 hour of history, so we extract data every hour  
✅ **API Rate Limiting** → We optimize requests to stay within the 4000 credit limit  

### **🔹 Loading Patterns**
✅ **Upsert Loading** → Avoids inserting duplicate aircraft records  
✅ **Incremental Load** → Only adds new or updated aircraft data  

### **🔹 Transformation Patterns**
✅ **Metadata-driven ETL** → Configuration via YAML for flexibility  
✅ **Jinja2 SQL Templates** → Dynamic queries using macros & variables  
✅ **Logging to Database** → Track ETL execution history  

---

## **5️⃣ Testing & Logging**
- **Unit Tests:** Validate data extraction & transformation steps  
- **Data Quality Checks:** Ensure no NULL values in key columns  
- **Logging:** Store execution logs in PostgreSQL  

---

## **6️⃣ Final Deliverables**
- ✅ **Fully functional ETL pipeline** running on a schedule  
- ✅ **PostgreSQL database** with cleaned aircraft data  
- ✅ **Cloud-deployed ETL (if using AWS)**  
- ✅ **Demo & presentation to class**  

---

Would you like help writing the **YAML config**, **SQL transformations**, or **AWS deployment scripts**? 🚀

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram.

Here is a sample solution architecture diagram:

![images/sample-solution-architecture-diagram.png](images/sample-solution-architecture-diagram.png)

## Breakdown of tasks

Here's a **Trello-style breakdown** for your **OpenSky Aircraft Data ETL Pipeline** project over the next 15 days. You can use **columns for tasks** and **cards for specific activities** within those tasks.

---

## **Trello Board Layout**  

### **Columns:**
1. **Backlog**
2. **In Progress**
3. **Completed**

---

### **Backlog** (Tasks to be done)
#### **1. Planning & Research (Days 1-2)**
- **Card 1:** Review OpenSky API documentation  
- **Card 2:** Define data schema for PostgreSQL  
- **Card 3:** Create project architecture diagram  

#### **2. Data Extraction (Days 3-5)**
- **Card 1:** Write Python script for API connection  
- **Card 2:** Implement incremental extraction (hourly requests)  
- **Card 3:** Store raw data in PostgreSQL  

#### **3. Data Transformation (Days 6-9)**
- **Card 1:** Clean data (handle nulls, normalize fields)  
- **Card 2:** Remove duplicates (implement upsert mechanism)  
- **Card 3:** Enrich data (calculate speed, add identifiers)  

#### **4. Data Loading & Storage (Days 10-11)**
- **Card 1:** Create final PostgreSQL table for transformed data  
- **Card 2:** Implement upsert loading to avoid duplicates  
- **Card 3:** Optimize database (indexes, performance tuning)  

#### **5. Automation & Deployment (Days 12-13)**
- **Card 1:** Automate ETL using Python schedule module  
- **Card 2:** Implement logging and monitoring system  
- **Card 3:** Optional: Dockerize the ETL pipeline  

#### **6. Analysis & Visualization (Days 14-15)**
- **Card 1:** Generate SQL queries for insights  
- **Card 2:** Create visualizations using Matplotlib/Seaborn  
- **Card 3:** Prepare final presentation and report  



Feel free to customize this breakdown according to your workflow or specific requirements! Let me know if you need any adjustments or additional help. 🚀
How is your project broken down? Who is doing what?

We recommend using a free Task board such as [Trello](https://trello.com/). This makes it easy to assign and track tasks to each individual.

Example:

![images/kanban-task-board.png](images/kanban-task-board.png)
