## Solution

- ***Upload input files to HDFS***
    
    ```bash
    $ pwd 
    
    /mnt/home/madanasekharnvsgmail/Project-1-Stock-Exchange-Data-Analysis/NYSE
    
    $ ll
    
    -rw-r--r-- 1 madanasekharnvsgmail madanasekharnvsgmail    40566 Jun 28 13:54 StockCompanies.csv
    -rw-r--r-- 1 madanasekharnvsgmail madanasekharnvsgmail 51286497 Jun 28 13:54 StockPrices.csv
    
    $ hadoop fs -mkdir NYSE
    $ hadoop fs -put StockCompanies.csv NYSE/StockCompanies.csv
    $ hadoop fs -put StockPrices.csv NYSE/StockPrices.csv
    $ hadoop fs -ls NYSE
    
    -rw-r--r--   3 madanasekharnvsgmail hadoop      40566 2022-06-29 14:05 NYSE/StockCompanies.csv
    -rw-r--r--   3 madanasekharnvsgmail hadoop   51286497 2022-06-29 10:20 NYSE/StockPrices.csv
    ```
    
- ***Create HIVE database & required source tables***
    
    ```sql
    hive> create database IF NOT EXISTS nms_seda;
    OK
    
    -- Change database/schema to nms_seda
    hive> user nms_seda;
    
    hive> show tables;
    OK
    
    -- Create HIVE tables for Stock_Prices & Strock_Companies
    
    hive> create table if not exists stock_prices (
        trading_date date, 
        symbol varchar(255), 
        open decimal(10,2), 
        close decimal(10,2), 
        low decimal(10,2), 
        high decimal(10,2), 
        volume int)
        row format delimited
        fields terminated by ','
        TBLPROPERTIES("skip.header.line.count"="1"); 
    
    hive> create table if not exists stock_companies (
        symbol varchar(255),
        company_name varchar(255),
        sector varchar(255),
        sub_industry varchar(255),
        headquarter varchar(255))
        row format delimited
        fields terminated by ','
        TBLPROPERTIES("skip.header.line.count"="1"); 
    
    hive> show tables;
    OK
    stock_companies
    stock_prices
    ```
    
- ***Load input files into source tables & verify***
    
    ```sql
    
    -- Loading source file into tables
    
    hive> LOAD DATA INPATH 'NYSE/StockCompanies.csv' INTO TABLE stock_companies;
    
    hive> LOAD DATA INPATH 'NYSE/StockPrices.csv' INTO TABLE stock_prices;
    
    -- Verify loaded data
    
    hive> SELECT * FROM nms_seda.stock_companies LIMIT 100; 
    
    hive> SELECT * FROM nms_seda.stock_prices LIMIT 100;
    ```
    
- ***Create required temp tables for data analysis***
    
    ```sql
    -- aggregated all stock metrics by year, month & company
    
    create table stock_prices_avg_by_year_month
    as
    SELECT date_format(sp.trading_date,'Y') AS trading_year,
           date_format(sp.trading_date,'M') AS trading_month,
           sp.symbol,
           sc.company_name,
           substr(sc.headquarter,instr(sc.headquarter,';')+2) STATE,
           sc.sector,
           sc.sub_industry,
           avg(sp.open) OPEN,
           avg(sp.close) CLOSE,
           avg(sp.low) low,
           avg(sp.high) high,
           avg(sp.volume) volume
    FROM nms_seda.stock_prices sp,
         nms_seda.stock_companies sc
    WHERE sp.symbol = sc.symbol
    GROUP BY date_format(sp.trading_date,'Y'),
             date_format(sp.trading_date,'M'),
             sp.symbol,
             sc.company_name,
             substr(sc.headquarter,instr(sc.headquarter,';')+2),
             sc.sector,
             sc.sub_industry;
    
    -- for each company start and end point on trading
    
    CREATE TABLE trading_timeline_by_company AS
    SELECT company_name,
           min(trading_year) first_year,
           min(trading_month) first_month,
           max(trading_year) last_year,
           max(trading_month) last_month
    FROM stock_prices_avg_by_year_month
    GROUP BY company_name;
    
    -- company wise first trading details
    
    CREATE TABLE first_trading_by_company AS
    SELECT s.*
    FROM nms_seda.stock_prices_avg_by_year_month s,
         nms_seda.trading_timeline_by_company t
    WHERE s.company_name = t.company_name
      AND s.trading_year = t.first_year
      AND s.trading_month = t.first_month;
    
    -- company wise last trading details
    
    CREATE TABLE last_trading_by_company AS
    SELECT s.*
    FROM nms_seda.stock_prices_avg_by_year_month s,
         nms_seda.trading_timeline_by_company t
    WHERE s.company_name = t.company_name
      AND s.trading_year = t.last_year
      AND s.trading_month = t.last_month;
    
    -- company wise growth rate details
    
    CREATE TABLE stock_growth_rate_by_company AS
    SELECT f.company_name,
           round(((l.close-f.open)/f.open)*100,
                 2) growth_rate
    FROM nms_seda.first_trading_by_company f,
         nms_seda.last_trading_by_company l
    WHERE f.company_name = l.company_name;
    
    -- Top 5 companies by growth rate
    
    CREATE TABLE top_5_company_by_growth_rate AS
    SELECT *
    FROM stock_growth_rate_by_company
    ORDER BY growth_rate DESC
    LIMIT 5;
    ```
    
- ***Top five companies that are good for investment***
    
    ```sql
    SELECT company_name,
           growth_rate
    FROM top_5_company_by_growth_rate;
    ```
    
    | company_name | growth_rate |
    | --- | --- |
    | Regeneron | 1492.89 |
    | Netflix Inc. | 1186.32 |
    | Ulta Salon Cosmetics & Fragrance Inc | 1091.9 |
    | Constellation Brands | 918.41 |
    | Broadcom | 827.07 |
    
- ***Best-growing industry by each state, having at least two or more industries mapped***
    
    ```sql
    -- Best-growing industry by each state, having at least two or more industries mapped
    
    CREATE TABLE stock_growth_rate_by_industry AS
    SELECT substr(c.headquarter,instr(c.headquarter,';')+2) STATE,
            c.sub_industry,
            round(avg(s.growth_rate),2) growth_rate
    FROM nms_seda.stock_growth_rate_by_company s,
         nms_seda.stock_companies c
    WHERE s.company_name = c.company_name
    GROUP BY substr(c.headquarter,instr(c.headquarter,';')+2),
             c.sub_industry
    HAVING count(c.sub_industry) > 2
    ORDER BY STATE,
             growth_rate DESC;
    
    SELECT STATE,
           sub_industry,
           growth_rate
    FROM stock_growth_rate_by_industry;
    ```
    
    | state | sub_industry | growth_rate |
    | --- | --- | --- |
    | California | Internet Software & Services | 311.17 |
    | California | Semiconductors | 236.4 |
    | California | Health Care Equipment | 187.3 |
    | California | REITs | 142.15 |
    | California | Semiconductor Equipment | 122.69 |
    | California | Application Software | 114.67 |
    | Ireland | Pharmaceuticals | 209.36 |
    | Massachusetts | Health Care Equipment | 175.15 |
    | New Jersey | Health Care Equipment | 129.85 |
    | New York | Diversified Financial Services | 275.17 |
    | New York | Broadcasting & Cable TV | 163.52 |
    | New York | Banks | 47.73 |
    | New York | Apparel; Accessories & Luxury Goods | 45.14 |
    | Ohio | Banks | 96.62 |
    | Oklahoma | Oil & Gas Exploration & Production | 56.58 |
    | Texas | Oil & Gas Refining & Marketing & Transportation | 161.02 |
    | Texas | Oil & Gas Exploration & Production | 50.05 |
    | Texas | Oil & Gas Equipment & Services | 4.89 |
- ***Pull worst, best, stable years by sector***
    
    ```sql
    -- trading timeline by sector and year
    
    CREATE TABLE trading_timeline_by_sector_year AS
    SELECT sector,
           trading_year,
           min(trading_month) first_month,
           max(trading_month) last_month
    FROM stock_prices_avg_by_year_month
    GROUP BY sector,
             trading_year;
    
    -- first month trading details by sector and year
    
    CREATE TABLE first_trading_by_sector_year AS         
    SELECT t.sector,
           t.trading_year,
           avg(s.OPEN) OPEN,
           avg(s.CLOSE) CLOSE,
           avg(s.low) low,
           avg(s.high) high,
           avg(s.volume) volume
    FROM nms_seda.stock_prices_avg_by_year_month s,
         nms_seda.trading_timeline_by_sector_year t
    WHERE s.sector = t.sector
      AND s.trading_year = t.trading_year
      AND s.trading_month = t.first_month
    GROUP BY t.sector,
             t.trading_year;
    
    -- last month trading details by sector and year
    
    CREATE TABLE last_trading_by_sector_year AS         
    SELECT t.sector,
           t.trading_year,
           avg(s.OPEN) OPEN,
           avg(s.CLOSE) CLOSE,
           avg(s.low) low,
           avg(s.high) high,
           avg(s.volume) volume
    FROM nms_seda.stock_prices_avg_by_year_month s,
         nms_seda.trading_timeline_by_sector_year t
    WHERE s.sector = t.sector
      AND s.trading_year = t.trading_year
      AND s.trading_month = t.last_month
    GROUP BY t.sector,
             t.trading_year;
    
    -- growth rate for each sector and year break-dwon
    
    CREATE TABLE stock_growth_rate_by_sector_year AS
    SELECT f.sector,
           f.trading_year,
           round(((l.close-f.open)/f.open)*100,
                 2) growth_rate
    FROM nms_seda.first_trading_by_sector_year f,
         nms_seda.last_trading_by_sector_year l
    WHERE f.sector = l.sector
      AND f.trading_year = l.trading_year;
    
    -- min, max and avg growth rates for each sector
    
    CREATE TABLE stock_growth_rate_by_sector AS
    SELECT sector,
           min(growth_rate) min_growth_rate,
           avg(growth_rate) avg_growth_rate,
           max(growth_rate) max_growth_rate
    FROM stock_growth_rate_by_sector_year
    GROUP BY sector;
    
    -- worst years for each sector based on growth rates
    
    CREATE TABLE stock_growth_rate_by_sector_worst_year AS
    SELECT s.sector,
           s.trading_year,
           s.growth_rate
    FROM stock_growth_rate_by_sector_year s,
         stock_growth_rate_by_sector t
    WHERE s.sector = t.sector
      AND s.growth_rate = t.min_growth_rate;
      
    -- best years for each sector based on growth rates
    
    CREATE TABLE stock_growth_rate_by_sector_best_year AS
    SELECT s.sector,
           s.trading_year,
           s.growth_rate
    FROM stock_growth_rate_by_sector_year s,
         stock_growth_rate_by_sector t
    WHERE s.sector = t.sector
      AND s.growth_rate = t.max_growth_rate;  
      
    -- stable years for each sector based on growth rates
    
    CREATE TABLE stock_growth_rate_by_sector_stable_year AS
    SELECT s.sector,
           s.trading_year,
           s.growth_rate
    FROM stock_growth_rate_by_sector_year s,
         stock_growth_rate_by_sector t
    WHERE s.sector = t.sector
      AND round(s.growth_rate,0) = round(t.avg_growth_rate,0);
    ```
    
    - ***Worst year***
        
        ```sql
        SELECT sector,
               trading_year,
               growth_rate
        FROM nms_seda.stock_growth_rate_by_sector_worst_year ;
        ```
        
        | sector | trading_year | growth_rate |
        | --- | --- | --- |
        | Consumer Discretionary | 2014 | 6.05 |
        | Consumer Staples | 2015 | -0.39 |
        | Energy | 2015 | -13.71 |
        | Financials | 2011 | -21.65 |
        | Health Care | 2010 | 0.04 |
        | Industrials | 2011 | -14.29 |
        | Information Technology | 2011 | -13.02 |
        | Materials | 2011 | -11.78 |
        | Real Estate | 2015 | -6.59 |
        | Telecommunications Services | 2015 | -12.32 |
        | Utilities | 2015 | -14.34 |
    - ***Best year***
        
        ```sql
        SELECT sector,
               trading_year,
               growth_rate
        FROM nms_seda.stock_growth_rate_by_sector_best_year;
        ```
        
        | sector | trading_year | growth_rate |
        | --- | --- | --- |
        | Consumer Discretionary | 2013 | 22.97 |
        | Consumer Staples | 2012 | 11.44 |
        | Energy | 2016 | 28.54 |
        | Financials | 2013 | 21.87 |
        | Health Care | 2013 | 16.03 |
        | Industrials | 2013 | 18.29 |
        | Information Technology | 2013 | 17.47 |
        | Materials | 2016 | 21.06 |
        | Real Estate | 2010 | 19.18 |
        | Telecommunications Services | 2012 | 18.14 |
        | Utilities | 2016 | 15.84 |
    - ***Stable year***
        
        ```sql
        SELECT sector,
               trading_year,
               growth_rate
        FROM nms_seda.stock_growth_rate_by_sector_stable_year;
        ```
        
        | sector | trading_year | growth_rate |
        | --- | --- | --- |
        | Consumer Staples | 2014 | 7.3 |
        | Real Estate | 2016 | 7.92 |
        | Utilities | 2011 | 3.8 |
