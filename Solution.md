## Solution

- Upload input files to HDFS
    
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
    
- Create HIVE database & required source tables
    
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
    
- Load input files into source tables & verify
    
    ```sql
    
    -- Loading source file into tables
    
    hive> LOAD DATA INPATH 'NYSE/StockCompanies.csv' INTO TABLE stock_companies;
    
    hive> LOAD DATA INPATH 'NYSE/StockPrices.csv' INTO TABLE stock_prices;
    
    -- Verify loaded data
    
    hive> SELECT * FROM nms_seda.stock_companies LIMIT 100; 
    
    hive> SELECT * FROM nms_seda.stock_prices LIMIT 100;
    ```
    
- Create required temp tables for data analysis
    
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
    
- Top five companies that are good for investment
    
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
    
- Best-growing industry by each state, having at least two or more industries mapped
