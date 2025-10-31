----------------- Clickbench -----------------

-- clickbench metadata schema. error if the schema already exists
CREATE SCHEMA lake_clickbench;

GRANT USAGE ON SCHEMA lake_clickbench TO PUBLIC;

-- static table to store clickbench queries
CREATE TABLE lake_clickbench.queries(
    id SERIAL PRIMARY KEY,
    query_text TEXT
);

GRANT SELECT ON lake_clickbench.queries TO PUBLIC;

-- populate the table with clickbench queries
INSERT INTO lake_clickbench.queries(query_text)
VALUES  ('SELECT COUNT(*) FROM hits;'),
        ('SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;'),
        ('SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;'),
        ('SELECT AVG(UserID) FROM hits;'),
        ('SELECT COUNT(DISTINCT UserID) FROM hits;'),
        ('SELECT COUNT(DISTINCT SearchPhrase) FROM hits;'),
        ('SELECT MIN(EventDate), MAX(EventDate) FROM hits;'),
        ('SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;'),
        ('SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;'),
        ('SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;'),
        ('SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '''' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;'),
        ('SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '''' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;'),
        ('SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '''' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;'),
        ('SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '''' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;'),
        ('SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '''' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;'),
        ('SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;'),
        ('SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;'),
        ('SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;'),
        ('SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;'),
        ('SELECT UserID FROM hits WHERE UserID = 435090932899640449;'),
        ('SELECT COUNT(*) FROM hits WHERE URL LIKE ''%google%'';'),
        ('SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE ''%google%'' AND SearchPhrase <> '''' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;'),
        ('SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE ''%Google%'' AND URL NOT LIKE ''%.google.%'' AND SearchPhrase <> '''' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;'),
        ('SELECT * FROM hits WHERE URL LIKE ''%google%'' ORDER BY EventTime LIMIT 10;'),
        ('SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '''' ORDER BY EventTime LIMIT 10;'),
        ('SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '''' ORDER BY SearchPhrase LIMIT 10;'),
        ('SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '''' ORDER BY EventTime, SearchPhrase LIMIT 10;'),
        ('SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '''' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;'),
        ('SELECT REGEXP_REPLACE(Referer, ''^https?://(?:www\\.)?([^/]+)/.*$'', ''\1'') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '''' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;'),
        ('SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;'),
        ('SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '''' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;'),
        ('SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '''' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;'),
        ('SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;'),
        ('SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;'),
        ('SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;'),
        ('SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;'),
        ('SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ''2013-07-01'' AND EventDate <= ''2013-07-31'' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '''' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;'),
        ('SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ''2013-07-01'' AND EventDate <= ''2013-07-31'' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '''' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;'),
        ('SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ''2013-07-01'' AND EventDate <= ''2013-07-31'' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;'),
        ('SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '''' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ''2013-07-01'' AND EventDate <= ''2013-07-31'' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;'),
        ('SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ''2013-07-01'' AND EventDate <= ''2013-07-31'' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;'),
        ('SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ''2013-07-01'' AND EventDate <= ''2013-07-31'' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;'),
        ('SELECT DATE_TRUNC(''minute'', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ''2013-07-14'' AND EventDate <= ''2013-07-15'' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC(''minute'', EventTime) ORDER BY DATE_TRUNC(''minute'', EventTime) LIMIT 10 OFFSET 1000;');

CREATE TYPE lake_clickbench.table_type AS ENUM ('pg_lake_iceberg', 'pg_lake', 'heap');

-- create bench table
CREATE OR REPLACE FUNCTION lake_clickbench.create(table_type lake_clickbench.table_type DEFAULT 'pg_lake_iceberg', clickbench_uri TEXT DEFAULT 's3://clickhouse-public-datasets/hits_compatible/hits.parquet')
RETURNS VOID AS $$
BEGIN
    -- Auto-infer the columns from the hits.parquet
    IF table_type = 'pg_lake_iceberg' THEN
        EXECUTE 'CREATE TABLE hits_auto () USING pg_lake_iceberg WITH (definition_from = ''' || clickbench_uri || ''')';
    ELSIF table_type = 'pg_lake' THEN
        EXECUTE 'CREATE FOREIGN TABLE hits_auto () SERVER pg_lake OPTIONS (path ''' || clickbench_uri || ''')';
    ELSE
        EXECUTE 'CREATE TABLE hits_auto () WITH (definition_from = ''' || clickbench_uri || ''')';
    END IF;

    -- hits.parquet uses integers for date and time,
    -- which most databases cannot pass directly
    -- to date/time functions. Hence we convert them to date and
    -- timestamptz in a view that wraps around the table
    CREATE OR REPLACE VIEW hits as SELECT
    WatchID,
    JavaEnable,
    Title,
    GoodEvent,
    to_timestamp(EventTime) AS EventTime,
    to_date(EventDate) AS EventDate,
    CounterID,
    ClientIP,
    RegionID,
    UserID,
    CounterClass,
    OS,
    UserAgent,
    URL,
    Referer,
    IsRefresh,
    RefererCategoryID,
    RefererRegionID,
    URLCategoryID,
    URLRegionID,
    ResolutionWidth,
    ResolutionHeight,
    ResolutionDepth,
    FlashMajor,
    FlashMinor,
    FlashMinor2,
    NetMajor,
    NetMinor,
    UserAgentMajor,
    UserAgentMinor,
    CookieEnable,
    JavascriptEnable,
    IsMobile,
    MobilePhone,
    MobilePhoneModel,
    Params,
    IPNetworkID,
    TraficSourceID,
    SearchEngineID,
    SearchPhrase,
    AdvEngineID,
    IsArtifical,
    WindowClientWidth,
    WindowClientHeight,
    ClientTimeZone,
    ClientEventTime,
    SilverlightVersion1,
    SilverlightVersion2,
    SilverlightVersion3,
    SilverlightVersion4,
    PageCharset,
    CodeVersion,
    IsLink,
    IsDownload,
    IsNotBounce,
    FUniqID,
    OriginalURL,
    HID,
    IsOldCounter,
    IsEvent,
    IsParameter,
    DontCountHits,
    WithHash,
    HitColor,
    LocalEventTime,
    Age,
    Sex,
    Income,
    Interests,
    Robotness,
    RemoteIP,
    WindowName,
    OpenerName,
    HistoryLength,
    BrowserLanguage,
    BrowserCountry,
    SocialNetwork,
    SocialAction,
    HTTPError,
    SendTiming,
    DNSTiming,
    ConnectTiming,
    ResponseStartTiming,
    ResponseEndTiming,
    FetchTiming,
    SocialSourceNetworkID,
    SocialSourcePage,
    ParamPrice,
    ParamOrderID,
    ParamCurrency,
    ParamCurrencyID,
    OpenstatServiceName,
    OpenstatCampaignID,
    OpenstatAdID,
    OpenstatSourceID,
    UTMSource,
    UTMMedium,
    UTMCampaign,
    UTMContent,
    UTMTerm,
    FromTag,
    HasGCLID,
    RefererHash,
    URLHash,
    CLID
    from hits_auto;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION lake_clickbench.create(table_type lake_clickbench.table_type, clickbench_uri TEXT) TO PUBLIC;

-- loads data into the bench table
CREATE OR REPLACE FUNCTION lake_clickbench.load(clickbench_uri TEXT DEFAULT 's3://clickhouse-public-datasets/hits_compatible/hits.parquet')
RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'hits_auto' AND relnamespace = current_schema :: regnamespace :: oid) THEN
        RAISE EXCEPTION 'first need to run "lake_clickbench.create()" to set up bench tables';
    END IF;

   -- bail out if read only analytics table
   IF EXISTS (
        SELECT 1 FROM pg_foreign_table ft JOIN pg_foreign_server fs ON (ft.ftserver = fs.oid)
         WHERE srvname = 'pg_lake' AND ftrelid = (SELECT oid FROM pg_class WHERE relname = 'hits_auto' AND relnamespace = current_schema :: regnamespace :: oid)
    ) THEN
        RETURN;
    END IF;

    EXECUTE 'COPY hits_auto FROM ''' || clickbench_uri || '''';
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION lake_clickbench.load(clickbench_uri TEXT) TO PUBLIC;

-- show a query
CREATE OR REPLACE FUNCTION lake_clickbench.show_query(query_id INT)
RETURNS TEXT AS $$
BEGIN
    IF query_id < 1 OR query_id > 43 THEN
        RAISE EXCEPTION 'query id must be between 1 and 43';
    END IF;

    RETURN (SELECT query_text FROM lake_clickbench.queries WHERE id = query_id);
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION lake_clickbench.show_query(INT) TO PUBLIC;

-- run a query
CREATE OR REPLACE FUNCTION lake_clickbench.run_query(query_id INT)
RETURNS VOID AS $$
DECLARE
    query_txt TEXT;
BEGIN
    IF query_id < 1 OR query_id > 43 THEN
        RAISE EXCEPTION 'query id must be between 1 and 43';
    END IF;

    SELECT query_text INTO query_txt FROM lake_clickbench.queries WHERE id = query_id;

    EXECUTE query_txt;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION lake_clickbench.run_query(INT) TO PUBLIC;

----------------- TPC-H -----------------

-- tpch metadata schema. error if the schema already exists
CREATE SCHEMA lake_tpch;

GRANT USAGE ON SCHEMA lake_tpch TO PUBLIC;

CREATE TYPE lake_tpch.table_type AS ENUM ('pg_lake_iceberg', 'pg_lake', 'heap');

-- creates the bench tables with generated data
CREATE FUNCTION lake_tpch.gen(location TEXT, table_type lake_tpch.table_type DEFAULT 'pg_lake_iceberg', scale_factor FLOAT4 DEFAULT 1.0, iteration_count INT DEFAULT 1)
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_lake_tpch_gen';

GRANT EXECUTE ON FUNCTION lake_tpch.gen(location TEXT, table_type lake_tpch.table_type, scale_factor FLOAT4, iteration_count INT) TO PUBLIC;

-- show queries
CREATE OR REPLACE FUNCTION lake_tpch.queries()
RETURNS TABLE(query_nr INT, query TEXT)
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_lake_tpch_queries';

GRANT EXECUTE ON FUNCTION lake_tpch.queries() TO PUBLIC;

-- run a query
CREATE OR REPLACE FUNCTION lake_tpch.run_query(query_id INT)
RETURNS VOID AS $$
DECLARE
    query_txt TEXT;
BEGIN
    IF query_id < 1 OR query_id > 22 THEN
        RAISE EXCEPTION 'query id must be between 1 and 22';
    END IF;

    EXECUTE 'SELECT query FROM lake_tpch.queries() WHERE query_nr = $1' INTO query_txt USING query_id;

    EXECUTE query_txt;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION lake_tpch.run_query(INT) TO PUBLIC;

-- creates the bench tables with generated data
CREATE FUNCTION lake_tpch.gen_partitioned(location TEXT, table_type lake_tpch.table_type DEFAULT 'pg_lake_iceberg', scale_factor FLOAT4 DEFAULT 1.0, iteration_count INT DEFAULT 1, 
                                         lineitem_partition_by text DEFAULT NULL,
                                         customer_partition_by text DEFAULT NULL,
                                         nation_partition_by text DEFAULT NULL,
                                         orders_partition_by text DEFAULT NULL,
                                         part_partition_by text DEFAULT NULL,
                                         partsupp_partition_by text DEFAULT NULL,
                                         region_partition_by text DEFAULT NULL,
                                         supplier_partition_by text DEFAULT NULL)
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_lake_tpch_gen_partitioned';

GRANT EXECUTE ON FUNCTION lake_tpch.gen_partitioned(location TEXT, table_type lake_tpch.table_type, scale_factor FLOAT4, iteration_count INT, 
                                                   lineitem_partition_by TEXT,
                                                   customer_partition_by text,
                                                   nation_partition_by text,
                                                   orders_partition_by text,
                                                   part_partition_by text,
                                                   partsupp_partition_by text,
                                                   region_partition_by text,
                                                   supplier_partition_by text) TO PUBLIC;


----------------- TPC-DS -----------------

CREATE SCHEMA lake_tpcds;

GRANT USAGE ON SCHEMA lake_tpcds TO PUBLIC;

CREATE TYPE lake_tpcds.table_type AS ENUM ('pg_lake_iceberg', 'pg_lake', 'heap');

-- creates the bench tables with generated data
CREATE FUNCTION lake_tpcds.gen(location TEXT, table_type lake_tpcds.table_type DEFAULT 'pg_lake_iceberg', scale_factor FLOAT4 DEFAULT 1.0)
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_lake_tpcds_gen';

GRANT EXECUTE ON FUNCTION lake_tpcds.gen(location TEXT, table_type lake_tpcds.table_type, scale_factor FLOAT4) TO PUBLIC;

-- show queries
CREATE OR REPLACE FUNCTION lake_tpcds.queries()
RETURNS TABLE(query_nr INT, query TEXT)
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_lake_tpcds_queries';

GRANT EXECUTE ON FUNCTION lake_tpcds.queries() TO PUBLIC;

-- run a query
CREATE OR REPLACE FUNCTION lake_tpcds.run_query(query_id INT)
RETURNS VOID AS $$
DECLARE
    query_txt TEXT;
BEGIN
    IF query_id < 1 OR query_id > 99 THEN
        RAISE EXCEPTION 'query id must be between 1 and 99';
    END IF;

    EXECUTE 'SELECT query FROM lake_tpcds.queries() WHERE query_nr = $1' INTO query_txt USING query_id;

    EXECUTE query_txt;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION lake_tpcds.run_query(INT) TO PUBLIC;
