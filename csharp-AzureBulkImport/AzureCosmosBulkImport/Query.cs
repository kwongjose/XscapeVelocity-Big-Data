using System;

public class Query
{
    public string MinMaxBySite { get; } = @"SELECT c.siteCode, DateTimePart(year, c.dateGMT) [year], min(c.sampleMeasurement)[minMeasurement], max(c.sampleMeasurement) [maxMeasurement]
        FROM c
        GROUP BY c.siteCode, DateTimePart(year, c.dateGMT)";

    public string AnnualPMAverage { get; } = @"SELECT DateTimePart(year, c.dateGMT) [year], AVG(c.sampleMeasurement) [averageSampleMeasurement]
        FROM c
        GROUP BY DateTimePart(year, c.dateGMT)";

    public string PrePostCovidDifference { get; } = @"SELECT preCovidAverage - postCovidAverage [difference]
        FROM (
            SELECT AVG(c.sampleMeasurement) [preCovidAverage]
            FROM c
            WHERE c.dateGMT < '2020-03-01'
        ) AS preCovid
        JOIN (
            SELECT AVG(c.sampleMeasurement) [postCovidAverage]
            FROM c
            WHERE c.dateGMT >= '2020-03-01'
        ) AS postCovid";

    public string UndefinedOrNullData { get; } = @"SELECT c.id [id]
        FROM c
        WHERE c.siteCode IS NULL
        OR c.dateGMT IS NULL
        OR c.sampleMeasurement IS NULL
        OR c.siteCode IS NOT DEFINED
        OR c.dateGMT IS NOT DEFINED
        OR c.sampleMeasurement IS NOT DEFINED";

    public string NegativeMeasurements { get; } = @"SELECT c.id [id]
        FROM c
        WHERE c.sampleMeasurement < 0";

    public string RepeatedMeasurements { get; } = @"SELECT c.id [id]
        FROM c
        WHERE EXISTS(
            SELECT 1
            FROM c AS d
            WHERE d.siteCode = c.siteCode
            AND d.sampleMeasurement = c.sampleMeasurement
            AND (
                d.dateGMT > c.dateGMT 
                or (d.dateGMT = c.dateGMT and d.timeGMT > c.timeGMT)
            )
            AND NOT EXISTS(
                SELECT 1
                FROM c as e
                WHERE d.siteCode = c.siteCode
                AND DateTimeToTicks(e.dateGMT)+DateTimeToTicks(e.timeGMT) between DateTimeToTicks(c.dateGMT)+DateTimeToTicks(c.timeGMT) AND DateTimeToTicks(d.dateGMT)+DateTimeToTicks(d.timeGMT)
                AND e.sampleMeasurement <> d.sampleMeasurement
            )
        )";

    public string minSampleMeasurement { get; } = "SELECT MIN(d.sampleMeasurement) FROM d";

    public string totalEntryCount { get; } = "SELECT COUNT(c.id) FROM c";

    public string totalEntryCountByDate { get; } = @"
        SELECT COUNT(c.id) 
        FROM c 
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";
    public string getMedianUpperValue { get; } = @"
        SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement ASC";

    public string getMedianLowerValue { get; } = @"SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement DESC";

    public string minSampleMeasurementByYear { get; } = @"
        SELECT MIN(c.sampleMeasurement) as minMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string maxSampleMeasurementByYear { get; } = @"
        SELECT MAX(c.sampleMeasurement) as maxMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";
}