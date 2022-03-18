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

    public string totalEntryCountRHDP { get; } = "SELECT COUNT(c.id) FROM c WHERE c.parameterName = @typeOfMeasure";

    public string totalEntryCountByDate { get; } = @"
        SELECT COUNT(c.id) 
        FROM c 
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string totalEntryCountByDateRHDP { get; } = @"
        SELECT COUNT(c.id)
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";

    public string totalEntryCountByDateAndSiteCodePM { get; } = @"
        SELECT COUNT(c.id) as count
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.siteCode = @siteCode)";

    public string totalEntryCountByDateAndSiteCodeRHDP { get; } = @"
        SELECT COUNT(c.id) as count
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure) AND (c.siteCode = @siteCode)";
    public string getMedianUpperValue { get; } = @"
        SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement ASC";
    public string getMedianUpperValueRHDP { get; } = @"
        SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)
        ORDER BY c.sampleMeasurement ASC";

    public string getMedianLowerValue { get; } = @"SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement DESC";
    public string getMedianLowerValueRHDP { get; } = @"SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)
        ORDER BY c.sampleMeasurement DESC";

    public string minSampleMeasurementByYear { get; } = @"
        SELECT MIN(c.sampleMeasurement) as minMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string minSampleMeasurementByYearRHDP { get; } = @"
        SELECT MIN(c.sampleMeasurement) as minMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";
    public string maxSampleMeasurementByYear { get; } = @"
        SELECT MAX(c.sampleMeasurement) as maxMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string maxSampleMeasurementByYearRHDP { get; } = @"
        SELECT MAX(c.sampleMeasurement) as maxMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";

    public string averageSampleMeasurementByDate { get; } = @"
        SELECT AVG(c.sampleMeasurement) as average
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string averageSampleMeasurementByDateRHDP { get; } = @"
        SELECT AVG(c.sampleMeasurement) as average
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND(c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";

    public string averageSampleMeasurementByDateAndSiteCode { get; } = @"
        SELECT AVG(c.sampleMeasurement) as average
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.siteCode = @siteCode)";

    public string averageSampleMeasurementByDateAndSiteCodeRHDP { get; } = @"
        SELECT AVG(c.sampleMeasurement) as average
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.siteCode = @siteCode) AND (c.parameterName = @typeOfMeasure)";

    public string countOccurencesOfMeasurementLessThan { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure";

    public string countOccurencesOfMeasurementLessThanRHDP { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure AND (c.parameterName = @typeOfMeasure)";

    public string countOccurencesOfMeasurementLessThanByDatePM { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string countOccurencesOfMeasurementLessThanByDateRHDP { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";

    public string countOccurencesOfMeasurementGreaterThanByDatePM { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement > @measure AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string countOccurencesOfMeasurementGreaterThanByDateRHDP { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement > @measure AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";

    public string stDevPM { get; } = @"
        SELECT SQRT(c.sumAvgDiffSquared / c.adjCount) as stDev
        FROM (
            SELECT SUM(c.avgDiffSqared) as sumAvgDiffSquared, COUNT(c.avgDiffSqared)-1 as adjCount
            FROM (
                SELECT SQUARE(c.sampleMeasurement - @averageMeasurement) as avgDiffSqared
                FROM c
                WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)
            ) as c
        ) as c";

    public string stDevRHDP { get; } = @"
        SELECT SQRT(c.sumAvgDiffSquared / c.adjCount) as stDev
        FROM (
            SELECT SUM(c.avgDiffSqared) as sumAvgDiffSquared, COUNT(c.avgDiffSqared)-1 as adjCount
            FROM (
                SELECT SQUARE(c.sampleMeasurement - @averageMeasurement) as avgDiffSqared
                FROM c
                WHERE c.sampleMeasurement >= 0.0 
                AND (c.dateGMT BETWEEN @startDate AND @endDate)
                AND c.parameterName = @typeOfMeasure
            ) as c
        ) as c";
}