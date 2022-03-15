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

    public string MedianSampleMeasurement { get; } = /*@"SELECT c.id
        FROM c
        WHERE c.sampleMeasurement < 0";
*/
/*        @"SELECT
(
 (SELECT MAX(c.sampleMeasurement) FROM
   (SELECT TOP 50 PERCENT sampleMeasurement FROM c ORDER BY c.sampleMeasurement) AS BottomHalf)
 +
 (SELECT MIN(c.sampleMeasurement) FROM
   (SELECT TOP 50 PERCENT c.sampleMeasurement FROM c ORDER BY c.sampleMeasurement DESC) AS TopHalf)
) / 2 AS Median";*/


/*@"SELECT AVG(middle_values) [median] FROM (
  SELECT t1.sampleMeasurement [middle_values] FROM
    (
      SELECT @row:=@row+1 [row], c.sampleMeasurement
      FROM c, (SELECT @row:=0) [r]
      WHERE c.dateGMT BETWEEN '2021-01-01' AND '2021-12-31' AS c.pmdata2021
      ORDER BY c.sampleMeasurement
    ) [t1],
    (
      SELECT COUNT(*) [count]
      FROM c
      WHERE c.dateGMT BETWEEN '2021-01-01' AND '2021-12-31' AS c.pmdata2021
    ) [t2]
    WHERE t1.row >= t2.count/2 and t1.row <= ((t2.count/2) +1)) [t3];";*/

// sort by sampleMeasurement
// use half of total rows to create floor
// take TOP 1 
// this would be "get top measurement from average"

/*        @"SELECT TOP 1 COUNT(1) AS pmConcentrationOccurences, c.sampleMeasurement
        FROM c
        GROUP BY c.sampleMeasurement";*/

/*        @"SELECT TOP 1 COUNT((SELECT COUNT('id') FROM c)/2)
        FROM (SELECT * FROM c WHERE c.sampleMeasurement > 0) 
        ORDER BY c.sampleMeasurement";*/
//half of my records are 10,107,273
//will COUNT(c.id)/2 work?
@"SELECT TOP @rowNum MIN(c.sampleMeasurement) as lowestMeasurement
FROM c
ORDER BY c.sampleMeasurement";
    public string minSampleMeasurement { get; } = "SELECT MIN(d.sampleMeasurement) FROM d";

    public string totalEntryCount { get; } = "SELECT COUNT(c.id) FROM c";

    public string totalEntryCountByDate { get; } = @"
        SELECT COUNT(c.id) 
        FROM c 
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";
    public string getSimpleMedian { get; } = @"
        SELECT TOP @rowNum MAX(c.sampleMeasurement) as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement ASC";

    public string getMedianLowerValue { get; } = @"SELECT TOP @rowNum MIN(c.sampleMeasurement) as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement DESC";

    //TODO: DELETE ME
    public string totalEntryCountByDateNoAboveZero { get; } = @"
        SELECT COUNT(c.id) 
        FROM c 
        WHERE c.dateGMT BETWEEN @startDate AND @endDate";
}