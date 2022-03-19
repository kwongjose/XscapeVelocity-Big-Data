            //Median, Min, and Max Measurements Per Year
            for (int i = 0; i < constant.yearlyDateArray.Length; i++)
            {
                // Get Median PM2.5 Data Per Year
                await this.GetMedianPerYearQuerySet(constant.yearlyDateArray[i], constant.yearlyDateArray[i + 1]);
                await this.GetMinMeasurePerYearQuerySet(constant.yearlyDateArray[i], constant.yearlyDateArray[i + 1]);
                await this.GetMaxMeasurePerYearQuerySet(constant.yearlyDateArray[i], constant.yearlyDateArray[i + 1]);
                //skip 1 because every other is an end date
                i++;
            }