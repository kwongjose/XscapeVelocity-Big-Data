            //Find Outlier Information via IQR
            for (int i = 0; i < constant.yearlyDateArray.Length; i++)
            {
                double[] lowerQuartileArray = await this.GetLowerQuartileByDateQuerySet(constant.yearlyDateArray[i], constant.yearlyDateArray[i + 1]);
                double[] upperQuartileArray = await this.GetUpperQuartileByDateQuerySet(constant.yearlyDateArray[i], constant.yearlyDateArray[i + 1]);

                //hardcoding this because its a school assignment
                double[] lowerBoundArray = new double[3] {0.0,0.0,0.0 };
                double[] upperBoundArray = new double[3] {0.0,0.0,0.0 };
                for (int j = 0; j < lowerQuartileArray.Length; j++)
                {
                    double iqr = (upperQuartileArray[j] - lowerQuartileArray[j]) * 1.5;
                    lowerBoundArray[j] = lowerQuartileArray[j] - iqr;
                    upperBoundArray[j] = upperQuartileArray[j] + iqr;

                }

                await this.GetOutliersByDateQuerySet(constant.yearlyDateArray[i], constant.yearlyDateArray[i + 1], lowerBoundArray, upperBoundArray);
                //skip 1 because every other is an end date
                i++;
            }
