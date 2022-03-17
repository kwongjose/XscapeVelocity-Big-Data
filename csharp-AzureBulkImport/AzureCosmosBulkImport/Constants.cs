using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

    public class Constants
    {
        public string[] monthlyDateArray { get; } = new string[96] {
                "2017-01-01","2017-01-31","2017-02-01","2017-02-28","2017-03-01","2017-03-31","2017-04-01","2017-04-30","2017-05-01","2017-05-31","2017-06-01","2017-06-30","2017-07-01","2017-07-31","2017-08-01","2017-08-31","2017-09-01","2017-09-30","2017-10-01","2017-10-31","2017-11-01","2017-11-30","2017-12-01","2017-12-31",
                "2018-01-01","2018-01-31","2018-02-01","2018-02-28","2018-03-01","2018-03-31","2018-04-01","2018-04-30","2018-05-01","2018-05-31","2018-06-01","2018-06-30","2018-07-01","2018-07-31","2018-08-01","2018-08-31","2018-09-01","2018-09-30","2018-10-01","2018-10-31","2018-11-01","2018-11-30","2018-12-01","2018-12-31",
                "2019-01-01","2019-01-31","2019-02-01","2019-02-28","2019-03-01","2019-03-31","2019-04-01","2019-04-30","2019-05-01","2019-05-31","2019-06-01","2019-06-30","2019-07-01","2019-07-31","2019-08-01","2019-08-31","2019-09-01","2019-09-30","2019-10-01","2019-10-31","2019-11-01","2019-11-30","2019-12-01","2019-12-31",
                "2020-01-01","2020-01-31","2020-02-01","2020-02-29","2020-03-01","2020-03-31","2020-04-01","2020-04-30","2020-05-01","2020-05-31","2020-06-01","2020-06-30","2020-07-01","2020-07-31","2020-08-01","2020-08-31","2020-09-01","2020-09-30","2020-10-01","2020-10-31","2020-11-01","2020-11-30","2020-12-01","2020-12-31"
             };
        public string[] yearlyDateArray { get; } = new string[8] { "2017-01-01", "2017-12-31", "2018-01-01", "2018-12-31", "2019-01-01", "2019-12-31", "2020-01-01", "2020-12-31" };

    public List<string> LoadSitesFromCSV()
        {
            //not *technically* a constant but its another huge list of stuff used for queries...so have at it
            List<string> sites = new List<string>();
            string temp1, temp2, temp3 = "";
        using (var reader = new StreamReader(@"..\..\..\aqs_sites_short.csv"))
        {
            reader.ReadLine(); //skipping header line

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var values = line.Split(',');

                temp1 = this.parseValue(values[0], "D2");
                temp2 = this.parseValue(values[1], "D3");
                temp3 = this.parseValue(values[2], "D4");

                sites.Add(temp1 + "-" + temp2 + "-" + temp3);

            }
        }

        return sites;
        }

    private string parseValue(string input, string f)
    {
        try
        {
            input = Int32.Parse(input).ToString(f);
        }
        catch (FormatException)
        {
            Console.WriteLine($"Unable to parse '{input}'");
        }

        return input;
    }

    public List<string> TestLoadSitesFromCSV()
    {
        //not *technically* a constant but its another huge list of stuff used for queries...so have at it
        List<string> sites = new List<string>();
        string temp1, temp2, temp3 = "";
        using (var reader = new StreamReader(@"..\..\..\test-sites.csv"))
        {
            reader.ReadLine(); //skipping header line

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                var values = line.Split(',');
                Console.WriteLine(values[0]);
                Console.WriteLine(values[0].GetType());

                temp1 = this.parseValue(values[0], "D2");
                temp2 = this.parseValue(values[1], "D3");
                temp3 = this.parseValue(values[2], "D4");

                Console.WriteLine(temp1 + "-" + temp2 + "-" + temp3);
                sites.Add(temp1 + "-" + temp2 + "-" + temp3);

            }
        }

        return sites;
    }
}
