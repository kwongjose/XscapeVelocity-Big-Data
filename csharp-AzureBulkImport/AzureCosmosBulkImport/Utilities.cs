using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


public class Utilities
{
    public static void PrintParams(string[] parameters)
    {
        Console.WriteLine("Parameters for Query");
        foreach (string param in parameters)
        {
            Console.WriteLine(param);
        }
    }

    public static void WriteToCSV(string siteCode, List<string> dates, string type, double[] measureArray)
    {

        foreach (string date in dates)
        {
            var csv = new StringBuilder();
            string newLine = ""; //empty string after each add
            newLine = string.Format("{0},{1},{2},{3},{4}", siteCode, date, measureArray[0], measureArray[1], measureArray[2]);
            csv.AppendLine(newLine);

            using (StreamWriter w = File.AppendText(siteCode + type + ".csv"))
            {
                w.Write(csv.ToString());
            }
        }

        Console.WriteLine("wrote file for " + siteCode);
    }

}
