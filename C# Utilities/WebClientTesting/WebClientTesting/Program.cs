using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;

namespace WebClientTesting
{
    class Program
    {
        static void Main(string[] args)
        {
            //WebRequest wrq = WebRequest.Create("http://www.reuters.com");
            //HttpWebRequest hwrq = (HttpWebRequest)WebRequest.Create("http://slashdot.org");
            //HttpWebRequest hwrq = (HttpWebRequest)WebRequest.Create("http://www.google.com/reader/");
            //HttpWebRequest hwrq = (HttpWebRequest)WebRequest.Create("http://www.google.com/reader/view/#overview-page");
            HttpWebRequest hwrq = (HttpWebRequest)WebRequest.Create("http://techbus.safaribooksonline.com/_ajax_loginpopup?__className=securelogin&__sugus=204416987&__version=6.0.2");
            string postData = "__formName=455407&portal=techbus&targetpage=%2Fhome&aph=ZUGTBNCTZrZYNHBJsHMGZJfThpAKKJFRNqZSZXVnBKRFBVGGFFPWQOGTBQzW&view=book&__login=barret.miller%40tyson.com&__password=protoss&__rememberpassword=&__loginsubmit=Continue&null";
            byte[] postDataBytes = Encoding.UTF8.GetBytes(postData);

            hwrq.Method = "POST";
            hwrq.ContentType = "text/plain; charset=utf-8";
            hwrq.ContentLength = postDataBytes.Length;
            Stream postStream = hwrq.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();
            HttpWebResponse wrs = (HttpWebResponse)hwrq.GetResponse();

            //while ((line = sr.ReadLine()) != null);
            //{
            //    Console.WriteLine(line);
            //    sb.Append(line); 
            //}
            //strm.Close();

            hwrq = (HttpWebRequest)WebRequest.Create("http://techbus.safaribooksonline.com/mybookmarks");
            hwrq.CookieContainer = new CookieContainer();
            hwrq.CookieContainer.Add(wrs.Cookies);
            wrs = (HttpWebResponse)hwrq.GetResponse();


            Stream strm = wrs.GetResponseStream();
            StreamReader sr = new StreamReader(strm);
            StringBuilder sb = new StringBuilder();
            string line;
            while ((line = sr.ReadLine()) != null)
            {
                Console.WriteLine(line);
                sb.Append(line);
            }


            Console.ReadLine();
        }
    }
}
