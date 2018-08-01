using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Data;
using System.Threading;

namespace WebClientTesting
{
    class RetailLinkLogin
    {
        static void Main(string[] args)
        {
            //HttpWebRequest hwrq1 = (HttpWebRequest)WebRequest.Create("https://retaillink.wal-mart.com/rl_security/rl_logon.aspx?ServerType=IIS1&redir=/");
            //HttpWebRequest hwrq2 = (HttpWebRequest)WebRequest.Create("https://rllogin.wal-mart.com/rl_security/ajax/LogonApp.rl_logon,vb_rl_security.ashx");
            //HttpWebResponse wrs1;
            //HttpWebResponse wrs2;

            CookieContainer accumulatedCookies = null;
            Stream postStream;
            
            string cookieHeader = "";
            string rlUser = "";
            string rlPassword = "";
            string cmUser = "";
            string cmPassword = "";
            string postData = "";
            byte[] postDataBytes;
            string jobId = "";
            string userAgent = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; InfoPath.2; MS-RTC LM 8; .NET4.0C; .NET4.0E)";
            string comanSID = "";
            string comanItemID = "9701824";//"009166950";//"009110958";
            string comanWarehouseID = "7077";
            Stream strm;
            StreamReader sr;
            StringBuilder sb;
            string result;
            System.IO.StreamWriter DDIRHtmlFile;

            
            // This was always the same, but seemed to be assigned in different responses.                                                    
            string rlLoginInfoCookie = "rlLoginInfo=6353545D6353505D6055535D675D; domain=.wal-mart.com; path=/";
            //string rlLoginInfoCookie = "rlLoginInfo=6B5D4B4252604B54474B67; domain=.wal-mart.com; path=/";
            // This is assigned the same way every time from https://retaillink.wal-mart.com/rl_security/rl_logon.aspx?ServerType=IIS1&redir=/
            string accessAttemptCookie = "rl_access_attempt=0; domain=.wal-mart.com; path=/";

            
            //REQUEST 1: Get initial cookie
            /*
            hwrq1.Method = "GET";
            hwrq1.UserAgent = userAgent;           
            wrs1 = (HttpWebResponse)hwrq1.GetResponse();
            cookieHeader = getCookieHeader(wrs1);
            accumulatedCookies = accumulateCookies(accumulatedCookies, cookieHeader);

            // REQUEST 2: Get rloginInfo cookie
            hwrq2.Method = "POST";
            hwrq2.Headers["Accept-Language"] = "en-us";
            hwrq2.Headers["ajax-session"] = "2";
            hwrq2.Referer = "https://rllogin.wal-mart.com/rl_security/rl_logon.aspx?ServerType=IIS1&CTAuthMode=BASIC&CTLoginErrorMsg=BAD_PWD_OR_USER&language=en&CTUser=&CT_ORIG_URL=%2Frl_security%2Frl_logon.aspx%3FServerType%3DIIS1%26redir%3D%2F&ct_orig_uri=%2Frl_security%2Frl_logon.aspx%3FServerType%3DIIS1%26redir%3D%2F";
            hwrq2.ContentType = "application/x-www-form-urlencoded";
            hwrq2.Headers["ajax-method"] = "setInfo";
            //hwrq2.AutomaticDecompression = DecompressionMethods.GZip;
            hwrq2.Headers["Accept-Encoding"] = "gzip, deflate";
            hwrq2.UserAgent = userAgent;
            hwrq2.Headers["Cache-Control"] = "no-cache";
            hwrq2.Expect = null;

            if (cookieHeader.Length > 0)
            {
                hwrq2 = addAllCookies(hwrq2, cookieHeader);
            }
            postData = buildRLoginInfoPostData("");
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            postStream = hwrq2.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();
            wrs2 = (HttpWebResponse)hwrq2.GetResponse();
            cookieHeader = getCookieHeader(wrs2);
          
            */

            accumulatedCookies = accumulateCookies(accumulatedCookies, accessAttemptCookie);
            accumulatedCookies = accumulateCookies(accumulatedCookies, rlLoginInfoCookie);

            /* Login to Retail Link  */
            // Retail Link Login url
            HttpWebRequest hwrq3 = (HttpWebRequest)WebRequest.Create("https://rllogin.wal-mart.com/rl_security/rl_logon.aspx?ServerType=IIS1&CTAuthMode=BASIC&CTLoginErrorMsg=BAD_PWD_OR_USER&language=en&CTUser=&CT_ORIG_URL=%2Frl_security%2Frl_logon.aspx%3FServerType%3DIIS1%26redir%3D%2F&ct_orig_uri=%2Frl_security%2Frl_logon.aspx%3FServerType%3DIIS1%26redir%3D%2F");                                                        
            HttpWebResponse wrs3;
            hwrq3.Method = "POST";
            hwrq3.UserAgent = userAgent;
            hwrq3.Referer = "https://rllogin.wal-mart.com/rl_security/rl_logon.aspx?ServerType=IIS1&CTAuthMode=BASIC&CTLoginErrorMsg=BAD_PWD_OR_USER&language=en&CTUser=&CT_ORIG_URL=%2Frl_security%2Frl_logon.aspx%3FServerType%3DIIS1%26redir%3D%2F&ct_orig_uri=%2Frl_security%2Frl_logon.aspx%3FServerType%3DIIS1%26redir%3D%2F";
            hwrq3.ContentType = "application/x-www-form-urlencoded";
            hwrq3.Headers["Cache-Control"] = "no-cache";
            hwrq3.CookieContainer = accumulatedCookies;
            postData = buildRetailLinkLoginPostData(rlUser, rlPassword);
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            hwrq3.ContentLength = postDataBytes.Length;
            postStream = hwrq3.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();
            wrs3 = (HttpWebResponse)hwrq3.GetResponse();

            wrs3.Close();
            
            
            /* Execute Retail Link Query */
            // Retail Link query submit url
            /*
            HttpWebRequest hwrq4 = (HttpWebRequest)WebRequest.Create("https://retaillink.wal-mart.com/mydss/Submit_Request.aspx?submitnow=false&applicationid=300");
            HttpWebResponse wrs4;
        
            hwrq4.Method = "POST";
            hwrq4.UserAgent = userAgent;
            hwrq4.Referer = "https://retaillink.wal-mart.com/mydss/Submit_Request.aspx?submitnow=false&applicationid=300";
            hwrq4.ContentType = "application/x-www-form-urlencoded";
            hwrq4.Headers["Cache-Control"] = "no-cache";
            hwrq4.CookieContainer = accumulatedCookies;

            postData = buildRetailLinkQueryPostData();
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            hwrq4.ContentLength = postDataBytes.Length;
            postStream = hwrq4.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();
            wrs4 = (HttpWebResponse)hwrq4.GetResponse();
            Stream strm = wrs4.GetResponseStream();
            StreamReader sr = new StreamReader(strm);
            StringBuilder sb = new StringBuilder();
            sb.Append(sr.ReadToEnd());
            string result = sb.ToString();
            Console.WriteLine(result);
            Console.WriteLine("JobID: " + jobId);

            // Check if submit was successful. This is possible under any account
            if (Regex.Match(result, @"Query Submitted").Success)
            {
                System.Text.RegularExpressions.Match m = Regex.Match(result, @"Job Id&nbsp;=&nbsp;(\d*)</center>");
                jobId = m.Groups[1].Value;
            }

            */

            /* Init Coman App on server */
            // Coman initialize URL
            HttpWebRequest comanInitWrq = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/?ukey=W2977");
            HttpWebResponse comanInitWrs;
            comanInitWrq.Method = "GET";
            comanInitWrq.UserAgent = userAgent;
            comanInitWrq.Referer = "https://retaillink.wal-mart.com/myfavorites/My_Favorites.aspx";
            comanInitWrq.CookieContainer = accumulatedCookies;
            comanInitWrq.Headers["Accept-Language"] = "en-us";
            comanInitWrq.Headers["Accept-Encoding"] = "gzip, deflate";
            comanInitWrs = (HttpWebResponse)comanInitWrq.GetResponse();
            strm = comanInitWrs.GetResponseStream();
            sr = new StreamReader(strm);
            sb = new StringBuilder(sr.ReadToEnd());
            result = sb.ToString();


            /* Login to Coman  */
            // Coman login url
            HttpWebRequest hwrq5 = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/entry");
            HttpWebResponse wrs5;
            hwrq5.Method = "POST";
            hwrq5.UserAgent = userAgent;
            hwrq5.Referer = "https://coman.wal-mart.com/WMHATS/?ukey=W2977";
            hwrq5.ContentType = "application/x-www-form-urlencoded";
            hwrq5.Headers["Cache-Control"] = "no-cache";
            hwrq5.CookieContainer = accumulatedCookies;

            postData = buildComanLoginPostData(cmUser, cmPassword);
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            hwrq5.ContentLength = postDataBytes.Length;
            postStream = hwrq5.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();
            wrs5 = (HttpWebResponse)hwrq5.GetResponse();
            strm = wrs5.GetResponseStream();
            sr = new StreamReader(strm);
            sb = new StringBuilder();
            sb.Append(sr.ReadToEnd());

            wrs5.Close();

            result = sb.ToString();
            System.Text.RegularExpressions.Match m = Regex.Match(result, "<INPUT TYPE=\"HIDDEN\" NAME=\"SESSIONID\" VALUE=\"(.*)\" />");
            comanSID = m.Groups[1].Value;
            //Console.WriteLine(result);
            Console.WriteLine("-->Logon to Coman complete.");
            Console.WriteLine("     Coman session ID: " + comanSID);

            /* Send CI post to Coman  -- definitely need to code around only doing this if necessary */
            // Check if CI needs to be posted due to old sessions being opened
            if (Regex.Match(result, @"SOLVE:Access&nbsp;:&nbsp;Reconnection&nbsp;-----------------------").Success)
            {
           
                HttpWebRequest comanCIWrq = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/entry");
                HttpWebResponse comanCIWrs;
                comanCIWrq.Method = "POST";
                comanCIWrq.UserAgent = userAgent;
                comanCIWrq.Referer = "https://coman.wal-mart.com/WMHATS/entry";
                comanCIWrq.ContentType = "application/x-www-form-urlencoded";
                comanCIWrq.Headers["Cache-Control"] = "no-cache";
                comanCIWrq.CookieContainer = accumulatedCookies;

                postData = buildComanContinueIgnorePostData(comanSID);
                postDataBytes = Encoding.UTF8.GetBytes(postData);
                comanCIWrq.ContentLength = postDataBytes.Length;
                postStream = comanCIWrq.GetRequestStream();
                postStream.Write(postDataBytes, 0, postDataBytes.Length);
                postStream.Close();

                comanCIWrs = (HttpWebResponse)comanCIWrq.GetResponse();
                strm = comanCIWrs.GetResponseStream();
                sr = new StreamReader(strm);
                sb = new StringBuilder();
                sb.Append(sr.ReadToEnd());

                comanCIWrs.Close();

                result = sb.ToString();
                //Console.WriteLine(result);
                Console.WriteLine("-->Other sessions ignored.");
            }

            /* Send duplicate CI post to Coman, because it always returns a "reconnection solve" page */
            // Have to create a completely new WebRequest for the exact same request, unfortunately.
            /*
            comanCIWrq = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/entry");
            comanCIWrq.Method = "POST";
            comanCIWrq.UserAgent = userAgent;
            comanCIWrq.Referer = "https://coman.wal-mart.com/WMHATS/entry";
            comanCIWrq.ContentType = "application/x-www-form-urlencoded";
            comanCIWrq.Headers["Cache-Control"] = "no-cache";
            comanCIWrq.CookieContainer = accumulatedCookies;

            postData = buildComanContinueIgnorePostData(comanSID);
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            comanCIWrq.ContentLength = postDataBytes.Length;
            postStream = comanCIWrq.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();

            comanCIWrs = (HttpWebResponse)comanCIWrq.GetResponse();
            strm = comanCIWrs.GetResponseStream();
            sr = new StreamReader(strm);
            sb = new StringBuilder();
            sb.Append(sr.ReadToEnd());
            result = sb.ToString();
            Console.WriteLine(result);
            */



            /* Send S post to Coman */
            Thread.Sleep(1000);
            //&nbsp;MAI&nbsp;:&nbsp;Primary&nbsp;Menu&nbsp;
            if (!Regex.Match(result, @"&nbsp;MAI&nbsp;:&nbsp;Primary&nbsp;Menu&nbsp;").Success)
            {
                throw new Exception("Not at correct primary menu screen");
            }
            HttpWebRequest comanSWrq = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/entry");
            HttpWebResponse comanSWrs;
            comanSWrq.Method = "POST";
            comanSWrq.UserAgent = userAgent;
            comanSWrq.Referer = "https://coman.wal-mart.com/WMHATS/entry";
            comanSWrq.ContentType = "application/x-www-form-urlencoded";
            comanSWrq.Headers["Cache-Control"] = "no-cache";
            comanSWrq.CookieContainer = accumulatedCookies;

            postData = buildComanSelectPostData(comanSID);
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            comanSWrq.ContentLength = postDataBytes.Length;
            postStream = comanSWrq.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();

            comanSWrs = (HttpWebResponse)comanSWrq.GetResponse();
            strm = comanSWrs.GetResponseStream();
            sr = new StreamReader(strm);
            sb = new StringBuilder();
            sb.Append(sr.ReadToEnd());

            comanSWrs.Close();

            result = sb.ToString();
            //Console.WriteLine(result);
            Console.WriteLine("-->Selected CORP1");


            /* Send Clear (escape) post */
            Thread.Sleep(1000);
            if (!Regex.Match(result, @"DISTRIBUTION&nbsp;INVENTORY&nbsp;CONTROL&nbsp;SYSTEM").Success)
            {
                throw new Exception("Not at correct DICS Screen");
            }
            HttpWebRequest comanLSWrq = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/entry");
            HttpWebResponse comanLSWrs;
            comanLSWrq.Method = "POST";
            comanLSWrq.UserAgent = userAgent;
            comanLSWrq.Referer = "https://coman.wal-mart.com/WMHATS/entry";
            comanLSWrq.ContentType = "application/x-www-form-urlencoded";
            comanLSWrq.Headers["Cache-Control"] = "no-cache";
            comanLSWrq.CookieContainer = accumulatedCookies;

            postData = buildLineSizePostData(comanSID);
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            comanLSWrq.ContentLength = postDataBytes.Length;
            postStream = comanLSWrq.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();

            comanLSWrs = (HttpWebResponse)comanLSWrq.GetResponse();
            strm = comanLSWrs.GetResponseStream();
            sr = new StreamReader(strm);
            sb = new StringBuilder();
            sb.Append(sr.ReadToEnd());

            comanLSWrs.Close();

            result = sb.ToString();
            //Console.WriteLine(result);
            Console.WriteLine("-->Logged off");



            /* Send DDIR post to Coman */
            //&nbsp;&nbsp;LOG&nbsp;&nbsp;OFF&nbsp;&nbsp;COMPLETE
            if (!Regex.Match(result, @"&nbsp;&nbsp;LOG&nbsp;&nbsp;OFF&nbsp;&nbsp;COMPLETE").Success)
            {
                throw new Exception("Not at correct Log off Screen");
            }
            Thread.Sleep(1000);
            HttpWebRequest comanDDIRWrq = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/entry");
            HttpWebResponse comanDDIRWrs;
            comanDDIRWrq.Method = "POST";
            comanDDIRWrq.UserAgent = userAgent;
            comanDDIRWrq.Referer = "https://coman.wal-mart.com/WMHATS/entry";
            comanDDIRWrq.ContentType = "application/x-www-form-urlencoded";
            comanDDIRWrq.Headers["Cache-Control"] = "no-cache";
            comanDDIRWrq.CookieContainer = accumulatedCookies;
            comanDDIRWrq.KeepAlive = true;

            postData = buildComanDDIRPostData(comanSID);
            postDataBytes = Encoding.UTF8.GetBytes(postData);
            comanDDIRWrq.ContentLength = postDataBytes.Length;
            postStream = comanDDIRWrq.GetRequestStream();
            postStream.Write(postDataBytes, 0, postDataBytes.Length);
            postStream.Close();

            comanDDIRWrs = (HttpWebResponse)comanDDIRWrq.GetResponse();
            strm = comanDDIRWrs.GetResponseStream();
            sr = new StreamReader(strm);
            sb = new StringBuilder();
            sb.Append(sr.ReadToEnd());

            comanDDIRWrs.Close();

            result = sb.ToString();
            //Console.WriteLine(result);
            Console.WriteLine("-->Entered DDIR");


            Console.WriteLine("Enter 'y' or 'n' (without quotes) to continue running queries against DDIR");
            string response = Console.ReadLine();
            if (response == "y")
            {
                /* PULL LIST OF ITEM WAREHOUSE COMBOS FROM TRA MART */
                //DAILY&nbsp;DEMAND&nbsp;AND<td class="HBLANK">&nbsp;<td class="HLYELLOW" colspan="20">INVENTORY&nbsp;RECORD&nbsp;&nbsp;&nbsp;&nbsp;
                if (!Regex.Match(result, "DAILY&nbsp;DEMAND&nbsp;AND<td class=\"HBLANK\">&nbsp;<td class=\"HLYELLOW\" colspan=\"20\">INVENTORY&nbsp;RECORD").Success)
                {
                    throw new Exception("Not at correct Log off Screen");
                }
                SqlConnection sqlConnection = new SqlConnection("Data Source=WHQWTRA01;Initial Catalog=TRA_MART_MSS;Integrated Security=SSPI;");
                SqlCommand cmd = new SqlCommand();
                SqlDataReader reader;

                cmd.CommandText = @"select top 10
	                                     warehouse_no,
	                                     item_no
                                    from fact_warehouse_invn fwi
                                    inner join dim_warehouse dw
	                                    on fwi.warehouse_key = dw.warehouse_key
                                    inner join dim_item di
	                                    on fwi.item_key = di.item_key
                                    inner join dim_date dd
	                                    on fwi.date_key = dd.date_key
                                    where 
                                    dd.fuzzy_date_num = 0
                                    and TRAITED_AND_VALID_STORE_CNT > 0";
                cmd.CommandType = CommandType.Text;
                cmd.Connection = sqlConnection;

                sqlConnection.Open();

                reader = cmd.ExecuteReader();
                string itemNo;
                string warehouseNo;

                DateTime startTime = DateTime.Now;
                /* Query DDIR for each item warehouse combo */
                while (reader.Read())
                {
                    warehouseNo = reader[0].ToString();
                    itemNo = reader[1].ToString();

                    // Loop through a lot of these
                    /* Coman query submit url */
                    HttpWebRequest comanQueryWrq = (HttpWebRequest)WebRequest.Create("https://coman.wal-mart.com/WMHATS/entry");
                    HttpWebResponse comanQueryWrs;
                    comanQueryWrq.Method = "POST";
                    comanQueryWrq.UserAgent = userAgent;
                    comanQueryWrq.Referer = "https://coman.wal-mart.com/WMHATS/entry";
                    comanQueryWrq.ContentType = "application/x-www-form-urlencoded";
                    comanQueryWrq.Headers["Cache-Control"] = "no-cache";
                    comanQueryWrq.CookieContainer = accumulatedCookies;

                    //postData = buildComanQueryPostData(comanSID, comanWarehouseID, comanItemID);
                    postData = buildComanQueryPostData(comanSID, warehouseNo, itemNo);
                    postDataBytes = Encoding.UTF8.GetBytes(postData);
                    comanQueryWrq.ContentLength = postDataBytes.Length;
                    postStream = comanQueryWrq.GetRequestStream();
                    postStream.Write(postDataBytes, 0, postDataBytes.Length);
                    postStream.Close();

                    comanQueryWrs = (HttpWebResponse)comanQueryWrq.GetResponse();
                    strm = comanQueryWrs.GetResponseStream();
                    sr = new StreamReader(strm);
                    sb = new StringBuilder();
                    sb.Append(sr.ReadToEnd());

                    // Close method closes the response stream and releases the connection to the resource for reuse by other requests.
                    comanQueryWrs.Close();
                    result = sb.ToString();

                    DDIRHtmlFile = new System.IO.StreamWriter("C:\\Users\\MILLERBARR\\Documents\\TRA\\Retail Link Automation\\DDIR\\Files\\" + warehouseNo + "_" + itemNo + "_" + System.DateTime.Today.Year + System.DateTime.Today.Month + System.DateTime.Today.Day + "_" + System.DateTime.Now.Hour + "." + System.DateTime.Now.Minute + "." + System.DateTime.Now.Second + ".html");
                    DDIRHtmlFile.Write(result);
                    DDIRHtmlFile.Close();
                    //Console.WriteLine("-->Query executed for " + warehouseNo + ", " + itemNo);
                    if (Regex.Match(result, @"PGM HI210 ABEND   19 0000  FILE ERROR OPENING THE Z\?PODTX1 FI").Success)
                    {
                        throw new Exception("Error returning file");
                    }


                    Thread.Sleep(5);
                }
                DateTime stopTime = DateTime.Now;
                TimeSpan duration = stopTime - startTime;
                Console.WriteLine("Process took : " + duration);
                reader.Close();
            }
            Console.ReadLine();
        }


        /* 
         * Method to add a cookie to the webrequest using the cookieContainer
         * Returns the webrequest
         */
        private static HttpWebRequest addAllCookies(HttpWebRequest hwrq, string cookieHeader)
        {
            return addCookie(hwrq, extractCookieName(cookieHeader), extractCookieValue(cookieHeader), extractCookiePath(cookieHeader), extractCookieDomain(cookieHeader));
        }


        /*
         * Method to extract cookie domain
         */
        private static string extractCookieDomain(string cookie)
        {
            string[] cookieParts = Regex.Split(cookie, @"; ");
            string domain = "";

            foreach (string part in cookieParts)
            {
                string[] cookieNameVal = part.Split('=');
                if (cookieNameVal[0] == "domain")
                {
                    domain = cookieNameVal[1];
                }
            }

            return domain;
        }

        /*
         * Method to extract cookie path
         */
        private static string extractCookiePath(string cookie)
        {
            string[] cookieParts = Regex.Split(cookie, @"; ");
            string path = "";

            foreach (string part in cookieParts)
            {
                string[] cookieNameVal = part.Split('=');
                if (cookieNameVal[0] == "path")
                {
                    path = cookieNameVal[1];
                }
            }

            return path;
        }

        /*
         * Method to extract cookie name
         */
        private static string extractCookieName(string cookie)
        {
            string[] cookieParts = Regex.Split(cookie, @"; ");
            string name = "";

            string[] cookieNameVal = cookieParts[0].Split('=');
            name = cookieNameVal[0];

            return name;
        }

        /*
         * Method to extract cookie path
         */
        private static string extractCookieValue(string cookie)
        {
            string[] cookieParts = Regex.Split(cookie, @"; ");
            string value = "";

            string[] cookieNameVal = cookieParts[0].Split('=');
            value = cookieNameVal[1];

            return value;
        }

        /* 
         * Method to add a single cookie to a web request and return that request
         */
        private static HttpWebRequest addCookie(HttpWebRequest hwrq, string cookieName, string cookieValue, string cookiePath, string cookieDomain)
        {
            if (hwrq.CookieContainer == null)
            {
                hwrq.CookieContainer = new CookieContainer();
            }

            hwrq.CookieContainer.Add(new Cookie(cookieName, cookieValue, cookiePath, cookieDomain));
            return hwrq;
        }


        /*
         * Method to accumulate all cookies across all requests into a single CookieContainer
         */
        private static CookieContainer accumulateCookies(CookieContainer cc, string cookieHeader)
        {
            string cookieName = extractCookieName(cookieHeader);
            string cookieValue = extractCookieValue(cookieHeader);
            string cookiePath = extractCookiePath(cookieHeader);
            string cookieDomain = extractCookieDomain(cookieHeader);
            if (cc == null)
            {
                cc = new CookieContainer();
            }
            cc.Add(new Cookie(cookieName, cookieValue, cookiePath, cookieDomain));
            return cc;
        }

        /* 
         * Method to add a cookie to a web request in the easiest way. 
         * Avoids setting up cookie container and cookie object and parsing
         */
        private static HttpWebRequest attachFullCookie(HttpWebRequest hwrq, string cookie)
        {
            hwrq.Headers.Add("Cookie", cookie);
            return hwrq;

            //hwrq = (HttpWebRequest)WebRequest.Create("http://techbus.safaribooksonline.com/mybookmarks");
            //hwrq.CookieContainer = new CookieContainer();
            //hwrq.CookieContainer.Add(wrs.Cookies);
            //wrs = (HttpWebResponse)hwrq.GetResponse();
        }

        /*
         * Method to extract a cookie from a web response object and return it as a string
         */
        private static string getCookieHeader(HttpWebResponse wrs)
        {
            // Pull out the cookie info string from the header
            WebHeaderCollection headers = wrs.Headers;
            string cookieHeader = "";
            if (headers["Set-Cookie"] != null)
            {
                cookieHeader = headers["Set-Cookie"];
            }

            return cookieHeader;
        }

        /* 
         * Method to build the viewstate string for posting and authentication
         */
        private static string buildRetailLinkLoginPostData(string user, string pass)
        {
            return "__EVENTTARGET=&__EVENTARGUMENT=&__VIEWSTATE=%2FwEPDwULLTE0OTY5MTU2ODkPZBYCZg9kFgICCQ9kFgJmD2QWAmYPZBYCAgEPZBYEAgEPZBYCZg9kFgJmD2QWAmYPZBYCZg9kFgJmD2QWAgICD2QWAmYPZBYCAgEPDxYEHgRUZXh0BUtFaXRoZXIgeW91ciB1c2VyIElEIG9yIHlvdXIgcGFzc3dvcmQgaXMgaW5jb3JyZWN0Ljxici8%2BUGxlYXNlIGxvZyBpbiBhZ2Fpbi4eB1Zpc2libGVnZGQCAg9kFgJmD2QWAgIBDw9kFgQeC29ubW91c2VvdmVyBRt0aGlzLmNsYXNzTmFtZT0nYnRuIGJ0bmhvdiceCm9ubW91c2VvdXQFFHRoaXMuY2xhc3NOYW1lPSdidG4nZGSwCX2uZpUCKecXONG5X3S%2BsRv4IA%3D%3D&__EVENTVALIDATION=%2FwEWBwLrua2MCgK145qeCgK7iKX4CAKgmu7gDwLB2tiHDgLKw6LdBQLvz%2FGACveMjMjlEu%2B0qlNQtjzjFHFJj%2F7u&hidFailedLoginCount=&redirect=%2Frl_security%2Frl_logon.aspx&hidPwdErrHandledFlag=FALSE&&txtUser=" + user + "&txtPass=" + pass + "&Login=Logon";
                //"__VIEWSTATE=dDwxMTU2NDQxNTM2O3Q8O2w8aTwwPjtpPDI%2BOz47bDx0PDtsPGk8Nz47PjtsPHQ8O2w8aTwwPjs%2BO2w8dDw7bDxpPDA%2BOz47bDx0PDtsPGk8MT47PjtsPHQ8O2w8aTwwPjtpPDE%2BO2k8Mj47PjtsPHQ8O2w8aTwwPjs%2BO2w8dDw7bDxpPDE%2BOz47bDx0PHA8cDxsPFRleHQ7PjtsPExvZ2luIFNjcmVlbjs%2BPjs%2BOzs%2BOz4%2BOz4%2BO3Q8O2w8aTwwPjs%2BO2w8dDw7bDxpPDA%2BOz47bDx0PDtsPGk8MD47PjtsPHQ8O2w8aTwwPjs%2BO2w8dDw7bDxpPDA%2BOz47bDx0PDtsPGk8MD47aTwxPjtpPDM%2BO2k8ND47aTw1PjtpPDY%2BOz47bDx0PDtsPGk8MD47PjtsPHQ8O2w8aTwwPjs%2BO2w8dDxwPHA8bDxUZXh0Oz47bDxFbnRlciB5b3VyIGxvZ2luIGluZm9ybWF0aW9uIGJlbG93Ljs%2BPjs%2BOzs%2BOz4%2BOz4%2BO3Q8O2w8aTwwPjs%2BO2w8dDw7bDxpPDA%2BOz47bDx0PDtsPGk8MD47aTwyPjs%2BO2w8dDxwPHA8bDxUZXh0Oz47bDxBdHRlbnRpb24gQXNzb2NpYXRlczo7Pj47Pjs7Pjt0PHA8cDxsPFRleHQ7PjtsPFlvdXIgUmV0YWlsIExpbmsgdXNlcmlkIGFuZCBwYXNzd29yZCBpcyB0aGUgc2FtZSBhcyB5b3VyIHdvcmtzdGF0aW9uIHVzZXJpZCBhbmQgcGFzc3dvcmQuOz4%2BOz47Oz47Pj47Pj47Pj47dDw7bDxpPDA%2BOz47bDx0PDtsPGk8MD47PjtsPHQ8cDxwPGw8VGV4dDs%2BO2w8VXNlciBJRDs%2BPjs%2BOzs%2BOz4%2BOz4%2BO3Q8O2w8aTwwPjs%2BO2w8dDw7bDxpPDA%2BOz47bDx0PHA8cDxsPFRleHQ7PjtsPFBhc3N3b3JkOz4%2BOz47Oz47Pj47Pj47dDw7bDxpPDE%2BOz47bDx0PDtsPGk8MD47PjtsPHQ8O2w8aTwxPjs%2BO2w8dDxwPHA8bDxUZXh0Oz47bDxGb3Jnb3QgeW91ciBQYXNzd29yZD87Pj47Pjs7Pjs%2BPjs%2BPjs%2BPjt0PDtsPGk8MD47PjtsPHQ8O2w8aTwwPjs%2BO2w8dDxwPHA8bDxUZXh0Oz47bDxcPGJcPk5vdGU6XDwvYlw%2BIGZvciBiZXN0IHJlc3VsdHMsIHNldCB5b3VyIG1vbml0b3IgcmVzb2x1dGlvbiB0byBhdCBsZWFzdCAxMDI0eDc2OCBhbmQgdXNlIEludGVybmV0IEV4cGxvcmVyLjs%2BPjs%2BOzs%2BOz4%2BOz4%2BOz4%2BOz4%2BOz4%2BOz4%2BOz4%2BOz4%2BO3Q8O2w8aTwwPjs%2BO2w8dDw7bDxpPDE%2BOz47bDx0PHA8cDxsPFRleHQ7PjtsPExvZ29uOz4%2BO3A8bDxvbm1vdXNlb3Zlcjtvbm1vdXNlb3V0Oz47bDx0aGlzLmNsYXNzTmFtZT0nYnRuIGJ0bmhvdic7dGhpcy5jbGFzc05hbWU9J2J0bic7Pj4%2BOzs%2BOz4%2BOz4%2BOz4%2BOz4%2BOz4%2BOz4%2BOz4%2BO3Q8O2w8aTwwPjs%2BO2w8dDxwPHA8bDxUZXh0Oz47bDxMb2FkaW5nLCBQbGVhc2UgV2FpdC4uLjs%2BPjs%2BOzs%2BOz4%2BOz4%2BOz5jXuFBqKMwFWRHstnf7ssCvwRj2A%3D%3D&hidFailedLoginCount=&redirect=%2Fhome%2F&hidPwdErrHandledFlag=FALSE&txtUser=" + user + "&txtPass=" + pass + "&Login=Logon";
        }

        /* 
         * Method to build the viewstate string for posting and authentication
         */
        private static string buildRLoginInfoPostData(string userInfo)
        {
            return "compName=\"NotAvail\"\r\nloginFailed=\"True\"";
            //userInfo=\"2E19540F1B2C36422E2D3127023A302017373A3215202B276F280A093B0909127D514B43724D061C3F1504073B070916694528201B20454B7C555E53050C0B173D1216531C3145467C545E5306170C17370B115C664B5548724B2B360645263F0045545D634B514060575E537C2B20277226292172574B437C50554460525E537C2B20277226292172564B437C55514662534B40625E455D1C20315311293753614B555D62515043644B53476A5E455D1C20315311293753614B555D665055457C575446605E455D1C20315311293753614B505D615552416B5E453A3C030A2333110D5D605E453E014837271145293E725D5E537C2B2027664B553069454B3D1731515D62204C0F\"\r\n
        }

        private static string buildRetailLinkQueryPostData()
        {
            return "__VIEWSTATE=%2FwEPDwUJMjA3OTIwMzMwD2QWAgIDD2QWAgIRDxYCHgRUZXh0BcEVPHRhYmxlIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIGJvcmRlcj0iMCIgY2VsbHNwYWNpbmc9IjAiIGNlbGxwYWRkaW5nPSIwIiBhbGlnbj0ibGVmdCI%2BPHRyPjx0ZCB2YWxpZ249Im1pZGRsZSIgY2xhc3M9InN1Ym1pdGVkVGV4dCIgYWxpZ249ImNlbnRlciI%2BIDwvdGQ%2BPC90cj48dHIgaGVpZ2h0PSIxMDAlIj48dGQgYWxpZ249ImxlZnQiIHZhbGlnbj0idG9wIj48dGFibGUgYWxpZ249ImxlZnQiIGJvcmRlcj0iMCIgIGNlbGxzcGFjaW5nPSIwIiBjZWxscGFkZGluZz0iMCIgPiA8dHI%2BPHRkIGFsaWduPSdyaWdodCcgbm93cmFwIHZhbGlnbj0ndG9wJyBjbGFzcz0gInN1Ym1pdGVkSGVhZGVyVGl0bGVzIj5SZXBvcnQgT3B0aW9uczombmJzcDs8L3RkPg0KPHRkIHZhbGlnbj0ndG9wJyBjbGFzcz0nc3VibWl0ZWRUZXh0Jz46CUl0ZW0gTmJyLEl0ZW0gRGVzYyAxLEl0ZW0gRGVzYyAyLFByaW1lIEl0ZW0gTmJyLFByaW1lIEl0ZW0gRGVzYyxQcmltZSBTaXplIERlc2MsQ29sb3IgRGVzYyxTaXplIERlc2MsSXRlbSBTdGF0dXMsSXRlbSBUeXBlLEl0ZW0gU3ViIFR5cGUsVVBDLFVQQyBEZXNjLENyZWF0ZSBEYXRlLEVmZmVjdGl2ZSBEYXRlLE9ic29sZXRlIERhdGUsVU9NIFNlbGwgUXR5LFVPTSBDb2RlLE1mZyBOYnIsSW50bCBDb2RlLFByb2R1Y3QgQ29kZSxQcm9kdWN0IERlc2NyaXB0aW9uLFNoZWxmIE51bWJlcixTaGVsZiBEZXNjcmlwdGlvbixQYWNrIFR5cGUsT3JkZXIgYm9vayBGbGFnLENvcnAgQ2FuY2VsIFdoZW4gT3V0IEZsYWcsUExVIE5icixSZXR1cm4gU2hlZXQgTmJyLE9yZGJrIFN0YXJzIFF0eSxXaHNlIEFsaWduLExhc3QgQ2hhbmdlIERhdGUsTGFzdCBDaGFuZ2UgVGltZSxFeHBpcmF0aW9uIERhdGUsU3RhdHVzIENoZyBEYXRlLFVuaXQgU2l6ZSxVbml0IFNpemUgVU9NLEFjY3QgRGVwdCBOYnIsT3JkZXIgRGVwdCBOYnIsRGVwdCBEZXNjLFN1YmNsYXNzLEdlbmRlciBOdW1iZXIsVmVuZG9yIE5hbWUsVmVuZG9yIE5icixWZW5kb3IgU3RrIE5icixWZW5kb3IgTmJyIERlcHQsVmVuZG9yIFNlcXVlbmNlIE5icixWTlBLIFF0eSxWTlBLIENvc3QsVk5QSyBDdWJpYyBGdCxWZW5kb3IgUGFjayBXZWlnaHQsRnV0dXJlIFZOUEsgQ29zdCxGdXR1cmUgRWZmZWN0aXZlIERhdGUsR1NJRCBOYnIsR1NJRCBEZXNjLFZOUEsgVVBDLFZOUEsgSGVpZ2h0LFZOUEsgTGVuZ3RoLFZOUEsgV2lkdGgsUGFsbGV0IFVQQyxDdWJpYyBPcmRlciBTaXppbmcgRmFjdG9yLFdIUEsgUXR5LFdIUEsgQ29zdCxXSFBLIFNlbGwsV0hQSyBDdWJpYyBGdCxXaHNlIFBhY2sgV2VpZ2h0LFdIUEsgVVBDLFdIUEsgSGVpZ2h0LFdIUEsgTGVuZ3RoLFdIUEsgV2lkdGgsQnV5ZXIgVXNlciBJRCxCdXllciBGdWxsIE5hbWUsQ2F0ZWdvcnkgVGVhbSBMZWFkIFVzZXIgSUQsQ2F0ZWdvcnkgVGVhbSBMZWFkIEZ1bGwgTmFtZSxETU0gVXNlciBJRCxETU0gRnVsbCBOYW1lLEdNTSBVc2VyIElELEdNTSBGdWxsIE5hbWUsRVZQIFVzZXIgSUQsRVZQIEZ1bGwgTmFtZSxQbGFubmVyIFVzZXIgSUQsUGxhbm5lciBGdWxsIE5hbWUsUExBTk5JTkcgTUdSIFVzZXIgSUQsUExBTk5JTkcgTUdSIEZ1bGwgTmFtZSxQTEFOTklORyBESVIgVXNlciBJRCxQTEFOTklORyBESVIgRnVsbCBOYW1lLEZpbmVsaW5lIE51bWJlcixGaW5lbGluZSBEZXNjcmlwdGlvbixEZXB0IFN1YmNhdGVnb3J5IERlc2NyaXB0aW9uLERlcHQgQ2F0ZWdvcnkgRGVzY3JpcHRpb24sQ2F0ZWdvcnkgTmJyIElOVEwsTURTRSBTdWJncm91cCBEZXNjcmlwdGlvbixNRFNFIFNlZ21lbnQgRGVzY3JpcHRpb24sVm5kciBNaW4gT3JkIFF0eSxQYWxsZXQgVGkgUXR5LFBhbGxldCBIaSBRdHksVW5pdCBSZXRhaWwsVW5pdCBDb3N0LE1VICUsTWluIE9yZGVyIFF0eSxNYXggT3JkZXIgUXR5LFNpZ25pbmcgRGVzYyxJdGVtIEhlaWdodCxJdGVtIExlbmd0aCxJdGVtIFdpZHRoLEl0ZW0gV2VpZ2h0PC90ZD48L3RyPjx0cj48dGQgYWxpZ249J3JpZ2h0JyBub3dyYXAgdmFsaWduPSd0b3AnIGNsYXNzPSAic3VibWl0ZWRIZWFkZXJUaXRsZXMiPkl0ZW0gU2VsZWN0aW9uOiZuYnNwOzwvdGQ%2BDQo8dGQgdmFsaWduPSd0b3AnIGNsYXNzPSdzdWJtaXRlZFRleHQnPkFsbCBMaW5rcyBEZXRhaWw8QlI%2BJm5ic3A7Jm5ic3A7Jm5ic3A7Jm5ic3A7VmVuZG9yIE5iciAoNi1kaWdpdCkgSXMgT25lIE9mICAzMTgzNDAsIDM5ODQxMiBPcjxCUj48L3RkPjwvdHI%2BPHRyPjx0ZCBhbGlnbj0ncmlnaHQnIG5vd3JhcCB2YWxpZ249J3RvcCcgY2xhc3M9ICJzdWJtaXRlZEhlYWRlclRpdGxlcyI%2BQnVzaW5lc3MgVW5pdCBTZWxlY3Rpb246Jm5ic3A7PC90ZD4NCjx0ZCB2YWxpZ249J3RvcCcgY2xhc3M9J3N1Ym1pdGVkVGV4dCc%2BU3RvcmUgVHlwZSBCcmVha2Rvd24gLS0mZ3Q7IEFsbCBTdG9yZXM8QlI%2BPC90ZD48L3RyPjx0cj48dGQgYWxpZ249J3JpZ2h0JyBub3dyYXAgdmFsaWduPSd0b3AnIGNsYXNzPSAic3VibWl0ZWRIZWFkZXJUaXRsZXMiPlRpbWVzOiZuYnNwOzwvdGQ%2BDQo8dGQgdmFsaWduPSd0b3AnIGNsYXNzPSdzdWJtaXRlZFRleHQnPkJ5IEZ1enp5IERhdGVzIC0tJmd0OyBUaW1lIFJhbmdlIDEgQ3VycmVudCBXZWVrPEJSPjwvdGQ%2BPC90cj48dHI%2BPHRkIGFsaWduPSdyaWdodCcgbm93cmFwIHZhbGlnbj0ndG9wJyBjbGFzcz0gInN1Ym1pdGVkSGVhZGVyVGl0bGVzIj5PcHRpb25zOiZuYnNwOzwvdGQ%2BDQo8dGQgdmFsaWduPSd0b3AnIGNsYXNzPSdzdWJtaXRlZFRleHQnPjwvdGQ%2BPC90cj4gPC90YWJsZT48L3RkPjwvdHI%2BPC90YWJsZT5kZCJK8Ivz9MDRTik3Sz26QIkjqwJJ&__EVENTVALIDATION=%2FwEWEQK7%2B6Z8Ar6blscKAq%2FB1JkLAs%2FtrvgBAt%2Fd17wHAp%2Br39QBAuWZyZYMAreZ8eINAob8kdMMAsCT%2FdINAsvb%2F4wHAtyEhegNAu3apuECAoapwsYLAtqcwoALAvG3yIoFAt%2Fo4IUMfugw%2BVM9zEtOuR8ZqyhxU1iLFoY%3D&Description=RLDS_ItemDetail_StoreDetail_Thursday_003_000&Compressed=1&submitnow=true&flag=1&Format=4&QId=Q680&RequestId=19527873&ExeId=264&Criteria=4%09%3A%09E200001%09%3A%09F2000010%09%3A%09report%09%3A%09column%09%3A%09Report+Columns+%09%3A%09Item+Nbr%09%7E%091800%09%7C%09Item+Desc+1%09%7E%091801%09%7C%09Item+Desc+2%09%7E%091802%09%7C%09Prime+Item+Nbr%09%7E%091799%09%7C%09Prime+Item+Desc%09%7E%091799a%09%7C%09Prime+Size+Desc%09%7E%091831a%09%7C%09Color+Desc%09%7E%091830%09%7C%09Size+Desc%09%7E%091831%09%7C%09Item+Status%09%7E%091832%09%7C%09Item+Type%09%7E%091833%09%7C%09Item+Sub+Type%09%7E%091894%09%7C%09UPC%09%7E%091834%09%7C%09UPC+Desc%09%7E%091835%09%7C%09Create+Date%09%7E%091836%09%7C%09Effective+Date%09%7E%091837%09%7C%09Obsolete+Date%09%7E%091838%09%7C%09UOM+Sell+Qty%09%7E%091839%09%7C%09UOM+Code%09%7E%091840%09%7C%09Mfg+Nbr%09%7E%091843%09%7C%09Intl+Code%09%7E%091844%09%7C%09Product+Code%09%7E%091845%09%7C%09Product+Description%09%7E%091867%09%7C%09Shelf+Number%09%7E%091852%09%7C%09Shelf+Description%09%7E%091871%09%7C%09Pack+Type%09%7E%091219%09%7C%09Order+book+Flag%09%7E%092025%09%7C%09Corp+Cancel+When+Out+Flag%09%7E%091825%09%7C%09PLU+Nbr%09%7E%095700%09%7C%09Return+Sheet+Nbr%09%7E%095702%09%7C%09Ordbk+Stars+Qty%09%7E%095708%09%7C%09Whse+Align%09%7E%095712%09%7C%09Last+Change+Date%09%7E%095719%09%7C%09Last+Change+Time%09%7E%095720%09%7C%09Expiration+Date%09%7E%095726%09%7C%09Status+Chg+Date%09%7E%095727%09%7C%09Unit+Size%09%7E%095742%09%7C%09Unit+Size+UOM%09%7E%095743%09%7C%09Acct+Dept+Nbr%09%7E%091805%09%7C%09Order+Dept+Nbr%09%7E%091810%09%7C%09Dept+Desc%09%7E%091811%09%7C%09Subclass%09%7E%091815%09%7C%09Gender+Number%09%7E%091862%09%7C%09Vendor+Name%09%7E%092000%09%7C%09Vendor+Nbr%09%7E%092001%09%7C%09Vendor+Stk+Nbr%09%7E%092005%09%7C%09Vendor+Nbr+Dept%09%7E%092008%09%7C%09Vendor+Sequence+Nbr%09%7E%092009%09%7C%09VNPK+Qty%09%7E%092010%09%7C%09VNPK+Cost%09%7E%092011%09%7C%09VNPK+Cubic+Ft%09%7E%092013%09%7C%09Vendor+Pack+Weight%09%7E%095456%09%7C%09Future+VNPK+Cost%09%7E%092014%09%7C%09Future+Effective+Date%09%7E%092015%09%7C%09GSID+Nbr%09%7E%098994%09%7C%09GSID+Desc%09%7E%098995%09%7C%09VNPK+UPC%09%7E%099937%09%7C%09VNPK+Height%09%7E%099938%09%7C%09VNPK+Length%09%7E%099939%09%7C%09VNPK+Width%09%7E%099940%09%7C%09Pallet+UPC%09%7E%099945%09%7C%09Cubic+Order+Sizing+Factor%09%7E%099949%09%7C%09WHPK+Qty%09%7E%092020%09%7C%09WHPK+Cost%09%7E%092021%09%7C%09WHPK+Sell%09%7E%092022%09%7C%09WHPK+Cubic+Ft%09%7E%092023%09%7C%09Whse+Pack+Weight%09%7E%095457%09%7C%09WHPK+UPC%09%7E%099941%09%7C%09WHPK+Height%09%7E%099942%09%7C%09WHPK+Length%09%7E%099943%09%7C%09WHPK+Width%09%7E%099944%09%7C%09Buyer+User+ID%09%7E%09335%09%7C%09Buyer+Full+Name%09%7E%09336%09%7C%09Category+Team+Lead+User+ID%09%7E%09337%09%7C%09Category+Team+Lead+Full+Name%09%7E%09338%09%7C%09DMM+User+ID%09%7E%09339%09%7C%09DMM+Full+Name%09%7E%09340%09%7C%09GMM+User+ID%09%7E%09341%09%7C%09GMM+Full+Name%09%7E%09342%09%7C%09EVP+User+ID%09%7E%09343%09%7C%09EVP+Full+Name%09%7E%09344%09%7C%09Planner+User+ID%09%7E%09345%09%7C%09Planner+Full+Name%09%7E%09346%09%7C%09PLANNING+MGR+User+ID%09%7E%09347%09%7C%09PLANNING+MGR+Full+Name%09%7E%09348%09%7C%09PLANNING+DIR+User+ID%09%7E%09349%09%7C%09PLANNING+DIR+Full+Name%09%7E%09350%09%7C%09Fineline+Number%09%7E%09351%09%7C%09Fineline+Description%09%7E%09352%09%7C%09Dept+Subcategory+Description%09%7E%09353%09%7C%09Dept+Category+Description%09%7E%09354%09%7C%09Category+Nbr+INTL%09%7E%091822%09%7C%09MDSE+Subgroup+Description%09%7E%09356%09%7C%09MDSE+Segment+Description%09%7E%09357%09%7C%09Vndr+Min+Ord+Qty%09%7E%099910%09%7C%09Pallet+Ti+Qty%09%7E%099911%09%7C%09Pallet+Hi+Qty%09%7E%099912%09%7C%09Unit+Retail%09%7E%091803%09%7C%09Unit+Cost%09%7E%091804%09%7C%09MU+%25%09%7E%091806%09%7C%09Min+Order+Qty%09%7E%095713%09%7C%09Max+Order+Qty%09%7E%095714%09%7C%09Signing+Desc%09%7E%091866%09%7C%09Item+Height%09%7E%099933%09%7C%09Item+Length%09%7E%099934%09%7C%09Item+Width%09%7E%099935%09%7C%09Item+Weight%09%7E%099936%09%5E%092%09%3A%09E10101%09%3A%09F101013%09%3A%09what%09%3A%09wm_link_type%09%3A%09-1%09%7E%09Selections+Include%09%3A%09All+Links+Detail%09%7E%0942%09%7E%091%09%7E%091%09%7E%094%09%7E%090%09%5E%091%09%3A%09E10101%09%3A%09F101012%09%3A%09what%09%3A%09Filter%09%3A%09Vendor+Nbr+%286-digit%29+Is+One+Of++318340%2C+398412+Or%09%7E%09-OR-%09%7E%094%2C10%09%7E%09318340%09%7E%09398412%09%5E%093%09%3A%09E200037%09%3A%09F1000010%09%3A%09when%09%3A%09Fuzzy%09%3A%09-1%09%7E%09Selections+Include%09%3A%09By+Fuzzy+Dates%09%24%09Time+Range+1+Current+Week%09%7E%09K01w00%09%7E%091%09%7E%091%09%7E%094%09%7E%090%09%5E%092%09%3A%09E10102%09%3A%09F101020%09%3A%09where%09%3A%09wm_store_type%09%3A%09-1%09%7E%09Selections+Include%09%3A%09Store+Type+Breakdown%09%24%09All+Stores%09%7E%09DSS1%09%7E%091%09%7E%091%09%7E%094%09%7E%090&AppId=300&Environment=&callOnload=true&DivisionId=1&CountryCode=US&hdnAllowSubmit=True&hdnSubmitable=True";
        }

        private static string buildComanLoginPostData(string user, string password)
        {
            return "SESSIONNUMBER=&PERF_TIMESTAMP=0&COMMAND=macrorun_v5MAI&CURSORPOSITION=1317&KeyboardToggle=1&SESSIONID=INVALID&LINESIZE=2&in_1316_8=" + user + "&in_1339_8=" + password;
        }

        private static string buildComanQueryPostData(string sessionID, string warehouseID, string itemID)
        {            
            return "SESSIONNUMBER=&PERF_TIMESTAMP=0&COMMAND=%5Benter%5D&CURSORPOSITION=175&KeyboardToggle=1&SESSIONID=" + sessionID + "&LINESIZE=2&in_90_4=" + warehouseID + "&in_168_9=" + itemID;
        }

        /* 
         * After logged in to Coman, this is the first step to get 
         * the server side application in the correct state 
         */
        private static string buildComanContinueIgnorePostData(string sessionID)
        {
            // CI stands for "Continue Ignore"
            return "SESSIONNUMBER=&PERF_TIMESTAMP=0&COMMAND=%5Benter%5D&CURSORPOSITION=103&KeyboardToggle=1&SESSIONID=" + sessionID + "&LINESIZE=2&in_101_60=CI";
        }

        /*
         * This is the second step to get the COMAN application on the server into 
         * the correct state.
         */
        private static string buildComanSelectPostData(string sessionID)
        {
            // S stands for "Select"
            return "SESSIONNUMBER=&PERF_TIMESTAMP=0&COMMAND=%5Benter%5D&CURSORPOSITION=403&KeyboardToggle=1&SESSIONID=" + sessionID + "&LINESIZE=2&in_402_2=S";
        }

        /*
         * This is the third step to get COMAN into the DDIR state
         */
        private static string buildComanDDIRPostData(string sessionID)
        {
            // S stands for "Select"
            return "SESSIONNUMBER=&PERF_TIMESTAMP=0&COMMAND=%5Benter%5D&CURSORPOSITION=5&KeyboardToggle=1&SESSIONID=" + sessionID + "&LINESIZE=2&in_1_80=DDIR";
        }

        /* 
         * This is necessary after the S post for some reason. All it seems to do based on post data is set the LINESIZE
         */
        private static string buildLineSizePostData(string sessionID)
        {
            return "SESSIONNUMBER=&PERF_TIMESTAMP=0&COMMAND=%5Bclear%5D&CURSORPOSITION=1082&KeyboardToggle=1&SESSIONID=" + sessionID + "&LINESIZE=2";
        }


        /*
         * Method that will return a string with all cookie names and values on a separate line
         */
        private static string prettyPrintCookies(CookieContainer cc)
        {

            StringBuilder cookieList = new StringBuilder();
            foreach (Cookie c in cc.GetCookies(new Uri("http://www.wal-mart.com/")))
            {
                cookieList.AppendLine(c.Name + "=" + c.Value);
            }

            return cookieList.ToString();
        }


    }
}
