using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using System.Data.Common;

using System.Text;
using Newtonsoft.Json.Linq;
using System.Web.Http;
using System.Security.Cryptography;

using System.Threading;
using System.Data;
using System.Configuration;

namespace SnowflakeADF
{
    public static class SnowflakeADF
    {
      


        static string key = System.Environment.GetEnvironmentVariable("passcode", EnvironmentVariableTarget.Process);

        [FunctionName("SnowflakeADF")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {


            string MyRecCount = "";
            string SampleOut = "";
            string sf_type = "";
            string sf_host = "";
            string SampleJSON;
            SampleJSON = @"{
                            ""account"": """",
                            ""host"": """",
                            ""userid"": """",
                            ""pw"": """",
                            ""database"": """",
                            ""schema"": """",
                            ""role"": """",
                            ""warehouse"": """",
                            ""query"":""""
                            }";


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
    
            string SFconnectionString = "";
            string SQL_Command = "";


             

            string QID = "";
            var request = req;
            var query = request.Query;
          

            //Check for existing URL parameters. If a parameter is found,
            //do a status check for a prev query otherwise execute the query.

            foreach (var item in query)
            {
           
                QID = item.Value;
                QID = DecryptString( QID);
                sf_type = "CHECK";
            }
        
            if(sf_type != "CHECK")
            {
                sf_type = "RUN";
            }



            string rooturl = "";



            Task<object>  task;
            var CheckResult = new object();

    
            if (sf_type.ToUpper() == "RUN")
            {



                if (requestBody.Length == 0)
                {
                    MyRecCount = null;
                    goto skipthis;
                }



                dynamic data = JsonConvert.DeserializeObject(requestBody);

                string Account_UID = data?.userid;
                string Account_PW = data?.pw;
                string Account_ID = data?.account;
                string Account_role = data?.role;
                 sf_host = data?.host;
                 sf_type = data?.type;
                string Account_db = data?.database;
                string Account_Schema = data?.schema;
                string Account_WH = data?.warehouse;
                SQL_Command = data?.query;

                if (sf_host.Length == 0)
                {
                    sf_host = ".";
                }
                else
                {
                    sf_host = "." + sf_host + ".";
                }

                string Account_Host = Account_ID + sf_host + "snowflakecomputing.com";

                SFconnectionString = "account={0};user={1};password={2};host={3};db={4};warehouse={5};role={6};schema={7};";
                SFconnectionString = String.Format(SFconnectionString, Account_ID, Account_UID, Account_PW, Account_Host, Account_db, Account_WH, Account_role, Account_Schema);

                task = RunQuery(SFconnectionString, SQL_Command);
                MyRecCount = task.Result.ToString();

                string RespBody = @"
                    {
                        ""id"": ""{0}"",
                        ""statusQueryGetUri"": ""{1}"",
                        ""sendEventPostUri"": "" "",
                        ""terminatePostUri"": "" "",
                        ""rewindPostUri"": "" "",
                        ""purgeHistoryDeleteUri"": "" ""
                    }
                ";

                string URLParam = "QID=" + MyRecCount + "CS=" + SFconnectionString;
                URLParam = EncryptString(URLParam);


                URLParam = "CHECK=" + URLParam;

                
                rooturl = "https://" + Environment.GetEnvironmentVariable("WEBSITE_HOSTNAME") + "/api/SnowflakeADF?" + URLParam;

               RespBody = RespBody.Replace("{0}", MyRecCount);
               RespBody = RespBody.Replace("{1}", rooturl);
                SampleOut = RespBody;
              
            }


            if (sf_type.ToUpper() == "CHECK")
            {

                SFconnectionString = QID.Split("CS=")[1];
                QID = QID.Split("CS=")[0].Replace("QID=","");
                CheckResult = CheckQuery(SFconnectionString, QID);
               
                SampleOut = CheckResult.ToString();  //"{\"Result\": \"" + MyRecCount + "\"}";

                var objects = JObject.Parse(SampleOut); // parse as array
                string Qstatus = (string)objects["Rows"][0]["EXECUTION_STATUS"];
                string returnURL = "https://" + req.Host.Value.ToString() + req.Path.Value.ToString() + req.QueryString.Value.ToString();

                if(Qstatus == "RUNNING")
                {


                    string runningBody = @"{
                            ""name"": ""Orchestration"",
                            ""instanceId"": ""sdfdf"",
                            ""runtimeStatus"": ""Running"",
                            ""input"": 10,
                            ""customStatus"": null,
                            ""output"": null,
                            ""createdTime"": ""2020-02-29T11:09:23Z"",
                            ""lastUpdatedTime"": ""2020-02-29T11:11:23Z""
                        }
                ";



                    var JAccept = JObject.Parse(runningBody);
                     return (ActionResult)new AcceptedResult( new Uri(returnURL), JAccept);
                 
                        }

                
                if (Qstatus == "SUCCESS")
                {
                    return (ActionResult)new OkObjectResult(SampleOut);
                }
                else
                {
                    return (ActionResult)new InternalServerErrorResult();
                }


            }



        skipthis:

            return MyRecCount != null
            ? (ActionResult)new OkObjectResult(SampleOut)
            : new BadRequestObjectResult("Please post followiing data in the request body: \n" + SampleJSON);

        }

        public static async Task<object> RunQuery(string SFC, string TSQL)
        {
            string MyResult = "";
            SnowflakeDbConnection myConnection = new SnowflakeDbConnection();
            SnowflakeDbConnection myConnection2 = new SnowflakeDbConnection();

            DateTime baseDate = new DateTime(2020, 1, 1);
            TimeSpan diff = DateTime.Now - baseDate;
            

            string TAG = " /* ADF" + diff.TotalMilliseconds.ToString() + "*/;";


            if (TSQL.IndexOf(";") > 0)
            {
                TSQL = TSQL.Replace(";", TAG);
            }
            else
            {
                TSQL = TSQL + TAG;
            }



            myConnection.ConnectionString = SFC;
            myConnection2.ConnectionString = SFC;
            try
            {
                
     
                if (myConnection.IsOpen() == false)
                {

                    await myConnection.OpenAsync();
                }
                SnowflakeDbCommand myCommandmaster = new SnowflakeDbCommand(myConnection);
           
                myCommandmaster.CommandText = TSQL;
                SnowflakeDbDataAdapter MasterSQLDataAdapter;
                MasterSQLDataAdapter = new SnowflakeDbDataAdapter(myCommandmaster);

                int TryCount = 6;
                try
                {
                    

                    try
                    {

                        _ = myCommandmaster.ExecuteNonQueryAsync();
                   

                    }
                    catch (Exception ex)
                    {
                      string b = ex.Message.ToString();
                    }



                    await myConnection2.OpenAsync();
                    SnowflakeDbCommand myCommandmaster2 = new SnowflakeDbCommand(myConnection2);
                    myCommandmaster2.CommandText = "select QUERY_ID as QID,  EXECUTION_STATUS as STATUS, ERROR_MESSAGE from table(information_schema.query_history()) WHERE  QUERY_TEXT LIKE '%" +  TAG + "' ORDER BY START_TIME DESC LIMIT 1;";

                    StringBuilder SB = new StringBuilder();

                    Thread.Sleep(5000);
                    DbDataReader reader =  myCommandmaster2.ExecuteReader();

                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    int RecCount = dt.Rows.Count;

                   


                  


                    if (RecCount ==0  )
                    {
                        for (int i = 0; i < TryCount; i++)
                        {
                            Thread.Sleep(10000);
                            reader = myCommandmaster2.ExecuteReader();

                            dt.Load(reader);
                            RecCount = dt.Rows.Count;

                            
                            if (RecCount > 0)
                            {
                               
                                goto checkQID;  

                            }
                            else
                            {
                                if(i == TryCount-1)
                                {
                                    throw new System.InvalidOperationException("Can't Find the QueryID in the Query Log tagged:" + TAG);
                                }
                            }

                        }
                    }

                    checkQID:

                    reader = myCommandmaster2.ExecuteReader();
                    MyResult = WriteReaderToJSON(SB,reader);


                      JObject MyError1 = JObject.Parse(MyResult);

                
                    MyResult  = (string)MyError1["Rows"][0]["QID"];


                    reader.Close();

              //---- CLOSING CANCELS THE QUERY
              //      myConnection2.Close();
              //      myConnection.Close();

                 
                    return MyResult;
                }
                catch (Exception ex)
                {
                    MyResult = ex.Message.ToString();
                    return MyResult;
                }

            }
            catch (Exception ex)
            {
                MyResult = ex.Message.ToString();
                return MyResult;
            }



        }

        public static  object CheckQuery(string SFC, string QueryID)
        {
            string MyResult = "";

            var MyError1 = new object();
            string TSQL = String.Format(" select * from table(information_schema.query_history()) WHERE QUERY_ID ='{0}';", QueryID);
            SnowflakeDbConnection myConnection = new SnowflakeDbConnection();
            myConnection.ConnectionString = SFC;
            try
            {
                SnowflakeDbCommand myCommandmaster = new SnowflakeDbCommand(myConnection);
                if (myConnection.IsOpen() == false)
                {

                     myConnection.Open();
                }

                myCommandmaster = new SnowflakeDbCommand(myConnection);
                myCommandmaster.CommandText = TSQL;
                SnowflakeDbDataAdapter MasterSQLDataAdapter;
                MasterSQLDataAdapter = new SnowflakeDbDataAdapter(myCommandmaster);
                try
                {

                    DbDataReader reader = myCommandmaster.ExecuteReader();

                    StringBuilder SB = new StringBuilder();

                    // MyResult = WriteReaderToJSON( SB, reader);

                    MyResult = WriteReaderToJSON(SB, reader);
                    MyError1 = JObject.Parse(MyResult);
                    reader.Close();
                    return MyError1;//new AcceptedResult();
                   // return MyError1;

                }
                catch (Exception ex)
                {
         
                   
                    MyResult = @"{
                                ""status"": ""Error"",
                                ""result"": ""{0}""
                                }";
                    MyResult = MyResult.Replace("{0}", ex.Message.ToString());

               
                    MyError1 = JObject.Parse(MyResult);
                     return MyError1;
                    
                }

            }
            catch (Exception ex)
            {
                MyResult = @"{ ""Status"":""Error"", ""Result"": ""{0}"" } ";
                String.Format( MyResult, ex.Message.ToString());

                MyError1 = JObject.Parse(MyResult);
                return MyError1;
            }



        }

        public static string WriteReaderToJSON(StringBuilder sb, DbDataReader reader)
        {

            if (reader == null || reader.FieldCount == 0)
            {
                sb.Append("null");
                return "";
            }

            int rowCount = 0;

            //  sb.Append(@"{ ""Status"":""Success""," );

            string x = "";
            //sb.Append(@"{""Status"":""Success"",\""Rows"":[");
            try
            {
                sb.Append(@"{""Status"":""Success"", ""Rows"":[");
                while (reader.Read())
                {
                    sb.Append("{");

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        sb.Append("\"" + reader.GetName(i) + "\":");

                        x = reader[i].ToString().Replace("\"", "\\" + "\"");
                        sb.Append("\"" + x + "\"");

                        sb.Append(i == reader.FieldCount - 1 ? "" : ",");

                    }

                    sb.Append("},");

                    rowCount++;
                }

                if (rowCount > 0)
                {
                    int index = sb.ToString().LastIndexOf(",");
                    sb.Remove(index, 1);
                }

                sb.Append("]}");

                return sb.ToString();
            }

            catch (Exception ex)
            {

                sb.Append(@"{""Status"": ""Error"" , ""Rows"": ""  " + ex.Message.ToString() + @"""}");
                //   MyError1 = JObject.Parse(MyResult);
                return sb.ToString();
            }
        }



        public static string EncryptString(string clearText)
        {
            string EncryptionKey = key;
            byte[] clearBytes = Encoding.Unicode.GetBytes(clearText);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateEncryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(clearBytes, 0, clearBytes.Length);
                        cs.Close();
                    }
                    clearText = Convert.ToBase64String(ms.ToArray());
                }
            }
            return clearText;
        }

        public static string DecryptString(string cipherText)
        {
            string EncryptionKey = key;
            cipherText = cipherText.Replace(" ", "+");
            byte[] cipherBytes = Convert.FromBase64String(cipherText);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateDecryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(cipherBytes, 0, cipherBytes.Length);
                        cs.Close();
                    }
                    cipherText = Encoding.Unicode.GetString(ms.ToArray());
                }
            }
            return cipherText;
        }




    }
}
