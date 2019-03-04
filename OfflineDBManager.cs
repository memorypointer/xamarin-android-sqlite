using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using System.IO;
using Mono.Data.Sqlite;
using Mono.Data;
using System.Collections;

namespace AndroidApplication1
{
    public static class OfflineDBManager
    {
        public static string m_ErrMsg = string.Empty;

        public static void CreateDB()
        {
            string consoletxt = "Tables not created";

            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");
            bool exists = File.Exists(dbPath);

            if (!exists)
                SqliteConnection.CreateFile(dbPath);

            var connection = new SqliteConnection("Data Source=" + dbPath);
            connection.Open();
            

            if (!exists)
            {
                // This is the first time the app has run and/or that we need the DB.
                // Copy a "template" DB from your assets, or programmatically create one.

                //@"DROP TABLE [Notifications];",
                var commands = new[]{
                        

                        @"CREATE TABLE [Notifications] (Id                      INTEGER PRIMARY KEY, 
                                                        issued_to               ntext, 
                                                        location                ntext, 
                                                        level                   ntext, 
                                                        photo1                  ntext, 
                                                        photo2                  ntext, 
                                                        photo3                  ntext,  
                                                        notice_date             ntext, 
                                                        notice_time             ntext, 
                                                        compliance_date         ntext, 
                                                        compliance_time         ntext, 
                                                        inspected_by            ntext, 
                                                        contract_id             ntext, 
                                                        notice_guid             ntext, 
                                                        notice_comments         ntext, 
                                                        signature1              ntext, 
                                                        signature2              ntext, 
                                                        clear_within            ntext, 
                                                        device_id               ntext,
                                                        user_first_name         ntext,
                                                        uploaded                INTEGER,
                                                        cleared_within          ntext,
                                                        closeout_photo1         ntext,
                                                        closeout_photo2         ntext,
                                                        closeout_signature1     ntext,
                                                        closeout_comments       ntext,
                                                        closeout_date           ntext,
                                                        closeout_time           ntext,
                                                        is_closeout             INTEGER DEFAULT 0,
                                                        closeout_uploaded       INTEGER DEFAULT 0,
                                                        noncompliance_photo1    ntext,
                                                        noncompliance_photo2    ntext,
                                                        noncompliance_comments  ntext,
                                                        is_noncompliance        INTEGER DEFAULT 0,
                                                        noncompliance_uploaded  INTEGER DEFAULT 0,
                                                        trade                   ntext,
                                                        trade_rate              ntext,
                                                        trade_hours             ntext,
                                                        container_type          ntext,
                                                        container_type_rate     ntext,
                                                        container_type_quant    ntext
                                                        );",

                        @"CREATE TABLE [Contract]     (Id                INTEGER PRIMARY KEY, 
                                                      contract_id       INTEGER, 
                                                      contract_name     ntext,
                                                      user_id           INTEGER);",

                        @"CREATE TABLE [Contractor]   (Id                INTEGER PRIMARY KEY, 
                                                      user_id           INTEGER, 
                                                      user_first_name   ntext);",

                        @"CREATE TABLE [Trade]        (Id               INTEGER PRIMARY KEY, 
                                                      trade             ntext, 
                                                      rate              REAL,
                                                      hours             REAL DEFAULT 0,
                                                      notification_id   INTEGER DEFAULT 0,
                                                      site_id           INTEGER DEFAULT 0);",

                        @"CREATE TABLE [ContainerType]    (Id          INTEGER PRIMARY KEY, 
                                                      container_type    ntext, 
                                                      rate              REAL,
                                                      quant             REAL DEFAULT 0,
                                                      notification_id   INTEGER DEFAULT 0,
                                                      site_id           INTEGER DEFAULT 0);"
                };
                
                foreach (var command in commands)
                {
                    using (var c = connection.CreateCommand())
                    {
                        c.CommandText = command;
                        c.ExecuteNonQuery();
                    }
                }

                consoletxt = "Tables created";
            }

            connection.Close();
            Console.WriteLine(consoletxt);
        }

        public static bool AddUserContract(int userID, int contractID)
        {

            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return false;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            try
            {
                connection.Open();

                using (var c = connection.CreateCommand())
                {
                    c.CommandText = "UPATE Contract SET user_id " + userID + " WHERE contract_id = " + contractID;
                    c.ExecuteNonQuery();
                }

            }
            catch (Exception ex)
            {
                
            }
            finally
            {
                connection.Close();
            }

            return true;
        }

        public static bool FetchUserContract(int userID, int contractID)
        {
            //Console.WriteLine("FetchUserContract USER ID = " + userID + " AND CONTRACT ID = " + contractID);

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");
            bool exists = File.Exists(dbPath);
            bool retValue = false;

            if (!exists)
                return false;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            try
            {
                connection.Open();

                // use `connection`...
                // here, we'll just append the contents to a TextView
                using (var contents = connection.CreateCommand())
                {
                    contents.CommandText = "SELECT contract_id, user_id FROM contract WHERE user_id = " + userID + " AND contract_id = " + contractID;
                    var r = contents.ExecuteReader();
                    //Console.WriteLine(r["user_id"].ToString() + "bubloo" + r["contract_id"].ToString());
                    retValue = r.HasRows;
                }
            }
            catch (Exception ex)
            {

            }
            finally
            {
                connection.Close();
            }

            return retValue;
        }

        public static System.Data.DataTable FetchContract()
        {

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");
            bool exists = File.Exists(dbPath);

            if (!exists)
                return null;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            // use `connection`...
            // here, we'll just append the contents to a TextView
            
            var query = "SELECT contract_id, contract_name FROM contract WHERE status = 1 ORDER BY id";
            var dt = RunQuery(connection, query);
            Console.WriteLine("FetchContract Has Rows" + dt.Rows.Count);
            return dt;
            
            //connection.Close();
        }

        public static System.Data.DataTable FetchContractor()
        {

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return null;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            // use `connection`...
            // here, we'll just append the contents to a TextView

            var query = "SELECT user_id, user_first_name FROM Contractor order by id";
            var dt = RunQuery(connection, query);
            Console.WriteLine("FetchContractor Has Rows" + dt.Rows.Count);
            return dt;
        }

        public static System.Data.DataTable FetchTrade()
        {

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return null;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            var query = "SELECT id, trade, hours FROM Trade WHERE site_id = " + Shared.contract_id;
            var dt = RunQuery(connection, query);

            Console.WriteLine("FetchTrade Has Rows " + dt.Rows.Count);
            return dt;
        }

        public static System.Data.DataTable FetchNoticeTrade(string site_id)
        {

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return null;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            var query = "SELECT trade, rate, hours FROM Trade WHERE site_id = " + site_id;
            var dt = RunQuery(connection, query);

            Console.WriteLine("FetchTrade Has Rows " + dt.Rows.Count);
            return dt;
        }

        public static System.Data.DataTable FetchContainerType()
        {

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return null;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            // use `connection`...
            // here, we'll just append the contents to a TextView

            var query = "SELECT id, container_type,quant FROM ContainerType WHERE site_id = " + Shared.contract_id;
            var dt = RunQuery(connection, query);
            Console.WriteLine("FetchContainerType Has Rows" + dt.Rows.Count);
            return dt;
        }

        public static System.Data.DataTable FetchNoticeContainerType(string site_id)
        {

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return null;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            // use `connection`...
            // here, we'll just append the contents to a TextView

            var query = "SELECT container_type,quant,rate FROM ContainerType WHERE site_id = " + site_id;
            var dt = RunQuery(connection, query);
            Console.WriteLine("FetchNoticeContainerType Has Rows" + dt.Rows.Count);
            return dt;
        }

        public static bool ContractorsExist()
        {
            //string dbPath = Path.Combine(
            //        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
            //        "notifications.db3");

            //bool exists = File.Exists(dbPath);

            //if (!exists)
            //    return null;

            //var connection = new SqliteConnection("Data Source=" + dbPath);
            //connection.Open();

            //// use `connection`...
            //// here, we'll just append the contents to a TextView
            //using (var contents = connection.CreateCommand())
            //{
            //    contents.CommandText = "SELECT user_id, user_first_name FROM Contractor order by id";
            //    var r = contents.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
            //    Console.WriteLine("Has Rows" + r.HasRows);
            //    return r;
            //}

            return true;
        }

        public static Hashtable FetchNotice(string query)
        {

            string dbPath = Path.Combine(
                    System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                    "notifications.db3");

            bool exists = File.Exists(dbPath);
            var noticeHash = new Hashtable();

            if (!exists)
                return null;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            try
            {
                connection.Open();

                // use `connection`...
                // here, we'll just append the contents to a TextView
                using (var contents = connection.CreateCommand())
                {
                    contents.CommandText = query;
                    var r = contents.ExecuteReader();

                    for (int col = 0; col < r.FieldCount; col++)
                    {

                        noticeHash.Add(r.GetName(col), r[col].ToString());
                    }

                    Console.WriteLine("FetchNotice Has Rows" + r.HasRows);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("FetchNotice " + ex.ToString());
            }
            finally
            {
                if (connection.State == System.Data.ConnectionState.Open) connection.Close();
            }
            
            return noticeHash;
        }

        public static void InsertContract(List<string> queryList)
        {
            
            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                SqliteConnection.CreateFile(dbPath);

            var connection = new SqliteConnection("Data Source=" + dbPath);
            try
            {

                connection.Open();

                queryList.Insert(0, "DELETE From Contract;");
                
                // insert data
                var InsCommands = queryList.ToArray();

                //InsCommands.SetValue("DELETE From Contract;", 0);

                foreach (var command in InsCommands)
                {
                    using (var c = connection.CreateCommand())
                    {
                        c.CommandText = command;
                        c.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {

            }
            finally
            {
                connection.Close(); 
            }
        }

        public static void InsertContractor(List<string> queryList)
        {

            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");
            bool exists = File.Exists(dbPath);
            if (!exists)
                return;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            try
            {
                connection.Open();

                // insert data
                var InsCommands = queryList.ToArray();

                foreach (var command in InsCommands)
                {
                    using (var c = connection.CreateCommand())
                    {
                        c.CommandText = command;
                        c.ExecuteNonQuery();
                    }
                }

                
            }
            catch (Exception ex)
            {

            }
            finally
            {
                connection.Close();
            }
        }

        public static bool InsertNotice(List<string> queryList)
        {
            
            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return false;

            var connection = new SqliteConnection("Data Source=" + dbPath);
            try
            {
                connection.Open();

                // insert data
                var InsCommands = queryList.ToArray();

                foreach (var command in InsCommands)
                {
                    using (var c = connection.CreateCommand())
                    {
                        c.CommandText = command;
                        c.ExecuteNonQuery();
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                m_ErrMsg = ex.ToString();
                Console.WriteLine(m_ErrMsg);
                return false;
            }
            finally
            {
                connection.Close();
            }
        }

        public static bool SetNoticeUploaded(SqliteConnection connection, string noticeID)
        {
            try
            {
                //string dbPath = Path.Combine(
                //            System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                //            "notifications.db3");

                //bool exists = File.Exists(dbPath);

                //if (!exists)
                //    return false;

                //var connection = new SqliteConnection("Data Source=" + dbPath);

                //connection.Open();

                // insert data
                //var InsCommands = queryList.ToArray();

                var command = "UPDATE [Notifications] SET uploaded = 1 WHERE Id = " + noticeID;

                try
                {
                    using (var c = connection.CreateCommand())
                    {
                        connection.Open();
                        c.CommandText = command;
                        c.ExecuteNonQuery();
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine("SetNoticeUploaded == " + ex.InnerException.ToString());
                }
                finally
                {
                    if (connection.State == System.Data.ConnectionState.Open) connection.Close();
                }
                //

                Console.WriteLine("SetNoticeUploaded true");

                return true;
            }
            catch (Exception ex)
            {
                //m_ErrMsg = ex.ToString();
                Console.WriteLine("SetNoticeUploaded " + ex.ToString());
                return false;
            }
        }

        public static bool SetCloseoutUploaded(SqliteConnection connection, string noticeID)
        {
            try
            {
                //string dbPath = Path.Combine(
                //            System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                //            "notifications.db3");

                //bool exists = File.Exists(dbPath);

                //if (!exists)
                //    return false;

                //var connection = new SqliteConnection("Data Source=" + dbPath);

                //connection.Open();

                // insert data
                //var InsCommands = queryList.ToArray();

                var command = "UPDATE [Notifications] SET closeout_uploaded = 1 WHERE Id = " + noticeID;

                try
                {
                    using (var c = connection.CreateCommand())
                    {
                        connection.Open();
                        c.CommandText = command;
                        c.ExecuteNonQuery();
                    }
                }
                finally
                {
                    if (connection.State == System.Data.ConnectionState.Open) connection.Close();
                }

                Console.WriteLine("Set closeout_uploaded Uploaded true");

                return true;
            }
            catch (Exception ex)
            {
                //m_ErrMsg = ex.ToString();
                Console.WriteLine("Set closeout_uploaded " + ex.ToString());
                return false;
            }
        }

        public static bool SetNCUploaded(SqliteConnection connection, string noticeID)
        {
            try
            {
                var command = "UPDATE [Notifications] SET noncompliance_uploaded = 1 WHERE Id = " + noticeID;

                try
                {
                    using (var c = connection.CreateCommand())
                    {
                        connection.Open();
                        c.CommandText = command;
                        c.ExecuteNonQuery();
                    }
                }
                finally
                {
                    if (connection.State == System.Data.ConnectionState.Open) connection.Close();
                }

                Console.WriteLine("Set closeout_uploaded Uploaded true");

                return true;
            }
            catch (Exception ex)
            {
                //m_ErrMsg = ex.ToString();
                Console.WriteLine("Set noncompliance_uploaded " + ex.ToString());
                return false;
            }
        }

        //public void InsertContract()
        //{
            
        //    //// insert data
        //    //var InsCommands = new[]{
        //    //            "INSERT INTO [Notifications] ([Key], [Value]) VALUES ('sample', 'text')"
        //    //        };
        //    //foreach (var command in InsCommands)
        //    //{
        //    //    using (var c = connection.CreateCommand())
        //    //    {
        //    //        c.CommandText = command;
        //    //        c.ExecuteNonQuery();
        //    //    }
        //    //}

        //}

        public static System.Data.DataTable RunQuery(SqliteConnection conn, string query)
        {
            //GetCon(); //sets a static variable conn with the connection info 

            try
            {
                //DUPLICATE THE DataAdapter.Fill Method since it's not available in Mono Android.Net 
                var dt = new System.Data.DataTable();
                //do the query to get the data 
                Mono.Data.Sqlite.SqliteCommand c = new SqliteCommand();
                c.CommandText = query;
                c.Connection = conn;

                conn.Open();
                var reader = c.ExecuteReader();
                var len = reader.FieldCount;

                // Create the DataTable columns
                for (int i = 0; i < len; i++)
                    dt.Columns.Add(reader.GetName(i), reader.GetFieldType(i));

                dt.BeginLoadData();

                var values = new object[len];

                // Add data rows
                while (reader.Read())
                {
                    for (int i = 0; i < len; i++)
                        values[i] = reader[i];

                    dt.Rows.Add(values);
                }

                dt.EndLoadData();

                reader.Close();
                reader.Dispose();

                //return the filled dataset 
                return dt;
            }
            catch (Exception ex)
            {
                Console.WriteLine("RunQuery() error running query: " + ex.Message);
            }
            finally
            {
                if (conn.State == System.Data.ConnectionState.Open) conn.Close();
            }

            return null;
        }

        public static bool AddTrade(string id, string hours)
        {

            if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(hours))
            {
                string dbPath = Path.Combine(
                            System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                            "notifications.db3");

                bool exists = File.Exists(dbPath);

                if (!exists)
                    return false;

                var connection = new SqliteConnection("Data Source=" + dbPath);

                try
                {
                    connection.Open();

                    using (var c = connection.CreateCommand())
                    {
                        var query = "UPDATE Trade SET hours = " + hours + " WHERE Id = " + id;
                        c.CommandText = query;
                        c.ExecuteNonQuery();
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine("AddTrade:" + ex.ToString());
                    return false;
                }
                finally
                {
                    connection.Close();
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        public static bool AddContainerType(string id, string quant)
        {

            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return false;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            try
            {
                connection.Open();

                using (var c = connection.CreateCommand())
                {
                    c.CommandText = "UPDATE ContainerType SET quant = " + quant + " WHERE Id = " + id;
                    c.ExecuteNonQuery();
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("AddContainerType:" + ex.ToString());
                return false;
            }
            finally
            {
                connection.Close();
            }

            return true;
        }

        public static bool ClearTrades(string siteID)
        {
            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return false;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            try
            {
                connection.Open();

                using (var c = connection.CreateCommand())
                {
                    c.CommandText = "UPDATE Trade SET hours = 0 WHERE site_id = " + siteID;
                    c.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("AddContainerType:" + ex.ToString());
                return false;
            }
            finally
            {
                connection.Close();
            }

            return true;
        }

        public static bool ClearContainerTypes(string siteID)
        {
            string dbPath = Path.Combine(
                        System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal),
                        "notifications.db3");

            bool exists = File.Exists(dbPath);

            if (!exists)
                return false;

            var connection = new SqliteConnection("Data Source=" + dbPath);

            try
            {
                connection.Open();
                using (var c = connection.CreateCommand())
                {
                    c.CommandText = "UPDATE ContainerType SET quant = 0 WHERE site_id = " + siteID;
                    c.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("AddContainerType:" + ex.ToString());
                return false;
            }
            finally
            {
                connection.Close();
            }
            return true;
        }


    }
}