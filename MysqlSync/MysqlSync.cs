using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;


namespace MysqlSync
{
    class MysqlProxy
    {
        MySqlConnection conn;

        public MysqlProxy(string user, string password, string ip, string database)
        {
            try
            {
                conn = new MySqlConnection();
                conn.ConnectionString =
                    "user id=" + user + ";Password=" + password + ";server=" + ip + ";database=" + database;
                conn.Open();
            }
            catch(Exception ex)
            {
                Console.WriteLine("====> " + ex.Message);
                Console.ReadKey();
            }
        }

        ~MysqlProxy()
        {
            try
            {
                conn.Close();
            }
            catch
            {
            }
        }

        public void getDatas(string sql, int column, List<string> datas)
        {
            //获取某一列的数据
            try
            {
                MySqlDataAdapter adapter = new MySqlDataAdapter(sql, conn);
                DataTable table = new DataTable();
                adapter.Fill(table);

                foreach (DataRow row in table.Rows)
                {
                    datas.Add(row[column].ToString());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("====> " + ex.Message);
                Console.ReadKey();
            }
        }

        public void getDatas(string sql, List<List<string>> datas)
        {
            //获取整个表的数据
            try
            {
                MySqlDataAdapter adapter = new MySqlDataAdapter(sql, conn);
                DataTable table = new DataTable();
                adapter.Fill(table);

                foreach (DataRow row in table.Rows)
                {
                    List<string> temp = new List<string>();
                    foreach (DataColumn column in table.Columns)
                    {
                        temp.Add(row[column].ToString());
                    }
                    datas.Add(temp);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("====> " + ex.Message);
                Console.ReadKey();
            }
        }

        public void update(string sql)
        {
            //更新数据库
            try
            {
                MySqlDataAdapter adapter = new MySqlDataAdapter(sql, conn);
                DataTable table = new DataTable();
                adapter.Fill(table);

                Console.WriteLine(sql);
            }
            catch(Exception ex)
            {
                Console.WriteLine("====> " + ex.Message);
                Console.ReadKey();
            }
        }
    }
}
