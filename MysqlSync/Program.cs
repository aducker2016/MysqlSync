using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MysqlSync;

namespace MysqlSync
{
    class Program
    {
        static void Main(string[] args)
        {
            /////////////////////////////////////////////
            //   源数据库的表结构 [同步]到 目标数据库  //
            //   等同于navicat的 [表结构同步] 功能     //
            /////////////////////////////////////////////

            if (8 != args.Length)
            {
                Console.WriteLine("\n用法:");
                Console.WriteLine("\nMysqlSync.exe 源user 源password 源ip 源database 目标user 目标password 目标ip 目标database");
                Console.ReadKey();
                return;
            }

            //
            //创建连接
            //
            MysqlProxy source = new MysqlProxy(args[0], args[1], args[2], args[3]);
            MysqlProxy target = new MysqlProxy(args[4], args[5], args[6], args[7]);

            //
            //删除表
            //
            List<string> sourceTables = new  List<string>();
            source.getDatas("show tables", 0, sourceTables);
            List<string> targetTables = new List<string>();
            target.getDatas("show tables", 0, targetTables);

            foreach (string table in targetTables)
            {
                if (!sourceTables.Contains(table))
                {
                    target.update("drop table " + table);
                }
            }

            //
            //添加表
            //
            List<string> checkTables = new List<string>();
            foreach (string table in sourceTables)
            {
                if (!targetTables.Contains(table))
                {
                    List<string> createTableStrs = new List<string>();
                    source.getDatas("show create table " + table, 1, createTableStrs);
                    if (createTableStrs.Any())
                    {
                        target.update(createTableStrs[0]);
                    }
                }
                else
                {
                    checkTables.Add(table);
                }
            }

            //
            //同步表
            //
            foreach (string table in checkTables)
            {
                //表字段信息
                Dictionary<string, string> sourceColumnMap = new Dictionary<string, string>();
                Dictionary<string, string> targetColumnMap = new Dictionary<string, string>();
                GetColumnMap(source, table, sourceColumnMap);
                GetColumnMap(target, table, targetColumnMap);

                foreach (var column in targetColumnMap)
                {
                    //删字段
                    if (!sourceColumnMap.ContainsKey(column.Key))
                    {
                        target.update("alter table " + table + " drop " + column.Key);
                    }
                }

                foreach (var column in sourceColumnMap)
                {
                    //加字段
                    if (!targetColumnMap.ContainsKey(column.Key))
                    {
                        target.update("alter table " + table + " add " + column.Value);
                    }
                    //改字段
                    else if (targetColumnMap[column.Key] != column.Value)
                    {
                        target.update("alter table " + table + " modify " + column.Value);
                    }
                }                

                //索引信息
                Dictionary<string, string> sourceIndexMap = new Dictionary<string, string>();
                Dictionary<string, string> targetIndexMap = new Dictionary<string, string>();
                GetIndexMap(source, table, sourceIndexMap);
                GetIndexMap(target, table, targetIndexMap);

                foreach (var index in targetIndexMap)
                {
                    //删索引
                    if (!sourceIndexMap.ContainsKey(index.Key) || sourceIndexMap[index.Key] != index.Value)
                    {
                        if (index.Key == "PRIMARY")
                        {
                            //注：修改在添加索引做，否则主键如果是自增会出错
                            if (!sourceIndexMap.ContainsKey(index.Key))
                            {
                                target.update("alter table " + table + " drop primary key");
                            }
                        }
                        else
                        {
                            target.update("alter table " + table + " drop index " + index.Key);
                        }
                    }
                }

                foreach (var index in sourceIndexMap)
                {
                    //加索引、改索引
                    if (!targetIndexMap.ContainsKey(index.Key) || targetIndexMap[index.Key] != index.Value)
                    {
                        if (index.Key == "PRIMARY")
                        {
                            if (!targetIndexMap.ContainsKey(index.Key))
                            {
                                target.update("alter table " + table + " add " + index.Value);
                            }
                            else
                            {
                                target.update("alter table " + table + " drop primary key, add " + index.Value);
                            }
                        }
                        else
                        {
                            target.update("alter table " + table + " add " + index.Value);
                        }                        
                    }
                }                
            }
        }

        static void GetColumnMap(MysqlProxy proxy, string table, Dictionary<string, string> columnMap)
        {
            List<string> createTableStrs = new List<string>();
            proxy.getDatas("show create table " + table, 1, createTableStrs);
            if (createTableStrs.Any())
            {
                string lastName = "";
                string[] columns = createTableStrs[0].ToString().Split('\n');
                foreach (string column in columns)
                {
                    if (column.Substring(0, 3) == "  `")
                    {
                        string[] temp1 = column.Split('`');
                        string[] temp2 = column.Split(',');
                        columnMap[temp1[1]] = temp2[0] + (lastName.Length > 0 ? " after " + lastName : "");
                        lastName = temp1[1];
                    }
                }
            }
        }

        static void GetIndexMap(MysqlProxy proxy, string table, Dictionary<string, string> indexMap)
        {
            List<List<string>> indexStrStrs = new List<List<string>>();
            proxy.getDatas("show keys from " + table, indexStrStrs);

            foreach (List<string> indexStrs in indexStrStrs)
            {
                string nonUnique = indexStrs[1];
                string keyName = indexStrs[2];
                string columnBame = indexStrs[4];

                if (!indexMap.ContainsKey(keyName))
                {
                    if (keyName == "PRIMARY")
                    {
                        indexMap[keyName] = "primary key (";
                    }
                    else if(nonUnique == "0")
                    {
                        indexMap[keyName] = "unique " + keyName + " (";
                    }
                    else
                    {
                        indexMap[keyName] = "index " + keyName + " (";
                    }
                }
                else
                {
                    indexMap[keyName] += ",";
                }

                indexMap[keyName] += columnBame;
            }

            string[] keys = indexMap.Keys.ToArray();
            foreach (string key in keys)
            {
                indexMap[key] += ")";
            }
        }
    }
}
