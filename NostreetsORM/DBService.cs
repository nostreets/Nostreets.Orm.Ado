using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web;
using System.ComponentModel.DataAnnotations.Schema;
using NostreetsExtensions.Helpers;
using NostreetsExtensions.Interfaces;
using NostreetsExtensions;
using System.Collections;

namespace NostreetsORM
{
    public class DBService<T> : SqlService, IDBService<T>
    {
        public DBService() : base()
        {
            try
            {
                SetUp();
                bool doesExist = CheckIfTableExist(_type),
                     isCurrent = !CheckIfTypeIsCurrent(_type) ? false : SubTablesAccessed.All(a => CheckIfTypeIsCurrent(a.Key)) ? true : false;

                if (!doesExist)
                {
                    CreateTable(_type);
                }
                else if (!isCurrent)
                {
                    UpdateTable();
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DBService(string connectionKey) : base(connectionKey)
        {

            try
            {
                SetUp();
                bool doesExist = CheckIfTableExist(_type),
                     isCurrent = !CheckIfTypeIsCurrent(_type) ? false : SubTablesAccessed.All(a => CheckIfTypeIsCurrent(a.Key)) ? true : false;

                if (!doesExist)
                {
                    DeleteReferences();
                    CreateTable(_type);
                }
                else if (!isCurrent)
                {
                    UpdateTable();
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public Dictionary<Type, Type[]> SubTablesAccessed { get { return GetSubTablesAccessed(); } }

        private Dictionary<string, string> ProcTemplates
        {
            get
            {
                return new Dictionary<string, string>
                {
                    { "Insert",  _partialProcs["InsertProcedure"]},
                    { "Update",  _partialProcs["UpdateProcedure"]},
                    { "SelectAll",  _partialProcs["SelectProcedure"]},
                    { "SelectBy",  _partialProcs["SelectProcedure"]},
                    { "Delete",  _partialProcs["DeleteProcedure"]}

                };
            }
        }

        private int _tableLayer = 0,
                    _procLayer = 0;
        private bool _tableCreation = false,
                     _procedureCreation = false;
        private Type _type = typeof(T);
        private Dictionary<string, string> _partialProcs = new Dictionary<string, string>();


        #region Internal Logic

        private void SetUp()
        {
            _partialProcs.Add("GetAllColumns", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}s'");
            _partialProcs.Add("GetAllProcs", "SELECT NAME FROM [dbo].[sysobjects] WHERE(type = 'P')");
            _partialProcs.Add("CheckIfTableExist", "Declare @IsTrue int = 0 IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}s') Begin Set @IsTrue = 1 End Select @IsTrue");
            _partialProcs.Add("InsertProcedure", "CREATE Proc [dbo].[{0}s_Insert] {1} As Begin Declare @NewId {2} Insert Into [dbo].{3}s({4}) Values({5}) Set @NewId = SCOPE_IDENTITY() Select @NewId End");
            _partialProcs.Add("UpdateProcedure", "CREATE Proc [dbo].[{0}s_Update] {1} As Begin {2} End");
            _partialProcs.Add("DeleteProcedure", "CREATE Proc [dbo].[{0}s_Delete] @{1} {2} As Begin Delete {0}s Where {1} = @{1} {3} End");
            _partialProcs.Add("SelectProcedure", "CREATE Proc [dbo].[{0}s_Select{5}] {3} AS Begin SELECT {1} FROM [dbo].[{0}s] {2} {4} End");
            _partialProcs.Add("NullCheckForUpdatePartial", "If @{2} Is Not Null Begin Update [dbo].{0}s {1} End ");
            _partialProcs.Add("CreateTableType", "CREATE TYPE [dbo].[{0}] AS TABLE( {1} )");
            _partialProcs.Add("CreateTable", "Declare @isTrue int = 0 Begin CREATE TABLE [dbo].[{0}s] ( {1} ); IF EXISTS(SELECT* FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}s') Begin Set @IsTrue = 1 End End Select @IsTrue");
            _partialProcs.Add("CreateColumn", "[{0}] {1} {2}");


            _partialProcs.Add("SelectStatement", " SELECT {0}");
            _partialProcs.Add("FromStatement", " FROM [dbo].[{0}s]");
            _partialProcs.Add("InsertIntoStatement", " INSERT INTO [dbo].[{0}s]({1})");
            _partialProcs.Add("ValuesStatement", " Values({2})");
            _partialProcs.Add("CopyTableStatement", "SELECT {2} INTO {1}s FROM {0}s");
            _partialProcs.Add("IfStatement", " IF {0} BEGIN {1} END");
            _partialProcs.Add("ElseStatement", " ELSE BEGIN {0} END");
            _partialProcs.Add("ElseIfStatement", " ELSE IF BEGIN {0} END");
            _partialProcs.Add("DeclareStatement", " DECLARE {0} {1} = {2}");
            _partialProcs.Add("DeleteRowsStatement", " DELETE {0}s");
            _partialProcs.Add("DropTableStatement", " DROP TABLE {0}s");
            _partialProcs.Add("DropTableTypeStatement", " DROP TYPE [dbo].[{0}]");
            _partialProcs.Add("DropProcedureStatement", " DROP PROCEDURE {0}");
            _partialProcs.Add("WhereStatement", " WHERE {0} BEGIN {1} END");
            _partialProcs.Add("CountStatement", " COUNT({0})");
            _partialProcs.Add("GroupByStatement", " GROUP BY {0}");
            _partialProcs.Add("PrimaryKeyStatement", "PRIMARY KEY CLUSTERED ([{0}] ASC)");
            _partialProcs.Add("IdentityInsertStatement", " SET IDENTITY_INSERT [dbo].[{0}s] {1}");

        }

        private bool ShouldNormalize(Type type)
        {
            return ((type != typeof(String) && type != typeof(Char)) && (type.BaseType == typeof(Enum) || type.IsCollection() || type.IsClass)) ? true : false;
        }

        private bool NeedsIdProp(Type type)
        {
            return !type.IsClass ? false : type.GetProperties()[0].PropertyType != typeof(int) ? true : false;
        }

        private string DeterminSQLType(Type type)
        {
            string statement = null;

            if (ShouldNormalize(type))
            {
                statement = "INT";
            }
            else
            {
                switch (type.Name)
                {
                    case nameof(String):
                        statement = "NVARCHAR (2000)";
                        break;

                    case nameof(Int16):
                        statement = "SMALLINT";
                        break;

                    case nameof(Int32):
                        statement = "INT";
                        break;

                    case nameof(Boolean):
                        statement = "BIT";
                        break;

                    case nameof(DateTime):
                        statement = "DATETIME2 (7)" + ((_tableCreation) ? "CONSTRAINT[DF_" + type.DeclaringType.Name + "s_" + type.Name + "] DEFAULT(getutcdate())" : "");
                        break;

                    default:
                        statement = "NVARCHAR (MAX)";
                        break;
                }
            }

            return statement;
        }

        private Type[] GetRelationships(Type type)
        {
            Type[] result = null;
            List<Type> list = null;
            List<PropertyInfo> relations = type.GetProperties().Where(a => ShouldNormalize(a.PropertyType)).ToList();

            if (relations != null && relations.Count > 0)
            {
                foreach (PropertyInfo prop in relations)
                {
                    if (list == null)
                        list = new List<Type>();

                    list.Add(prop.PropertyType);
                }

                result = list.Distinct().ToArray();
            }

            return result;
        }

        private Dictionary<Type, Type[]> GetSubTablesAccessed()
        {
            List<Type> typesToCheck = GetRelationships(_type).Distinct().ToList();
            Dictionary<Type, Type[]> result = new Dictionary<Type, Type[]>();
            Type[] relations = null;


            for (int i = 0; i < typesToCheck.Count; i++)
            {
                relations = GetRelationships(typesToCheck[i]);

                result.Add(typesToCheck[i], relations);

                if (relations != null)
                    typesToCheck.AddRange(relations);

            }

            return result;
        }

        private void UpdateTable()
        {
            Action<Type> backUpAndDropTbl = (a) =>
            {
                if (a.BaseType != typeof(Enum))
                    CreateBackupTable(a);

                DropTable(a);
            };
            Action<Type> createAndUpdateFromBackUp = (a) =>
            {
                CreateTable(a);

                if (a.BaseType != typeof(Enum))
                {
                    UpdateRows(a);
                    DropBackupTable(a);
                }
            };

            List<Type> typesToUpdate = SubTablesAccessed.Keys.ToList();
            List<Type> backedUpTbls = new List<Type> { _type };


            backUpAndDropTbl(_type);
            foreach (Type tbl in typesToUpdate)
            {
                backUpAndDropTbl(tbl);

                if (SubTablesAccessed[tbl] == null)
                    createAndUpdateFromBackUp(tbl);

                else
                {
                    backedUpTbls.Prepend(tbl);
                }
            }

            foreach (Type backedTbl in backedUpTbls)
                createAndUpdateFromBackUp(_type);

        }

        private string GetCRUDProcsForEnum(Type type, KeyValuePair<string, string> template)
        {
            if (type.BaseType != typeof(Enum))
                throw new Exception("type's BaseType has to be typeof(Enum)...");

            string query = null;

            string inputParams = null,
                   columns = null,
                   values = null,
                   select = null,
                   joins = null,
                   deletes = null,
                   updatesParams = null;

            List<string> inputs = new List<string>(),
                         colm = new List<string>(),
                         val = new List<string>(),
                         sel = new List<string>(),
                         jns = new List<string>(),
                         dels = new List<string>(),
                         updtParams = new List<string>(),
                         innerUpdt = new List<string>();


            //inputs.Add("@Value " + DeterminSQLType(typeof(string)));
            //updtParams.Add("@Id " + DeterminSQLType(typeof(int)) + ", @Value " + DeterminSQLType(typeof(string)) + " = Null");
            //innerUpdt.Add("SET Value = @Value WHERE " + type.Name + "s.Id = @Id");
            //colm.Add("Value");
            //val.Add("@Value");
            //sel.Add(type.Name + "s.[Id], " + type.Name + "s.[Value]");


            inputParams = "@Value " + DeterminSQLType(typeof(string)); //String.Join(" ", inputs.ToArray());
            columns = "Value"; //String.Join(" ", colm.ToArray());
            values = "@Value"; // String.Join(" ", val.ToArray());
            select = type.Name + "s.[Id], " + type.Name + "s.[Value]"; //String.Join(" ", sel.ToArray());
            joins = String.Join(" ", jns.ToArray());
            deletes = String.Join(" ", dels.ToArray());
            updatesParams = "@Id " + DeterminSQLType(typeof(int)) + ", @Value " + DeterminSQLType(typeof(string)) + " = Null"; //String.Join(" ", updtParams.ToArray());


            switch (template.Key)
            {
                case "Insert":
                    query = String.Format(template.Value, type.Name, inputParams, DeterminSQLType(typeof(int)), type.Name, columns, values);
                    break;

                case "Update":
                    string innerQuery = String.Format(_partialProcs["NullCheckForUpdatePartial"], type.Name, /*innerUpdt[num]*/"SET Value = @Value WHERE " + type.Name + "s.Id = @Id", "Value");
                    query = String.Format(template.Value, type.Name, " @Id INT, " + inputParams, innerQuery);
                    break;

                case "SelectAll":
                    query = String.Format(template.Value, type.Name, select, "", "", "", "All");
                    break;

                case "SelectBy":
                    query = String.Format(template.Value, type.Name, select, "", "@Id " + DeterminSQLType(typeof(int)), "Where " + type.Name + "s.Id = @Id", "ById");
                    break;

                case "Delete":
                    query = String.Format(template.Value, type.Name, "Id", DeterminSQLType(typeof(int)), deletes);
                    break;
            }

            #region Legacy
            //if (temps[interval] == sqlInsertTemp)
            //{
            //    query = String.Format(temps[interval], type.Name, inputParams, DeterminSQLType(typeof(int)/*, type.Name*/), type.Name, columns, values);
            //}
            //else if (temps[interval] == sqlSelectTemp)
            //{
            //    if (interval == 1)
            //    {
            //        query = String.Format(temps[interval], type.Name, select, "", "", "", "All");
            //    }
            //    else
            //    {
            //        query = String.Format(temps[interval], type.Name, select, "", "@Id " + DeterminSQLType(typeof(int)/*, type.Name*/), "Where " + type.Name + "s.Id = @Id", "ById");
            //    }
            //}
            //else if (temps[interval] == sqlUpdateTemp)
            //{

            //    string innerQuery = null;
            //    for (int num = 0; num < innerUpdt.Count; num++)
            //    {

            //        innerQuery += String.Format(sqlUpdateNullCheckTemp, type.Name, innerUpdt[num], "Value");
            //    }

            //    query = String.Format(temps[interval], type.Name, " @Id INT, " + inputParams, innerQuery);
            //}
            //else if (temps[interval] == sqlDeleteTemp)
            //{

            //    query = String.Format(temps[interval], type.Name, "Id", DeterminSQLType(typeof(int)/*, type.Name*/), deletes);
            //} 
            #endregion

            return query;
        }

        private string GetCRUDProcsForClass(Type type, KeyValuePair<string, string> template)
        {
            if (type.IsClass && (type == typeof(String) || type == typeof(Char)))
                throw new Exception("type's Type has to be a custom data type...");

            if (NeedsIdProp(type))
                type = type.AddProperty(typeof(int), "Id");

            string query = null;

            string inputParams = null,
                   columns = null,
                   values = null,
                   select = null,
                   joins = null,
                   deletes = null,
                   updatesParams = null;

            List<string> inputs = new List<string>(),
                         colm = new List<string>(),
                         val = new List<string>(),
                         sel = new List<string>(),
                         jns = new List<string>(),
                         dels = new List<string>(),
                         updtParams = new List<string>(),
                         innerUpdt = new List<string>();

            PropertyInfo[] props = type.GetProperties();

            for (int i = 0; i < props.Length; i++)
            {
                string PK = (props[i].PropertyType.BaseType == typeof(Enum) ? "Id" : props[i].Name);

                if (i > 0)
                {
                    inputs.Add("@" + props[i].Name + " " + DeterminSQLType(props[i].PropertyType) + (i == props.Length - 1 ? "" : ","));

                    colm.Add(props[i].Name +
                        (ShouldNormalize(props[i].PropertyType) ? "Id" : "") + (i == props.Length - 1 ? "" : ","));

                    val.Add("@" + props[i].Name + (i == props.Length - 1 ? "" : ","));
                }

                updtParams.Add("@" + props[i].Name + DeterminSQLType(props[i].PropertyType) + (i == 0 ? "" : " = NULL") + (i == props.Length - 1 ? "" : ","));

                innerUpdt.Add("SET " + props[i].Name +
                    (ShouldNormalize(props[i].PropertyType) ? "Id" : "") + " = @" + props[i].Name + " WHERE " + type.Name + "s." + props[0].Name + " = @" + props[0].Name);


                if (ShouldNormalize(props[i].PropertyType))
                {
                    jns.Add("Inner Join " + props[i].PropertyType.Name + "s AS " + props[i].Name + "Id On " + props[i].Name + "Id." + (props[i].PropertyType.BaseType == typeof(Enum) ? "Id" : props[i].Name + "Id") + " = " + type.Name + "s." + props[i].Name + "Id");

                    if (props[i].PropertyType.BaseType != typeof(Enum))
                    {
                        dels.Add("Delete " + props[i].Name + "s Where " + PK + " = (Select " + PK + " From " + type.Name + " Where " + PK + " = @" + PK + ")");
                    }
                }

                sel.Add(type.Name + "s.[" + props[i].Name +
                    (ShouldNormalize(props[i].PropertyType) ? "Id" : "") + "]" + (i == props.Length - 1 ? " " : ","));
            }

            inputParams = String.Join(" ", inputs.ToArray());
            columns = String.Join(" ", colm.ToArray());
            values = String.Join(" ", val.ToArray());
            select = String.Join(" ", sel.ToArray());
            joins = String.Join(" ", jns.ToArray());
            deletes = String.Join(" ", dels.ToArray());
            updatesParams = String.Join(" ", updtParams.ToArray());

            switch (template.Key)
            {
                case "Insert":
                    query = String.Format(template.Value, type.Name, inputParams, DeterminSQLType(props[0].PropertyType), type.Name, columns, values);
                    break;

                case "Update":
                    string innerQuery = null;
                    for (int q = 1; q < innerUpdt.Count; q++)
                    {
                        innerQuery += String.Format(_partialProcs["NullCheckForUpdatePartial"], type.Name, innerUpdt[q], props[q].Name);
                    }

                    query = String.Format(template.Value, type.Name, "@Id INT, " + inputParams, innerQuery);
                    break;

                case "SelectAll":
                    query = String.Format(template.Value, type.Name, select, joins, "", "", "All");
                    break;

                case "SelectBy":
                    query = String.Format(template.Value, type.Name, select, joins, "@" + props[0].Name + " " + DeterminSQLType(props[0].PropertyType), "Where " + type.Name + "s." + props[0].Name + " = @" + props[0].Name, "ById");
                    break;

                case "Delete":
                    query = String.Format(template.Value, type.Name, props[0].Name, DeterminSQLType(props[0].PropertyType), deletes);
                    break;
            }

            #region Legacy
            //if (temps[x] == sqlInsertTemp)
            //{
            //    query = String.Format(temps[x], type.Name, inputParams, DeterminSQLType(props[0].PropertyType), type.Name, columns, values);
            //}
            //else if (temps[x] == sqlSelectTemp)
            //{
            //    if (x == 1)
            //    {
            //        query = String.Format(temps[x], type.Name, select, joins, "", "", "All");
            //    }
            //    else
            //    {
            //        query = String.Format(temps[x], type.Name, select, joins, "@" + props[0].Name + " " + DeterminSQLType(props[0].PropertyType/*, type.Name*/), "Where " + type.Name + "s." + props[0].Name + " = @" + props[0].Name, "ById");
            //    }
            //}
            //else if (temps[x] == sqlUpdateTemp)
            //{

            //    string innerQuery = null;
            //    for (int q = 1; q < innerUpdt.Count; q++)
            //    {
            //        innerQuery += String.Format(sqlUpdateNullCheckTemp, type.Name, innerUpdt[q], props[q].Name);
            //    }

            //    query = String.Format(temps[x], type.Name, "@Id INT, " + inputParams, innerQuery);
            //}
            //else if (temps[x] == sqlDeleteTemp)
            //{

            //    query = String.Format(temps[x], type.Name, props[0].Name, DeterminSQLType(props[0].PropertyType/*, type.Name*/), deletes);
            //}

            #endregion

            return query;
        }

        private string GetTableCreationQuery(Type type)
        {
            List<string> columns = new List<string>();

            if (type.BaseType == typeof(Enum))
            {
                for (int i = 0; i < 2; i++)
                {
                    columns.Add(
                        String.Format(
                            _partialProcs["CreateColumn"],
                            i == 0 ? "Id" : "Value",
                            DeterminSQLType(i == 0 ? typeof(int) : typeof(string)),
                            (i == 0)
                                ? "IDENTITY (1, 1) NOT NULL, "
                                : "NOT NULL, CONSTRAINT [PK_" + type.Name + "s] PRIMARY KEY CLUSTERED ([Id] ASC)")
                     );
                }
            }
            else if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
            {


                List<string> FKs = new List<string>();
                PropertyInfo[] props = type.GetProperties();


                foreach (PropertyInfo item in props)
                {
                    string FK = null;

                    columns.Add(
                        String.Format(
                            _partialProcs["CreateColumn"],

                            !ShouldNormalize(item.PropertyType)
                                ? item.Name
                                : !item.PropertyType.IsCollection()
                                ? item.Name + "Id"
                                : item.PropertyType.GetTypeOfT().Name + "CollectionId",

                            DeterminSQLType(item.PropertyType),

                            (props[0] == item && item.GetType() == typeof(int))
                                ? "IDENTITY (1, 1) NOT NULL, "
                                : props[props.Length - 1] == item
                                ? "NOT NULL, CONSTRAINT [PK_" + type.Name + "s] PRIMARY KEY CLUSTERED ([" + props[0].Name + "] ASC)," + String.Join(", ", FKs.ToArray())
                                : "NOT NULL, "
                        )
                    );


                    if (ShouldNormalize(item.PropertyType) && (!item.PropertyType.IsSystemType() || item.PropertyType.IsCollection()))
                    {
                        string normalizedTblPK = null;


                        if (!item.PropertyType.IsCollection())
                        {
                            normalizedTblPK = CreateTable(item.PropertyType);

                            FK = "CONSTRAINT [FK_" + type.Name + "s_" + item.Name + "] FOREIGN KEY ([" + item.Name + "Id]) REFERENCES [dbo].[" + item.PropertyType.Name + 's' /*normalizedTbl["Name"]*/ + "] ([" + normalizedTblPK/*normalizedTbl["PK"]*/ + "])";

                        }
                        else
                        {
                            Type collectionType = item.PropertyType.GetTypeOfT();

                            if (!collectionType.IsSystemType())
                            {
                                normalizedTblPK = CreateIntermaiateTable(item.PropertyType);

                                FK = "CONSTRAINT [FK_" + type.Name + "s_" + collectionType.Name + "Collection] FOREIGN KEY ([" + item.Name + "CollectionId]) REFERENCES [dbo].[" + collectionType.Name + "Collection] ([" + normalizedTblPK/*normalizedTbl["PK"]*/ + "])"; 
                            }
                        }


                        FKs.Add(FK);

                    }
                }
            }



            string table = String.Concat(columns.ToArray());
            string query = String.Format(_partialProcs["CreateTable"], type.Name, table);

            return query;
        }

        private void DeleteReferences()
        {
            foreach (Type tbl in SubTablesAccessed.Keys)
            {
                DropTable(tbl);
                DropBackupTable(tbl);
                DropProcedures(tbl);
            }

            DropTable(_type);
            DropBackupTable(_type);
            DropProcedures(_type);
        }

        #endregion


        #region Queries To Write

        private void AddEnumsAsRows(Type type)
        {
            if (type.BaseType != typeof(Enum))
                throw new Exception("type's BaseType has to be a Enum...");

            var fields = type.GetFields();
            for (int i = 1; i < fields.Length; i++)
            {
                DataProvider.ExecuteNonQuery(() => Connection,
                                "dbo." + type.Name + "s_Insert",
                                (param) => param.Add(new SqlParameter("Value", fields[i].Name)),
                                null);
            }
        }

        private string CreateTable(Type type)
        {
            ++_tableLayer;
            _tableCreation = true;
            string result = null;

            if (NeedsIdProp(type))
                type = type.AddProperty(typeof(int), "Id");


            if (!CheckIfTableExist(type))
            {
                string query = GetTableCreationQuery(type);


                int isTrue = 0;
                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        isTrue = reader.GetSafeInt32(0);
                    },
                    null,
                    mod => mod.CommandType = CommandType.Text);


                if (isTrue == 1)
                {
                    CreateProcedures(type);

                    if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                    {
                        result = type.GetProperties()[0].Name;
                    }
                    else if (type.BaseType == typeof(Enum))
                    {
                        AddEnumsAsRows(type);
                        result = "Id";
                    }
                }

            }
            else
            {

                if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                {
                    result = type.GetProperties()[0].Name;

                }
                else if (type.BaseType == typeof(Enum))
                {
                    result = "Id";
                }



            }

            --_tableLayer;
            _tableCreation = (_tableLayer == 0) ? false : true;

            return result;
        }

        private void CreateBackupTable(Type type)
        {
            if (CheckIfTableExist(type))
            {
                string sqlTemp = _partialProcs["CopyTableStatement"];
                string query = String.Format(sqlTemp, type.Name, "temp" + type.Name, "*");
                object result = null;

                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        result = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = CommandType.Text);
            }

        }

        private void CreateProcedures(Type type)
        {
            ++_procLayer;
            _procedureCreation = true;

            //string sqlInsertTemp = _partialProcs["InsertProcedure"],
            //       sqlUpdateTemp = _partialProcs["UpdateProcedure"],
            //       sqlSelectTemp = _partialProcs["SelectProcedure"],
            //       sqlDeleteTemp = _partialProcs["DeleteProcedure"],
            //       sqlUpdateNullCheckTemp = _partialProcs["NullCheckForUpdatePartial"];


            //string[] temps = { sqlInsertTemp, sqlSelectTemp, sqlSelectTemp, sqlUpdateTemp, sqlDeleteTemp };

            foreach (KeyValuePair<string, string> template in ProcTemplates)
            {

                string query = null;

                //string inputParams = null,
                //       columns = null,
                //       values = null,
                //       select = null,
                //       joins = null,
                //       deletes = null,
                //       updatesParams = null;

                //List<string> inputs = new List<string>(),
                //             colm = new List<string>(),
                //             val = new List<string>(),
                //             sel = new List<string>(),
                //             jns = new List<string>(),
                //             dels = new List<string>(),
                //             updtParams = new List<string>(),
                //             innerUpdt = new List<string>();

                if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                {
                    query = GetCRUDProcsForClass(type, template);

                }
                else if (type.BaseType == typeof(Enum))
                {
                    query = GetCRUDProcsForEnum(type, template);
                }


                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        object id = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = System.Data.CommandType.Text);
            }

            --_procLayer;
            _procedureCreation = (_procLayer == 0) ? false : true;

        }

        private string CreateIntermaiateTable(Type type)
        {
            if (!type.IsCollection())
                throw new Exception("type has to implement IEnumerable...");

            string result = null;

            if (!CheckIfTableExist(type))
            {
                List<string> columns = new List<string>();
                Type collectionType = type.GetTypeOfT();


                string PK = CreateTable(collectionType);
                string FK = "CONSTRAINT [FK_" + collectionType.Name + "List_" + collectionType.Name + "] FOREIGN KEY ([" + collectionType.Name + "Id]) REFERENCES [dbo].[" + collectionType.Name + "s] ([" + PK + "])";

                for (int i = 0; i < 2; i++)
                {
                    columns.Add(
                        String.Format(
                            _partialProcs["CreateColumn"],
                            i == 0 ? "Id" : collectionType.Name + "Id",
                            DeterminSQLType(i == 0 ? typeof(int) : typeof(string)),
                            (i == 0)
                                ? "IDENTITY (1, 1) NOT NULL, "
                                : "NOT NULL, CONSTRAINT [PK_" + type.Name + "s] PRIMARY KEY CLUSTERED ([Id] ASC)," + FK
                        )
                    );
                }

                string table = String.Concat(columns.ToArray());
                string query = String.Format(_partialProcs["CreateTable"], collectionType.Name + "Collection", table);


                DataProvider.ExecuteCmd(() => Connection,
                      query,
                       null,
                       (reader, set) =>
                       {
                           object output = reader.GetValue(0);
                       },
                       null,
                       mod => mod.CommandType = CommandType.Text);

            }
            else
            {
                result = "Id";
            }

            return result;

        }

        private void DropBackupTable(Type type)
        {
            if (CheckIfBackUpExist(type))
            {
                string sqlTemp = _partialProcs["DropTableStatement"];
                string query = String.Format(sqlTemp, "temp" + type.Name);
                object result = null;

                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        result = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = CommandType.Text);
            }
        }

        private void DropProcedures(Type type)
        {
            List<string> classProcs = GetAllProcs(type);

            if (classProcs != null && classProcs.Count > 0)
            {
                foreach (string proc in classProcs)
                {
                    string sqlTemp = _partialProcs["DropProcedureStatement"];
                    string query = String.Format(sqlTemp, proc);
                    object result = null;

                    DataProvider.ExecuteCmd(() => Connection,
                       query,
                        null,
                        (reader, set) =>
                        {
                            result = DataMapper<object>.Instance.MapToObject(reader);
                        },
                        null, mod => mod.CommandType = CommandType.Text);
                }
            }


        }

        private void DropTable(Type type)
        {
            if (CheckIfTableExist(type))
            {
                string sqlTemp = _partialProcs["DropTableStatement"];
                string query = String.Format(sqlTemp, type.Name);

                object result = null;

                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        result = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = CommandType.Text);

                DropProcedures(type);
            }
        }

        private void UpdateRows(Type type)
        {
            if (CheckIfTableExist(type) && CheckIfBackUpExist(type))
            {
                object result = null;
                PropertyInfo[] props = type.GetProperties();
                List<string> oldColumns = GetOldColumns(type);
                List<string> matchingColumns = oldColumns.Where(a => props.Any(b => a == ((ShouldNormalize(b.PropertyType)) ? b.Name + "Id" : b.Name))).ToList();

                string columns = String.Join(", ", matchingColumns);

                string query = _partialProcs["IdentityInsertStatement"].FormatString(type.Name, "ON");
                query += _partialProcs["InsertIntoStatement"].FormatString(type.Name, columns);
                query += _partialProcs["SelectStatement"].FormatString(columns);
                query += _partialProcs["FromStatement"].FormatString("temp" + type.Name);

                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        result = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = CommandType.Text);
            }

        }

        #endregion

        #region Queries To Read

        private List<string> GetOldColumns(Type type)
        {
            string query = _partialProcs["GetAllColumns"].FormatString("temp" + type.Name);
            List<string> list = null;

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    string column = DataMapper<string>.Instance.MapToObject(reader);
                    if (list == null) { list = new List<string>(); }
                    list.Add(column);
                },
                null, mod => mod.CommandType = CommandType.Text);


            return list;

        }

        private List<string> GetAllProcs(Type type)
        {
            string query = _partialProcs["GetAllProcs"];
            List<string> list = null;

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    string proc = DataMapper<string>.Instance.MapToObject(reader);
                    if (list == null) { list = new List<string>(); }
                    list.Add(proc);
                },
                null, mod => mod.CommandType = CommandType.Text);


            List<string> result = list?.Where(a => a.Contains(type.Name)).ToList();

            return result;
        }

        private bool CheckIfEnumIsCurrent(Type type)
        {
            bool result = false;
            Dictionary<int, string> currentEnums = type.ToDictionary(),
                                    dbEnums = null;

            DataProvider.ExecuteCmd(() => Connection,
                "SELECT * FROM {0}s".FormatString(type.Name), null,
                (reader, set) =>
                {
                    if (dbEnums == null)
                        dbEnums = new Dictionary<int, string>();

                    dbEnums.Add(reader.GetSafeInt32(0), reader.GetSafeString(1));

                }, null, cmd => cmd.CommandType = CommandType.Text);

            if (currentEnums.Count == dbEnums.Count) { result = true; }

            return result;
        }

        private bool CheckIfTableExist(Type type)
        {
            string sqlTemp = _partialProcs["CheckIfTableExist"];
            string query = String.Format(sqlTemp, (type.IsCollection()) ? type.Name + "Collection" : type.Name);

            int isTrue = 0;
            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    isTrue = reader.GetSafeInt32(0);
                },
                null, mod => mod.CommandType = CommandType.Text);

            if (isTrue == 1) { return true; }

            return false;
        }

        private bool CheckIfBackUpExist(Type type)
        {
            string sqlTemp = _partialProcs["CheckIfTableExist"];
            string query = String.Format(sqlTemp, "temp" + type.Name);

            int isTrue = 0;
            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    isTrue = reader.GetSafeInt32(0);
                },
                null, mod => mod.CommandType = CommandType.Text);

            if (isTrue == 1) { return true; }

            return false;
        }

        private bool CheckIfTypeIsCurrent(Type type)
        {
            bool result = true;

            if (!CheckIfTableExist(type)) { result = false; }
            else if (!type.IsEnum)
            {
                List<PropertyInfo> baseProps = type.GetProperties().ToList();
                List<string> columnsInTable = DataProvider.GetSchema(() => Connection, type.Name + 's');
                Func<PropertyInfo, bool> predicate = (a) =>
                {
                    bool _result = false;
                    _result = columnsInTable.Any(b => b == (!(ShouldNormalize(a.PropertyType)) ? a.Name : a.PropertyType.IsCollection() ? a.PropertyType.GetTypeOfT().Name + "CollectionId" : a.Name + "Id"));

                    return _result;
                };


                List<PropertyInfo> excludedProps = baseProps.GetPropertiesByAttribute<NotMappedAttribute>(type);
                List<PropertyInfo> includedProps = (excludedProps.Count > 0) ? baseProps.Where(a => excludedProps.Any(b => b.Name != a.Name)).ToList() : baseProps;
                List<PropertyInfo> matchingProps = includedProps.Where(a => columnsInTable.Any(b => b == ((ShouldNormalize(a.PropertyType)) ? a.Name + "Id" : a.Name))).ToList();

                if (matchingProps.Count != includedProps.Count)
                {
                    result = false;
                }

            }
            else
            {
                result = CheckIfEnumIsCurrent(type);
            }

            return result;

        }
        #endregion

        #region Public Methods

        public List<T> GetAll()
        {
            List<T> list = null;
            DataProvider.ExecuteCmd(() => Connection, "dbo." + _type.Name + "s_SelectAll",
                null,
                (reader, set) =>
                {
                    T chart = DataMapper<T>.Instance.MapToObject(reader);
                    if (list == null) { list = new List<T>(); }
                    list.Add(chart);
                });
            return list;
        }

        public void Delete(object id)
        {
            DataProvider.ExecuteNonQuery(() => Connection, "dbo." + _type.Name + "s_Delete",
                param => param.Add(new SqlParameter(typeof(T).GetProperties()[0].Name, typeof(T).GetProperties()[0].GetValue(id))));
        }

        public T Get(object id)
        {
            T chart = default(T);
            DataProvider.ExecuteCmd(() => Connection, "dbo." + _type.Name + "s_SelectById",
                param => param.Add(new SqlParameter(typeof(T).GetProperties()[0].Name, typeof(T).GetProperties()[0].GetValue(id))),
                (reader, set) =>
                {
                    chart = DataMapper<T>.Instance.MapToObject(reader);
                });
            return chart;
        }

        public object Insert(T model)
        {
            object id = 0;
            DataProvider.ExecuteCmd(() => Connection, "dbo." + _type.Name + "s_Insert",
                       param =>
                       {
                           foreach (var prop in typeof(T).GetProperties())
                           {

                               if (prop.Name != "Id")
                               {
                                   if (prop.PropertyType.BaseType.Name == nameof(Enum))
                                   {
                                       param.Add(new SqlParameter(prop.Name, (int)prop.GetValue(model)));

                                   }
                                   else
                                   {
                                       param.Add(new SqlParameter(prop.Name, prop.GetValue(model)));
                                   }
                               }
                           }
                       },
                      (reader, set) =>
                      {
                          id = DataMapper<object>.Instance.MapToObject(reader);
                      });
            return id;
        }

        public void Update(T model)
        {
            DataProvider.ExecuteNonQuery(() => Connection, "dbo." + _type.Name + "s_Update",
                       param =>
                       {
                           param.Add(new SqlParameter(typeof(T).GetProperties()[0].Name, typeof(T).GetProperties()[0].GetValue(model)));
                           foreach (var prop in typeof(T).GetProperties())
                           {
                               param.Add(new SqlParameter(prop.Name, prop.GetValue(model)));
                           }
                       }
                       );
        }
        #endregion

    }

    public class DBService<T, IdType> : SqlService, IDBService<T, IdType>
    {
        public DBService() : base()
        {
            try
            {
                SetUp();

                if (!CheckIfTableExist(_type))
                {
                    CreateTable(_type);
                }
                else if (!CheckIfTypeIsCurrent(_type))
                {
                    UpdateTable(_type);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DBService(string connectionKey) : base(connectionKey)
        {

            try
            {
                SetUp();

                if (!CheckIfTableExist(_type))
                {
                    CreateTable(_type);
                }
                else if (!CheckIfTypeIsCurrent(_type))
                {
                    UpdateTable(_type);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public Type[] TablesAccessed { get { return GetTablesAccessed(); } }

        private bool _tableCreation = false;
        private bool _procedureCreation = false;
        private Type _type = typeof(T);
        private Dictionary<string, string> _partialProcs = new Dictionary<string, string>();


        #region Internal Logic

        private void SetUp()
        {
            _partialProcs.Add("GetAllColumns", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}s'");
            _partialProcs.Add("GetAllProcs", "SELECT NAME FROM dbo.sysobjects WHERE(type = 'P')");
            _partialProcs.Add("CheckIfTableExist", "Declare @IsTrue int = 0 IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}s') Begin Set @IsTrue = 1 End Select @IsTrue");
            _partialProcs.Add("InsertProcedure", "CREATE Proc [dbo].[{0}s_Insert] {1} As Begin Declare @NewId {2} Insert Into dbo.{3}s({4}) Values({5}) Set @NewId = SCOPE_IDENTITY() Select @NewId End");
            _partialProcs.Add("UpdateProcedure", "CREATE Proc [dbo].[{0}s_Update] {1} As Begin {2} End");
            _partialProcs.Add("DeleteProcedure", "CREATE Proc [dbo].[{0}s_Delete] @{1} {2} As Begin Delete {0}s Where {1} = @{1} {3} End");
            _partialProcs.Add("SelectProcedure", "CREATE Proc [dbo].[{0}s_Select{5}] {3} AS Begin SELECT {1} FROM [dbo].[{0}s] {2} {4} End");
            _partialProcs.Add("NullCheckForUpdatePartial", "If @{2} Is Not Null Begin Update dbo.{0} s {1} End ");
            _partialProcs.Add("CreateTable", "Declare @isTrue int = 0 Begin CREATE TABLE [dbo].[{0}s] ( {1} ); IF EXISTS(SELECT* FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}s') Begin Set @IsTrue = 1 End End Select @IsTrue");
            _partialProcs.Add("CreateColumn", "[{0}] {1} {2}");


            _partialProcs.Add("SelectStatement", " SELECT {0}");
            _partialProcs.Add("FromStatement", " FROM [dbo].[{0}s]");
            _partialProcs.Add("InsertStatement", " INSERT INTO dbo.{0}s({1})");
            _partialProcs.Add("ValuesStatement", " Values({2})");
            _partialProcs.Add("CopyTableStatement", "SELECT {2} INTO {1}s FROM {0}s");
            _partialProcs.Add("IfStatement", " IF {0} BEGIN {1} END");
            _partialProcs.Add("ElseStatement", " ELSE BEGIN {0} END");
            _partialProcs.Add("ElseIfStatement", " ELSE IF BEGIN {0} END");
            _partialProcs.Add("DeclareStatement", " DECLARE {0} {1} = {2}");
            _partialProcs.Add("DeleteRowsStatement", " DELETE {0}s");
            _partialProcs.Add("DropTableStatement", " DROP TABLE {0}s");
            _partialProcs.Add("DropProcedureStatement", " DROP PROCEDURE {0}");
            _partialProcs.Add("WhereStatement", " WHERE {0} BEGIN {1} END");
            _partialProcs.Add("CountStatement", " COUNT({0})");
            _partialProcs.Add("GroupByStatement", " GROUP BY {0}");

        }

        private bool ShouldNormalize(Type type)
        {
            return ((type.BaseType == typeof(Enum) || type.IsClass) && (type != typeof(String) && type != typeof(Char))) ? true : false;
        }

        private string DeterminSQLType(Type type)
        {
            string statement = null;
            if ((type.BaseType.Name == nameof(Enum) || type.IsClass) && (type.Name != nameof(String) && type.Name != nameof(Char)))
            {
                statement = "INT";
            }
            else
            {
                switch (type.Name)
                {
                    case nameof(String):
                        statement = "NVARCHAR (2000)";
                        break;

                    case nameof(Int16):
                        statement = "SMALLINT";
                        break;

                    case nameof(Int32):
                        statement = "INT";
                        break;

                    case nameof(Boolean):
                        statement = "BIT";
                        break;

                    case nameof(DateTime):
                        statement = "DATETIME2 (7)" + ((_tableCreation) ? "CONSTRAINT[DF_" + type.DeclaringType.Name + "s_" + type.Name + "] DEFAULT(getutcdate())" : "");
                        break;

                    default:
                        statement = "NVARCHAR (MAX)";
                        break;
                }
            }

            return statement;
        }

        private Type[] GetTablesAccessed()
        {
            List<Type> result = new List<Type>()
            {
                _type
            };
            PropertyInfo[] types = _type.GetProperties().Where(a => ShouldNormalize(a.PropertyType)).ToArray();
            foreach (PropertyInfo prop in types)
            {
                result.Add(prop.PropertyType);
            }
            return result.ToArray();
        }

        private void UpdateTable(Type type)
        {
            foreach (Type table in TablesAccessed)
            {
                CreateBackupTable(type);

                DropTable(table);

                CreateTable(type);

                UpdateRows(type);

                DropBackupTable(type);
            }
        }
        #endregion

        #region Queries 

        private List<string> GetAllColumns(Type type)
        {
            string query = _partialProcs["GetAllColumns"].FormatString(type.Name);
            List<string> list = null;

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    string column = DataMapper<string>.Instance.MapToObject(reader);
                    if (list == null) { list = new List<string>(); }
                    list.Add(column);
                },
                null, mod => mod.CommandType = CommandType.Text);


            return list;

        }

        private List<string> GetAllProcs(Type type)
        {
            string query = _partialProcs["GetAllProcs"];
            List<string> list = null;

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    string proc = DataMapper<string>.Instance.MapToObject(reader);
                    if (list == null) { list = new List<string>(); }
                    list.Add(proc);
                },
                null, mod => mod.CommandType = CommandType.Text);


            List<string> result = list.Where(a => a.Contains(type.Name)).ToList();

            return result;
        }

        private void DropBackupTable(Type type)
        {
            string sqlTemp = _partialProcs["DropTableStatement"];
            string query = String.Format(sqlTemp, "temp" + type.Name);
            object result = null;

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    result = DataMapper<object>.Instance.MapToObject(reader);
                },
                null, mod => mod.CommandType = CommandType.Text);
        }

        private void DropProcedures(Type type)
        {
            List<string> classProcs = GetAllProcs(type);

            foreach (string proc in classProcs)
            {
                string sqlTemp = _partialProcs["DropProcedureStatement"];
                string query = String.Format(sqlTemp, proc);
                object result = null;

                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        result = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = CommandType.Text);
            }


        }

        private void DropTable(Type type)
        {
            string sqlTemp = _partialProcs["DropTableStatement"];
            string query = String.Format(sqlTemp, type.Name + 's');

            object result = null;

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    result = DataMapper<object>.Instance.MapToObject(reader);
                },
                null, mod => mod.CommandType = CommandType.Text);

            DropProcedures(_type);
        }

        private bool CheckIfEnumIsCurrent(Type type)
        {
            bool result = false;
            Dictionary<int, string> currentEnums = type.ToDictionary(),
                                    dbEnums = null;

            DataProvider.ExecuteCmd(() => Connection,
                "SELECT * FROM {0}s".FormatString(type.Name), null,
                (reader, set) =>
                {
                    if (dbEnums == null)
                        dbEnums = new Dictionary<int, string>();

                    dbEnums.Add(reader.GetSafeInt32(0), reader.GetSafeString(1));

                }, null, cmd => cmd.CommandType = CommandType.Text);

            if (currentEnums.Count == dbEnums.Count) { result = true; }

            return result;
        }

        private bool CheckIfTableExist(Type type)
        {
            string sqlTemp = _partialProcs["CheckIfTableExist"];
            string query = String.Format(sqlTemp, type.Name);

            int isTrue = 0;
            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    isTrue = reader.GetSafeInt32(0);
                },
                null, mod => mod.CommandType = CommandType.Text);

            if (isTrue == 1) { return true; }

            return false;
        }

        private bool CheckIfTypeIsCurrent(Type type)
        {
            bool result = true;
            List<PropertyInfo> baseProps = type.GetProperties().ToList();
            List<string> columnsInTable = DataProvider.GetSchema(() => Connection, type.Name + 's');
            Func<PropertyInfo, bool> predicate = (a) =>
            {
                bool _result = false;
                _result = columnsInTable.Any(b => b == a.Name);


                if (a.PropertyType.BaseType == typeof(Enum))
                    _result = CheckIfEnumIsCurrent(a.PropertyType);


                return _result;
            };


            List<PropertyInfo> excludedProps = baseProps.GetPropertiesByAttribute<NotMappedAttribute>(type);
            List<PropertyInfo> includedProps = (excludedProps.Count > 0) ? baseProps.Where(a => excludedProps.Any(b => b.Name != a.Name)).ToList() : baseProps;
            List<PropertyInfo> matchingProps = includedProps.Where(predicate).ToList();

            if (matchingProps.Count != includedProps.Count)
            {
                result = false;
            }

            return result;
        }

        private void CreateBackupTable(Type type)
        {
            string sqlTemp = _partialProcs["CopyTableStatement"];
            string query = String.Format(sqlTemp, type.Name, "temp" + type.Name, "*");
            object result = null;

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    result = DataMapper<object>.Instance.MapToObject(reader);
                },
                null, mod => mod.CommandType = CommandType.Text);

        }

        private void CreateProcedures(Type type)
        {
            _procedureCreation = true;

            string sqlInsertTemp = _partialProcs["InsertProcedure"],
                   sqlUpdateTemp = _partialProcs["UpdateProcedure"],
                   sqlSelectTemp = _partialProcs["SelectProcedure"],
                   sqlDeleteTemp = _partialProcs["DeleteProcedure"],
                   sqlUpdateNullCheckTemp = _partialProcs["NullCheckForUpdatePartial"];


            string[] temps = { sqlInsertTemp, sqlSelectTemp, sqlSelectTemp, sqlUpdateTemp, sqlDeleteTemp };

            for (int x = 0; x < temps.Length; x++)
            {

                string query = null;

                string inputParams = null,
                       columns = null,
                       values = null,
                       select = null,
                       joins = null,
                       deletes = null,
                       updatesParams = null;

                List<string> inputs = new List<string>(),
                             colm = new List<string>(),
                             val = new List<string>(),
                             sel = new List<string>(),
                             jns = new List<string>(),
                             dels = new List<string>(),
                             updtParams = new List<string>(),
                             innerUpdt = new List<string>();

                if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                {
                    PropertyInfo[] props = type.GetProperties();

                    for (int i = 0; i < props.Length; i++)
                    {
                        string PK = (props[i].PropertyType.BaseType == typeof(Enum) ? "Id" : props[i].Name);

                        if (i > 0)
                        {
                            inputs.Add("@" + props[i].Name + " " + DeterminSQLType(props[i].PropertyType) + (i == props.Length - 1 ? "" : ","));

                            colm.Add(props[i].Name +
                                (/*(props[i].PropertyType.BaseType.Name == nameof(Enum) || props[i].PropertyType.IsClass) && (props[i].PropertyType.Name != nameof(String) && props[i].PropertyType.Name != nameof(Char))*/
                                ShouldNormalize(props[i].PropertyType) ? "Id" : "") + (i == props.Length - 1 ? "" : ","));

                            val.Add("@" + props[i].Name + (i == props.Length - 1 ? "" : ","));
                        }

                        updtParams.Add("@" + props[i].Name + DeterminSQLType(props[i].PropertyType) + (i == 0 ? "" : " = NULL") + (i == props.Length - 1 ? "" : ","));

                        innerUpdt.Add("SET " + props[i].Name +
                            (/*(props[i].PropertyType.BaseType.Name == nameof(Enum) || props[i].PropertyType.IsClass) && (props[i].PropertyType.Name != nameof(String) && props[i].PropertyType.Name != nameof(Char))*/
                            ShouldNormalize(props[i].PropertyType) ? "Id" : "") + " = @" + props[i].Name + " WHERE " + type.Name + "s." + props[0].Name + " = @" + props[0].Name);


                        if (/*(props[i].PropertyType.BaseType.Name == nameof(Enum) || props[i].PropertyType.IsClass) && (props[i].PropertyType.Name != nameof(String) && props[i].PropertyType.Name != nameof(Char))*/ShouldNormalize(props[i].PropertyType))
                        {
                            jns.Add("Inner Join " + props[i].PropertyType.Name + "s AS " + props[i].Name + "Id On " + props[i].Name + "Id." + (props[i].PropertyType.BaseType == typeof(Enum) ? "Id" : props[i].Name + "Id") + " = " + type.Name + "s." + props[i].Name + "Id");

                            if (!props[i].PropertyType.Namespace.Contains("System") && props[i].PropertyType.BaseType != typeof(Enum))
                            {
                                dels.Add("Delete " + props[i].Name + "s Where " + PK + " = (Select " + PK + " From " + type.Name + " Where " + PK + " = @" + PK + ")");
                            }
                        }

                        sel.Add(type.Name + "s.[" + props[i].Name + (/*(props[i].PropertyType.BaseType.Name == nameof(Enum) || props[i].PropertyType.IsClass) && (props[i].PropertyType.Name != nameof(String) && props[i].PropertyType.Name != nameof(Char))*/
                            ShouldNormalize(props[i].PropertyType) ? "Id" : "") + "]" + (i == props.Length - 1 ? " " : ","));
                    }

                    inputParams = String.Join(" ", inputs.ToArray());
                    columns = String.Join(" ", colm.ToArray());
                    values = String.Join(" ", val.ToArray());
                    select = String.Join(" ", sel.ToArray());
                    joins = String.Join(" ", jns.ToArray());
                    deletes = String.Join(" ", dels.ToArray());
                    updatesParams = String.Join(" ", updtParams.ToArray());

                    if (temps[x] == sqlInsertTemp)
                    {
                        query = String.Format(temps[x], type.Name, inputParams, DeterminSQLType(props[0].PropertyType), type.Name, columns, values);
                    }
                    else if (temps[x] == sqlSelectTemp)
                    {
                        if (x == 1)
                        {
                            query = String.Format(temps[x], type.Name, select, joins, "", "", "All");
                        }
                        else
                        {
                            query = String.Format(temps[x], type.Name, select, joins, "@" + props[0].Name + " " + DeterminSQLType(props[0].PropertyType/*, type.Name*/), "Where " + type.Name + "s." + props[0].Name + " = @" + props[0].Name, "ById");
                        }
                    }
                    else if (temps[x] == sqlUpdateTemp)
                    {

                        string innerQuery = null;
                        for (int q = 1; q < innerUpdt.Count; q++)
                        {
                            innerQuery += String.Format(sqlUpdateNullCheckTemp, type.Name, innerUpdt[q], props[q].Name);
                        }

                        query = String.Format(temps[x], type.Name, "@Id INT, " + inputParams, innerQuery);
                    }
                    else if (temps[x] == sqlDeleteTemp)
                    {

                        query = String.Format(temps[x], type.Name, props[0].Name, DeterminSQLType(props[0].PropertyType/*, type.Name*/), deletes);
                    }


                }
                else if (type.BaseType == typeof(Enum))
                {

                    inputs.Add("@Value " + DeterminSQLType(typeof(string)));
                    updtParams.Add("@Id " + DeterminSQLType(typeof(int)) + ", @Value " + DeterminSQLType(typeof(string)) + " = Null");
                    innerUpdt.Add("SET Value = @Value WHERE " + type.Name + "s.Id = @Id");
                    colm.Add("Value");
                    val.Add("@Value");
                    sel.Add(type.Name + "s.[Id], " + type.Name + "s.[Value]");


                    inputParams = String.Join(" ", inputs.ToArray());
                    columns = String.Join(" ", colm.ToArray());
                    values = String.Join(" ", val.ToArray());
                    select = String.Join(" ", sel.ToArray());
                    joins = String.Join(" ", jns.ToArray());
                    deletes = String.Join(" ", dels.ToArray());
                    updatesParams = String.Join(" ", updtParams.ToArray());

                    if (temps[x] == sqlInsertTemp)
                    {
                        query = String.Format(temps[x], type.Name, inputParams, DeterminSQLType(typeof(int)/*, type.Name*/), type.Name, columns, values);
                    }
                    else if (temps[x] == sqlSelectTemp)
                    {
                        if (x == 1)
                        {
                            query = String.Format(temps[x], type.Name, select, "", "", "", "All");
                        }
                        else
                        {
                            query = String.Format(temps[x], type.Name, select, "", "@Id " + DeterminSQLType(typeof(int)/*, type.Name*/), "Where " + type.Name + "s.Id = @Id", "ById");
                        }
                    }
                    else if (temps[x] == sqlUpdateTemp)
                    {

                        string innerQuery = null;
                        for (int num = 0; num < innerUpdt.Count; num++)
                        {

                            innerQuery += String.Format(sqlUpdateNullCheckTemp, type.Name, innerUpdt[num], "Value");
                        }

                        query = String.Format(temps[x], type.Name, " @Id INT, " + inputParams, innerQuery);
                    }
                    else if (temps[x] == sqlDeleteTemp)
                    {

                        query = String.Format(temps[x], type.Name, "Id", DeterminSQLType(typeof(int)/*, type.Name*/), deletes);
                    }
                }


                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        object id = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = System.Data.CommandType.Text);
            }

            _procedureCreation = false;

        }

        private Dictionary<string, string> CreateTable(Type type)
        {
            _tableCreation = true;

            Dictionary<string, string> result = null;
            List<string> columns = new List<string>();
            List<string> FKs = new List<string>();

            if (CheckIfTableExist(type))
            {
                result = new Dictionary<string, string>();
                result.Add("Name", type.Name + "s");

                if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                {
                    result.Add("PK", type.GetProperties()[0].Name);
                }
                else if (type.BaseType == typeof(Enum))
                {
                    result.Add("PK", "Id");
                }
                return result;
            }
            else if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
            {
                PropertyInfo[] props = type.GetProperties();
                string endingTable = "NOT NULL, CONSTRAINT [PK_" + type.Name + "s] PRIMARY KEY CLUSTERED ([" + props[0].Name + "] ASC)";


                foreach (var item in props)
                {
                    string FK = null;

                    columns.Add(
                        String.Format(
                            _partialProcs["CreateColumn"],
                           /* ((item.PropertyType.BaseType.Name == nameof(Enum) || item.PropertyType.IsClass) && (item.PropertyType.Name != nameof(String) && item.PropertyType.Name != nameof(Char)))*/ ShouldNormalize(item.PropertyType) ? item.Name + "Id" : item.Name,
                            DeterminSQLType(item.PropertyType),
                            props[0] == item ? "IDENTITY (1, 1) NOT NULL, " : props[props.Length - 1] == item ? endingTable + "," + String.Join(", ", FKs.ToArray()) : "NOT NULL, ")
                        );


                    if (/*item.PropertyType.IsClass && (item.PropertyType.Name != nameof(String) && item.PropertyType.Name != nameof(Char)) || item.PropertyType.BaseType.Name == nameof(Enum)*/ShouldNormalize(item.PropertyType))
                    {
                        Dictionary<string, string> normalizedTbl = CreateTable(item.PropertyType);
                        FK = "CONSTRAINT [FK_" + type.Name + "s_" + item.Name + "] FOREIGN KEY ([" + item.Name + "Id]) REFERENCES [dbo].[" + normalizedTbl["Name"] + "] ([" + normalizedTbl["PK"] + "])";
                        FKs.Add(FK);
                    }
                }
            }
            else if (type.BaseType == typeof(Enum))
            {
                string endingTable = "NOT NULL, CONSTRAINT [PK_" + type.Name + "s] PRIMARY KEY CLUSTERED ([Id] ASC)";
                for (int i = 0; i < 2; i++)
                {
                    columns.Add(
                        String.Format(
                            _partialProcs["CreateColumn"],
                            i == 0 ? "Id" : "Value",
                            DeterminSQLType(i == 0 ? typeof(int) : typeof(string)),
                            i == 0 ? "IDENTITY (1, 1) NOT NULL, " : endingTable)
                     );
                }
            }


            string table = String.Concat(columns.ToArray());
            string query = String.Format(_partialProcs["CreateTable"], type.Name, table);



            int isTrue = 0;
            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    isTrue = reader.GetSafeInt32(0);
                },
                null,
                mod => mod.CommandType = System.Data.CommandType.Text);


            if (isTrue == 1)
            {
                CreateProcedures(type);
                result = new Dictionary<string, string>();
                result.Add("Name", type.Name + "s");

                if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                {
                    result.Add("PK", type.GetProperties()[0].Name);
                }
                else if (type.BaseType == typeof(Enum))
                {
                    var fields = type.GetFields();
                    result.Add("PK", "Id");

                    for (int i = 1; i < fields.Length; i++)
                    {
                        DataProvider.ExecuteNonQuery(() => Connection,
                                        "dbo." + type.Name + "s_Insert",
                                        (param) => param.Add(new SqlParameter("Value", fields[i].Name)),
                                        null);
                    }
                }
            }


            _tableCreation = false;
            return result;
        }

        private void UpdateRows(Type type)
        {
            object result = null;
            PropertyInfo[] props = type.GetProperties();
            List<string> columns = new List<string>();

            for (int i = 0; i < props.Length; i++)
            {
                if (i > 0)
                {
                    columns.Add(props[i].Name + (ShouldNormalize(props[i].PropertyType) ? "Id" : "") + (i == props.Length - 1 ? "" : ","));
                }
            }

            string inputs = String.Join(" ", columns.ToArray());

            List<string> oldColumns = GetAllColumns(type);
            string query = _partialProcs["InsertStatement"].FormatString(type.Name, inputs);
            query += _partialProcs["SelectStatement"].FormatString("*") + _partialProcs["FromStatement"].FormatString("temp" + type.Name);

            DataProvider.ExecuteCmd(() => Connection,
               query,
                null,
                (reader, set) =>
                {
                    result = DataMapper<object>.Instance.MapToObject(reader);
                },
                null, mod => mod.CommandType = CommandType.Text);

        }

        #endregion

        public List<T> GetAll()
        {
            List<T> list = null;
            DataProvider.ExecuteCmd(() => Connection, "dbo." + _type.Name + "s_SelectAll",
                null,
                (reader, set) =>
                {
                    T chart = DataMapper<T>.Instance.MapToObject(reader);
                    if (list == null) { list = new List<T>(); }
                    list.Add(chart);
                });
            return list;
        }

        public void Delete(IdType id)
        {
            DataProvider.ExecuteNonQuery(() => Connection, "dbo." + _type.Name + "s_Delete",
                param => param.Add(new SqlParameter(typeof(T).GetProperties()[0].Name, typeof(T).GetProperties()[0].GetValue(id))));
        }

        public T Get(IdType id)
        {
            T chart = default(T);
            DataProvider.ExecuteCmd(() => Connection, "dbo." + _type.Name + "s_SelectById",
                param => param.Add(new SqlParameter(typeof(T).GetProperties()[0].Name, typeof(T).GetProperties()[0].GetValue(id))),
                (reader, set) =>
                {
                    chart = DataMapper<T>.Instance.MapToObject(reader);
                });
            return chart;
        }

        public IdType Insert(T model)
        {
            IdType id = default(IdType);
            DataProvider.ExecuteCmd(() => Connection, "dbo." + _type.Name + "s_Insert",
                       param =>
                       {
                           foreach (var prop in typeof(T).GetProperties())
                           {

                               if (prop.Name != "Id")
                               {
                                   if (prop.PropertyType.BaseType.Name == nameof(Enum))
                                   {
                                       param.Add(new SqlParameter(prop.Name, (int)prop.GetValue(model)));

                                   }
                                   else
                                   {
                                       param.Add(new SqlParameter(prop.Name, prop.GetValue(model)));
                                   }
                               }
                           }
                       },
                      (reader, set) =>
                      {
                          id = DataMapper<IdType>.Instance.MapToObject(reader);
                      });
            return id;
        }

        public void Update(T model)
        {
            DataProvider.ExecuteNonQuery(() => Connection, "dbo." + _type.Name + "s_Update",
                       param =>
                       {
                           param.Add(new SqlParameter(typeof(T).GetProperties()[0].Name, typeof(T).GetProperties()[0].GetValue(model)));
                           foreach (var prop in typeof(T).GetProperties())
                           {
                               param.Add(new SqlParameter(prop.Name, prop.GetValue(model)));
                           }
                       }
                       );
        }

    }
}