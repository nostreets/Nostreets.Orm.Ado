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
using NostreetsExtensions.Utilities;
using System.Data.Common;
using System.Data.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;

namespace NostreetsORM
{
    public class DBService : SqlService, IDBService
    {
        public DBService(Type type) : base()
        {
            try
            {
                if (NeedsIdProp(type))
                    throw new Exception("type's first public property needs to be an type of int named Id to be managed by DBService...");

                if (!ShouldNormalize(type))
                    throw new Exception("type's needs to be a custom class to be managed by DBService...");

                _type = type;
                SetUp();

                bool doesExist = CheckIfTableExist(_type),
                     isCurrent = TablesAccessed.All(a => CheckIfTypeIsCurrent(a.Key));

                if (!doesExist)
                {
                    DeleteCreationScraps();
                    CreateTable(_type);
                }
                else if (!isCurrent)
                {
                    KeyValuePair<Type, Type[]>[] tablesToUpdate = TablesAccessed.Where(a => !CheckIfTypeIsCurrent(a.Key)).ToArray();

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        BackupAndDropType(tbl.Key);

                    CreateTable(_type);

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        UpdateTableFromBackup(tbl.Key);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DBService(Type type, string connectionKey) : base(connectionKey)
        {

            try
            {
                if (NeedsIdProp(type))
                    throw new Exception("type's first public property needs to be an type of int named Id to be managed by DBService...");

                if (!ShouldNormalize(type))
                    throw new Exception("type's needs to be a custom class to be managed by DBService...");

                _type = type;
                SetUp();

                bool doesExist = CheckIfTableExist(_type),
                     isCurrent = TablesAccessed.All(a => CheckIfTypeIsCurrent(a.Key));

                if (!doesExist)
                {
                    DeleteCreationScraps();
                    CreateTable(_type);
                }
                else if (!isCurrent)
                {
                    KeyValuePair<Type, Type[]>[] tablesToUpdate = TablesAccessed.Where(a => !CheckIfTypeIsCurrent(a.Key)).ToArray();

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        BackupAndDropType(tbl.Key);

                    CreateTable(_type);

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        UpdateTableFromBackup(tbl.Key);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DBService(Type type, bool nullLock) : base()
        {
            try
            {
                if (NeedsIdProp(type))
                    throw new Exception("type's first public property needs to be an type of int named Id to be managed by DBService...");

                if (!ShouldNormalize(type))
                    throw new Exception("type's needs to be a custom class to be managed by DBService...");

                _type = type;
                _nullLock = nullLock;

                SetUp();

                bool doesExist = CheckIfTableExist(_type),
                     isCurrent = TablesAccessed.All(a => CheckIfTypeIsCurrent(a.Key));

                if (!doesExist)
                {
                    DeleteCreationScraps();
                    CreateTable(_type);
                }
                else if (!isCurrent)
                {
                    KeyValuePair<Type, Type[]>[] tablesToUpdate = TablesAccessed.Where(a => !CheckIfTypeIsCurrent(a.Key)).ToArray();

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        BackupAndDropType(tbl.Key);

                    CreateTable(_type);

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        UpdateTableFromBackup(tbl.Key);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public DBService(Type type, string connectionKey, bool nullLock) : base(connectionKey)
        {

            try
            {
                if (NeedsIdProp(type))
                    throw new Exception("type's first public property needs to be an type of int named Id to be managed by DBService...");

                if (!ShouldNormalize(type))
                    throw new Exception("type's needs to be a custom class to be managed by DBService...");


                _type = type;
                _nullLock = nullLock;

                SetUp();

                bool doesExist = CheckIfTableExist(_type),
                     isCurrent = TablesAccessed.All(a => CheckIfTypeIsCurrent(a.Key));

                if (!doesExist)
                {
                    DeleteCreationScraps();
                    CreateTable(_type);
                }
                else if (!isCurrent)
                {
                    KeyValuePair<Type, Type[]>[] tablesToUpdate = TablesAccessed.Where(a => !CheckIfTypeIsCurrent(a.Key)).ToArray();

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        BackupAndDropType(tbl.Key);

                    CreateTable(_type);

                    foreach (KeyValuePair<Type, Type[]> tbl in tablesToUpdate)
                        UpdateTableFromBackup(tbl.Key);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public Dictionary<Type, Type[]> TablesAccessed
        {
            get
            {
                Dictionary<Type, Type[]> subTables = GetSubTablesAccessed();
                subTables = subTables.Prepend(_type, GetRelationships(_type));
                return subTables;
            }
        }

        public Func<DataContext> DBContext { get => () => new DataContext(Connection); }

        private Dictionary<string, string> ProcTemplates
        {
            get
            {
                return new Dictionary<string, string>
                {
                    { "Insert",  _partialProcs["InsertWithNewIDProcedure"]},
                    { "InsertWithID",  _partialProcs["InsertWithIDProcedure"]},
                    { "Update",  _partialProcs["UpdateProcedure"]},
                    { "SelectAll",  _partialProcs["SelectProcedure"]},
                    { "SelectBy",  _partialProcs["SelectProcedure"]},
                    { "Delete",  _partialProcs["DeleteProcedure"]}

                };
            }
        }

        private bool _tableCreation = false,
                     _nullLock = false;
        private int _tableLayer = 0;
        private Type _type = null;
        private Dictionary<string, string> _partialProcs = new Dictionary<string, string>();

        #region Internal Logic

        private void SetUp()
        {
            _partialProcs.Add("InsertWithNewIDProcedure", "CREATE Proc [dbo].[{0}_Insert] {1} As Begin Declare @NewId {2} Insert Into [dbo].{0}({3}) Values({4}) Set @NewId = SCOPE_IDENTITY() Select @NewId End");
            _partialProcs.Add("InsertWithIDProcedure", "CREATE Proc [dbo].[{0}_Insert] {1} As Begin Insert Into [dbo].{0}({2}) Values({3}) End");
            _partialProcs.Add("UpdateProcedure", "CREATE Proc [dbo].[{0}_Update] {1} As Begin {2} End");
            _partialProcs.Add("DeleteProcedure", "CREATE Proc [dbo].[{0}_Delete] @{1} {2} As Begin Delete {0} Where {1} = @{1} {3} End");
            _partialProcs.Add("SelectProcedure", "CREATE Proc [dbo].[{0}_Select{5}] {3} AS Begin SELECT {1} FROM [dbo].[{0}] {2} {4} End");
            _partialProcs.Add("NullCheckForUpdatePartial", "If @{2} Is Not Null Begin Update [dbo].{0} {1} End ");


            _partialProcs.Add("GetPKOfTable", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{0}'");
            _partialProcs.Add("GetAllColumns", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}'");
            _partialProcs.Add("GetAllProcs", "SELECT NAME FROM [dbo].[sysobjects] WHERE(type = 'P')");
            _partialProcs.Add("CheckIfTableExist", "Declare @IsTrue int = 0 IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}') Begin Set @IsTrue = 1 End Select @IsTrue");
            _partialProcs.Add("CreateTableType", "CREATE TYPE [dbo].[{0}] AS TABLE( {1} )");
            _partialProcs.Add("CreateTable", "Declare @isTrue int = 0 Begin CREATE TABLE [dbo].[{0}] ( {1} ); IF EXISTS(SELECT* FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}') Begin Set @IsTrue = 1 End End Select @IsTrue");
            _partialProcs.Add("CreateColumn", "[{0}] {1} {2}");


            _partialProcs.Add("SelectStatement", " SELECT {0}");
            _partialProcs.Add("FromStatement", " FROM [dbo].[{0}]");
            _partialProcs.Add("InsertIntoStatement", " INSERT INTO [dbo].[{0}]({1})");
            _partialProcs.Add("ValuesStatement", " Values({2})");
            _partialProcs.Add("CopyTableStatement", "SELECT {2} INTO {1} FROM {0}");
            _partialProcs.Add("IfStatement", " IF {0} BEGIN {1} END");
            _partialProcs.Add("ElseStatement", " ELSE BEGIN {0} END");
            _partialProcs.Add("ElseIfStatement", " ELSE IF BEGIN {0} END");
            _partialProcs.Add("DeclareStatement", " DECLARE {0} {1} = {2}");
            _partialProcs.Add("DeleteRowsStatement", " DELETE {0}");
            _partialProcs.Add("DropTableStatement", " DROP TABLE {0}");
            _partialProcs.Add("DropTableTypeStatement", " DROP TYPE [dbo].[{0}]");
            _partialProcs.Add("DropProcedureStatement", " DROP PROCEDURE {0}");
            _partialProcs.Add("WhereStatement", " WHERE {0}");
            _partialProcs.Add("BeginEndStatement", " BEGIN {1} END");
            _partialProcs.Add("CountStatement", " COUNT({0})");
            _partialProcs.Add("GroupByStatement", " GROUP BY {0}");
            _partialProcs.Add("PrimaryKeyStatement", "PRIMARY KEY CLUSTERED ([{0}] ASC)");
            _partialProcs.Add("IdentityInsertStatement", " SET IDENTITY_INSERT [dbo].[{0}] {1}");

        }

        private bool ShouldNormalize(Type type)
        {
            //return ((type != typeof(String) && type != typeof(Char)) && (!type.IsSystemType() || type.IsCollection() && !type.GetTypeOfT().IsSystemType()) && (type.BaseType == typeof(Enum) || type.IsClass)) ? true : false;

            return (type.IsSystemType())
                  ? false
                  : (type.IsCollection())
                  ? false
                  : (type.IsClass || type.IsEnum)
                  ? true
                  : false;
        }

        private bool NeedsIdProp(Type type)
        {
            return !type.IsClass ? false : type.GetProperties()[0].PropertyType != typeof(int) ? true : type.GetProperties()[0].Name.ToLower().Contains("id") ? false : true;
        }

        private string GetTableName(Type type, string prefix = null)
        {
            string result = null;
            if (!type.IsCollection())
            {
                result = (type.Name.IsPlural()) ? type.Name + "es" : type.Name + "s";
            }
            else
            {
                result = type.GetTypeOfT().Name + "Collections";
            }

            return (prefix != null) ? prefix + result : result;
        }

        private object GetNormalizedSchema(Type type, string prefix = null)
        {
            List<string> propNames = new List<string>();
            List<Type> propTypes = new List<Type>();

            if (!type.IsCollection())
            {
                if (NeedsIdProp(type))
                    type = type.AddProperty(typeof(int), "Id");

                PropertyInfo[] baseProps = type.GetProperties();

                foreach (PropertyInfo prop in baseProps)
                {
                    if (ShouldNormalize(prop.PropertyType))
                    {
                        propNames.Add(prop.Name + "Id");
                        propTypes.Add(typeof(int));
                    }
                    else if (!prop.PropertyType.IsCollection())
                    {
                        propNames.Add(prop.Name);
                        propTypes.Add(prop.PropertyType);
                    }
                }
            }
            else
            {
                if (prefix[prefix.Length - 1] == '_')
                    prefix = prefix.Remove(prefix.Length - 1);

                Type collectionType = type.GetTypeOfT();
                propNames.AddRange(new[] { prefix + "Id", collectionType.Name + "Id" });
                propTypes.AddRange(new[] { typeof(int), typeof(int) });
            }
            return ClassBuilder.CreateObject(type.Name, propNames.ToArray(), propTypes.ToArray());

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
                        statement = "DATETIME2 (7)" + ((_tableCreation) ? "CONSTRAINT[DF_" + GetTableName(type.DeclaringType) + "_" + type.Name + "] DEFAULT(getutcdate())" : "");
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
            List<PropertyInfo> relations = type.GetProperties().Where(a => ShouldNormalize(a.PropertyType) || (a.PropertyType.IsCollection() && !a.PropertyType.GetTypeOfT().IsSystemType())).ToList();

            if (relations != null && relations.Count > 0)
            {
                foreach (PropertyInfo prop in relations)
                {
                    Type propType = prop.PropertyType;

                    if (list == null)
                        list = new List<Type>();

                    list.Add(propType);
                }

                result = list?.Distinct().ToArray();
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

        private void BackupAndDropType(Type type)
        {
            List<KeyValuePair<Type, Type[]>> tblsToBackUp = TablesAccessed.Where(a => (a.Value != null && a.Value.Any(b => b == type))).ToList() ?? null;
            Type[] typeRelations = (TablesAccessed.Keys.Contains(type)) ? TablesAccessed[type] : null;



            if (tblsToBackUp != null && tblsToBackUp.Count > 0)
                foreach (KeyValuePair<Type, Type[]> tbl in tblsToBackUp)
                    if (!CheckIfBackUpExist(tbl.Key))
                        BackupAndDropType(tbl.Key);



            if (typeRelations != null && typeRelations.Any(a => a.IsCollection()))
                foreach (Type list in typeRelations.Where(a => a.IsCollection()))
                {
                    CreateBackupTable(list, type.Name + "_");
                    DropTable(list, type.Name + "_");
                }


            if (!type.IsEnum)
                CreateBackupTable(type);


            DropTable(type);
        }

        private void UpdateTableFromBackup(Type type)
        {
            if (type.IsEnum)
                return;

            Type[] typeRelations = (TablesAccessed[type] == null) ? TablesAccessed[type] : TablesAccessed[type].Where(a => !a.IsCollection()).ToArray(),
                   collectionRelations = (TablesAccessed[type] == null) ? null : TablesAccessed[type].Where(a => a.IsCollection()).ToArray();

            if (typeRelations != null)
                foreach (Type tbl in typeRelations)
                    UpdateTableFromBackup(tbl);


            UpdateRows(type);

            if (collectionRelations != null && collectionRelations.Length > 0)
            {
                foreach (Type collection in collectionRelations)
                {
                    UpdateTableFromBackup(collection.GetTypeOfT());
                    UpdateRows(collection, type.Name + "_");
                }
            }

        }

        private string GetProcsForCollection(Type type, string prefix, KeyValuePair<string, string> template)
        {
            if (!type.IsCollection())
                throw new Exception("type has to implement IEnumerable...");

            if (prefix == null)
                throw new Exception("prefix cannot be null...");

            Type collType = type.GetTypeOfT();
            string skimmedPrefix = prefix.Remove(prefix.Length - 1),
                   query = null,
                   inputParams = null,
                   columns = null,
                   values = null,
                   select = null;

            inputParams = "@{2}Id {1}, @{0}Id {1}".FormatString(collType.Name, DeterminSQLType(typeof(int)), skimmedPrefix);
            columns = "[{1}Id], [{0}Id] ".FormatString(collType.Name, skimmedPrefix);
            values = "@{1}Id, @{0}Id ".FormatString(collType.Name, skimmedPrefix);
            select = "{0}.[{2}Id], {0}.[{1}Id]".FormatString(GetTableName(type, prefix), collType.Name, skimmedPrefix);

            switch (template.Key)
            {
                case "InsertWithID":
                    query = String.Format(template.Value, GetTableName(type, prefix), inputParams, columns, values);
                    break;

                case "SelectAll":
                    query = String.Format(template.Value, GetTableName(type, prefix), select, "", "", "", "All");
                    break;

                case "Delete":
                    query = String.Format(template.Value, GetTableName(type, prefix), skimmedPrefix + "Id", DeterminSQLType(typeof(int)), "");
                    break;

            }

            return query;
        }

        private string GetProcsForEnum(Type type, KeyValuePair<string, string> template)
        {
            if (type.BaseType != typeof(Enum))
                throw new Exception("type's BaseType has to be typeof(Enum)...");

            string query = null;

            string inputParams = null,
                   columns = null,
                   values = null,
                   select = null;
            //joins = null,
            //deletes = null,
            //updatesParams = null;

            inputParams = "@Value " + DeterminSQLType(typeof(string));
            columns = "Value";
            values = "@Value";
            select = GetTableName(type) + ".[Id], " + GetTableName(type) + ".[Value]";
            //updatesParams = "@Id " + DeterminSQLType(typeof(int)) + ", @Value " + DeterminSQLType(typeof(string)) + " = Null";


            switch (template.Key)
            {
                case "Insert":
                    query = String.Format(template.Value, GetTableName(type), inputParams, DeterminSQLType(typeof(int)), columns, values);
                    break;

                case "Update":
                    string innerQuery = String.Format(_partialProcs["NullCheckForUpdatePartial"], GetTableName(type), "SET Value = @Value WHERE " + GetTableName(type) + ".Id = @Id", "Value");
                    query = String.Format(template.Value, GetTableName(type), " @Id INT, " + inputParams, innerQuery);
                    break;

                case "SelectAll":
                    query = String.Format(template.Value, GetTableName(type), select, "", "", "", "All");
                    break;

                case "SelectBy":
                    query = String.Format(template.Value, GetTableName(type), select, "", "@Id " + DeterminSQLType(typeof(int)), "Where " + GetTableName(type) + ".Id = @Id", "ById");
                    break;
            }

            return query;
        }

        private string GetProcsForClass(Type type, KeyValuePair<string, string> template)
        {
            if (!ShouldNormalize(type))
                throw new Exception("type's Type has to be a custom data type...");

            if (type.IsEnum)
                return GetProcsForEnum(type, template);



            List<int> skippedProps = new List<int>(); ;
            string query = null;

            string inputParams = null,
                   columns = null,
                   values = null,
                   select = null,
                   joins = null,
                   delete = null;

            List<string> inputs = new List<string>(),
                         colm = new List<string>(),
                         val = new List<string>(),
                         sel = new List<string>(),
                         jns = new List<string>(),
                         dels = new List<string>(),
                         innerUpdt = new List<string>();

            PropertyInfo[] props = type.GetProperties();

            for (int i = 0; i < props.Length; i++)
            {
                string PK = (props[i].PropertyType.IsEnum || NeedsIdProp(props[i].PropertyType) ? "Id" : GetPKOfTable(props[i].PropertyType));


                if (props[i].PropertyType.IsCollection())
                {
                    skippedProps.Add(i);
                    continue;
                }

                if (i > 0)
                {
                    inputs.Add("@" + props[i].Name + " "
                        + DeterminSQLType(props[i].PropertyType)
                        + ((props[props.Length - 1] == props[i] || (props[props.Length - 1] == props[i + 1] && props[i + 1].PropertyType.IsCollection()))
                        ? "" : ",")
                    );


                    colm.Add('[' +
                        ((ShouldNormalize(props[i].PropertyType)
                            ? props[i].Name + "Id"
                            : props[i].Name))

                            + ((props[props.Length - 1] == props[i] || (props[props.Length - 1] == props[i + 1] && props[i + 1].PropertyType.IsCollection()))
                                ? "]" : "],")
                    );


                    val.Add(
                        "@" + props[i].Name
                        + ((props[props.Length - 1] == props[i] || (props[props.Length - 1] == props[i + 1] && props[i + 1].PropertyType.IsCollection()))
                        ? "" : ",")
                    );


                    innerUpdt.Add(
                        "SET [" +
                        ((ShouldNormalize(props[i].PropertyType)
                                ? props[i].Name + "Id"
                                : props[i].Name))
                         + "] = @" + props[i].Name
                         + " WHERE " + GetTableName(type) + "."
                         + props[0].Name + " = @" + props[0].Name
                    );

                }
                else
                    skippedProps.Add(i);


                if (ShouldNormalize(props[i].PropertyType))
                {
                    jns.Add(
                        "Inner Join " + GetTableName(props[i].PropertyType)
                        + " AS _" + props[i].Name
                        + " On _" + props[i].Name + "." +
                        (props[i].PropertyType.IsEnum || NeedsIdProp(props[i].PropertyType)
                            ? "Id"
                            : GetPKOfTable(props[i].PropertyType))
                        + " = " + GetTableName(type) + "."
                        + props[i].Name + "Id"
                    );



                    //dels.Add(
                    //    "Delete " + props[i].Name
                    //    + " Where " +
                    //    (props[i].PropertyType.IsEnum || NeedsIdProp(props[i].PropertyType)
                    //        ? "Id"
                    //        : GetPKOfTable(props[i].PropertyType))
                    //    + " = (Select " + props[i].Name + "Id"
                    //    + " From " + GetTableName(type)
                    //    + " Where " + PK + " = @" + PK + ")");
                }

                sel.Add(
                    GetTableName(type) + ".[" +
                    ((ShouldNormalize(props[i].PropertyType)
                            ? props[i].Name + "Id"
                            : props[i].Name))
                    + "]"
                    + ((props[props.Length - 1] == props[i] || (props[props.Length - 1] == props[i + 1] && props[i + 1].PropertyType.IsCollection()))
                        ? " "
                        : ",")
                );
            }


            if (dels.Count > 0)
                dels.Reverse();


            inputParams = String.Join(" ", inputs.ToArray());
            columns = String.Join(" ", colm.ToArray());
            values = String.Join(" ", val.ToArray());
            select = String.Join(" ", sel.ToArray());
            joins = String.Join(" ", jns.ToArray());
            delete = "DELETE {0} WHERE {1} = @{1} "; //String.Join(" ", dels.ToArray());


            switch (template.Key)
            {
                case "Insert":
                    query = String.Format(template.Value, GetTableName(type), inputParams, DeterminSQLType(props[0].PropertyType), columns, values);
                    break;

                case "Update":
                    string innerQuery = null;
                    for (int i = 0, x = 0, y = 0; i < props.Length; i++)
                    {
                        x = y + i;
                        if (skippedProps.Count > 0 && skippedProps.Any(a => a == i))
                        {
                            y--;
                            continue;
                        }

                        innerQuery += String.Format(_partialProcs["NullCheckForUpdatePartial"], GetTableName(type), innerUpdt[x], props[i].Name);
                    }

                    query = String.Format(template.Value, GetTableName(type), "@{0} INT, ".FormatString(props[0].Name) + inputParams, innerQuery);
                    break;

                case "SelectAll":
                    query = String.Format(template.Value, GetTableName(type), select, joins, "", "", "All");
                    break;

                case "SelectBy":
                    query = String.Format(template.Value, GetTableName(type), select, joins, '@' + props[0].Name + " " + DeterminSQLType(props[0].PropertyType), "Where " + GetTableName(type) + '.' + props[0].Name + " = @" + props[0].Name, "ById");
                    break;

                case "Delete":
                    query = String.Format(template.Value, GetTableName(type), props[0].Name, DeterminSQLType(props[0].PropertyType), "");
                    break;
            }

            return query;
        }

        private string GetCreateTableQuery(Type type)
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
                                : "NOT NULL, CONSTRAINT [PK_" + GetTableName(type) + "] PRIMARY KEY CLUSTERED ([Id] ASC)")
                     );
                }
            }
            else if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
            {
                List<string> FKs = new List<string>();
                PropertyInfo[] props = type.GetProperties();


                for (int i = 0; i < props.Length; i++)
                {
                    string FK = null;


                    if (ShouldNormalize(props[i].PropertyType))
                    {
                        string normalizedTblPK = CreateTable(props[i].PropertyType);

                        FK = "CONSTRAINT [FK_" + GetTableName(type) + "_" + props[i].Name + "] FOREIGN KEY ([" + props[i].Name + "Id]) REFERENCES [dbo].[" + GetTableName(props[i].PropertyType) + "] ([" + normalizedTblPK + "])";

                        if (FK != null)
                            FKs.Add(FK);
                    }
                    else if (props[i].PropertyType.IsCollection() && !props[i].PropertyType.GetTypeOfT().IsSystemType())
                    {
                        CreateTable(props[i].PropertyType.GetTypeOfT());
                        continue;
                    }
                    else if (props[i].PropertyType.IsCollection())
                        continue;


                    columns.Add(
                            String.Format(
                                _partialProcs["CreateColumn"],

                                !ShouldNormalize(props[i].PropertyType)
                                    ? props[i].Name
                                    : props[i].Name + "Id",

                                DeterminSQLType(props[i].PropertyType),

                                (props[0] == props[i])
                                    ? "IDENTITY (1, 1) NOT NULL, "
                                    : "{0}NULL, ".FormatString((_nullLock || ShouldNormalize(props[i].PropertyType)) ? "NOT " : "")
                            )
                        );
                }

                columns.Add("CONSTRAINT [PK_" + GetTableName(type) + "] PRIMARY KEY CLUSTERED ([" + props[0].Name + "] ASC)," + String.Join(", ", FKs.ToArray()));
            }



            string table = String.Concat(columns.ToArray());
            string query = String.Format(_partialProcs["CreateTable"], GetTableName(type), table);

            return query;
        }

        private string GetCreateIntermaiateTableQuery(Type parentClass, Type collection)
        {
            if (!parentClass.GetProperties().Any(a => a.PropertyType == collection))
                throw new Exception("parentClass does not have any properties of the collection Type");

            List<string> columns = new List<string>();
            Type listType = collection.GetTypeOfT();


            string PK = CreateTable(listType);
            string FKs = " CONSTRAINT [FK_" + GetTableName(collection) + "_" + GetTableName(listType) + "] FOREIGN KEY ([" + listType.Name + "Id]) REFERENCES [dbo].[" + GetTableName(listType) + "] ([" + PK + "])";
            FKs += ", CONSTRAINT [FK_" + GetTableName(parentClass) + "_" + GetTableName(collection) + "] FOREIGN KEY ([" + parentClass.Name + "Id]) REFERENCES [dbo].[" + GetTableName(parentClass) + "] ([" + PK + "])";



            for (int i = 0; i < 2; i++)
            {
                columns.Add(
                    String.Format(
                        _partialProcs["CreateColumn"],
                        i == 0 ? parentClass.Name + "Id" : listType.Name + "Id",
                        DeterminSQLType(typeof(int)),
                        "NOT NULL, " + ((i == 0) ? "" : FKs)
                    )
                );
            }

            string table = String.Concat(columns.ToArray());
            string query = String.Format(_partialProcs["CreateTable"], GetTableName(collection, parentClass.Name + '_'), table);

            return query;
        }

        private void DeleteCreationScraps()
        {

            DropTable(_type);
            DropBackupTable(_type);
            DropProcedures(_type);

            List<Type> tbls = TablesAccessed.Keys.ToList();
            for (int i = 0; i < tbls.Count; i++)
            {
                if (tbls[i].GetProperties().Length > 0 && tbls[i].GetProperties().Any(a => a.PropertyType.IsCollection()))
                {
                    PropertyInfo[] collection = tbls[i].GetProperties().Where(a => a.PropertyType.IsCollection()).ToArray();
                    foreach (PropertyInfo prop in collection)
                    {
                        if (!prop.PropertyType.GetTypeOfT().IsSystemType())
                        {
                            DropTable(prop.PropertyType, tbls[i].Name + '_');
                            DropBackupTable(prop.PropertyType, tbls[i].Name + '_');
                            DropProcedures(prop.PropertyType, tbls[i].Name + '_');
                        }

                        if (tbls.Contains(prop.PropertyType))
                            tbls.Remove(prop.PropertyType);
                    }
                }

                DropTable(tbls[i]);
                DropBackupTable(tbls[i]);
                DropProcedures(tbls[i]);
            }
        }

        #endregion

        #region Internal Writes

        private void UpdateRows(Type type, string prefix = null)
        {
            if (CheckIfBackUpExist(type, prefix) && CheckIfTableExist(type, prefix))
            {
                object result = null;
                string query = null;

                if (type.IsCollection())
                {
                    string prefixed = prefix.Remove(prefix.Length - 1, 1);

                    query = _partialProcs["InsertIntoStatement"].FormatString(GetTableName(type, prefix), "{0}Id, {1}Id".FormatString(prefixed, type.GetTypeOfT().Name));
                    query += _partialProcs["SelectStatement"].FormatString("{0}Id, {1}Id".FormatString(prefixed, type.GetTypeOfT().Name));
                    query += _partialProcs["FromStatement"].FormatString("temp" + GetTableName(type, prefix));
                }
                else
                {
                    List<PropertyInfo> baseProps = type.GetProperties().ToList();
                    List<PropertyInfo> excludedProps = baseProps.GetPropertiesByAttribute<NotMappedAttribute>(type);
                    excludedProps.AddRange(baseProps.Where(a => a.PropertyType.IsCollection()));
                    List<PropertyInfo> includedProps = (excludedProps.Count > 0) ? baseProps.Where(a => !excludedProps.Contains(a)).ToList() : baseProps;

                    List<string> oldColumns = GetOldColumns(type);
                    List<string> matchingColumns = oldColumns.Where(a => includedProps.Any(b => a == ((ShouldNormalize(b.PropertyType)) ? b.Name + "Id" : b.Name))).ToList();


                    string columns = String.Join(", ", matchingColumns);

                    query = _partialProcs["IdentityInsertStatement"].FormatString(GetTableName(type), "ON");
                    query += _partialProcs["InsertIntoStatement"].FormatString(GetTableName(type), columns);
                    query += _partialProcs["SelectStatement"].FormatString(columns);
                    query += _partialProcs["FromStatement"].FormatString("temp" + GetTableName(type));


                }

                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        result = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = CommandType.Text);


                DropBackupTable(type, prefix);
            }

        }

        private void AddEnumsAsRows(Type type)
        {
            if (type.BaseType != typeof(Enum))
                throw new Exception("type's BaseType has to be a Enum...");

            var fields = type.GetFields();
            for (int i = 1; i < fields.Length; i++)
            {
                DataProvider.ExecuteNonQuery(() => Connection,
                                "dbo." + GetTableName(type) + "_Insert",
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
                string query = GetCreateTableQuery(type);
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


                if (isTrue != 1)
                    throw new Exception("{0} Table Creation was not successful...".FormatString(type.Name));


                CreateIntermaiateTables(type);
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
            else
            {

                if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                {
                    result = type.GetProperties()[0].Name;

                }
                else if (type.IsEnum)
                {
                    result = "Id";
                }



            }

            --_tableLayer;
            _tableCreation = (_tableLayer == 0) ? false : true;

            return result;
        }

        private void CreateIntermaiateTables(Type type)
        {
            ++_tableLayer;

            if (type.GetProperties().Length > 0 && type.GetProperties().Any(a => a.PropertyType.IsCollection()))
            {
                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (!prop.PropertyType.IsCollection())
                        continue;

                    if (prop.PropertyType.GetTypeOfT().IsSystemType())
                        continue;

                    if (!CheckIfTableExist(type))
                        throw new Exception("{0} has to be a table in the database to make an intermediate table between the two...".FormatString(type.Name));


                    int isTrue = 0;
                    string query = GetCreateIntermaiateTableQuery(type, prop.PropertyType);


                    DataProvider.ExecuteCmd(() => Connection,
                          query,
                           null,
                           (reader, set) =>
                           {
                               isTrue = reader.GetSafeInt32(0);
                           },
                           null,
                           mod => mod.CommandType = CommandType.Text);


                    if (isTrue != 1)
                        throw new Exception("Intermediate Table Create between {0} and {1} was not successful...".FormatString(type.Name, prop.PropertyType.Name));


                    CreateProcedures(prop.PropertyType, type.Name + "_");
                }
            }

            --_tableLayer;

            //return result;

        }

        private void CreateBackupTable(Type type, string prefix = null)
        {
            if (CheckIfTableExist(type, prefix) && !CheckIfBackUpExist(type, prefix))
            {
                string query = _partialProcs["CopyTableStatement"].FormatString(GetTableName(type, prefix), "temp" + GetTableName(type, prefix), "*");
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

        private void CreateProcedures(Type type, string prefix = null)
        {
            foreach (KeyValuePair<string, string> template in ProcTemplates)
            {

                string query = null;

                if (type.IsCollection())
                {
                    if (template.Key == "Insert" || template.Key == "Update" || template.Key == "SelectBy")
                        continue;

                    query = GetProcsForCollection(type, prefix, template);
                }
                else if (type.BaseType == typeof(Enum))
                {
                    if (template.Key == "Delete" || template.Key == "InsertWithID")
                        continue;

                    query = GetProcsForEnum(type, template);
                }
                else if (type.IsClass && (type != typeof(String) || type != typeof(Char)))
                {
                    if (template.Key == "InsertWithID")
                        continue;

                    query = GetProcsForClass(type, template);

                }


                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        object id = DataMapper<object>.Instance.MapToObject(reader);
                    },
                    null, mod => mod.CommandType = CommandType.Text);
            }


        }

        private void DropBackupTable(Type type, string prefix = null)
        {
            if (CheckIfBackUpExist(type, prefix))
            {
                string sqlTemp = _partialProcs["DropTableStatement"];
                string query = String.Format(sqlTemp, "temp" + GetTableName(type, prefix));
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

        private void DropProcedures(Type type, string prefix = null)
        {
            List<string> classProcs = GetAllProcs(type, prefix);

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

        private void DropTable(Type type, string prefix = null)
        {
            if (CheckIfTableExist(type, prefix))
            {
                string sqlTemp = _partialProcs["DropTableStatement"];
                string query = String.Format(sqlTemp, GetTableName(type, prefix));

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

        #endregion

        #region Internal Reads

        private string GetPKOfTable(Type type)
        {
            if (type.IsEnum || NeedsIdProp(type))
                return "Id";


            string result = null;
            if (CheckIfTableExist(type))
            {
                string query = _partialProcs["GetPKOfTable"].FormatString(GetTableName(type));


                DataProvider.ExecuteCmd(() => Connection,
                   query,
                    null,
                    (reader, set) =>
                    {
                        result = reader.GetString(0);
                    },
                    null, mod => mod.CommandType = CommandType.Text);

            }

            return result;
        }

        private List<string> GetOldColumns(Type type)
        {
            string query = _partialProcs["GetAllColumns"].FormatString("temp" + GetTableName(type));
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

        private bool CheckIfEnumIsCurrent(Type type)
        {
            bool result = false;
            Dictionary<int, string> currentEnums = type.ToDictionary(),
                                    dbEnums = null;

            DataProvider.ExecuteCmd(() => Connection,
                "SELECT * FROM {0}".FormatString(GetTableName(type)), null,
                (reader, set) =>
                {
                    if (dbEnums == null)
                        dbEnums = new Dictionary<int, string>();

                    dbEnums.Add(reader.GetSafeInt32(0), reader.GetSafeString(1));

                }, null, cmd => cmd.CommandType = CommandType.Text);

            if (currentEnums.Count == dbEnums.Count) { result = true; }

            return result;
        }

        private bool CheckIfTypeIsCurrent(Type type)
        {
            if (type.IsCollection())
                return true;

            else if (!CheckIfTableExist(type))
                return false;

            else if (type.IsEnum)
                return CheckIfEnumIsCurrent(type);

            else
            {
                List<string> columnsInTable = DataProvider.GetSchema(() => Connection, GetTableName(type));
                List<PropertyInfo> baseProps = type.GetProperties().ToList();


                List<PropertyInfo> excludedProps = baseProps.GetPropertiesByAttribute<NotMappedAttribute>(type);
                excludedProps.AddRange(baseProps.Where(a => a.PropertyType.IsCollection()));

                List<PropertyInfo> includedProps = (excludedProps.Count > 0) ? baseProps.Where(a => !excludedProps.Contains(a)).ToList() : baseProps;

                if (NeedsIdProp(type))
                {
                    for (int i = 0; i < includedProps.Count; i++)
                        if (includedProps[i].Name == "Id")
                            includedProps.Remove(includedProps[i]);

                    includedProps.Prepend(type.AddProperty(typeof(int), "Id").GetProperty("Id"));
                }


                foreach (string col in columnsInTable)
                    if (!includedProps.Any(a => ((ShouldNormalize(a.PropertyType)) ? a.Name + "Id" : a.Name) == col))
                        return false;


                foreach (PropertyInfo prop in includedProps)
                    if (!columnsInTable.Any(a => ((ShouldNormalize(prop.PropertyType)) ? prop.Name + "Id" : prop.Name) == a))
                        return false;



                if (includedProps.Any(a => ShouldNormalize(a.PropertyType)))
                {
                    PropertyInfo[] propsToCheck = includedProps.Where(a => ShouldNormalize(a.PropertyType)).DistinctBy(a => a.PropertyType).ToArray();
                    foreach (PropertyInfo propToCheck in propsToCheck)
                        if (!CheckIfTypeIsCurrent(propToCheck.PropertyType))
                            return false;
                }


                if (excludedProps.Any(a => a.PropertyType.IsCollection() && !a.PropertyType.GetTypeOfT().IsSystemType()))
                {
                    PropertyInfo[] propsToCheck = excludedProps.Where(a => a.PropertyType.IsCollection() && !a.PropertyType.GetTypeOfT().IsSystemType()).Distinct().ToArray();
                    foreach (PropertyInfo propToCheck in propsToCheck)
                    {
                        Type listType = propToCheck.PropertyType.GetTypeOfT();

                        if (!CheckIfTypeIsCurrent(listType))
                            return false;

                        else if (!CheckIfTableExist(propToCheck.PropertyType, type.Name + "_"))
                            return false;

                    }

                }
            }

            return true;

        }

        private List<string> GetAllProcs(Type type, string prefix = null)
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


            List<string> result = list?.Where(a => a.Contains(GetTableName(type, prefix))).ToList();

            return result;
        }

        private bool CheckIfTableExist(Type type, string prefix = null)
        {
            string sqlTemp = _partialProcs["CheckIfTableExist"];
            string query = String.Format(sqlTemp, GetTableName(type, prefix));

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

        private bool CheckIfBackUpExist(Type type, string prefix = null)
        {
            string sqlTemp = _partialProcs["CheckIfTableExist"];
            string query = String.Format(sqlTemp, "temp" + GetTableName(type, prefix));

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

        #endregion

        #region Private Acess Methods

        private List<object> GetAll(Type type, ref Dictionary<Type, List<object>> tblEntities, string prefix = null)
        {
            if (tblEntities == null)
                tblEntities = new Dictionary<Type, List<object>>();

            List<object> entities = null;
            Type tableType = GetNormalizedSchema(type, prefix).GetType();
            Dictionary<Type, List<object>> tableObjs = tblEntities;


            DataProvider.ExecuteCmd(() => Connection, "dbo." + GetTableName(type, prefix) + "_SelectAll",
                null,
                (reader, set) =>
                {
                    object tableObj = tableType.Instantiate();
                    tableObj = DataMapper.MapToObject(reader, tableType);

                    if (!tableObjs.Any(a => a.Key == type))
                        tableObjs.Add(type, new List<object>());

                    tableObjs[type].Add(tableObj);
                });


            if (!type.IsCollection())
            {
                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (ShouldNormalize(prop.PropertyType) && !prop.PropertyType.IsEnum)
                        GetAll(prop.PropertyType, ref tblEntities);

                    else if (prop.PropertyType.IsCollection() && !prop.PropertyType.GetTypeOfT().IsSystemType())
                    {
                        GetAll(prop.PropertyType, ref tblEntities, type.Name + "_");
                        GetAll(prop.PropertyType.GetTypeOfT(), ref tblEntities);
                    }
                }

                if (tblEntities.Any(a => a.Key == type))
                {
                    foreach (object tbl in tblEntities[type])
                    {
                        if (entities == null)
                            entities = new List<object>();

                        object entity = InstantateFromIds(type, tbl, tblEntities);

                        entities.Add(entity);
                    }
                }
            }


            return entities;
        }

        private object Get(Type type, object id)
        {
            object result = type.Instantiate();
            object tableObj = GetNormalizedSchema(type);
            Type tableType = tableObj.GetType();



            DataProvider.ExecuteCmd(() => Connection, "dbo." + GetTableName(type) + "_SelectById",
                param => param.Add(new SqlParameter((!type.IsEnum && !NeedsIdProp(type)) ? type.GetProperties()[0].Name : "Id", id)),
                (reader, set) =>
                {
                    tableObj = DataMapper.MapToObject(reader, tableType);
                });


            result = InstantateFromTable(type, tableType);

            return result;
        }

        private void Delete(Type type, object id)
        {
            object result = type.Instantiate();
            PropertyInfo[] baseProps = type.GetProperties();
            object tableObj = GetNormalizedSchema(type);
            Type tableType = tableObj.GetType();



            DataProvider.ExecuteCmd(() => Connection, "dbo." + GetTableName(type) + "_SelectById",
                param => param.Add(new SqlParameter((!type.IsEnum && !NeedsIdProp(type)) ? type.GetProperties()[0].Name : "Id", id)),
                (reader, set) =>
                {
                    tableObj = DataMapper.MapToObject(reader, tableType);
                });



            foreach (PropertyInfo arr in baseProps.Where(a => a.PropertyType.IsCollection() && !a.PropertyType.GetTypeOfT().IsSystemType()))
                DeleteCollection((int)id, type, arr.PropertyType.GetTypeOfT());




            DataProvider.ExecuteNonQuery(() => Connection, "dbo." + GetTableName(type) + "_Delete",
               param => param.Add(new SqlParameter((NeedsIdProp(type)) ? "Id" : type.GetProperties()[0].Name, id)));



            foreach (PropertyInfo prop in baseProps.Where(a => ShouldNormalize(a.PropertyType) && !a.PropertyType.IsEnum))
                Delete(prop.PropertyType, tableObj.GetPropertyValue(prop.Name + "Id"));


        }

        private object Insert(object model, Type type, ref Dictionary<KeyValuePair<Type, Type>, KeyValuePair<object, object[]>> relations)
        {
            if (model == null)
                model = type.Instantiate();

            object id = null;
            Dictionary<Type, object> refs = new Dictionary<Type, object>();
            PropertyInfo[] normalizedProps = type.GetProperties().Where(a =>
                                                             (!a.PropertyType.IsEnum && ShouldNormalize(a.PropertyType)
                                                             || (a.PropertyType.IsCollection() && !a.PropertyType.GetTypeOfT().IsSystemType())))
                                                             .ToArray();
            if (normalizedProps.Length > 0)
            {
                foreach (PropertyInfo prop in normalizedProps)
                {
                    if (prop.PropertyType.IsCollection() && !prop.PropertyType.GetTypeOfT().IsSystemType())
                    {
                        object[] arr = (model.GetPropertyValue(prop.Name) == null) ? null : ((IEnumerable<object>)model.GetPropertyValue(prop.Name)).ToArray();

                        if (arr != null && arr.Length > 0)
                        {
                            Type typeInList = arr[0].GetType();
                            List<object> ids = new List<object>();

                            foreach (object item in arr)
                            {
                                object subId = Insert(item, typeInList, ref relations);
                                ids.Add(subId);
                            }

                            relations.Add(new KeyValuePair<Type, Type>(type, typeInList), new KeyValuePair<object, object[]>(0, ids.ToArray()));
                        }
                    }
                    else if (ShouldNormalize(prop.PropertyType) && !prop.PropertyType.IsEnum)
                    {
                        object subId = Insert(model.GetPropertyValue(prop.Name), prop.PropertyType, ref relations);
                        relations.Add(new KeyValuePair<Type, Type>(type, prop.PropertyType), new KeyValuePair<object, object[]>(0, new[] { subId }));
                    }
                }
            }


            foreach (PropertyInfo prop in type.GetProperties())
            {
                if (relations.Any(a => a.Key.Key == type && a.Key.Value == prop.PropertyType))
                {
                    object[] vals = relations.FirstOrDefault(a => a.Key.Key == type && a.Key.Value == prop.PropertyType).Value.Value;
                    if (vals.Length == 1)
                        refs.Add(prop.PropertyType, vals[0]);
                }
            }


            id = Insert(model, type, refs);


            for (int i = 0; i < relations.Count; i++)
            {
                var relation = relations.ElementAt(i);
                if (relation.Key.Key == type)
                {
                    relations[relation.Key] = new KeyValuePair<object, object[]>(id, relation.Value.Value);

                    if (relation.Value.Value.Length > 1)
                        foreach (object val in relation.Value.Value)
                            InsertRelationship(relation.Key.Key, relation.Key.Value, (int)id, (int)val);
                }
            }


            return id;
        }

        private object Insert(object model, Type type, Dictionary<Type, object> ids = null)
        {
            if (ids != null && ids.Values.Any(a => a.GetType().IsCollection()))
                throw new Exception("ids.Values cannot be a collection...");


            if (model.GetType() != type)
                throw new Exception("model Parameter is the wrong type...");


            object id = 0;

            DataProvider.ExecuteCmd(() => Connection, "dbo." + GetTableName(type) + "_Insert",
                       param =>
                       {
                           PropertyInfo[] props = type.GetProperties();

                           foreach (PropertyInfo prop in props)
                           {

                               if (prop.Name == "Id")
                                   continue;

                               else if (prop.PropertyType.IsCollection())
                                   continue;

                               else if (prop.PropertyType.IsEnum)
                                   if (prop.GetValue(model) != null)
                                       param.Add(new SqlParameter(prop.Name, (int)prop.GetValue(model)));
                                   else
                                       throw new Exception("Any property in model that is an Enum cannot be null");

                               else if (ShouldNormalize(prop.PropertyType) && ids.Keys.Any(a => a == prop.PropertyType))
                                   param.Add(new SqlParameter(prop.Name, ids[prop.PropertyType]));

                               else
                               {
                                   object value = null;
                                   if (prop.GetValue(model) != null)
                                       value = prop.GetValue(model);
                                   else
                                       value = DBNull.Value;

                                   param.Add(new SqlParameter(prop.Name, value));
                               }
                           }
                       },
                      (reader, set) =>
                      {
                          id = DataMapper<object>.Instance.MapToObject(reader);
                      });

            return id;
        }

        private void Update(object model, int id, Type type)
        {

            if (model == null)
                model = type.Instantiate();

            object result = type.Instantiate();
            object tableObj = GetNormalizedSchema(type);
            Type tableType = tableObj.GetType();


            DataProvider.ExecuteCmd(() => Connection, "dbo." + GetTableName(type) + "_SelectById",
                param => param.Add(new SqlParameter((!type.IsEnum && !NeedsIdProp(type)) ? type.GetProperties()[0].Name : "Id", id)),
                (reader, set) =>
                {
                    tableObj = DataMapper.MapToObject(reader, tableType);
                });



            DataProvider.ExecuteNonQuery(() => Connection, "dbo." + GetTableName(type) + "_Update",
                      param =>
                      {
                          PropertyInfo[] props = type.GetProperties();

                          if (NeedsIdProp(type))
                              param.Add(new SqlParameter("Id", id));

                          foreach (PropertyInfo prop in props)
                          {
                              if (prop.PropertyType.IsCollection())
                                  continue;

                              else if (prop.PropertyType.IsEnum)
                                  if (prop.GetValue(model) != null)
                                      param.Add(new SqlParameter(prop.Name, (int)prop.GetValue(model)));
                                  else
                                      throw new Exception("Any property in model that is an Enum cannot be null");

                              else if (ShouldNormalize(prop.PropertyType))
                                  param.Add(new SqlParameter(prop.Name, tableObj.GetPropertyValue(prop.Name + "Id")));

                              else
                              {
                                  object value = null;
                                  if (model.GetPropertyValue(prop.Name) != null)
                                      value = model.GetPropertyValue(prop.Name);
                                  else
                                      value = DBNull.Value;

                                  param.Add(new SqlParameter(prop.Name, value));
                              }
                          }
                      });



            foreach (PropertyInfo prop in type.GetProperties())
            {
                if (prop.PropertyType.IsCollection() && prop.PropertyType.GetTypeOfT().IsSystemType())
                    continue;

                else if (ShouldNormalize(prop.PropertyType) && !prop.PropertyType.IsEnum)
                    Update(model.GetPropertyValue(prop.Name), (int)tableObj.GetPropertyValue(prop.Name + "Id"), prop.PropertyType);

                else if (prop.PropertyType.IsCollection())
                {
                    Type listType = prop.PropertyType.GetTypeOfT();
                    object[] list = model.GetPropertyValue(prop.Name) == null ? null : ((IEnumerable<object>)model.GetPropertyValue(prop.Name)).ToArray();
                    int[] ids = GetCollectionIds(id, type, listType);
                    int i = 0;

                    if (ids != null && ids.Length > 0)
                    {
                        foreach (int childId in ids)
                        {
                            if (list == null || list[i] == null)
                                DeleteRelationship(type, listType, id, childId);

                            else
                                Update(list[i], childId, listType);

                            i++;
                        }
                    }

                    if (list != null && list.Length > i)
                        for (; i < list.Length; i++)
                        {
                            object childId = Insert(list[i], listType);
                            InsertRelationship(type, listType, id, (int)childId);
                        }
                }
            }

        }

        private void InsertRelationship(Type parent, Type child, int parentId, int childId)
        {
            string collectionTbl = parent.Name + "_" + child.Name + "Collections";
            Type listType = parent.GetProperties().FirstOrDefault(a => a.PropertyType.IsCollection() && a.PropertyType.GetTypeOfT() == child).PropertyType;

            if (new[] { parent, child, listType }.All(a => CheckIfTableExist(a, (a == listType) ? parent.Name + "_" : null)))
            {

                DataProvider.ExecuteNonQuery(() => Connection, "dbo." + collectionTbl + "_Insert",
                       param =>
                       {
                           for (int i = 0; i < 2; i++)
                           {
                               if (i == 0)
                                   param.Add(new SqlParameter(parent.Name + "Id", parentId));

                               else
                                   param.Add(new SqlParameter(child.Name + "Id", childId));
                           }
                       }, null, null, null);

            }
        }

        private void DeleteRelationship(Type parent, Type child, int parentId, int childId)
        {
            Type listType = parent.GetProperties().FirstOrDefault(a => a.PropertyType.IsCollection() && a.PropertyType.GetTypeOfT() == child).PropertyType;

            string collectionTbl = parent.Name + "_" + child.Name + "Collections",
                   query = _partialProcs["DeleteRowsStatement"].FormatString(collectionTbl)
                         + _partialProcs["WhereStatement"].FormatString("{0}Id = {2} AND {1}Id = {3}".FormatString(parent.Name, child.Name, parentId.ToString(), childId.ToString()));



            if (new[] { parent, child, listType }.All(a => CheckIfTableExist(a, (a == listType) ? parent.Name + "_" : null)))
            {
                DataProvider.ExecuteNonQuery(() => Connection, query,
                       null, null, cmd => cmd.CommandType = CommandType.Text, null);
            }

            Delete(child, childId);
        }

        private List<object> GetMultiple(Type type, int[] ids)
        {
            List<object> entities = null;
            if (ids != null && ids.Length > 0)
            {
                List<object> tableObjs = null;
                Type tableType = GetNormalizedSchema(type).GetType();
                string query = _partialProcs["SelectStatement"].FormatString("*")
                                    + _partialProcs["FromStatement"].FormatString(GetTableName(type))
                                    + _partialProcs["WhereStatement"].FormatString(GetPKOfTable(type)
                                    + " IN (" + String.Join(", ", ids) + ") ");


                DataProvider.ExecuteCmd(() => Connection, query, null,
                    (reader, set) =>
                    {
                        object tableObj = tableType.Instantiate();
                        tableObj = DataMapper.MapToObject(reader, tableType);

                        if (tableObjs == null)
                            tableObjs = new List<object>();

                        tableObjs.Add(tableObj);

                    }, null, cmd => cmd.CommandType = CommandType.Text);



                foreach (object obj in tableObjs)
                {
                    object entity = InstantateFromTable(type, obj);

                    //object entity = type.Instantiate();

                    //if (entities == null)
                    //    entities = new List<object>();

                    //foreach (PropertyInfo prop in type.GetProperties())
                    //{
                    //    if (prop.PropertyType.IsCollection() && prop.PropertyType.GetTypeOfT().IsSystemType())
                    //        continue;

                    //    else if (ShouldNormalize(prop.PropertyType) && !prop.PropertyType.IsEnum)
                    //    {
                    //        object property = Get(prop.PropertyType, obj.GetPropertyValue(prop.Name + "Id"));
                    //        entity.SetPropertyValue(prop.Name, property);
                    //    }
                    //    else if (prop.PropertyType.IsCollection())
                    //    {
                    //        Type listType = prop.PropertyType.GetTypeOfT();

                    //        int[] collectionIds = GetCollectionIds((int)obj.GetPropertyValue(prop.Name + "Id"), type, listType);

                    //        entity.SetPropertyValue(prop.Name, collectionIds);

                    //    }
                    //    else
                    //    {
                    //        object property = obj.GetPropertyValue(prop.Name);
                    //        entity.SetPropertyValue(prop.Name, property);
                    //    }
                    //}

                    entities.Add(entity);
                }

            }


            return entities;
        }

        private void DeleteMultiple(Type type, int[] ids)
        {
            if (ids == null && ids.Length == 0)
                return;

            object result = null;
            List<object> tableObjs = null;
            Type tableType = GetNormalizedSchema(type).GetType();
            PropertyInfo[] baseProps = type.GetProperties();


            string query = _partialProcs["SelectStatement"].FormatString("*")
                            + _partialProcs["FromStatement"].FormatString(GetTableName(type))
                            + _partialProcs["WhereStatement"].FormatString(GetPKOfTable(type)
                            + " IN (" + String.Join(", ", ids) + ") ");
            DataProvider.ExecuteCmd(() => Connection, query, null,
                (reader, set) =>
                {
                    object tableObj = tableType.Instantiate();
                    tableObj = DataMapper.MapToObject(reader, tableType);

                    if (tableObjs == null)
                        tableObjs = new List<object>();

                    tableObjs.Add(tableObj);

                }, null, cmd => cmd.CommandType = CommandType.Text);




            foreach (object item in tableObjs)
                foreach (PropertyInfo arr in baseProps.Where(a => a.PropertyType.IsCollection() && !a.PropertyType.GetTypeOfT().IsSystemType()))
                    DeleteCollection((int)item.GetPropertyValue((NeedsIdProp(arr.PropertyType)) ? "Id" : type.GetProperties()[0].Name), type, arr.PropertyType.GetTypeOfT());




            query = _partialProcs["DeleteRowsStatement"].FormatString(GetTableName(type))
                    + _partialProcs["WhereStatement"].FormatString(GetPKOfTable(type)
                    + " IN (" + String.Join(", ", ids) + ") ");
            DataProvider.ExecuteCmd(() => Connection, query,
                       null,
                       (reader, set) =>
                       {
                           result = reader.GetSafeInt32(0);
                       }, null, cmd => cmd.CommandType = CommandType.Text);



            foreach (object item in tableObjs)
                foreach (PropertyInfo prop in baseProps.Where(a => ShouldNormalize(a.PropertyType) && !a.PropertyType.IsEnum))
                    Delete(prop.PropertyType, item.GetPropertyValue(prop.Name + "Id"));


            //foreach (object item in tableObjs)
            //{
            //    foreach (PropertyInfo prop in type.GetProperties())
            //    {
            //        if (prop.PropertyType.IsCollection() && prop.PropertyType.GetTypeOfT().IsSystemType())
            //            continue;

            //        else if (prop.PropertyType.IsCollection())
            //            DeleteCollection((int)item.GetPropertyValue((NeedsIdProp(prop.PropertyType)) ? "Id" : type.GetProperties()[0].Name), type, prop.PropertyType.GetTypeOfT());

            //        else if (ShouldNormalize(prop.PropertyType) && !prop.PropertyType.IsEnum)
            //            Delete(prop.PropertyType, item.GetPropertyValue(prop.Name + "Id"));
            //    }
            //}

        }

        private int[] GetCollectionIds(int parentId, Type parentType, Type childType)
        {
            List<int> ids = new List<int>();
            string query = _partialProcs["SelectStatement"].FormatString(childType.Name + "Id")
                         + _partialProcs["FromStatement"].FormatString(parentType.Name + "_" + childType.Name + "Collections")
                         + _partialProcs["WhereStatement"].FormatString(parentType.Name + "Id = " + parentId);

            DataProvider.ExecuteCmd(() => Connection, query,
                null,
                (reader, set) =>
                {
                    int id = reader.GetSafeInt32(0);
                    ids.Add(id);
                }, null, cmd => cmd.CommandType = CommandType.Text);


            //result = GetMultiple(childType, ids.Cast<object>().ToArray());

            return ids?.ToArray();
        }

        private void DeleteCollection(int parentId, Type parentType, Type childType)
        {
            object result = null;
            int[] objIds = GetCollectionIds(parentId, parentType, childType);
            string query = _partialProcs["DeleteRowsStatement"].FormatString(parentType.Name + "_" + childType.Name + "Collections")
                         + _partialProcs["WhereStatement"].FormatString(parentType.Name + "Id = " + parentId);



            DataProvider.ExecuteCmd(() => Connection, query,
                       null,
                       (reader, set) =>
                       {
                           result = reader.GetSafeInt32(0);
                       }, null, cmd => cmd.CommandType = CommandType.Text);

            DeleteMultiple(childType, objIds?.ToArray());

        }

        private object InstantateFromTable(Type type, object tblOfType)
        {
            object result = type.Instantiate();

            foreach (PropertyInfo prop in type.GetProperties())
            {
                if (prop.PropertyType.IsCollection() && prop.PropertyType.GetTypeOfT().IsSystemType())
                    continue;

                else if (ShouldNormalize(prop.PropertyType) && !prop.PropertyType.IsEnum)
                {
                    object property = Get(prop.PropertyType, tblOfType.GetPropertyValue(prop.Name + "Id"));
                    result.SetPropertyValue(prop.Name, property);
                }
                else if (prop.PropertyType.IsCollection())
                {
                    Type listType = prop.PropertyType.GetTypeOfT();
                    int[] collectionIds = GetCollectionIds((int)tblOfType.GetPropertyValue(prop.Name + "Id"), type, listType);
                    List<object> collection = GetMultiple(listType, collectionIds);

                    result.SetPropertyValue(prop.Name, collection);
                }
                else
                {
                    object property = tblOfType.GetPropertyValue(prop.Name);
                    result.SetPropertyValue(prop.Name, property);
                }
            }

            return result;
        }

        private object InstantateFromIds(Type type, object tblOfType, Dictionary<Type, List<object>> tblEntities)
        {
            object entity = type.Instantiate();

            if (!type.IsCollection())
            {
                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (prop.PropertyType.IsCollection() && prop.PropertyType.GetTypeOfT().IsSystemType())
                        continue;

                    else if (ShouldNormalize(prop.PropertyType) && !prop.PropertyType.IsEnum)
                    {
                        object rowOfProp = tblEntities[prop.PropertyType]
                                             .FirstOrDefault(a => a.GetPropertyValue((NeedsIdProp(prop.PropertyType)) ? "Id" : prop.PropertyType.GetProperties()[0].Name)
                                             .Equals(tblOfType.GetPropertyValue(prop.Name + "Id")));

                        object property = InstantateFromIds(prop.PropertyType, rowOfProp, tblEntities);

                        entity.SetPropertyValue(prop.Name, property);
                    }
                    else if (prop.PropertyType.IsCollection())
                    {
                        List<object> collection = null;
                        Type listType = prop.PropertyType.GetTypeOfT();

                        if (tblEntities.Any(a => a.Key == prop.PropertyType))
                        {
                            object[] relations = tblEntities[prop.PropertyType]
                                                                .Where(a => a.GetPropertyValue(type.Name + "Id")
                                                                .Equals(tblOfType.GetPropertyValue((NeedsIdProp(prop.PropertyType)) ? "Id" : prop.PropertyType.GetProperties()[0].Name))).ToArray();

                            List<object> rowsOfList = tblEntities[listType].Where(a =>
                                                        relations.Any(b => b.GetPropertyValue(listType.Name + "Id")
                                                        .Equals(a.GetPropertyValue((NeedsIdProp(listType)) ? "Id" : listType.GetProperties()[0].Name)))).ToList();


                            foreach (object item in rowsOfList)
                            {
                                if (collection == null)
                                    collection = new List<object>();

                                object obj = InstantateFromIds(listType, item, tblEntities);

                                collection.Add(obj);
                            }
                        }



                        entity.SetPropertyValue(prop.Name, collection.Cast(listType));
                    }
                    else
                    {
                        object property = tblOfType.GetPropertyValue((prop.PropertyType.IsEnum) ? prop.Name + "Id" : prop.Name);
                        entity.SetPropertyValue(prop.Name, property);
                    }
                }
            }


            return entity;
        }

        #endregion

        #region Public Acess Methods

        public List<object> GetAll()
        {
            Dictionary<Type, List<object>> container = new Dictionary<Type, List<object>>();
            return GetAll(_type, ref container);
        }

        public void Delete(object id)
        {
            if (id.GetType() != typeof(int))
                throw new Exception("id's Type has to be the type of int to be able to Delete...");

            Delete(_type, id);
        }

        public object Get(object id)
        {
            if (id.GetType() != typeof(int))
                throw new Exception("id's Type has to be the type of int to be able to Get...");

            return Get(_type, id);
        }

        public object Insert(object model)
        {
            if (model.GetType() != _type)
                throw new Exception("model's Type has to be the type of T in DBService<T> to be able to Insert...");

            object id = null;
            Dictionary<KeyValuePair<Type, Type>, KeyValuePair<object, object[]>> relations = new Dictionary<KeyValuePair<Type, Type>, KeyValuePair<object, object[]>>();
            id = Insert(model, _type, ref relations);

            //foreach (PropertyInfo p in model.GetType().GetProperties())
            //{
            //    if (!prop.PropertyType.IsCollection())
            //        continue;


            //    KeyValuePair<KeyValuePair<Type, Type>, KeyValuePair<object, object[]>> relateIds = relations.FirstOrDefault(a => a.Key.Key == model.GetType() && a.Key.Value == prop.PropertyType.GetTypeOfT());

            //    if (relateIds.Value.Value != null && relateIds.Value.Value.Length > 1)
            //        foreach (object val in relateIds.Value.Value)
            //            InsertRelationship(relateIds.Key.Key, relateIds.Key.Value, (int)val, (int)relateIds.Value.Key);
            //}


            return id;
        }

        public void Update(object model)
        {
            if (model.GetType() != _type)
                throw new Exception("model's Type has to be the type of T in DBService<T> to be able to Update...");

            if (model.GetPropertyValue("Id") == null || (int)model.GetPropertyValue("Id") == 0)
                throw new Exception("model's Id propery has to equal an PK in the Database to be able to Update...");

            Update(model, (int)model.GetPropertyValue("Id"), _type);
        }

        public IEnumerable<object> Where(Func<object, bool> predicate)
        {
            //using (DataContext context = DBContext())
            //    DbCommand cmd = context.GetCommand(((IEnumerable<object>)context.GetTable(_type)).Where(predicate).AsQueryable());

            return GetAll().Where(predicate);
        }

        #endregion
    }

    public class DBService<T> : IDBService<T> where T : class
    {
        public DBService()
        {
            _baseSrv = new DBService(typeof(T));
        }

        public DBService(string connectionKey)
        {
            _baseSrv = new DBService(typeof(T), connectionKey);
        }

        public DBService(bool nullLock)
        {
            _baseSrv = new DBService(typeof(T), nullLock);
        }

        public DBService(string connectionKey, bool nullLock)
        {
            _baseSrv = new DBService(typeof(T), connectionKey, nullLock);
        }

        private DBService _baseSrv = null;

        public List<T> GetAll()
        {
            List<T> result = null;
            Type listType = null;
            List<object> list = _baseSrv.GetAll();

            if (list != null)
            {
                if (list.Count > 0)
                    listType = list[0].GetType();

                if (listType != typeof(T))
                    throw new Exception("objects in list are not the right Type of entity to access..");

                if (result == null)
                    result = list.Cast<T>().ToList();
                //foreach (object item in list)
                //    result.Add((T)item);
            }

            return result;
        }

        public T Get(object id)
        {
            T result = default(T);
            object item = _baseSrv.Get(id);

            if (item.GetType() != typeof(T))
                throw new Exception("item is not the right Type of entity to access..");

            result = (T)item;

            return result;
        }

        public object Insert(T model)
        {
            return _baseSrv.Insert(model);
        }

        public void Update(T model)
        {
            _baseSrv.Update(model);
        }

        public IEnumerable<T> Where(Func<T, bool> predicate)
        {
            return GetAll().Where(predicate);
        }

        public void Delete(object id)
        {
            _baseSrv.Delete(id);
        }
    }

    public class DBService<T, IdType> : IDBService<T, IdType> where T : class
    {
        public DBService()
        {
            _baseSrv = new DBService(typeof(T));
        }

        public DBService(string connectionKey)
        {
            _baseSrv = new DBService(typeof(T), connectionKey);
        }

        public DBService(bool nullLock)
        {
            _baseSrv = new DBService(typeof(T), nullLock);
        }

        public DBService(string connectionKey, bool nullLock)
        {
            _baseSrv = new DBService(typeof(T), connectionKey, nullLock);
        }

        private DBService _baseSrv = null;

        public T Get(IdType id)
        {
            T result = default(T);
            object item = _baseSrv.Get(id);

            if (item.GetType() != typeof(T))
                throw new Exception("item is not the right Type of entity to access..");

            result = (T)item;

            return result;
        }

        public IdType Insert(T model)
        {
            IdType result = default(IdType);

            object id = _baseSrv.Insert(model);

            if (id.GetType() != typeof(IdType))
                throw new Exception("id is not the right Type...");

            result = (IdType)id;

            return result;
        }

        public void Delete(IdType id)
        {
            _baseSrv.Delete(id);
        }

        public List<T> GetAll()
        {
            List<T> result = null;
            Type listType = null;
            List<object> list = _baseSrv.GetAll();

            if (list != null)
            {
                if (list.Count > 0)
                    listType = list[0].GetType();

                if (listType != typeof(T))
                    throw new Exception("objects in list are not the right Type of entity to access..");

                if (result == null)
                    result = list.Cast<T>().ToList();
                //foreach (object item in list)
                //    result.Add((T)item);
            }

            return result;
        }

        public void Update(T model)
        {
            _baseSrv.Update(model);
        }

        public IEnumerable<T> Where(Func<T, bool> predicate)
        {
            return GetAll().Where(predicate);
        }
    }

    public class DBService<T, IdType, AddType, UpdateType> : IDBService<T, IdType, AddType, UpdateType> where T : class
    {
        public DBService()
        {
            _baseSrv = new DBService(typeof(T));
        }

        public DBService(string connectionKey)
        {
            _baseSrv = new DBService(typeof(T), connectionKey);
        }

        public DBService(bool nullLock)
        {
            _baseSrv = new DBService(typeof(T), nullLock);
        }

        public DBService(string connectionKey, bool nullLock)
        {
            _baseSrv = new DBService(typeof(T), connectionKey, nullLock);
        }

        private DBService _baseSrv = null;

        public T Get(IdType id)
        {
            T result = default(T);
            object item = _baseSrv.Get(id);

            if (item.GetType() != typeof(T))
                throw new Exception("item is not the right Type of entity to access..");

            result = (T)item;

            return result;
        }

        public IdType Insert(T model)
        {
            IdType result = default(IdType);

            object id = _baseSrv.Insert(model);

            if (id.GetType() != typeof(IdType))
                throw new Exception("id is not the right Type...");

            result = (IdType)id;

            return result;
        }

        public void Delete(IdType id)
        {
            _baseSrv.Delete(id);
        }

        public List<T> GetAll()
        {
            List<T> result = null;
            Type listType = null;
            List<object> list = _baseSrv.GetAll();

            if (list != null)
            {
                if (list.Count > 0)
                    listType = list[0].GetType();

                if (listType != typeof(T))
                    throw new Exception("objects in list are not the right Type of entity to access..");

                if (result == null)
                    result = list.Cast<T>().ToList();
                //foreach (object item in list)
                //    result.Add((T)item);
            }

            return result;
        }

        public void Update(T model)
        {
            _baseSrv.Update(model);
        }

        public IEnumerable<T> Where(Func<T, bool> predicate)
        {
            return GetAll().Where(predicate);
        }

        public IdType Insert(AddType model, Converter<AddType, T> converter)
        {
            return Insert(converter(model));
        }

        public void Update(UpdateType model, Converter<UpdateType, T> converter)
        {
            Update(converter(model));
        }
    }

}