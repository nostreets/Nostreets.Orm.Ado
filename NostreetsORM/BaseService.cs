using NostreetsORM.Interfaces;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Web.Configuration;

namespace NostreetsORM
{
    public abstract class BaseService
    {
        public BaseService()
        {
            _connectionKey = "DefaultConnection";
        }

        public BaseService(string connectionKey)
        {
            _connectionKey = connectionKey;
        }

        private string _connectionKey;
        private SqlConnection _connection;

        public SqlConnection Connection
        {
            get { return GetConnection(); }

        }

        protected static IDao DataProvider
        {
            get { return Utilities.DataProvider.SqlInstance; }
        }

        public void ChangeSqlConnection(string connectionKey)
        {
            _connectionKey = connectionKey;
        }

        private SqlConnection GetConnection()
        {
            return new SqlConnection(WebConfigurationManager.ConnectionStrings[_connectionKey].ConnectionString);
        }


    }
}