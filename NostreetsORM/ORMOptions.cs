using NostreetsExtensions.DataControl.Classes;
using NostreetsExtensions.Interfaces;

namespace NostreetsORM
{
    public class ORMOptions
    {
        public static bool HasWipedDB { get; set; } = false;

        public bool WipeDB { get; set; } = false;

        public bool NullLock { get; set; } = false;

        public string ConnectionKey { get; set; } = "DefaultConnection";

        public IDBService<Error> ErrorLog { get; set; } = null;

    }
}
