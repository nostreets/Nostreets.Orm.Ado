# Nostreets.Orm.Ado
###  Lightweight Object Relational Mapper using String Concatenation For C#
I used Entity Framework's Code First Concept and made a mapper that creates the tables and standerd procdures of the given object. The object will undergo normalization and multiple tables will be created if the object properties are custom data types. Uses the string given in the constructor to determine the key for the ConnectionString in WebConfig file. DefaultConnection is the key by default.

#### Example
```C#
using Nostreets.Orm.Ado;

//CustomClass PK is the first public property by default or needs to be targeted via [Key] attribute and is an type of Int32, Guid, or String and contains Id in the name to be managed by DBService...

DBService<CustomClass> srv = new DBService(CustomClass, "SomeKeyInWebConfig");
DBService<CustomClass> srv = new DBService<CustomClass>("SomeKeyInWebConfig");
DBService<CustomClass> srv = new DBService<CustomClass, int>("SomeKeyInWebConfig");
DBService<CustomClass> srv = new DBService<CustomClass, int, CustomClassAddRequest, CustomClassUpdateRequest>("SomeKeyInWebConfig");

srv.Backup("Custom Back Path (C://ORMBackups is traget if null)");

srv.Insert(obj);
srv.Update(obj);
srv.Delete(7);
List<CustomClass> objs = srv.GetAll();
IEnumerable<object> obj = srv.Where((a) => {a.Id == 7});
CustomClass obj = srv.FirstOrDefault((a) => {a.Type == "Classic"});
CustomClass obj = srv.Get(9);
```

***

- Dependencies
  - 
    - NostreetsExtensions