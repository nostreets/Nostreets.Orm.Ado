# NostreetsORM
###  Lightweight Object Relational Mapper using
I used Entity Framework's Code First Concept and made a mapper that the creates tables and procdures of the given object. The object 
will undergo normalization and multiple tables will be created if the object properties are custom data types. Uses the string given in
the constructor to determine the key for the ConnectionString in WebConfig file. DefaultConnection is the key by default.

### Example
```

DBService<CustomClass> srv = new DBService<CustomClass>("SomeKeyInWebConfig");

List<CustomClass> list = srv.GetAll();

CustomClass list = srv.Get(9);

srv.Insert(obj);

srv.Update(obj);

srv.Delete(7);
```
