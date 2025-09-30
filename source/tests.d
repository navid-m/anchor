module tests;

unittest
{
    import anchordb;
    import std.stdio;

    struct Person
    {
        string name;
        int age;
    }

    auto db = new AnchorDB("./db");

    db.put("user1", Person("Alice", 30));
    db.put("user2", Person("Bob", 25));

    auto alice = db.get!Person("user1");
    writeln("Alice: ", alice.name, " age ", alice.age);

    auto bob = db.get!Person("user2");
    writeln("Bob: ", bob.name, " age ", bob.age);

    db.put("user1", Person("Alice", 31));
    alice = db.get!Person("user1");
    writeln("Updated Alice: ", alice.name, " age ", alice.age);

    db.del("user2");
    try
    {
        bob = db.get!Person("user2");
    }
    catch (Exception e)
    {
        writeln("Bob deleted: ", e.msg);
    }

    db.close();
}
