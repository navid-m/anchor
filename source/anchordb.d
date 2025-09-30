module anchordb;

import std.json;
import std.file;
import std.path;
import std.stdio;
import std.string;
import std.exception;
import std.conv;
import std.array;
import std.format;
import std.traits;

public class AnchorDB
{
    string dbPath;
    string dataFilePath;
    File dataFile;
    string[string] index;
    bool[string] deleted;

    /** 
     * Constructs a new AnchorDB instance.
     *
     * Params:
     *   path = Path to the database directory.
     */
    public this(string path)
    {
        dbPath = path;
        dataFilePath = buildPath(dbPath, "data.log");
        if (!exists(dbPath))
            mkdirRecurse(dbPath);
        loadIndex();
        dataFile = File(dataFilePath, "ab");
    }

    /**
     * Put a key-value pair into the database.
     * Value is serialized to JSON.
     */
    void put(T)(string key, T value)
    {
        JSONValue json = serializeToJSON(value);
        auto jsonStr = json.toString;
        auto entry = format("%s:%s\n", key, jsonStr);
        auto pos = dataFile.tell();
        dataFile.write(entry);
        dataFile.flush();
        index[key] = to!string(pos);
        deleted.remove(key);
    }

    /**
     * Get a value by key, deserialized from JSON.
     */
    T get(T)(string key)
    {
        if (key in deleted)
            throw new Exception("Key not found: " ~ key);

        auto posStr = index.get(key, null);

        if (posStr is null)
            throw new Exception("Key not found: " ~ key);

        auto pos = to!ulong(posStr);
        auto readFile = File(dataFilePath, "rb");

        readFile.seek(pos);
        auto line = readFile.readln();
        readFile.close();

        auto parts = line.split(":");

        if (parts.length < 2)
            throw new Exception("Corrupted data");

        auto jsonStr = parts[1 .. $].join(":");
        auto json = parseJSON(jsonStr);

        return deserializeFromJSON!T(json);
    }

    /**
     * Delete a key-value pair.
     */
    void del(string key)
    {
        if (key in index)
        {
            deleted[key] = true;
        }
    }

    /**
     * Close the database, flush to disk.
     */
    void close()
    {
        dataFile.close();
        saveIndex();
    }

    void loadIndex()
    {
        auto indexFile = buildPath(dbPath, "index.json");
        if (exists(indexFile))
        {
            auto content = readText(indexFile);
            auto json = parseJSON(content);
            if ("index" in json)
            {
                foreach (string key, value; json["index"].object)
                {
                    index[key] = value.str;
                }
            }
            if ("deleted" in json)
            {
                foreach (string key, value; json["deleted"].object)
                {
                    deleted[key] = value.boolean;
                }
            }
        }
        replayLog();
    }

    void replayLog()
    {
        if (!exists(dataFilePath))
            return;
        auto file = File(dataFilePath, "rb");
        while (!file.eof)
        {
            auto pos = file.tell();
            auto line = file.readln();
            if (line.empty)
                break;
            auto parts = line.split(":");
            if (parts.length >= 2)
            {
                auto key = parts[0];
                index[key] = to!string(pos);
            }
        }
        file.close();
    }

    /**
     * Compact the log file to remove old entries and deleted keys.
     */
    void compact()
    {
        auto tempFile = buildPath(dbPath, "data.temp");
        auto outFile = File(tempFile, "wb");
        string[string] newIndex;

        dataFile.close();
        auto readFile = File(dataFilePath, "rb");

        foreach (key, posStr; index)
        {
            if (key in deleted)
                continue;
            auto pos = to!ulong(posStr);
            readFile.seek(pos);
            auto line = readFile.readln();
            auto newPos = outFile.tell();
            outFile.write(line);
            newIndex[key] = to!string(newPos);
        }
        readFile.close();
        outFile.close();
        rename(tempFile, dataFilePath);
        dataFile = File(dataFilePath, "ab");
        index = newIndex;
        deleted = null;
    }

    void saveIndex()
    {
        auto indexFile = buildPath(dbPath, "index.json");
        JSONValue json;
        json["index"] = JSONValue(index);
        json["deleted"] = JSONValue(deleted);
        std.file.write(indexFile, json.toString);
    }
}

private JSONValue serializeToJSON(T)(T value)
{
    static if (is(T == struct))
    {
        JSONValue json = JSONValue.emptyObject;
        foreach (member; __traits(allMembers, T))
        {
            static if (__traits(compiles, __traits(getMember, value, member)))
            {
                alias MemberType = typeof(__traits(getMember, value, member));
                static if (!isFunction!MemberType)
                {
                    json[member] = serializeToJSON(__traits(getMember, value, member));
                }
            }
        }
        return json;
    }
    else static if (is(T == string))
        return JSONValue(value);
    else static if (is(T : long))
        return JSONValue(value);
    else static if (is(T : double))
        return JSONValue(value);
    else static if (is(T == bool))
        return JSONValue(value);
    else static if (is(T == typeof(null)))
        return JSONValue(null);
    else
        static assert(0, "Unsupported type for serialization: " ~ T.stringof);

}

private T deserializeFromJSON(T)(JSONValue json)
{
    static if (is(T == struct))
    {
        T result;
        foreach (member; __traits(allMembers, T))
        {
            static if (__traits(compiles, __traits(getMember, result, member)))
            {
                alias MemberType = typeof(__traits(getMember, result, member));
                static if (!isFunction!MemberType)
                {
                    if (member in json)
                    {
                        __traits(getMember, result, member) = deserializeFromJSON!MemberType(
                            json[member]);
                    }
                }
            }
        }
        return result;
    }
    else static if (is(T == string))
        return json.str;
    else static if (is(T : long))
        return cast(T) json.integer;
    else static if (is(T : double))
        return cast(T) json.floating;
    else static if (is(T == bool))
        return json.boolean;
    else
        static assert(0, "Unsupported type for deserialization: " ~ T.stringof);
}
