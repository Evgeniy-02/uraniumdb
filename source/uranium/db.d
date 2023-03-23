module uranium.db;

import std.stdio : File, writeln;

import std.conv : to;
import std.array : appender, split, array;
import std.concurrency : send, receiveOnly, Tid, thisTid;
import std.random : randomCover, MinstdRand0, randomSample, unpredictableSeed;
import std.range : array;
import std.algorithm : filter, each;
import std.uni : isWhite;
import std.algorithm.iteration : joiner, map, filter;
import std.string : strip;
import std.file : isFile, dirEntries, SpanMode, remove;
import std.path : baseName;

/**
 * Responses sent by Uranium
 */
struct Response
{
    ResponseType type;
    string value;

    string toString()
    {
        import std.conv;

        switch (type) with (ResponseType)
        {
        case Error:
        case Status:
            return value;

        default:
            return "";
        }
    }

}

/**
 * Types of responses
 */
enum ResponseType : byte
{
    Invalid,
    Status,
    Error,
    Nil
}

/// Directory for transaction logs
enum LOG_DIR = "./log/";

/**
  * Uranium is the simple database which helps 
  * to get random set of the N elements 
  * (It uses for the machine learning)
  * 
  * Supported operations:
  * "C arg1 arg2 argN"       - add elements to DB
  * "R 5"                    - get 5 random elements
  * "U oldVal newVal"        - replace oldVal on newVal
  * "D value"                - hide value in DB
  * "close"                  - close connection
  * "recover transactions"   - recover aborted transactions
  */
class Uranium(V)
{

private:
    V[] _state;

    void add(V[] vals...)
    {
        _state ~= vals;
    }

    V[] get(size_t n)
    {
        auto rnd = MinstdRand0(unpredictableSeed);

        version (D_LP64) // https://issues.dlang.org/show_bug.cgi?id=15147
            return _state.randomCover(rnd).filter!"!a.empty".array()[0 .. n];

    }

    void update(V old, V n)
    {
        import std.array, std.parallelism;

        foreach (i, e; parallel(_state)) //parallel foreach
            if (e == old)
                _state[i] = n;

        _state ~= n;
    }

    void remove(V[] vals...)
    {
        foreach (v; vals)
            update(v, "");
    }

    Response recoverTransactions()
    {
        string[] logfiles = dirEntries(LOG_DIR, "*.log", SpanMode.shallow).filter!(a => a.isFile)
            .map!((return a) => baseName(a.name))
            .array; // https://dlang.org/phobos/std_file.html#dirEntries

        auto result = appender!(Response[]);
        if (!logfiles.length)
            return Response(ResponseType.Status, "transaction logs is empty");
        foreach (filename; logfiles)
        {
            auto file = File(LOG_DIR ~ filename);
            file.readln.split("&&").map!"a.strip"
                .filter!"!a.empty"
                .each!(request => execute(request));
        }
        return Response(ResponseType.Status, "success");
    }

    /**
     * Parser for requests
     */
    R execute(R = Response)(string request)
    {
        string[] p = request.strip.split!isWhite;
        switch (p[0])
        {
        case "C":
            add(p[1 .. $]);
            break;
        case "R":
            return Response(ResponseType.Status, get(p[1].to!ulong).joiner(" ").to!string);
        case "U":
            update(p[1], p[2]);
            break;
        case "D":
            remove(p[1 .. $]);
            break;
        case "recover":
            if (p[1] == "transactions")
                return recoverTransactions();
            goto default;
        default:
            return Response(ResponseType.Invalid, "invalid operation");
        }
        return Response(ResponseType.Status, "ok");
    }

public:

    this(string filename, size_t capacity = 10000)
    {
        import std.stdio : File;
        import std.algorithm : filter;

        auto state = appender!(V[]);
        state.reserve(capacity);
        auto reader = File(filename, "r");
        auto lines = reader.byLine().filter!"!a.empty"();
        foreach (ref line; lines)
        {
            state ~= line.to!string;
        }

        _state = state[];
    }

    /**
     * Receive requests from thread queue and execute them
     */
    void runRequestReceiver()
    {
        for (;;)
        {
            auto result = appender!(Response[]);
            result.reserve(10);

            auto requests = receiveOnly!(Tid, string);
            // debug writeln(__FILE__ ~ ":" ~ __LINE__ ~ requests);
            if (requests[1] == "close")
            {
                result ~= Response(ResponseType.Status, "good bye");
                requests[0].send(result[].idup);
                break;
            }

            foreach (request; requests[1].split("&&"))
            {
                result ~= execute(request);
            }

            requests[0].send(result[].idup);
        }
    }

    override string toString()
    {
        return to!string(_state);
    }

}

/**
 * Interface for transactions in DBs
 */
interface Transaction
{

    /**
     * Save transaction to log
     */
    Transaction opCall(string request);

    /**
     * Run all requests from log
     */
    Response[] commit();
}

/**
 * Transaction implementation for Uranium DB
 */
class TransactionImpl : Transaction
{
    private auto _log = appender!(string[]);
    private File* _logfile;
    private Tid _dbId;

    this(Tid dbId)
    {
        _dbId = dbId;
        _logfile = new File(LOG_DIR ~ "transaction-" ~ thisTid.to!string ~ ".log", "w");
    }

    override Transaction opCall(string request)
    {
        _log ~= request;
        _logfile.write(request ~ " && ");
        return this;
    }

    override Response[] commit()
    {
        _dbId.send(thisTid, _log[].joiner(" && ").to!string);
        auto response = receiveOnly!(immutable(Response)[]).dup;

        _logfile.close;
        _logfile.name.remove;

        return response;
    }
}

Transaction newTransaction(Tid dbId)
{
    return new TransactionImpl(dbId);
}
