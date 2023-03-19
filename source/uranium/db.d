module uranium.db;
import std.stdio : writeln;

enum ResponseType : byte
{
    Invalid,
    Status,
    Error,
    Nil
}

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

/** Operations
*/
enum Op : byte
{
    C, // create
    R, // read
    U, // update
    D // delete
}

/**
  * Uranium is the simple database which halps 
  * to get random set of the N elements
  * 
  * Supported operations:
  * "C arg1 arg2 argN" - add elements to DB
  * "R 5"              - get 5 random elements
  * "U oldVal newVal"  - replace oldVal on newVal
  * "D value"          - hide value in DB
  */
class Uranium(V)
{
    import std.conv : to;

private:
    V[] _state;

    void add(V[] vals...)
    {
        _state ~= vals;
    }

    V[] get(size_t n)
    {
        import std.random : randomCover, MinstdRand0, randomSample, unpredictableSeed;
        import std.range : array;
        import std.algorithm : filter;

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

public:

    this(string filename)
    {
        import std.array : appender;
        import std.stdio : File;
        import std.algorithm : filter;

        auto state = appender!(V[]);
        auto reader = File(filename, "r");
        auto lines = reader.byLine().filter!"!a.empty"();
        foreach (ref line; lines)
        {
            state ~= line.to!string;
        }

        // debug writeln("after read ", state);
        _state = cast(V[]) state[];
    }

    /**
	 * Call Uranium using any type T that can be converted to a string
	 *
	 * Examples:
	 * auto db = new Uranium!string("smiles_db.um");
	 * ---
	 * db(Op.C, " O ", "CC COC CC(=O)O"); add elements: O CC COC CC(=O)O
     * writeln(db(Op.R, "5")); // get 5 random elements
     * db(Op.U "CC", "C1=CC=CC=C1"); // replace CC on C1=CC=CC=C1
     * db(Op.D, "C1=CC=CC=C1", "C"); // delete C1=CC=CC=C1 and "C"
	 * ---
	 */
    R opCall(R = Response, T...)(Op op, T args)
    {
        import std.array : appender;

        auto request = appender!string;
        request ~= op.to!string;

        foreach (arg; args)
            request ~= arg.to!string;

        return opCall(cast(string) request[]);
    }

    /**
	 * Call Uranium using request string
	 *
	 * Examples:
	 * auto db = new Uranium!string("smiles_db.um");
	 * ---
	 * db("C O CC COC CC(=O)O"); // add elements: O CC COC CC(=O)O
     * db("R 5");                // get 5 random elements
     * db("U CC C1=CC=CC=C1");   // replace CC on C1=CC=CC=C1
     * db("D C1=CC=CC=C1");      // delete C1=CC=CC=C1
	 * ---
	 */
    R opCall(R = Response)(string request)
    {
        import std.array : split;
        import std.uni : isWhite;
        import std.algorithm.iteration : joiner;
        import std.string : strip;

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
        default:
            return Response(ResponseType.Invalid, "invalid operation");
        }
        return Response(ResponseType.Status, "ok");
    }

    override string toString()
    {
        return to!string(_state);
    }

}
