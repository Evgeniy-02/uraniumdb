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

unittest
{
    struct sttr
    {
        int i;
        sttr* next;
    }

    auto ur = new Uranium!(string, sttr);
    auto a = sttr(1, null);
    ur.put("sttr", a);
    assert(ur.get("sttr") == a);
}

class Uranium(V)
{
private:
    V[] _state;

    void add(V val)
    {
        _state ~= val;
    }

    V[] get(int n, bool function(V) filter)
    {
        import std.array, std.parallelism;
        import std.random : randomCover,MinstdRand0, unpredictableSeed;

        auto r = appender!(V[]);
        r.reserve(100); // 100 elements default
        foreach (e; parallel(_state)) //parallel foreach
        {
            if (filter(e))
            {
                r ~= e;
            }
        }

        debug writeln(_state);
        debug writeln(r[]);

        return cast(V[]) r[];
    }
    
    void remove(V val){
    	_state -= val;
    }
    
    void update(V old, V n){
    	_state -= old;
    	_state += n;
    }

    //    void update(V oldVal, V newVal)
    //    {
    //        auto oldVal = _state[key];
    //        _state[key] = newVal;
    //        return oldVal;
    //    }
    //
    //    void remove(V key)
    //    {
    //        auto oldVal = _state[key];
    //        _state.remove(key);
    //        return oldVal;
    //
    //    }

public:

    this(string filename)
    {
        import std.array : appender;
        import std.stdio : File;
        import std.algorithm : filter;
        import std.conv : to;

        auto state = appender!(V[]);
        auto reader = File(filename, "r");
        auto lines = reader.byLine().filter!"!a.empty"();
        foreach (ref line; lines)
        {
            debug writeln(line);
            state ~= line.to!string;
        }

        debug writeln("after read", state);
        _state = cast(V[]) state[];
        debug writeln(_state);
    }

    /**
	 * Call Urinium using any type T that can be converted to a string
	 *
	 * Examples:
	 *
	 * ---
	 * send("SET name Adil")
	 * send("GET", "*") == send("GET *")
	 * ---
	 */
    R opCall(R = Response, T...)(Op op, T args)
    {
        import std.stdio;
        import std.conv;

        Response[] r;
        r ~= Response(ResponseType.Status, "ok");
        switch (op) with (Op)
        {
        case C:
            debug writeln("call C");
            break;
        case R:
            // return Response(ResponseType.Status, get(args[0].to!int, (e) => true).to!string);
            debug writeln(get(args[0].to!int, (e) => true).to!string);
            break;
        default:
            break;
        }
        return cast(R) r[0];
    }

    override string toString()
    {
        import std.conv : to;

        return to!string(_state);
    }

}
