import std.stdio : writeln;
import uranium.db;
import std.concurrency;

void main()
{
    auto db = spawn({ new Uranium!string("smiles_db.um").runRequestReceiver(); });
    // CRUD operations
    db.run("C O CC COC CC(=O)O"); //add elements: O CC COC CC(=O)O
    // db(Op.C, " O ", "CC COC CC(=O)O"); - the same
    writeln(db.run("R 5")); // get 5 random elements
    // db("U CC C1=CC=CC=C1"); // replace CC on C1=CC=CC=C1
    // db(Op.D, "C1=CC=CC=C1", "C"); // delete C1=CC=CC=C1 and C
    // 
    auto tr1 = db.newTransaction;
    tr1("C CN")("C CCl")("R 5");
    tr1("U CCl CCCl");
    writeln(tr1.commit);

    db.send(thisTid, "close");
}

Response run(ref Tid dbId, string request)
{
    dbId.send(thisTid, request);
    return receiveOnly!(immutable(Response)[])[0];
}
