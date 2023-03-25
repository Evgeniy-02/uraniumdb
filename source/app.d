import std.stdio : writeln;
import uranium.db;
import std.concurrency;

void main()
{
    auto db = spawn({ new Uranium!string("smiles_db.um").runRequestReceiver(); });
    // CRUD operations
    db.run("C O CC COC CC(=O)O"); //add elements: O CC COC CC(=O)O
    writeln("get 5 elements: ", db.run("R 5")); // get 5 random elements
    db.run("U CC C1=CC=CC=C1"); // replace CC on C1=CC=CC=C1
    db.run("D C1=CC=CC=C1 C"); // delete C1=CC=CC=C1 and C

    // recover transactions from a crash
    writeln("recovery: ", db.run("recover transactions"));

    spawn((Tid db0) {
        auto tr1 = db0.newTransaction;
        tr1("C CN")("C CCl")("R 5");
        tr1("U CCl CCCl");
        writeln(tr1.commit);

        ownerTid.send(true);
    }, db);

    spawn((Tid db0) {
        auto tr2 = db0.newTransaction;
        tr2("C CN")("C CCl")("R 5");
        writeln(tr2.commit);

        ownerTid.send(true);
    }, db);

    // wait for transaction finishing before close db
    receiveOnly!bool;
    receiveOnly!bool;

    // finish Uranium thread
    while (db.run("close") != Response(ResponseType.Status, "good bye"))
    {
    }
}

/**
 * Send requests to Uranium and receive responses
 */
Response run(ref Tid dbId, string request)
{
    dbId.send(thisTid, request);
    return receiveOnly!(immutable(Response)[])[0];
}
