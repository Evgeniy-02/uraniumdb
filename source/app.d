import std.stdio : writeln;
import uranium.db : Uranium, Op;

void main()
{
    auto db = new Uranium!string("smiles_db.um");
    // CRUD operations
    db("C O CC COC CC(=O)O"); //add elements: O CC COC CC(=O)O
    // db(Op.C, " O ", "CC COC CC(=O)O"); - the same
    writeln(db("R 5")); // get 5 random elements
    db("U CC C1=CC=CC=C1"); // replace CC on C1=CC=CC=C1
    db(Op.D, "C1=CC=CC=C1", "C"); // delete C1=CC=CC=C1 and C
}
