import std.stdio : writeln;
import uranium.db : Uranium, Op;

void main()
{
    auto db = new Uranium!string("smiles_db.um");
    writeln(db(Op.R, 4));
}
