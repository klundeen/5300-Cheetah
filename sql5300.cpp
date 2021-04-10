
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;

// CREATE A DIRECTORY IN YOUR HOME DIR ~/cpsc5300/data before running this
const char *HOME = "cpsc5300/data";
const char *EXAMPLE = "milestone.db";
const unsigned int BLOCK_SZ = 4096;

string selectInfo(const SelectStatement *statement);
string createInfo(const CreateStatement *statement);
string printExpr(const Expr *expr);
string operExpr(const Expr *expr);
string printTableRefInfo(const TableRef *table);
string columnDefinitionToString(const ColumnDefinition *col);
string execute(const SQLStatement *result);

string columnDefinitionToString(const ColumnDefinition *col) {
    string ret(col->name);
    switch(col->type) {
    case ColumnDefinition::DOUBLE:
        ret += " DOUBLE ";
        break;
    case ColumnDefinition::INT:
        ret += " INT ";
        break;
    case ColumnDefinition::TEXT:
        ret += " TEXT ";
        break;
    default:
        ret += " ...";
        break;
    }
    return ret;
}

string printExpr(const Expr *expr)
{
    string ret;
    switch (expr->type)
    {
    case kExprStar:
        ret += "*";
        break;
    case kExprColumnRef:
        if(expr->table != NULL) {
            ret += expr->table;
            ret += ".";
        }
        break;
    // case kExprTableColumnRef: inprint(expr->table, expr->name, numIndent); break;
    case kExprLiteralFloat:
        ret += to_string(expr->fval);
        break;
    case kExprLiteralInt:
        ret += to_string(expr->ival);
        break;
    case kExprLiteralString:
        ret += expr->name;
        break;
    case kExprFunctionRef:
        ret += expr->name;
        ret += expr->expr->name;
        break;
    case kExprOperator:
        ret += operExpr(expr);
        break;
    default:
        ret += "Unrecognized expression type %d\n";
        break;
    }
    if (expr->alias != NULL)
    {
        ret += expr->alias;
    }
    return ret;
}

string operExpr(const Expr *expr)
{
    string ret;
    if (expr == NULL)
    {
        ret += "null";
        return ret;
    }

    switch (expr->opType)
    {
    case Expr::SIMPLE_OP:
        ret += expr->opChar;
        break;
    case Expr::AND:
        ret += "AND";
        break;
    case Expr::OR:
        ret += "OR";
        break;
    case Expr::NOT:
        ret += "NOT";
        break;
    default:
        ret += expr->opType;
        break;
    }
    ret += printExpr(expr->expr);
    if (expr->expr2 != NULL)
        ret += printExpr(expr->expr2);

    return ret;
}

string printTableRefInfo(const TableRef *table)
{
    string ret;
    switch (table->type)
    {
    case kTableName:
        ret += table->name;
        break;
    case kTableSelect:
        ret += execute(table->select);
        break;
    case kTableJoin:

        ret += printTableRefInfo(table->join->left);
        ret += " LEFT ";
        ret += "JOIN ";
        ret += " " + printTableRefInfo(table->join->right);
        ret +=  " ON ";
        ret += " " + printExpr(table->join->condition);

        break;
    case kTableCrossProduct:
        for (TableRef *tbl : *table->list)
            ret += printTableRefInfo(tbl);
        break;
    }
    if (table->alias != NULL)
    {
        ret += " AS ";
        ret += table->alias;
    }

    return ret;
}

string execute(const SQLStatement *result)
{

    cout << "Entered Execute" << endl;

    string ret;
    switch (result->type())
    {
    case kStmtSelect:
    {
        const SelectStatement *selectStmt = (const SelectStatement *)result;
        ret = selectInfo(selectStmt);
        return ret;
    }

    case kStmtCreate:
    {
        const CreateStatement *createStmt = (const CreateStatement *)result;
        ret = createInfo(createStmt);
        return ret;
    }
    }

    return "null";

    
}

string selectInfo(const SelectStatement *stmt)
{
    cout << "Entered select" << endl;
    string ret("SELECT ");
    for (Expr *expr : *stmt->selectList)
    {
        ret += printExpr(expr);
    }

    ret += " FROM " + printTableRefInfo(stmt->fromTable);

    if (stmt->whereClause != NULL)
    {
        ret += printExpr(stmt->whereClause);
    }
    return ret;
}

string createInfo(const CreateStatement *stmt)
{
    cout << "Entered create" << endl;
    string ret("CREATE TABLE ");
    ret += stmt->tableName;
    ret += " ";

    for (ColumnDefinition* column : *stmt->columns) 
        ret+= columnDefinitionToString(column);

    return ret;
}

int main(void)
{

    //setting up db env?

    const char *home = std::getenv("HOME");
    std::string envdir = std::string(home) + "/" + HOME;

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ);                                               // Set record length to 4K
    db.open(NULL, EXAMPLE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

    char block[BLOCK_SZ];
    Dbt data(block, sizeof(block));
    int block_number;
    Dbt key(&block_number, sizeof(block_number));
    block_number = 1;
    strcpy(block, "hello!");
    db.put(NULL, &key, &data, 0); // write block #1 to the database

    Dbt rdata;
    db.get(NULL, &key, &rdata, 0); // read block #1 from the database

    //user loop

    string input = "";
    while (input != "quit")
    {

        cout << "SQL> ";
        getline(cin, input);

        cout << endl;
        SQLParserResult *result = SQLParser::parseSQLString(input);
        if (result->isValid())
        {
            printf("Parsed successfully!\n");
            printf("Number of statements: %lu\n", result->size());

            for (uint i = 0; i < result->size(); i++)
            {
                cout << execute(result->getStatement(i)) << endl;
            }


        }

      

        else
        {
            fprintf(stderr, "Given string is not a valid SQL query.\n");
            fprintf(stderr, "%s (L%d:%d)\n",
                    result->errorMsg(),
                    result->errorLine(),
                    result->errorColumn());
            delete result;
        }
    }

    return EXIT_SUCCESS;
}
