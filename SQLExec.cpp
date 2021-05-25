/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"
#include "EvalPlan.h"
#include "ParseTreeToString.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
 * Inserts the data into the table.
 * @param statement : InsertStatement
 * @return QueryResult
 */
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    // get the table and construct ValueDict
    Identifier table_name = statement->tableName;
    DbRelation &table = SQLExec::tables->get_table(table_name);
    ValueDict row;
    
    // checking values/columns provided match the columns of the table
    const ColumnNames &const_table_columns = table.get_column_names();
    ColumnNames table_columns = const_table_columns;
    const ColumnAttributes &const_column_attr = table.get_column_attributes();
    ColumnAttributes table_attr = const_column_attr;
    
    if(table_columns.size() != statement->values->size()) {
        throw SQLExecError("DbRelationError: Don't know how to handle NULLs, defaults, etc. yet");
    }

    // if column names are provided, map column names to values in order
    if(statement->columns != NULL) {
        for(size_t i = 0; i < statement->columns->size(); i++) {
            Identifier columnName = statement->columns->at(i);

            int index;
            auto it = find(table_columns.begin(), table_columns.end(), columnName);
            if(it != table_columns.end())
                index = it - table_columns.begin();
            else
                throw SQLExecError("Invalid column name");

            // use index to lookup datatype in column attributes
            if(table_attr[index].get_data_type() == ColumnAttribute::INT)
                row[columnName] = Value(statement->values->at(i)->ival);
            else
                row[columnName] = Value(statement->values->at(i)->name);
        }
    }
    //columns not provided then add to columns in table order
    else {
        for(size_t i = 0; i < table_columns.size(); i++) {
            if(table_attr[i].get_data_type() == ColumnAttribute::INT)
                row[table_columns.at(i)] = Value(statement->values->at(i)->ival);
            else
                row[table_columns.at(i)] = Value(statement->values->at(i)->name);
        }
    }

    //do the insert
    Handle insertHandle = table.insert(&row);
    
    //add to indices
    IndexNames indices = SQLExec::indices->get_index_names(table_name);
    int numberOfInd = 0;
    for(Identifier indexName: indices) {
        numberOfInd++;
        DbIndex &index = SQLExec::indices->get_index(table_name, indexName);
        index.insert(insertHandle);
    }

    string ret = string("successfully inserted 1 row into ") + string(table_name) + string(" and ") + to_string(numberOfInd) + string(" indices");
    return new QueryResult(ret); 
}

/**
 * Pulls equality predicates, AND operations and parse tree
 */
ValueDict* get_where_conjunction(const hsql::Expr *expr, const ColumnNames *col_names) {
    if(expr->type != kExprOperator)
        throw DbRelationError("Operator is not supported");
    
    ValueDict* rows = new ValueDict;
    
    switch(expr->opType) {
        case Expr::AND: {
            ValueDict* ret = get_where_conjunction(expr->expr, col_names);
            
            if (ret != nullptr) {
                rows->insert(ret->begin(), ret->end());
            }

            ret = get_where_conjunction(expr->expr2, col_names);
            rows->insert(ret->begin(), ret->end());
            break;
            }
        
        case Expr::SIMPLE_OP: {
            if(expr->opChar != '=')
                throw DbRelationError("Only equality predicates currently supported");
            
            Identifier col = expr->expr->name;

            if(find(col_names->begin(), col_names->end(), col) == col_names->end()){
                throw DbRelationError("Unknown column " + col + "'");
            }

            if(expr->expr2->type == kExprLiteralString)
                rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->name)));
            
            else if(expr->expr2->type == kExprLiteralInt)
                rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->ival)));
            
            else
                throw DbRelationError("Type is not supported");
            break;
            }
        
        default:
            throw DbRelationError("Only support AND conjunctions");
    }
    return rows;
}

/**
 * Delete the data from the table.
 * @param statement : DeleteStatement
 * @return QueryResult
 */
QueryResult *SQLExec::del(const DeleteStatement *statement) {
    Identifier table_name = statement->tableName;
    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);
    ColumnNames column_names;

    for (auto const column: table.get_column_names()){
        column_names.push_back(column);
    }

    //make the evaluation plan
    EvalPlan *plan = new EvalPlan(table);
    ValueDict *where = new ValueDict;

    if (statement->expr != nullptr) {
        try {
            where = get_where_conjunction(statement->expr, &column_names);
        } catch (exception &e) {
            throw;
        }
        plan = new EvalPlan(where, plan);

    }
    //and execute it to get a list of handles
    EvalPlan *optd = plan->optimize();
    EvalPipeline pipeline = optd->pipeline();

    IndexNames index_names = SQLExec::indices->get_index_names(table_name);
    Handles *handles = pipeline.second;
    
    u_long n = handles->size();
    u_long numberOfInd = index_names.size();

    //now delete all the handles
    for( auto const &handle: *handles) {
        for (unsigned int i = 0; i < index_names.size(); i++) {
            DbIndex &index = SQLExec::indices->get_index(table_name, index_names[i]);
            index.del(handle);
        }
    }
    //remove from table
    for (auto const& handle: *handles){
        table.del(handle);
    }

    string ret = string("successfully deleted ") + to_string(n)+ string(" rows from ") + string(table_name) + string(" and ") + to_string(numberOfInd) + string(" indices");
    return new QueryResult(ret);
}


/**
 * Selects the data from the table and shows it.
 * @param statement : SelectStatement
 * @return QueryResult
 */
QueryResult *SQLExec::select(const SelectStatement *statement) {
    Identifier table_name = statement->fromTable->name;
    DbRelation& table = tables->get_table(table_name);

    // make the evaluation plan with selection
    EvalPlan* plan = new EvalPlan(table);
    if (statement->whereClause != nullptr) {
        plan = new EvalPlan(get_where_conjunction(statement->whereClause), plan);
    }

    ColumnAttributes* colAttributes = new ColumnAttributes;
    ColumnNames* colNames = new ColumnNames;
    // check if the select type is *, then evaluates for all the columns
    if (statement->selectList->at(0)->type == kExprStar) {
        *colNames = table.get_column_names();
        *colAttributes = table.get_column_attributes();
        plan = new EvalPlan(EvalPlan::ProjectAll, plan);
    }
    else {
        // certain columns are selected, then evaluates these columns
        for (auto const& col : *statement->selectList) {
            colNames->push_back(col->name);
        }
        *colAttributes = *table.get_column_attributes(*columnNames);
        plan = new EvalPlan(colNames, plan);
    }
    // get the optimized plan
    EvalPlan* optimized = plan->optimized();

    // execute
    ValueDicts* rows = optimized->evaluate();
    return new QueryResult(colNames, colAttributes, rows, " successfully returned " + to_string(rows->size()) + " rows"); 
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception &e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier iname = statement->indexName;
    Identifier name = statement->name;
    ValueDict where;
    where["table_name"] = name;
    where["index_name"] = iname;
    DbIndex& index = SQLExec::indices->get_index(name,iname);
    index.drop();
    Handles *hands = SQLExec::indices->select(&where);
    
    for (auto const &handle: *hands) {
        SQLExec::indices->del(handle);
    }
    
    return new QueryResult("Dropped index " + iname);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    
    Identifier table_name = statement->tableName;
    
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("column_name");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");
    
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT)); // everything
    
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    
    Handles *handles = SQLExec::indices->select();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices -> project(handle,column_names);
        Identifier table_name = row->at("index_name").s;

        //check if the table not present in Table's or Column's TABLE_NAME
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME) {
            rows->push_back(row);
        } else {
            delete row;
        }
    }
    u_long n = rows->size();
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    //u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    u_long n = rows->size();
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

