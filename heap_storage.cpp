#include "heap_storage.h"
#include "db_cxx.h"
#include <cstring>
using namespace std;
typedef u_int16_t u16;
typedef u_int32_t u32;

/**
 * ---------------------------Test Method---------------------------
 */
bool test_heap_storage()
{
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exsts ok" << endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    cout << "try insert" << endl;
    table.insert(&row);
    cout << "insert ok" << endl;
    // Handles *handles = table.select();
    // cout << "select ok " << handles->size() << endl;

    // ValueDict *result = table.project((*handles)[0]);
    // cout << "project ok" << endl;
    // Value value = (*result)["a"];
    // if (value.n != 12)
    //     return false;
    // value = (*result)["b"];
    // if (value.s != "Hello!")
    //     return false;
    table.drop();
    return true;
}

/**
 * ---------------------------Slotted Page---------------------------
 */

/**
 * Constructor
 * 
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
    if (is_new)
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
    else
    {
        get_header(this->num_records, this->end_free);
    }
}

/**
 * Add a new record to the block. Return its id.
 * 
 */

RecordID SlottedPage::add(const Dbt *data)
{
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**
 * Get a record from the block. Return Nullptr if it has been deleted/does not exist.
 * 
 */

Dbt *SlottedPage::get(RecordID record_id)
{
    u16 size;
    u16 loc;
    this->get_header(size, loc, record_id);
    if (loc == 0)
    {
        return nullptr; //tombstone
    }
    return new Dbt(this->address(loc), size);
}

/**
 * Replace the record with the given data. Raises ValueError if it won't fit.
 * 
 */
void SlottedPage::put(RecordID record_id, const Dbt &data)
{
    u16 size = this->num_records;
    u16 loc = this->end_free;
    this->get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();
    if (new_size > size)
    {
        u16 extra = new_size - size;
        if (!(this->has_room(extra)))
        {
            throw DbBlockNoRoomError("not enough room for new record in the blocks");
        }
        this->slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), size);
    }
    else
    {
        memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }
    this->get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
}

/**
* Mark the given record_id as deleted by changing its size to zero and its location to 0.
* Compact the rest of the data in the block. But keep the record ids the same for everyone.
*/

void SlottedPage::del(RecordID record_id)
{
    u16 size;
    u16 loc;
    this->get_header(size, loc, record_id);
    this->put_header(record_id, 0, 0);
    this->slide(loc, loc + size);
}

/**
 * Returns a sequence of all non-deleted record ids.
 * 
 */
RecordIDs *SlottedPage::ids(void)
{
    RecordIDs *IDs = new RecordIDs();
    u16 size;
    u16 loc;

    for (u16 i = 1; i < this->num_records + 1; i++)
    {
        size = this->num_records;
        loc = this->end_free;
        this->get_header(size, loc, i);
        if (loc != 0)
        {
            IDs->push_back(i);
        }
    }
    return IDs;
}

//Protected Methods for SlottedPage
//________________
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}
// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset)
{
    return *(u16 *)this->address(offset);
}

/**
 * Calculate if we have room to store a record with given size. The size should include the 4 bytes for the header, too, if this is an add. 
 * 
 */
bool SlottedPage::has_room(u_int16_t size)
{
    u16 available;
    available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}

/**
 * Calculate if we have room to store a record with given size. The size should include the 4 bytes for the header, too, if this is an add. 
 * 
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
     int shift = end - start;
    if (shift == 0)
        return;

    // slide data
    void *to = this->address((u16) (this->end_free + 1 + shift));
    void *from = this->address((u16) (this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    memmove(to, from, bytes);

    // fix up headers to the right
    RecordIDs *record_ids = ids();
    for (auto const &record_id : *record_ids) {
        u16 size, loc;
        get_header(size, loc, record_id);
        if (loc <= start) {
            loc += shift;
            put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    put_header();
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n)
{
    *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset)
{
    return (void *)((char *)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

/**
 * ---------------------------Heap File---------------------------
 */

HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
    this->dbfilename = this->name + ".db";
}
/**
 * Wrapper for Berkeley DB open functions
 */
void HeapFile::db_open(uint flags)
{

    if (!this->closed)
    {
        return;
    }
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

    this->last = flags ? 0 : get_block_count();
    this->closed = false;
}

/**
 * Create physical file.
 */
void HeapFile::create(void)
{

    this->db_open(DB_CREATE | DB_EXCL);

    SlottedPage *block = get_new();
    delete block;
}

/**
 * Delete physical file.
 */
void HeapFile::drop(void)
{
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

/**
 * Open physical file.
 */
void HeapFile::open(void)
{
    db_open();
}

/**
 * Close physical file.
 */
void HeapFile::close(void)
{
    this->db.close(0);
    this->closed = true;
}

/**
 * Allocate a new block for the database file and add to BerkeleyDB.
 */
SlottedPage *HeapFile::get_new(void)
{
    char block[4096];
    //creates a place in memory for block
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    this->last += 1;
    u_int32_t block_number = this->last;
    Dbt key(&block_number, sizeof(block_number));
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization done to it
    delete page;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, this->last);
}

/**
 * Grab a block/slotted page from the database file
 *
 */
SlottedPage *HeapFile::get(BlockID block_id)
{
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    cout <<"before slotted page heap file get" << endl;
    this->db.get(nullptr, &key, &data, 0);
    cout <<"after slotted page heap file get" << endl;
    return new SlottedPage(data, block_id, false);
}

/**
 * Write a block back to the database file
 */
void HeapFile::put(DbBlock *block)
{
    u_int32_t blockID = block->get_block_id();
    Dbt key(&blockID, sizeof(blockID));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 * Return a sequence of all block ids.
 */
BlockIDs *HeapFile::block_ids()
{
    BlockIDs *ids = new BlockIDs();
    for (u32 i = 1; i < this->last + 1; i++)
    {
        ids->push_back(i);
    }
    return ids;
}
uint32_t HeapFile::get_block_count() {
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    uint32_t bt_ndata = stat->bt_ndata;
    free(stat);
    return bt_ndata;
}
/**
 * ---------------------------Heap Table---------------------------
 */



/**
 * Heap Table Constructor
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name)
{
}

/**
 * Execute: CREATE TABLE <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation
 */
void HeapTable::create()
{
    cout << "creating table" << endl;
    this->file.create();
}


/**
 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create_if_not_exists()
{
    try {
        open();
    } catch (DbException &e) {
        create();
    }
}

/**
 * Execute: DROP TABLE <table_name>
 */
void HeapTable::drop()
{
    this->file.drop();
}

/**
 * Open existing table. Enables: insert, update, delete, select, project
 */
void HeapTable::open()
{
    file.open();
}

/**
 * Closes the table. Disables: insert, update, delete, select, project
 */
void HeapTable::close()
{
    this->file.close();
}

/**
 * Expect row to be a dictionary with column name keys.
 * Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
 * Return the handle of the inserted rowt
 */
Handle HeapTable::insert(const ValueDict *row)
{
    this->open();
    return this->append(this->validate(row));
}

/**
 * Assumes row is fully fleshed-out. Appends a record to the file.
 */
Handle HeapTable::append(const ValueDict *row)
{
    Dbt *data = this->marshal(row);
    cout <<"before invalid get" << endl;
    SlottedPage *page = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try
    {
        record_id = page->add(data);
    }
    catch (DbRelationError e)
    {
		delete page;
        page = this->file.get_new();
        record_id = page->add(data);
    }

    this->file.put(page);
	delete page;
	delete[] (char *) data->get_data();
	delete data;
    Handle p;
    p.first = this->file.get_last_block_id();
    p.second = record_id;
    return p;
}

/**
 * Check if the given row is acceptable to insert. Raise ValueError if not.
 * Otherwise return the full row dictionary.
 */
ValueDict *HeapTable::validate(const ValueDict *row)
{

    ValueDict *dict = new ValueDict();
    

    for (Identifier c : this->column_names)
    {
		Value v;
        //Identifier column = this->column_names[c];
        if (row->find(c) == row->end())
        {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        }
        else
        {

            v = row->find(c)->second;
        }

        dict->insert(pair<Identifier, Value>(c, v));
    }

    return dict;
}

/**
 * Data marshaling
 */
Dbt *HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;

        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t *) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(u16 *) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

/**
 * Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
 * If handles is specified, then use those as the base set of records to apply a refined selection to.
 * Returns a list of handles for qualifying rows.
 */
Handles *HeapTable::select(const ValueDict *where)
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids)
    {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
	 throw DbRelationError("Not implemented");
}

void HeapTable::del(const Handle handle) {
	open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

Handles *HeapTable::select()
{
    return select(nullptr);
}

ValueDict *HeapTable::project(Handle handle)
{
    return project(handle, &this->column_names);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
        return row;
    ValueDict *result = new ValueDict();
    for (auto const &column_name: *column_names) {
        if (row->find(column_name) == row->end())
            throw DbRelationError("table does not have column named '" + column_name + "'");
        (*result)[column_name] = (*row)[column_name];
    }
    delete row;
    return result;
}

ValueDict *HeapTable::unmarshal(Dbt *data)
{
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char *) data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t *) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    return row;
}

