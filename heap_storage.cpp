#include "heap_storage.h"
#include "db_cxx.h"
#include <cstring>

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
    std::cout << "create ok" << std::endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    // Handles *handles = table.select();
    // std::cout << "select ok " << handles->size() << std::endl;

    // ValueDict *result = table.project((*handles)[0]);
    // std::cout << "project ok" << std::endl;
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
    u16 headerloc = size + loc;
    if (loc == 0)
    {
        return nullptr; //tombstone
    }
    return (Dbt *)(this->block.get_data() + headerloc);
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
    RecordIDs *IDs;
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
    u16 shift = end - start;
    if (shift == 0)
    {
        return;
    }
    memcpy(this->address(this->end_free + 1 + shift), memcpy(this->address(this->end_free + 1), NULL, start), end);

    for (u16 i = 0; i < this->num_records; i++)
    {
        u16 size;
        u16 loc;
        this->get_header(size, loc, this->ids()->at(i));
        if (loc <= start)
        {
            loc += shift;
            this->put_header(this->ids()->at(i), size, loc);
        }
    }
    this->end_free += shift;
    this->put_header();
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

// HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0){}

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
    this->dbfilename = this->name + ".db";
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

    if (flags == 0)
    {
        DB_BTREE_STAT stat;
        this->db.stat(nullptr, &stat, DB_FAST_STAT);
        this->last = stat.bt_ndata;
    }
    else
    {
        this->last = 0;
    }
    this->closed = false;
}

/**
 * Create physical file.
 */
void HeapFile::create(void)
{

    this->db_open(DB_CREATE);

    SlottedPage *block = get_new();
    this->put(block);
}

/**
 * Delete physical file.
 */
void HeapFile::drop(void)
{
    this->open();
    this->close();
    remove(this->dbfilename.c_str());
}

/**
 * Open physical file.
 */
void HeapFile::open(void)
{
    this->db_open();
    //???????
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
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    this->last += 1;
    u_int32_t block_number = this->last;
    Dbt key(&block_number, sizeof(block_number));
    db.put(NULL, &key, &data, 0); // write block to the database
    db.get(NULL, &key, &data, 0); // read block #1 from the database

    return new SlottedPage(data, this->last, true);
}

/**
 * Grab a block/slotted page from the database file
 *
 */
SlottedPage *HeapFile::get(BlockID block_id)
{
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    std::cout <<"before slotted page heap file get" << std::endl;
    this->db.get(nullptr, &key, &data, 0);
    std::cout <<"after slotted page heap file get" << std::endl;
    return new SlottedPage(data, block_id, false);
}

/**
 * Write a block back to the database file
 */
void HeapFile::put(DbBlock *block)
{
    u_int32_t blockID = block->get_block_id();
    Dbt key(&blockID, sizeof(blockID));
    this->db.put(NULL, &key, block->get_block(), 0);
}

/**
 * Return a sequence of all block ids.
 */
BlockIDs *HeapFile::block_ids()
{
    BlockIDs *ids;
    for (u32 i = 1; i < this->last + 1; i++)
    {
        ids->push_back(i);
    }
    return ids;
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
    std::cout << "creating table" << std::endl;
    this->file.create();
}


/**
 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create_if_not_exists()
{
    try
    {
        this->open();
    }
    catch (DbRelationError e)
    {
        this->create();
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
    this->file.open();
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
    std::cout <<"before invalid get" << std::endl;
    SlottedPage *page = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try
    {
        record_id = page->add(data);
    }
    catch (DbRelationError e)
    {
        page = this->file.get_new();
        record_id = page->add(data);
    }

    this->file.put(page);
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
    Value v;

    for (Identifier c : this->column_names)
    {
        //Identifier column = this->column_names[c];
        if (row->find(c) == row->end())
        {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        }
        else
        {

            v = row->find(c)->second;
        }

        dict->insert(std::pair<Identifier, Value>(c, v));
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
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            uint size = value.s.length();
            *(u16 *)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else
        {
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

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}

Handles *HeapTable::select()
{
    Handles *fixme = new Handles();
    return fixme;
}

ValueDict *HeapTable::project(Handle handle)
{
    ValueDict *fixme = new ValueDict();
    return fixme;
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    ValueDict *fixme = new ValueDict();
    return fixme;
}

ValueDict *HeapTable::unmarshal(Dbt *data)
{
    ValueDict *fixme = new ValueDict();
    return fixme;
}
