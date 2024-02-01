#include "heap_storage.h"
#include <cstring>
#include <stdexcept>
#include <string>
#include <db_cxx.h>
#include <vector>



// Helper functions to work with bytes and records
typedef uint16_t u16;

/**
 * SlottedPage implementation
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("Not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = static_cast<u16>(data->get_size());
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    std::memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0) // Record has been deleted
        return nullptr;
    return new Dbt(this->address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (data.get_size() > size) {
        throw DbBlockNoRoomError("New data does not fit in the original space");
    }
    std::memcpy(this->address(loc), data.get_data(), data.get_size());
}

void SlottedPage::del(RecordID record_id) {
    put_header(record_id, 0, 0); // Mark the record as deleted
}

RecordIDs* SlottedPage::ids(void) {
    auto ids = new RecordIDs();
    for (u16 id = 1; id <= this->num_records; ++id) {
        u16 size, loc;
        get_header(size, loc, id);
        if (loc != 0) // If not deleted
            ids->push_back(id);
    }
    return ids;
}

bool SlottedPage::has_room(u16 size) {
    // Calculate available space considering the new record header
    u16 required_space = size + sizeof(u16) * 2; // Size and location for header
    u16 available_space = this->end_free - (this->num_records * sizeof(u16) * 2 + sizeof(u16) * 2);
    return required_space <= available_space;
}

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // Block header
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    // Calculate the amount to slide
    int slide_amount = end - start;
    if (slide_amount == 0) return; // No sliding needed

    // Adjust pointers for all records that will be affected by the slide
    // This is just a placeholder logic.

    // Move the data in the block
    // memmove(destination, source, number_of_bytes);
    memmove((char*)this->block.get_data() + start + slide_amount,
            (char*)this->block.get_data() + start,
            this->end_free - start); // Adjust this calculation as needed

    // Update end_free to reflect the new position of the end of free space
    this->end_free += slide_amount; // Adjust this as necessary
}


u16 SlottedPage::get_n(u16 offset) {
    return *reinterpret_cast<u16*>(this->address(offset));
}

void SlottedPage::put_n(u16 offset, u16 n) {
    *reinterpret_cast<u16*>(this->address(offset)) = n;
}

void* SlottedPage::address(u16 offset) {
    return static_cast<void*>(static_cast<char*>(this->block.get_data()) + offset);
}

/**
 * HeapFile implementation
 */
void HeapFile::create() {
    if (!closed) {
        throw std::runtime_error("File is already open");
    }
    this->db_open(DB_CREATE | DB_EXCL);
    this->closed = false;
    // Create an empty block to act as the first block
    SlottedPage* page = this->get_new();
    delete page; // We don't actually need the page; just need to increment `last`
}

void HeapFile::drop() {
    this->close();
    if (std::remove(this->dbfilename.c_str()) != 0) {
        throw std::runtime_error("Error deleting database file");
    }
}

void HeapFile::open() {
    if (!closed) {
        return; // File is already open
    }
    this->db_open();
    this->closed = false;
}

void HeapFile::close() {
    if (closed) {
        return; // File is already closed
    }
    db.close(0);
    closed = true;
}

SlottedPage* HeapFile::get_new() {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // Write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // Write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0); // Berkeley DB now manages the memory
    return page;
}

SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0); // Get the block from Berkeley DB
    return new SlottedPage(data, block_id);
}

void HeapFile::put(DbBlock* block) {
    BlockID block_id = block->get_block_id(); // Store the block ID in a local variable
    Dbt key(&block_id, sizeof(block_id)); // Now you can take the address of block_id
    this->db.put(nullptr, &key, block->get_block(), 0); // Write the block back to the file
}

BlockIDs* HeapFile::block_ids() {
    BlockIDs* ids = new BlockIDs();
    for (u_int32_t i = 1; i <= this->last; i++) {
        ids->push_back(i);
    }
    return ids;
}

void HeapFile::db_open(uint flags) {
    if (!this->closed) {
        return; // Database is already open
    }
    this->db.set_flags(DB_RECNUM); // Use record numbers
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0);
    this->closed = false;

    if (flags == DB_CREATE) {
        this->last = 0;
    } else {
        // Retrieve the last block id from the database's metadata
        // This part may require custom logic to track the last block id effectively
    }
}

/**
 * HeapTable implementation
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name) {}


void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->file.open(); // Attempt to open, which succeeds if the file exists
    } catch (const std::exception& e) {
        // If opening fails, assume the file does not exist and create it
        this->create();
    }
}

void HeapTable::drop() {
    this->file.drop();
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    Dbt *data = this->marshal(row);
    SlottedPage *page = this->file.get_new();
    RecordID record_id = page->add(data);
    BlockID block_id = page->get_block_id();
    delete data;
    delete page;
    return std::make_pair(block_id, record_id);
}

Handles* HeapTable::select(const ValueDict *where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = this->file.block_ids();
    for (auto const& block_id : *block_ids) {
        SlottedPage* block = this->file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id : *record_ids) {
            handles->push_back(std::make_pair(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    // Example implementation logic:
    // 1. Fetch the record using the handle.
    // 2. Apply the changes from new_values to the record.
    // 3. Marshal the updated record and write it back to the storage.

    // This is a placeholder.
}

void HeapTable::del(const Handle handle) {
    // Example implementation logic:
    // 1. Locate the record using the handle.
    // 2. Mark the record as deleted in the storage system.

    // This is a placeholder. 
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    ValueDict *projected_values = new ValueDict();
    // Example implementation logic:
    // 1. Fetch the record using the handle.
    // 2. Extract and return the values for the specified columns.

    // This is a placeholder.
    return projected_values;
}

ValueDict* HeapTable::project(Handle handle) {
    SlottedPage* block = this->file.get(handle.first);
    Dbt* data = block->get(handle.second);
    ValueDict* row = this->unmarshal(data);
    delete data;
    delete block;
    return row;
}

ValueDict *HeapTable::validate(const ValueDict *row) {
    // Example implementation logic:
    // 1. Check if the values in 'row' adhere to the schema and constraints of the table.
    // 2. Return a possibly modified version of 'row' that conforms to the table's requirements, or throw an exception if invalid.

    // This is a placeholder. 
    return new ValueDict(*row); // Simply returns a copy for the sake of the example.
}


// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
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

ValueDict* HeapTable::unmarshal(Dbt* data) {
    char* bytes = static_cast<char*>(data->get_data());
    ValueDict* row = new ValueDict();
    uint offset = 0;
    uint col_num = 0;

    for (const auto& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            int32_t val = *(reinterpret_cast<int32_t*>(bytes + offset));
            offset += sizeof(int32_t);
            (*row)[column_name] = Value(val);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = *(reinterpret_cast<u16*>(bytes + offset));
            offset += sizeof(u16);
            std::string val(bytes + offset, size);
            offset += size;
            (*row)[column_name] = Value(val);
        } else {
            throw DbRelationError("Unknown data type while unmarshaling");
        }
    }
    return row;
}

Handles *HeapTable::select() {
    Handles *handles = new Handles();
    // Logic to iterate over all records and add their handles to the list
    // This is just a placeholder.
    return handles;
}

Handle HeapTable::append(const ValueDict *row) {
    // Convert row to Dbt data structure
    Dbt *data = marshal(row);
    
    // Get a new block if necessary and add the record
    BlockID block_id = 0; // Determine the appropriate block ID
    SlottedPage *block = dynamic_cast<SlottedPage*>(this->file.get_new()); // Or use existing block

    RecordID record_id = block->add(data);
    this->file.put(block); // Write the block back to the file

    delete data; // Clean up the marshaled data

    return std::make_pair(block_id, record_id);
}



// test function -- returns true if all tests pass
bool test_heap_storage() {
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
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
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
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}