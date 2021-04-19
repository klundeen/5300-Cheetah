#include "heap_storage.h"

typedef u_int16_t u16;

/**
 * ---------------------------Test Method---------------------------
 */
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

/**
 * ---------------------------Slotted Page---------------------------
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


// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}
Dbt SlottedPage::*get(RecordID record_id){
    u16 size;
    u16 loc;
    this->get_header(&size, &loc, record_id);
    u16 headerloc = size + loc;
    if(loc == 0){
        return nullptr;
    }
    return (Dbt)(this->block.get_data() + headerloc);
}
void SlottedPage::put(RecordID record_id, const Dbt &data){
    u16 size = this->num_records;
    u16 loc = this->end_free;
    this->get_header(size,loc,record_id);
    u16 new_size = (u16)data.get_size();
    if(new_size > size){
        u16 extra = new_size - size;
        if(!(this->has_room(extra))){
            throw DbBlockNoRoomError("not enough room for new record in the blocks");
        }
        this->slide(loc, loc - extra);
        memcpy(this->address(loc-extra),data.get_data(),size);
    }else{
        memcpy(this->address(loc),data.get_data(),new_size);
        this->slide(loc+new_size,loc+size);
    }
    this->get_header(&size,&loc,record_id);
    this->put_header(record_id,new_size,loc);
}

void SlottedPage::del(RecordID record_id){
    u16 size;
    u16 loc;
    this->get_header(&size,&loc,record_id);
    this->put_header(id,0,0);
    this->slide(loc, loc + size);
}
RecordIDs SlottedPage::*ids(void){
    RecordIDs* IDs;
    u16 size;
    u16 loc;

    for(u16 i = 1; i<this->num_records+1; i++){
        size = this->num_records;
        loc = this->end_free;
        this->get_header(size,loc,i);
        if(loc != 0){
            IDs->push_back(i);
        }
    }
    return IDs;
}

//Protected Methods for SlottedPage
//________________
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0){
    size = get_n(4*id);
    loc = get_n(4*id + 2);
}
// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}
bool SlottedPage::has_room(u_int16_t size){
    u16 available;
    available = this->end_free - (this->num_records +1) * 4;
    return size <= available;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end){
    u16 shift = end - start;
    if(shift == 0){
        return
    }
    memcpy(this->address(this->end_free+1+shift),(memcpy(this->address(this->end_free +1),data.get_data(),start),end);

     for(u16 i = 0; i <this->num_records; i++){
         u16 size;
         u16 loc;
         this->get_header(&size,&loc,this->ids()->at(i));
         if(loc <= start){
             loc += shift;
             this->put_header(this->ids()->at(i),size, loc;
         }
     }
     this->end_free += shift;
     this->put_header();
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}



/**
 * ---------------------------Heap File---------------------------
 */

HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

void HeapFile::db_open(uint flags){

}

void HeapFile::create(void){

}

void HeapFile::drop(void){

}

void HeapFile::open(void){

}

void HeapFile::close(void){

}

SlottedPage* HeapFile::get_new(void){

}

SlottedPage* HeapFile::get(BlockID block_id){
    
}

void HeapFile::put(DbBlock *block){

}

BlockIDs* HeapFile::block_ids(){

}






/**
 * ---------------------------Heap Table---------------------------
 */

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






Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}
