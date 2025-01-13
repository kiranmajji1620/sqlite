
#include <stdio.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

typedef struct buffer_struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

// Using enums for switch statements ensures we are not missing out any case of the enum.  
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert; // used in the insert statement.
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0) -> Attribute)

// offset is the position where the attribute starts:
// attribute    size    offset
// id           4          0
// username     32         4
// email        255        36
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// Structure of a table:

const uint32_t PAGE_SIZE = 4096; // take page size as 4 kb
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
const uint32_t TABLES_MAX_ROWS = TABLE_MAX_PAGES*ROWS_PER_PAGE;

typedef struct {
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
} Table;

InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer -> buffer = NULL;
    input_buffer -> buffer_length = 0;
    input_buffer -> input_length = 0;

    return input_buffer;
}

void print_prompt(){
    printf("db > ");
}

void read_input(InputBuffer* input_buffer){
    // when the buffersize given in less than the input text or  if the buffersize is initialized as 0, getline reallocates memory.
    //buffer starts as null, so getline allocates enough memory to hold the line of input and makes buffer point to it.
    ssize_t bytes_read = getline(&(input_buffer -> buffer), &(input_buffer -> buffer_length), stdin);
    // If getline fails to take input and store it to buffer, it returns -1 instead of the read bytes.. so we use ssize_t.
    if(bytes_read <= 0){
        printf("Error reading input.\n");
        exit(EXIT_FAILURE);
    }

    // removing the trailing new line character read in the buffer
    input_buffer -> input_length = bytes_read - 1;
    input_buffer -> buffer[bytes_read - 1] = 0; // we do this to end the string wrt c replacing "\n" with "\0" which is the termination
}

void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer -> buffer); // we do this because getline allocates memory for the same coz buffer was set to NULL initially.
    free(input_buffer);
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer){ // currently only handles ".exit", has room for more.
    if(strcmp(input_buffer -> buffer, ".exit") == 0){
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }
    else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement){
    if(strncmp(input_buffer -> buffer, "insert", 6) == 0){ // insert statement might be followed by some data.
        statement -> type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buffer -> buffer, "insert %d %s %s", &(statement -> row_to_insert.id), statement -> row_to_insert.username, statement -> row_to_insert.email);
        if(args_assigned < 3){
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if(strcmp(input_buffer -> buffer, "select") == 0){ // assuming simple SELECT statement, complex parsing might be done.
        statement -> type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}






// compact representation 
//(table row -> compact)
void serialize_row(Row* source, void* destination){
    // copy the contents of size ID_SIZE in the address(source -> id) into destination + offset(0)
    memcpy(destination + ID_OFFSET, &(source -> id), ID_SIZE); 
    memcpy(destination + USERNAME_OFFSET, &(source -> username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source -> email), EMAIL_SIZE);
}

//(compact -> table row)
void deserialize_row(void* source, Row* destination){
    memcpy(&(destination -> id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination -> username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination -> email), source + EMAIL_OFFSET, EMAIL_SIZE);
}



void print_row(Row* row){
    printf("(%d, %s, %s)\n", row -> id, row -> username, row -> email);
}
// Finding the location of a page:
void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num/ROWS_PER_PAGE;
    void* page = table -> pages[page_num];
    if(page == NULL){
        // allocate memory only when we access the page for the first time.
        page = table -> pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num%ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset*ROW_SIZE;
    return page + byte_offset;
}

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

ExecuteResult execute_insert(Statement* statement, Table* table){
    if(table -> num_rows >= TABLES_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement -> row_to_insert);

    serialize_row(row_to_insert, row_slot(table, table -> num_rows));// we are inserting after last row
    table -> num_rows += 1; // increase the no of rows.
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    Row row;
    for(uint32_t i = 0; i < table -> num_rows; i++){
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}
ExecuteResult execute_statement(Statement* statement, Table* table){
    switch(statement -> type){
        case(STATEMENT_INSERT):
            return execute_insert(statement, table);
        case(STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

// Create a table
Table* new_table() {
    Table* table = (Table*) malloc(sizeof(Table));
    table -> num_rows = 0;
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        table -> pages[i] = NULL;
    }
    return table;
}

void free_table(Table* table){
    for(int i = 0; table -> pages[i]; i++){
        free(table -> pages[i]);
    }
    free(table);
}
// Prints the prompt, gets the line of input, process the line of input.
int main(int argc, char* argv[]){
    Table* table = new_table();
    InputBuffer* input_buffer = new_input_buffer();
    while(1){
        print_prompt();
        read_input(input_buffer);
        // if(strcmp(input_buffer -> buffer, ".exit") == 0){ //Parsing
        //     printf("Exiting the db. \n");
        //     close_input_buffer(input_buffer);
        //     exit(EXIT_SUCCESS);
        // }
        // else {
        //     printf("Unrecognized command. '%s', \n", input_buffer -> buffer);
        // }

        // Parsing
        if(input_buffer -> buffer[0] == '.'){ // Handling Meta Commands
            switch(do_meta_command(input_buffer)) {
                case(META_COMMAND_SUCCESS):
                    continue;
                case(META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized meta command. '%s'\n", input_buffer -> buffer);
                    continue;
            }
        }

        Statement statement;
        switch(prepare_statement(input_buffer, &statement)){
            case(PREPARE_SUCCESS):
                // execute_statement(&statement); we don't do this because switch is only to validate.
                break;
            case(PREPARE_SYNTAX_ERROR):
                printf("Syntax error. could not parse statement.\n");
                continue;
            case(PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at the start of '%s'. \n", input_buffer->buffer);
                continue;
        }
        switch(execute_statement(&statement, table)){
            case(EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case(EXECUTE_TABLE_FULL):
                printf("Error, Table FULL.\n");
                break;
        }
    }
}
