
#include <stdio.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>


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
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
} Statement;

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
    if(strncmp(input_buffer -> buffer, "INSERT", 6) == 0){ // insert statement might be followed by some data.
        statement -> type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if(strcmp(input_buffer -> buffer, "SELECT") == 0){ // assuming simple SELECT statement, complex parsing might be done.
        statement -> type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement){
    switch(statement -> type){
        case(STATEMENT_INSERT):
            printf("Here we will insert. \n");
            break;
        case(STATEMENT_SELECT):
            printf("Here we will select. \n");
            break;
    }
}


// Prints the prompt, gets the line of input, process the line of input.
int main(int argc, char* argv[]){
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
            case(PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at the start of '%s'. \n", input_buffer->buffer);
                continue;
        }
        execute_statement(&statement);
        printf("Executed. \n");
    }
}
