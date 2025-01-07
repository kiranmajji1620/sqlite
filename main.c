
#include <stdio.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>


typedef struct buffer_struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

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
    ssize_t bytes_read = getline(&(input_buffer -> buffer), &(input_buffer -> buffer_length), stdin);

    if(bytes_read <= 0){
        printf("Error reading input.\n");
        exit(EXIT_FAILURE);
    }

    // removing the trailing new line character read in the buffer
    input_buffer -> input_length = bytes_read - 1;
    input_buffer -> buffer[bytes_read - 1] = 0; // we do this to end the string wrt c replacing "\n"
}

void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer -> buffer); // we do this because getline allocates memory for the same coz it was set to NULL initially.
    free(input_buffer);
}



// Prints the prompt, gets the line of input, process the line of input.
int main(int argc, char* argv[]){
    InputBuffer* input_buffer = new_input_buffer();
    while(1){
        print_prompt();
        read_input(input_buffer);
        if(strcmp(input_buffer -> buffer, ".exit") == 0){
            printf("Exiting the db. \n");
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        }
        else {
            printf("Unrecognized command. '%s', \n", input_buffer -> buffer);
        }
    }
}
