#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

typedef enum{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED
}MetaCommandResult;

typedef enum{
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT,
	PREPARE_SYNTAX_ERROR
}PrepareResult;

typedef enum{
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
}ExecuteResult;

typedef enum{
	STATEMENT_INSERT,
	STATEMENT_SELECT,
	STATEMENT_EXIT
}StatementType;



#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

typedef struct{
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE];
	char email[COLUMN_EMAIL_SIZE];
}Row;

// 为了将行尽可能多的放入到页面中，需要将数据紧凑的放入到内存块中，每个数据都将计算指定位置。
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// 数据页
//@TABLE_MAX_PAGES 每个表最多可存储100页
//@PAGE_SIZE 一般操作系统的页的大小都为4096字节
//@TABLE_MAX_ROWS 通过每页有多少行计算出每个表最多有多少行

#define TABLE_MAX_PAGES 100

const uint32_t PAGE_SIZE = 4096;
// 平均每页有多少行
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
/*
    nums_rows  表行数
*/
typedef struct {
	uint32_t nums_rows;
	void*pages[TABLE_MAX_PAGES];
}Table;

typedef struct{
	StatementType type;
	Row row_to_insert;
}Statement;


Table * new_table(){
	Table * table = malloc(sizeof(Table));
	table->nums_rows = 0;
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
		table->pages[i] = NULL;
	}
	return table;
}

void free_table(Table*table){
	for (int i = 0; table->pages[i]; i++){
		free(table->pages[i]);
	}
	free(table);
}

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// 创建结构体InputBuffer
// buffer  缓冲区内容，以字符串表示
// buffer_length   分配的缓冲区大小
// input_length    输入的内容大小
typedef struct{
	char * buffer;
	size_t buffer_length;
	ssize_t input_length;
}InputBuffer;

InputBuffer * new_input_buffer(){
	InputBuffer * input = (InputBuffer*)malloc(sizeof(InputBuffer));
	if (NULL != input){
		input->buffer = NULL;
		input->buffer_length = 0;
		input->input_length = 0;

	}
		return input;
}

void serialize_row(Row*source, void*destination){
	memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
	memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
	memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void*source, Row*destination){
	memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/*返回该行在page中的字节位置*/
void* row_slot(Table * table, uint32_t row_num){
	// 第几页
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void * page = table->pages[page_num];
	if (page == NULL){
		page = table->pages[page_num] = malloc(PAGE_SIZE);
	}
	// 所在页的起始位置 第几行
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
    // 第几个字节
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

void print_prompt(){
	printf("db > ");
}

PrepareResult prepare_statement(InputBuffer*input, Statement*statement){
    if (strncmp(input->buffer, "insert", 6) == 0){
	    statement->type = STATEMENT_INSERT;
		int arg_assigned = sscanf(
			input->buffer, "insert %d %s %s", &(statement->row_to_insert.id), statement->row_to_insert.username, statement->row_to_insert.email
		);
		if (arg_assigned < 3) {
	        return PREPARE_SYNTAX_ERROR;
        }
		return PREPARE_SUCCESS;
	}
	if(strcmp(input->buffer, "select") == 0){
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	
	if(strcmp(input->buffer, "exit") == 0){
		statement->type = STATEMENT_EXIT;
		return PREPARE_SUCCESS;
	}
	
	return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement*statement, Table*table){
	if (table->nums_rows >= TABLE_MAX_ROWS){
		return EXECUTE_TABLE_FULL;
	}
	Row*row_to_insert = &(statement->row_to_insert);
	serialize_row(row_to_insert, row_slot(table, table->nums_rows));
	table->nums_rows += 1;
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement*statement, Table*table){
	Row row;
	for (uint32_t i = 0; i < table->nums_rows; i++){
		deserialize_row(row_slot(table, i), &row);
		print_row(&row);
	}
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement*statement, Table*table){
	switch (statement->type){
	case (STATEMENT_INSERT):
		/* code */
		return execute_insert(statement, table);
	case (STATEMENT_SELECT):
		return execute_select(statement, table);
	case (STATEMENT_EXIT):
	    exit(EXIT_SUCCESS);
		break;
	}
}

void read_input(InputBuffer*input){
	ssize_t byted_read = getline(&(input->buffer), &(input->buffer_length), stdin);
	if (byted_read <= 0){
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}

	input->input_length = byted_read - 1;
	input->buffer[byted_read-1] = 0;
}

void close_input_buffer(InputBuffer*input){
	free(input->buffer);
	free(input);
}

int main(int argc, char const *argv[])
{
	Table * table = new_table();
	InputBuffer * input_buffer = new_input_buffer();
	while (true){
		print_prompt();
		read_input(input_buffer);
		
		Statement statement;
	    switch (prepare_statement(input_buffer, &statement)){
	    case (PREPARE_SUCCESS):
		    break;
		case (PREPARE_SYNTAX_ERROR):
		    printf("Syntax error. Could not parse statement.\n");
			continue;
	    case (PREPARE_UNRECOGNIZED_STATEMENT):
	        printf("Uncongnized keyword at start of '%s' \n", input_buffer->buffer);
		    continue;
	    }

		switch (execute_statement(&statement, table))
		{
		case (EXECUTE_SUCCESS):
		    printf("Executed. \n");
			break;
		case (EXECUTE_TABLE_FULL):
		    printf("Error: Table full. \n");
			break;
		}
    }

	
	return 0;
}