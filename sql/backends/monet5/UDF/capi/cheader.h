
#include <stdint.h>

typedef void *(*malloc_function_ptr)(size_t);

typedef struct {
	unsigned char day;
	unsigned char month;
	int year;
} cudf_data_date;

typedef struct {
	unsigned int ms;
	unsigned char seconds;
	unsigned char minutes;
	unsigned char hours;
} cudf_data_time;

typedef struct {
	cudf_data_date date;
	cudf_data_time time;
} cudf_data_timestamp;

typedef struct {
	size_t size;
	void* data;
} cudf_data_blob;

#define DEFAULT_STRUCT_DEFINITION(type, typename)                              \
	struct cudf_data_struct_##typename                                         \
	{                                                                          \
		type *data;                                                            \
		size_t count;                                                          \
		type null_value;                                                       \
		double scale;                                                          \
		int (*is_null)(type value);                                            \
		void (*initialize)(void *self, size_t count);                          \
		void *bat;                                                             \
	}

DEFAULT_STRUCT_DEFINITION(int8_t, bit);
DEFAULT_STRUCT_DEFINITION(int8_t, bte);
DEFAULT_STRUCT_DEFINITION(int16_t, sht);
DEFAULT_STRUCT_DEFINITION(int, int);
DEFAULT_STRUCT_DEFINITION(int64_t, lng);
DEFAULT_STRUCT_DEFINITION(float, flt);
DEFAULT_STRUCT_DEFINITION(double, dbl);
DEFAULT_STRUCT_DEFINITION(char*, str);
DEFAULT_STRUCT_DEFINITION(cudf_data_date, date);
DEFAULT_STRUCT_DEFINITION(cudf_data_time, time);
DEFAULT_STRUCT_DEFINITION(cudf_data_timestamp, timestamp);
DEFAULT_STRUCT_DEFINITION(cudf_data_blob, blob);
DEFAULT_STRUCT_DEFINITION(size_t, oid);
