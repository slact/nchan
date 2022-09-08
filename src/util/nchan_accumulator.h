#ifndef NCHAN_ACCUMULATOR_H
#define NCHAN_ACCUMULATOR_H

#include <stddef.h>
#include <nginx.h>

typedef enum {
  ACCUMULATOR_EXP_DECAY_FLOAT,
  ACCUMULATOR_SUM
} nchan_accumulator_type_t;
  

typedef struct {
  double value;
  double weight;
  double lambda;
} nchan_accumulator_float_exponentially_decayed_data_t;

typedef struct {
  ngx_atomic_int_t value;
  ngx_atomic_int_t weight;
} nchan_accumulator_sum_data_t;

typedef struct {
  union {
    nchan_accumulator_float_exponentially_decayed_data_t  ed_float;
    nchan_accumulator_sum_data_t                          sum;
  }     data;
  
  ngx_time_t last_update; 
  
  nchan_accumulator_type_t type;
} nchan_accumulator_t;

int nchan_accumulator_update(nchan_accumulator_t *acc, double val);
int nchan_accumulator_atomic_update(nchan_accumulator_t *acc, double val);

double nchan_accumulator_value(nchan_accumulator_t *acc);
double nchan_accumulator_weight(nchan_accumulator_t *acc);
double nchan_accumulator_value(nchan_accumulator_t *acc);
int nchan_accumulator_init(nchan_accumulator_t *acc, nchan_accumulator_type_t type, double halflife);
int nchan_accumulator_merge(nchan_accumulator_t *merge_dst, nchan_accumulator_t *merge_src);
void nchan_accumulator_reset(nchan_accumulator_t *acc);

#endif //NCHAN_ACCUMULATOR_H
