#include <nchan_module.h>
#include <math.h>

#include "nchan_accumulator.h"

// based on work done for Queris (c) 2011-2017 Leo Ponomarev
// https://github.com/slact/queris/blob/master/lib/queris/indices.rb#L661-L712
// licensed under the MIT license, same as Nchan

int nchan_accumulator_init(nchan_accumulator_t *acc, nchan_accumulator_type_t type, double halflife) {
  switch(type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      if(halflife <= 0) {
        return 0;
      }
      acc->data.ed_float.value = 0;
      acc->data.ed_float.weight = 0;
      acc->data.ed_float.lambda = (double)1 / halflife;
      break;
    
    case ACCUMULATOR_SUM:
      acc->data.sum.value = 0;
      acc->data.sum.weight = 0;
      break;
      
    default:
      return 0;
  }
  ngx_memzero(&acc->last_update, sizeof(acc->last_update));
  acc->type = type;
  
  return 1;
}

static double ngx_time_t_diff(ngx_time_t *t0, ngx_time_t *t1) {
  if(t1->sec == t0->sec && t1->msec == t0->msec) {
    return 0;
  }
  return ((double)t1->sec - t0->sec)*1000 + ((ngx_int_t)(t1->msec) - (ngx_int_t)(t0->msec));
}

static void nchan_accumulator_rebase_to_time(nchan_accumulator_t *acc, ngx_time_t *t1) {
  double         diff   = ngx_time_t_diff(&acc->last_update, t1);
  double         pow_mult;
  
  if(diff == 0) {
    return;
  }
  
  acc->last_update = *t1;
  switch(acc->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      //the accurate way
      pow_mult = pow(2, -1 * acc->data.ed_float.lambda * diff);
      acc->data.ed_float.value = acc->data.ed_float.value * pow_mult;
      acc->data.ed_float.weight= acc->data.ed_float.weight * pow_mult;
      break;
    default:
      //not supported yet
      break;
  }
}

static void nchan_accumulator_rebase_to_now(nchan_accumulator_t *acc) {
  ngx_time_t    *t1     = ngx_timeofday();
  nchan_accumulator_rebase_to_time(acc, t1);
}


int nchan_accumulator_update(nchan_accumulator_t *acc, double val) {
  switch(acc->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      //the accurate way
      nchan_accumulator_rebase_to_now(acc);
      acc->data.ed_float.value += val;
      acc->data.ed_float.weight += 1;
      break;
    case ACCUMULATOR_SUM:
      acc->data.sum.value += val;
      acc->data.sum.weight += 1;
      break;
    default:
      //not supported yet
      return 0;
  }
  return 1;
}

int nchan_accumulator_atomic_update(nchan_accumulator_t *acc, double val) {
  
  int intval;
  switch(acc->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      //not possible for now
      return 0;
    
    case ACCUMULATOR_SUM:
      intval = val;
      ngx_atomic_fetch_add((ngx_atomic_uint_t *)(&acc->data.ed_float.value), intval);
      ngx_atomic_fetch_add((ngx_atomic_uint_t *)(&acc->data.ed_float.weight), 1);
      return 1;
    
    default:
      //not supported yet
      return 0;
  }
}



double nchan_accumulator_average(nchan_accumulator_t *acc) {
  switch(acc->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      if(acc->data.ed_float.weight == 0) {
        return 0;
      }
      return acc->data.ed_float.value / acc->data.ed_float.weight;
    
    case ACCUMULATOR_SUM:
      if(acc->data.sum.weight == 0) {
        return 0;
      }
      return acc->data.sum.value / acc->data.sum.weight;
    
    default:
      //not implemented
      return -1;
  }
}

double nchan_accumulator_value(nchan_accumulator_t *acc) {
  switch(acc->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      nchan_accumulator_rebase_to_now(acc);
      return acc->data.ed_float.value;
    
    case ACCUMULATOR_SUM:
      return acc->data.sum.value;
      
    default:
      //not implemented
      return -1;
  }
}

double nchan_accumulator_weight(nchan_accumulator_t *acc) {
  switch(acc->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      nchan_accumulator_rebase_to_now(acc);
      return acc->data.ed_float.weight;
      
    case ACCUMULATOR_SUM:
      return acc->data.sum.weight;
    
    default:
      //not implemented
      return -1;
  }
}


int nchan_accumulator_merge(nchan_accumulator_t *dst, nchan_accumulator_t *src) {
  if(dst->type != src->type) {
    //refuse to merge different-type averagers
    return 0;
  }
  
  nchan_accumulator_t tmp;
  
  switch(dst->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      tmp = *src;
      nchan_accumulator_rebase_to_now(dst);
      nchan_accumulator_rebase_to_now(&tmp);
      if(tmp.data.ed_float.lambda != dst->data.ed_float.lambda) {
        if(tmp.data.ed_float.weight + dst->data.ed_float.weight == 0) {
          return 0;
        }
        dst->data.ed_float.lambda = (dst->data.ed_float.lambda * dst->data.ed_float.weight + tmp.data.ed_float.lambda * tmp.data.ed_float.weight) / (dst->data.ed_float.weight + tmp.data.ed_float.weight);
      }
      dst->data.ed_float.value += tmp.data.ed_float.value;
      dst->data.ed_float.weight += tmp.data.ed_float.weight;
      return 1;
    
    case ACCUMULATOR_SUM:
      dst->data.sum.value += src->data.sum.value;
      dst->data.sum.weight += src->data.sum.weight;
      return 1;
    
    default:
      return 0;
  }
}

void nchan_accumulator_reset(nchan_accumulator_t *acc) {
  switch(acc->type) {
    case ACCUMULATOR_EXP_DECAY_FLOAT:
      acc->data.ed_float.value = 0;
      acc->data.ed_float.weight = 0;
      break;
    
    case ACCUMULATOR_SUM:
      acc->data.sum.value = 0;
      acc->data.sum.weight = 0;
      break;
      
    default:
      //do nothing
      break;
  }
}

