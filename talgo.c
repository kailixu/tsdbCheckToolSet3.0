/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "talgo.h"

void *taosbsearch(const void *key, const void *base, int32_t nmemb, int32_t size, __compar_fn_t compar, int32_t flags) {
  uint8_t *p;
  int32_t  lidx;
  int32_t  ridx;
  int32_t  midx;
  int32_t  c;

  if (nmemb <= 0) return NULL;

  lidx = 0;
  ridx = nmemb - 1;
  while (lidx <= ridx) {
    midx = (lidx + ridx) / 2;
    p = (uint8_t *)base + size * midx;

    c = compar(key, p);
    if (c == 0) {
      if (flags == TD_GT){
        lidx = midx + 1;
      } else if(flags == TD_LT){
        ridx = midx - 1;
      }else{
        break;
      }
    } else if (c < 0) {
      ridx = midx - 1;
    } else {
      lidx = midx + 1;
    }
  }

  if (flags == TD_EQ) {
    return c ? NULL : p;
  } else if (flags == TD_GE) {
    return (c <= 0) ? p : (midx + 1 < nmemb ? p + size : NULL);
  } else if (flags == TD_LE) {
    return (c >= 0) ? p : (midx > 0 ? p - size : NULL);
  } else if (flags == TD_GT) {
    return (c < 0) ? p : (midx + 1 < nmemb ? p + size : NULL);
  } else if (flags == TD_LT) {
    return (c > 0) ? p : (midx > 0 ? p - size : NULL);
  } else {
    ASSERT(0);
  }
}
