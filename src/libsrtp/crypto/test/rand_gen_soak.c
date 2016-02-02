/*
 * Soak test the RNG for exhaustion failures
 */

/*
 *	
 * Copyright (c) 2001-2006, Cisco Systems, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Cisco Systems, Inc. nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifdef HAVE_CONFIG_H
    #include <config.h>
#endif

#include <stdio.h>           /* for printf() */
#include "getopt_s.h"
#include "crypto_kernel.h"

#define BUF_LEN (MAX_PRINT_STRING_LEN/2)

int main(int argc, char *argv[])
{
    int q;
    int num_octets = 0;
    err_status_t status;
    uint32_t iterations = 0;
    int print_values = 0;

    if (argc == 1) {
        exit(255);
    }

    status = crypto_kernel_init();
    if (status) {
        printf("error: crypto_kernel init failed\n");
        exit(1);
    }

    while (1) {
        q = getopt_s(argc, argv, "pvn:");
        if (q == -1) {
            break;
        }
        switch (q) {
        case 'p':
            print_values = 1;
            break;
        case 'n':
            num_octets = atoi(optarg_s);
            if (num_octets < 0 || num_octets > BUF_LEN) {
                exit(255);
            }
            break;
        case 'v':
            num_octets = 30;
            print_values = 0;
            break;
        default:
            exit(255);
        }
    }

    if (num_octets > 0) {
        while (iterations < 300000) {
            uint8_t buffer[BUF_LEN];

            status = crypto_get_random(buffer, num_octets);
            if (status) {
                printf("iteration %d error: failure in random source\n", iterations);
                exit(255);
            } else if (print_values) {
                printf("%s\n", octet_string_hex_string(buffer, num_octets));
            }
            iterations++;
        }
    }

    status = crypto_kernel_shutdown();
    if (status) {
        printf("error: crypto_kernel shutdown failed\n");
        exit(1);
    }

    return 0;
}

