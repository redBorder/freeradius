/*
 *  rlm_kafka_log	Send the accounting information to an Apache kafka
 *                  topic
 *
 *  Version:    $Id$
 *
 *  Author:     Eugenio Perez <eupm90@gmail.com>
 *              Based on Nicolas Baradakis sql_log
 *
 *  Copyright (C) 2014 Eneo Tecnologia
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */

#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>

#include "rlm_kafka_log.c"

// Just mocking
const char *radius_dir = RADDBDIR;
const char *progname = "rbtest";
const char *radmin_version = "rbtest version " RADIUSD_VERSION_STRING
#ifdef RADIUSD_VERSION_COMMIT
" (git #" RADIUSD_VERSION_COMMIT ")"
#endif
;


/*
 *      The rest of this is because the conffile.c, etc. assume
 *      they're running inside of the server.  And we don't (yet)
 *      have a "libfreeradius-server", or "libfreeradius-util".
 */
int debug_flag = 0;
struct main_config_t mainconfig;
char *request_log_file = NULL;
char *debug_log_file = NULL;
int radius_xlat(UNUSED char *out, UNUSED int outlen, UNUSED const char *fmt,
                UNUSED REQUEST *request, UNUSED RADIUS_ESCAPE_STRING func)
{
        return -1;
}


struct testcase {
	const char *mac;
	int64_t expected;
};

static void mac_partitioner_testcase(const struct testcase *tc,size_t tc_len) {
	size_t i;
	for (i=0;i<tc_len;++i) {
		const int64_t mac_partitioner0_rc = mac_partitioner0(tc[i].mac,
							strlen(tc[i].mac));
		assert(mac_partitioner0_rc == tc[i].expected);
	}
}

static void mac_partitioner_test(void) {
	size_t i=0;
	/* Test 1: valid macs */
	struct testcase testcase[] = {
		{
			.mac = "01:23:45:67:89:ab",
			.expected = 1250999896491
		},{
			.mac = "01-23-45-67-89-ab",
			.expected = 1250999896491
		},{
			.mac = "012345-6789ab",
			.expected = 1250999896491
		},{
			.mac = "0123456789ab",
			.expected = 1250999896491
		},{
			.mac = "01:23:45:67:89:ab",
			.expected = 1250999896491
		}
	};

	mac_partitioner_testcase(testcase,sizeof(testcase)/sizeof(testcase[0]));

	/* Tests 2: invalid hex */
	char *mac_cpy = strdup("01:34:67:9a:cd:f0");
	for(i=0;i<strlen(mac_cpy);i++) {
		char aux = mac_cpy[i];
		if (mac_cpy[i] == ':') {
			continue;
		}

		mac_cpy[i] = 'g';
		assert(-1 == mac_partitioner0(mac_cpy,strlen(mac_cpy)));
		mac_cpy[i] = aux;
	}
	free(mac_cpy);

	/* Test 3: Invalid len */
	struct testcase testcase3[] = {
		{
			.mac = "1:23:45:67:89:ab",
			.expected = -1
		},{
			.mac = "01:3:45:67:89:ab",
			.expected = -1
		},{
			.mac = "01:23:5:67:89:ab",
			.expected = -1
		},{
			.mac = "01:23:45:7:89:ab",
			.expected = -1
		},{
			.mac = "01:23:45:67:9:ab",
			.expected = -1
		},{
			.mac = "01:23:45:67:89:b",
			.expected = -1
		},{
			.mac = "0-23-45-67-89-ab",
			.expected = -1
		},{
			.mac = "01-2-45-67-89-ab",
			.expected = -1
		},{
			.mac = "01-23-4-67-89-ab",
			.expected = -1
		},{
			.mac = "01-23-45-6-89-ab",
			.expected = -1
		},{
			.mac = "01-23-45-67-8-ab",
			.expected = -1
		},{
			.mac = "01-23-45-67-89-a",
			.expected = -1
		},{
			.mac = "0-6789ab",
			.expected = -1
		},{
			.mac = "01-6789ab",
			.expected = -1
		},{
			.mac = "0123-6789ab",
			.expected = -1
		},{
			.mac = "01234-6789ab",
			.expected = -1
		},{
			.mac = "012345-6",
			.expected = -1
		},{
			.mac = "012345-67",
			.expected = -1
		},{
			.mac = "012345-678",
			.expected = -1
		},{
			.mac = "012345-6789",
			.expected = -1
		},{
			.mac = "012345-6789a",
			.expected = -1
		},{
			.mac = "0123456789a",
			.expected = -1
		}
	};

	mac_partitioner_testcase(testcase3,
					sizeof(testcase3)/sizeof(testcase3[0]));
}

int main(void) {
	mac_partitioner_test();
	return 0;
}
