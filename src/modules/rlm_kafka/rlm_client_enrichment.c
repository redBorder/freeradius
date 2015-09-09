/*
 *  rlm_client_enrichment	Mantains an per client enrichment database
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

#include "rlm_client_enrichment.h"
#include <assert.h>
#include <stdlib.h>
#include <freeradius-devel/rad_assert.h>
#include <include/radiusd.h>

#define CLIENT_ENRICHMENT_MAGIC 0x13313a1d13313a1dL

struct client_enrichment {
#ifdef CLIENT_ENRICHMENT_MAGIC
	uint64_t magic;
#endif
	const char *name;
	const char *enrichment;
};

static uint32_t client_hash(const void *voidclient) {
	const struct client_enrichment *client = voidclient;
#ifdef CLIENT_ENRICHMENT_MAGIC
	assert(CLIENT_ENRICHMENT_MAGIC == client->magic);
#endif

	return fr_hash_string(client->name);
}

static int client_cmp(const void *voidclient1, const void *voidclient2) {
const struct client_enrichment *client1 = voidclient1;
const struct client_enrichment *client2 = voidclient2;
#ifdef CLIENT_ENRICHMENT_MAGIC
	assert(CLIENT_ENRICHMENT_MAGIC == client1->magic);
	assert(CLIENT_ENRICHMENT_MAGIC == client2->magic);
#endif

	return strcmp(client1->name,client2->name);
}

client_enrichment_db_t *client_enrichment_db_new(void) {
	return fr_hash_table_create(client_hash,
					  client_cmp,free);
}

/* Do a dynamic-allocated snprintf, and return size in parameter */
static char *heap_snprinf(size_t *size,const char *fmt,...) {
	va_list args;

	va_start (args, fmt);
	const ssize_t needed_size = vsnprintf (NULL,0,fmt, args);
	if(needed_size < 0) {
		char errbuf[BUFSIZ];
		strerror_r(errno,errbuf,sizeof(errbuf));
		radlog(L_ERR, "rlm_kafka_log: Couldn't parse format string: %s",
			errbuf);
		return NULL;
	}
	va_end (args);

	char *ret = calloc(1,needed_size+1);
	if(NULL == ret) {
		radlog(L_ERR, "rlm_kafka_log: Can't allocate string (out of memory?)");
		return NULL;
	}

	va_start (args, fmt);
	const ssize_t printf_rc = vsprintf (ret,fmt, args);
	if(printf_rc < 0) {
		char errbuf[BUFSIZ];
		strerror_r(errno,errbuf,sizeof(errbuf));
		radlog(L_ERR, "rlm_kafka_log: Couldn't print format string: %s",
			errbuf);
		free(ret);
		return NULL;
	}
	va_end (args);

	*size = needed_size;

	return ret;
}

static int client_enrichment_db_add0(client_enrichment_db_t *db,
            const char *client_name,size_t client_name_len,
            const char *enrichment,size_t enrichment_len) {

	// Need this aux ptrs for memcpy
	char *dst_client_name=NULL,*dst_client_enrichment=NULL;

	/* We will allocate all structure in one memory segment */
	struct client_enrichment *client = calloc(1,sizeof(*client) + client_name_len + 
		enrichment_len + 2);

	if(NULL == client) {
		radlog(L_ERR,"Couldn't allocate client (out of memory?)");
		return 0;
	}

#ifdef CLIENT_ENRICHMENT_MAGIC
	client->magic = CLIENT_ENRICHMENT_MAGIC;
#endif

	client->name = dst_client_name = (char *)&client[1];
	client->enrichment = dst_client_enrichment = dst_client_name + client_name_len + 1;

	memcpy(dst_client_name,client_name,client_name_len);
	memcpy(dst_client_enrichment,enrichment,enrichment_len);

	/* And save it in database */
	const int insert_rc = fr_hash_table_insert(db, client);
	if(0 == insert_rc) {
		radlog(L_ERR,"Can't insert client %s because it already exists",client_name);
		return 0;
	}

	return 1;
}

int client_enrichment_db_add(client_enrichment_db_t *db,const char *client_name,
                                                const char *enrichment) {
	size_t real_enrichment_len = 0;
	char *real_enrichment = heap_snprinf(&real_enrichment_len,",\"enrichment\":%s",enrichment);
	if(NULL == real_enrichment) {
		radlog(L_ERR,"Can't allocate real enrichment (out of memory?)");
		return 0;
	}

	const int rc = client_enrichment_db_add0(db,client_name,strlen(client_name),
		real_enrichment,real_enrichment_len);

	free(real_enrichment);

	return rc;
}


const char *client_enrichment_db_get(client_enrichment_db_t *db,const char *client_name) {
	const struct client_enrichment dummy_client = {
#ifdef CLIENT_ENRICHMENT_MAGIC
		.magic = CLIENT_ENRICHMENT_MAGIC,
#endif
		.name = client_name,
		.enrichment = NULL
	};

	struct client_enrichment *client = fr_hash_table_finddata(db, &dummy_client);
	return client ? client->enrichment : NULL;
}

void client_enrichment_db_free(client_enrichment_db_t *db) {
	// this will free all saved items
	fr_hash_table_free(db);
}
