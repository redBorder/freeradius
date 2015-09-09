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

#ifndef RLM_CLIENT_ENRICHMENT_H
#define RLM_CLIENT_ENRICHMENT_H

#include <stdint.h>
#include <string.h>
#include "include/hash.h"

typedef fr_hash_table_t client_enrichment_db_t;

client_enrichment_db_t *client_enrichment_db_new(void);
int client_enrichment_db_add(client_enrichment_db_t *db,const char *client,const char *enrichment);
const char *client_enrichment_db_get(client_enrichment_db_t *db,const char *client);
void client_enrichment_db_free(client_enrichment_db_t *db);

#endif  /* RLM_CLIENT_ENRICHMENT_H */
