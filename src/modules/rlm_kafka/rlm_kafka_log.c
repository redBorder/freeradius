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

#include <freeradius-devel/ident.h>
RCSID("$Id$")

#include <freeradius-devel/radiusd.h>
#include <freeradius-devel/modules.h>
#include <freeradius-devel/rad_assert.h>

#include <fcntl.h>
#include <sys/stat.h>

static int kafka_log_instantiate(CONF_SECTION *conf, void **instance);
static int kafka_log_detach(void *instance);
static int kafka_log_accounting(void *instance, REQUEST *request);

#define MAX_QUERY_LEN 4096

/*
 *	Define a structure for our module configuration.
 */
typedef struct rlm_kafka_log_config_t {
	char		*broker;
	char		*topic;
	int 		port;
	CONF_SECTION	*conf_section;
} rlm_kafka_log_config_t;

/*
 *	A mapping of configuration file names to internal variables.
 */
static const CONF_PARSER module_config[] = {
	{"broker", PW_TYPE_STRING_PTR,
	 offsetof(rlm_kafka_log_config_t,broker), NULL, "localhost"},
	{"topic", PW_TYPE_STRING_PTR,
	 offsetof(rlm_kafka_log_config_t,topic), NULL, NULL},
	{"port", PW_TYPE_INTEGER,
	 offsetof(rlm_kafka_log_config_t,port), NULL, NULL},

	{ NULL, -1, 0, NULL, NULL }	/* end the list */
};

/*
 *	Do any per-module initialization that is separate to each
 *	configured instance of the module.  e.g. set up connections
 *	to external databases, read configuration files, set up
 *	dictionary entries, etc.
 *
 *	If configuration information is given in the config section
 *	that must be referenced in later calls, store a handle to it
 *	in *instance otherwise put a null pointer there.
 */
static int kafka_log_instantiate(CONF_SECTION *conf, void **instance)
{
	rlm_kafka_log_config_t	*inst;

	/*
	 *      Set up a storage area for instance data.
	 */
	inst = calloc(1, sizeof(rlm_kafka_log_config_t));
	if (inst == NULL) {
	        radlog(L_ERR, "rlm_kafka_log: Not enough memory");
	        return -1;
	}

	/*
	 *	If the configuration parameters can't be parsed,
	 *	then fail.
	 */
	if (cf_section_parse(conf, inst, module_config) < 0) {
		radlog(L_ERR, "rlm_kafka_log: Unable to parse parameters");
		kafka_log_detach(inst);
		return -1;
	}

	inst->conf_section = conf;
	*instance = inst;


	return 0;
}

/*
 *	Say goodbye to the cruel world.
 */
static int kafka_log_detach(void *instance)
{
	int i;
	char **p;
	rlm_kafka_log_config_t *inst = (rlm_kafka_log_config_t *)instance;

	/*
	 *	Free up dynamically allocated string pointers.
	 */
	for (i = 0; module_config[i].name != NULL; i++) {
		if (module_config[i].type != PW_TYPE_STRING_PTR) {
			continue;
		}

		/*
		 *	Treat 'config' as an opaque array of bytes,
		 *	and take the offset into it.  There's a
		 *      (char*) pointer at that offset, and we want
		 *	to point to it.
		 */
		p = (char **) (((char *)inst) + module_config[i].offset);
		if (!*p) { /* nothing allocated */
			continue;
		}
		free(*p);
		*p = NULL;
	}
	free(inst);
	return 0;
}

/*
 *	Write the line into kafka
 */
static int kafka_log_produce(rlm_kafka_log_config_t *inst, REQUEST *request, const char *line)
{

	/* ERROR EXAMPLE */
	#if 0	
	if ((fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0666)) < 0) {
		radlog_request(L_ERR, 0, request, "Couldn't open file %s: %s",
			       path, strerror(errno));
		return RLM_MODULE_FAIL;
	}
	#endif
	RDEBUG("Producing kafka_log_accounting: %s",line);
	return RLM_MODULE_OK;
}

static char *packet2buffer(rlm_kafka_log_config_t *inst, const REQUEST *request){
	const RADIUS_PACKET *packet = request->packet;
#define BUFFER_SIZE 4096 // @TODO pass to 

	char *buffer = calloc(BUFFER_SIZE,sizeof(char));
	size_t cursor = 0;
	cursor += snprintf(buffer,BUFFER_SIZE - cursor,"{\"timestamp\":%lu,",time(NULL));
	if ((packet->code > 0) && (packet->code < FR_MAX_PACKET_CODE))
		cursor += snprintf(buffer + cursor,BUFFER_SIZE - cursor,"\"packet_type\":\"%s\"",fr_packet_codes[packet->code]);
	else
		cursor += snprintf(buffer + cursor,BUFFER_SIZE - cursor,"\"packet_type\":\"%d\"",packet->code);

	snprintf(buffer + cursor,BUFFER_SIZE - cursor, "}");

	return buffer;

	#if 0
	
	/*
	 *	Write the information to the file.
	 */
	if (!compat) {
		/*
		 *	Print out names, if they're OK.
		 *	Numbers, if not.
		 */
		if ((packet->code > 0) &&
		    (packet->code < FR_MAX_PACKET_CODE)) {
			fprintf(fp, "\tPacket-Type = %s\n",
				fr_packet_codes[packet->code]);
		} else {
			fprintf(fp, "\tPacket-Type = %d\n", packet->code);
		}
	}

	if (inst->log_srcdst) {
		VALUE_PAIR src_vp, dst_vp;

		memset(&src_vp, 0, sizeof(src_vp));
		memset(&dst_vp, 0, sizeof(dst_vp));
		src_vp.operator = dst_vp.operator = T_OP_EQ;

		switch (packet->src_ipaddr.af) {
		case AF_INET:
			src_vp.name = "Packet-Src-IP-Address";
			src_vp.type = PW_TYPE_IPADDR;
			src_vp.attribute = PW_PACKET_SRC_IP_ADDRESS;
			src_vp.vp_ipaddr = packet->src_ipaddr.ipaddr.ip4addr.s_addr;
			dst_vp.name = "Packet-Dst-IP-Address";
			dst_vp.type = PW_TYPE_IPADDR;
			dst_vp.attribute = PW_PACKET_DST_IP_ADDRESS;
			dst_vp.vp_ipaddr = packet->dst_ipaddr.ipaddr.ip4addr.s_addr;
			break;
		case AF_INET6:
			src_vp.name = "Packet-Src-IPv6-Address";
			src_vp.type = PW_TYPE_IPV6ADDR;
			src_vp.attribute = PW_PACKET_SRC_IPV6_ADDRESS;
			memcpy(src_vp.vp_strvalue,
			       &packet->src_ipaddr.ipaddr.ip6addr,
			       sizeof(packet->src_ipaddr.ipaddr.ip6addr));
			dst_vp.name = "Packet-Dst-IPv6-Address";
			dst_vp.type = PW_TYPE_IPV6ADDR;
			dst_vp.attribute = PW_PACKET_DST_IPV6_ADDRESS;
			memcpy(dst_vp.vp_strvalue,
			       &packet->dst_ipaddr.ipaddr.ip6addr,
			       sizeof(packet->dst_ipaddr.ipaddr.ip6addr));
			break;
		default:
			break;
		}

		vp_print(fp, &src_vp);
		vp_print(fp, &dst_vp);

		src_vp.name = "Packet-Src-IP-Port";
		src_vp.attribute = PW_PACKET_SRC_PORT;
		src_vp.type = PW_TYPE_INTEGER;
		src_vp.vp_integer = packet->src_port;
		dst_vp.name = "Packet-Dst-IP-Port";
		dst_vp.attribute = PW_PACKET_DST_PORT;
		dst_vp.type = PW_TYPE_INTEGER;
		dst_vp.vp_integer = packet->dst_port;

		vp_print(fp, &src_vp);
		vp_print(fp, &dst_vp);
	}

	/* Write each attribute/value to the log file */
	for (pair = packet->vps; pair != NULL; pair = pair->next) {
		DICT_ATTR da;
		da.attr = pair->attribute;

		if (inst->ht &&
		    fr_hash_table_finddata(inst->ht, &da)) continue;

		/*
		 *	Don't print passwords in old format...
		 */
		if (compat && (pair->attribute == PW_USER_PASSWORD)) continue;

		/*
		 *	Print all of the attributes.
		 */
		vp_print(fp, pair);
	}

	/*
	 *	Add non-protocol attibutes.
	 */
	if (compat) {
		if (request->proxy) {
			char proxy_buffer[128];

			inet_ntop(request->proxy->dst_ipaddr.af,
				  &request->proxy->dst_ipaddr.ipaddr,
				  proxy_buffer, sizeof(proxy_buffer));
			fprintf(fp, "\tFreeradius-Proxied-To = %s\n",
				proxy_buffer);
			RDEBUG("Freeradius-Proxied-To = %s",
				proxy_buffer);
		}

		fprintf(fp, "\tTimestamp = %ld\n",
			(unsigned long) request->timestamp);
	}

	fprintf(fp, "\n");
	#endif
}

/*
 *	Write accounting information to this module's database.
 */
static int kafka_log_accounting(void *instance, REQUEST *request)
{
	int		ret;
	char		querystr[MAX_QUERY_LEN];
	const char	*cfquery;
	rlm_kafka_log_config_t	*inst = (rlm_kafka_log_config_t *)instance;
	VALUE_PAIR	*pair;
	DICT_VALUE	*dval;
	CONF_PAIR	*cp;

	rad_assert(request != NULL);
	rad_assert(request->packet != NULL);

	RDEBUG("Processing kafka_log_accounting");

	/* Find the Acct Status Type. */
	if ((pair = pairfind(request->packet->vps, PW_ACCT_STATUS_TYPE)) == NULL) {
		radlog_request(L_ERR, 0, request, "Packet has no account status type");
		return RLM_MODULE_INVALID;
	}

	/* Search the query in conf section of the module */
	if ((dval = dict_valbyattr(PW_ACCT_STATUS_TYPE, pair->vp_integer)) == NULL) {
		radlog_request(L_ERR, 0, request, "Unsupported Acct-Status-Type = %d",
			       pair->vp_integer);
		return RLM_MODULE_NOOP;
	}

	/*
	if ((cp = cf_pair_find(inst->conf_section, dval->name)) == NULL) {
		RDEBUG("Couldn't find an entry %s in the config section",
		       dval->name);
		return RLM_MODULE_NOOP;
	}
	*/

	char *kafka_buffer = packet2buffer(inst,request);
	if(kafka_buffer){
		kafka_log_produce(inst,request,kafka_buffer);
		free(kafka_buffer);
	}

	/* Xlat the query */
	#if 0
	ret = sql_xlat_query(inst, request, cfquery, querystr, sizeof(querystr));
	if (ret != RLM_MODULE_OK)
		return ret;

	return kafka_log_produce(inst, request, querystr);
	#endif
	return RLM_MODULE_OK;
}

/*
 *	The module name should be the only globally exported symbol.
 *	That is, everything else should be 'static'.
 *
 *	If the module needs to temporarily modify it's instantiation
 *	data, the type should be changed to RLM_TYPE_THREAD_UNSAFE.
 *	The server will then take care of ensuring that the module
 *	is single-threaded.
 */
module_t rlm_kafka_log = {
	RLM_MODULE_INIT,
	"sql_log",
	RLM_TYPE_CHECK_CONFIG_SAFE | RLM_TYPE_HUP_SAFE,		/* type */
	kafka_log_instantiate,		/* instantiation */
	kafka_log_detach,			/* detach */
	{
		NULL,			/* authentication */
		NULL,			/* authorization */
		NULL,			/* preaccounting */
		kafka_log_accounting,	/* accounting */
		NULL,			/* checksimul */
		NULL,			/* pre-proxy */
		NULL,			/* post-proxy */
		NULL			/* post-auth */
	},
};
