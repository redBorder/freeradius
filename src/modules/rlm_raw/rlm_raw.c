/*
 * rlm_raw.c
 *
 * Version:	$Id$
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program; if not, write to the Free Software
 *   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 *
 * Copyright 2000,2006  The FreeRADIUS server project
 * Copyright 2000  your name <your address>
 */

#include <freeradius-devel/ident.h>
RCSID("$Id$")

#include <freeradius-devel/radiusd.h>
#include <freeradius-devel/modules.h>

/*
 *	Define a structure for our module configuration.
 *
 *	These variables do not need to be in a structure, but it's
 *	a lot cleaner to do so, and a pointer to the structure can
 *	be used as the instance handle.
 */
typedef struct rlm_raw_t {
	char *xlat_name;
} rlm_raw_t;

typedef struct radius_packet_t {
	uint8_t code;
	uint8_t id;
	uint8_t length[2];
	uint8_t vector[AUTH_VECTOR_LEN];
	uint8_t data[1];
} radius_packet_t;

/*
 *	Dynamically xlat for %{raw:...} out of the
 *	decoded RADIUS attributes of the raw packet.
 */
static int raw_xlat(void *instance, REQUEST *request,
		    char *fmt, char *out, size_t outlen,
		    RADIUS_ESCAPE_STRING func)
{
	uint8_t strvalue[MAX_STRING_LEN];
	uint32_t lvalue;
	uint32_t vendorcode = 0;
	int attribute = 0;
	int vendorlen = 0;
	int sublen;
	int offset;
	char name[40];
	int attr;
	int type = PW_TYPE_OCTETS;
	ATTR_FLAGS flags;
	DICT_ATTR *da;
	DICT_VALUE *dv;
	char buf[1024];
	char *a = NULL;
	radius_packet_t *hdr = (radius_packet_t *)request->packet->data;
	uint8_t *ptr = hdr->data;
	uint8_t *subptr;
	int length = request->packet->data_len - AUTH_HDR_LEN;
	int attrlen = ptr[1];
	time_t t;
	struct tm s_tm;

	/*
	 *	The "format" string is the attribute name.
	 */
	if (!(da = dict_attrbyname(fmt)))
		return 0;
	strncpy(name, da->name, sizeof(name));
	attr = da->attr;
	type = da->type;
	flags = da->flags;
	
	while (length > 0) {
		if (vendorlen > 0) {
			attribute = *ptr++ | (vendorcode << 16);
			attrlen = *ptr++;
		} else {
			attribute = *ptr++;
			attrlen = *ptr++;
		}

		attrlen -= 2;
		length -= 2;

		/*
		 *	This could be a Vendor-Specific attribute.
		 */
		if ((vendorlen <= 0) && (attribute == PW_VENDOR_SPECIFIC)) {
			/* The attrlen was checked to be >= 6, in rad_recv */
			memcpy(&lvalue, ptr, 4);
			vendorcode = ntohl(lvalue);

			/*
			 *	This is an implementation issue.
			 *	We currently pack vendor into the upper
			 *	16 bits of a 32-bit attribute number,
			 *	so we can't handle vendor numbers larger
			 *	than 16 bits.
			 */
			if (vendorcode <= 65535) {
				/*
				 *	First, check to see if the
				 *	sub-attributes fill the VSA, as
				 *	defined by the RFC.  If not, then it
				 *	may be a USR-style VSA, or it may be a
				 *	vendor who packs all of the
				 *	information into one nonsense
				 *	attribute.
				 */
				subptr = ptr + 4;
				sublen = attrlen - 4;

				while (sublen > 0) {
					/* Too short or too long */
					if ((subptr[1] < 2) || (subptr[1] > sublen))
						break;

					/* Just right */
					sublen -= subptr[1];
					subptr += subptr[1];
				}

				if (!sublen) {
					ptr += 4;
					vendorlen = attrlen - 4;
					attribute = *ptr++ | (vendorcode << 16);
					attrlen = *ptr++;
					attrlen -= 2;
					length -= 6;
				} else if ((vendorcode == VENDORPEC_USR) &&
						((ptr[4] == 0) && (ptr[5] == 0)) &&
						(attrlen >= 8) &&
						(dict_attrbyvalue((vendorcode << 16) | (ptr[6] << 8) | ptr[7]))) {
					attribute = ((vendorcode << 16) | (ptr[6] << 8) | ptr[7]);
					ptr += 8;
					attrlen -= 8;
					length -= 8;
				}
			}
		}

		if (attribute == attr)
			break;

		ptr += attrlen;
		length -= attrlen;
		if (vendorlen > 0)
			vendorlen -= (attrlen + 2);
	}

	if (attribute != attr)
		return 0;

	switch (type) {
		/*
		 *	The attribute may be zero length,
		 *	or it may have a tag, and then no data...
		 */
		case PW_TYPE_STRING:
			offset = 0;
			if (flags.has_tag && (attrlen > 0) && (TAG_VALID_ZERO(*ptr) || (flags.encrypt == FLAG_ENCRYPT_TUNNEL_PASSWORD))) {
				attrlen--;
				offset = 1;
			}
			memcpy(strvalue, ptr + offset, attrlen);

			/*
			 *	NAS-Port may have multiple integer values?
			 *	This is an internal server extension...
			 */
			if (attribute == PW_NAS_PORT)
				a = (char *)strvalue;
			else {
				fr_print_string((char *)strvalue, attrlen, buf, sizeof(buf));
				a = buf;
			}	       
			break;

		case PW_TYPE_OCTETS:
			/* attrlen always < MAX_STRING_LEN */
			memcpy(strvalue, ptr, attrlen);
			break;

		case PW_TYPE_INTEGER:
		case PW_TYPE_DATE:
		case PW_TYPE_IPADDR:
			/*
			 *	Check for RFC compliance.  If the
			 *	attribute isn't compliant, turn it
			 *	into a string of raw octets.
			 *
			 *	Also set the lvalue to something
			 *	which should never match anything.
			 */
			if (attrlen != 4) {
				type = PW_TYPE_OCTETS;
				memcpy(strvalue, ptr, attrlen);
				break;
			}

			memcpy(&lvalue, ptr, 4);

			if (type != PW_TYPE_IPADDR) {
				lvalue = ntohl(lvalue);

				/*
				 *	Tagged attributes of type integer have
				 *	special treatment.
				 */
				if (type == PW_TYPE_INTEGER) {
					if (flags.has_tag)
						lvalue &= 0x00ffffff;

					/*
					 *	Try to get the name for integer
					 *	attributes.
					 */
					if ((dv = dict_valbyattr(attribute, lvalue)))
						a = dv->name;
					else {
						snprintf(buf, sizeof(buf), "%u", lvalue);
						a = buf;
					}
				} else {
					t = lvalue;
					strftime(buf, sizeof(buf), "%b %e %Y %H:%M:%S %Z", localtime_r(&t, &s_tm));
					a = buf;
				}
			} else
				a = ip_ntoa(buf, lvalue);
			break;

		/*
		 *	IPv6 interface ID is 8 octets long.
		 */
		case PW_TYPE_IFID:
			memcpy(strvalue, ptr, attrlen);
			if (attrlen != 8)
				type = PW_TYPE_OCTETS;
			else
				a = ifid_ntoa(buf, sizeof(buf), strvalue);
			break;

		/*
		 *	IPv6 addresses are 16 octets long.
		 */
		case PW_TYPE_IPV6ADDR:
			memcpy(strvalue, ptr, attrlen);
			if (attrlen != 16)
				type = PW_TYPE_OCTETS;
			else
				a = ipv6_ntoa(buf, sizeof(buf), strvalue);
			break;

		default:
			DEBUG("rlm_raw: %s (Unknown Type %d)", name, type);
			break;
	}

	if (type == PW_TYPE_OCTETS) {
		strcpy(buf, "0x");
		a = buf + 2;
		for (t = 0; t < attrlen; t++) {
			sprintf(a, "%02x", strvalue[t]);
			a += 2;
		}
		a = buf;
	}

	strncpy(out, a?a:"UNKNOWN-TYPE", outlen);
	DEBUG2("rlm_raw: %s = %s", name, out);

	return strlen(out);
}


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
static int raw_instantiate(CONF_SECTION *conf, void **instance)
{
	rlm_raw_t *data;
	char *xlat_name;

	/*
	 *	Set up storage area for instance data.
	 */
	if (!(data = rad_malloc(sizeof(*data))))
		return -1;
	memset(data, 0, sizeof(*data));

	if (!(xlat_name = cf_section_name2(conf)))
		xlat_name = cf_section_name1(conf);
	if (xlat_name) {
		data->xlat_name = strdup(xlat_name);
		xlat_register(xlat_name, raw_xlat, data);
	}

	*instance = data;

	return 0;
}

static int raw_detach(void *instance)
{
	rlm_raw_t *data = (rlm_raw_t *)instance;

	xlat_unregister(data->xlat_name, raw_xlat, instance);
	free(data->xlat_name);

	free(instance);
	return 0;
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
module_t rlm_raw = {
	RLM_MODULE_INIT,
	"raw",
	RLM_TYPE_THREAD_SAFE,		/* type */
	raw_instantiate,		/* instantiation */
	raw_detach,			/* detach */
	{
		NULL,	/* authentication */
		NULL,	/* authorization */
		NULL,	/* preaccounting */
		NULL,	/* accounting */
		NULL,	/* checksimul */
		NULL,			/* pre-proxy */
		NULL,			/* post-proxy */
		NULL			/* post-auth */
	},
};
