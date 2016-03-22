#
# Makefile
#
# Version:	$Id$
#

TARGET      = rlm_kafka_log
SRCS        = rlm_kafka_log.c rlm_client_enrichment.c
RLM_LDFLAGS = -lrdkafka -lrbutils
OBJECTS     = ../../main/.libs/log.o ../../main/conffile.lo ../../main/util.lo

include ../rules.mak

# Some modules tests

## this uses the RLM_CFLAGS and RLM_LIBS and SRCS defs to make TARGET.
# This needs to be after include rules.mak because of the use of variables.
#
test$(EXEEXT): test.lo $(CLIENTLIBS)
	$(LIBTOOL) --mode=link --tag=CC $(CC) $(LDFLAGS) $(RLM_LDFLAGS) -o $@ test.lo rlm_client_enrichment.lo $(LIBRADIUS) $(OBJECTS) $(LIBS)

test.lo: CFLAGS += -DRADIUSD_VERSION_STRING=\"${RADIUSD_VERSION_STRING}\"
test.lo: test.c $(HEADERS)
	$(LIBTOOL) --mode=compile --tag=CC $(CC) $(CFLAGS) $(RLM_CFLAGS) -c test.c
