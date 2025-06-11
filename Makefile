-include config.mk

mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
HOME := $(dir $(mkfile_path))
BUILD_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
$(info BUILD_DIR is: ${BUILD_DIR}, HOME is: $(HOME))

# ib or dpdk
env := $(shell cat env.txt 2>/dev/null || echo "ib")

### eRPC
ERPC_PATH="$(BUILD_DIR)/eRPC"
ERPC_CFLAGS_DPDK := -I $(ERPC_PATH)/src -DERPC_DPDK=true -march=native -I /usr/include/dpdk -DERPC_LOG_LEVEL=6 -DERPC_TESTING=false -DGFLAGS_IS_A_DLL=0
ERPC_LDFLAGS_DPDK := -L $(ERPC_PATH)/build -Wl,--whole-archive -ldpdk -Wl,--no-whole-archive -lerpc -lpthread  -lnuma -ldl -lgflags  -ldl -libverbs -lmlx4 -lmlx5

ERPC_CFLAGS_IB := -I $(ERPC_PATH)/src -DERPC_INFINIBAND=true -march=native -I /usr/include/dpdk -DERPC_LOG_LEVEL=6 -DERPC_TESTING=false -DGFLAGS_IS_A_DLL=0
ERPC_LDFLAGS_IB := -L $(ERPC_PATH)/build -Wl,--whole-archive -ldpdk -Wl,--no-whole-archive -lpthread -lerpc -lnuma -ldl -lgflags -ldl -libverbs -lmlx4 -lmlx5

CXX_INCLUDES = -I $(ERPC_PATH)/third_party/googletest/googletest/include -I $(ERPC_PATH)/third_party/googletest/googletest -isystem $(ERPC_PATH)/third_party/asio/include -I $(ERPC_PATH)/src -isystem $(ERPC_PATH)/third_party -isystem /usr/include/dpdk -I$(ERPC_PATH)/third_party/gflags/include -I$(ERPC_PATH)/third_party/HdrHistogram_c/src
CXX_INCLUDES := $(CXX_INCLUDES)
### Options ###

DEBUG ?= 0
CHECK_INVARIANTS ?= 0
COCO ?= 0

# 0 = libc malloc
# 1 = jemalloc
# 2 = tcmalloc
# 3 = flow
USE_MALLOC_MODE ?= 1

# Available modes
#   * perf
#   * backoff
#   * factor-gc
#   * factor-gc-nowriteinplace
#   * factor-fake-compression
#   * sandbox
MODE ?= perf

STO_RMW ?= 0
HASHTABLE ?= 0
GPROF ?= 0

OPACITY ?= 0

CC ?= gcc
CXX ?= c++

# customized options
PAXOS_LIB_ENABLED ?= 0
DISABLE_MULTI_VERSION ?= 0
MICRO_BENCHMARK ?= 0
# this is for TPC-C on NewOrder on MEGA replication
MEGA_BENCHMARK ?= 0
# this is for Microbench on NewOrder on MEGA replication
MEGA_BENCHMARK_MICRO ?= 0
TRACKING_LATENCY ?= 0
SHARDS ?= 1
# if merge to one key
MERGE_KEYS_GROUPS ?= $(SHARDS)
# shard failure optimization
FAIL_NEW_VERSION ?= 0
# Simulate 1 shard per thread
SIMULATE_ONE_SHARD_PER_THREAD ?= 0
# Tracking rollback transactions
TRACKING_ROLLBACK ?= 0

###############

DEBUG_S=$(strip $(DEBUG))
CHECK_INVARIANTS_S=$(strip $(CHECK_INVARIANTS))
EVENT_COUNTERS_S=$(strip $(EVENT_COUNTERS))
USE_MALLOC_MODE_S=$(strip $(USE_MALLOC_MODE))
MODE_S=$(strip $(MODE))
STO_RMW_S=$(strip $(STO_RMW))
HASHTABLE_S=$(strip $(HASHTABLE))
GPROF_S=$(strip $(GPROF))
MASSTREE_CONFIG:=CC="$(CC)" CXX="$(CXX)" --enable-max-key-len=1024
PAXOS_LIB_ENABLED_S=$(strip $(PAXOS_LIB_ENABLED))
DISABLE_MULTI_VERSION_S=$(strip $(DISABLE_MULTI_VERSION))
MICRO_BENCHMARK_S=$(strip $(MICRO_BENCHMARK))
COCO_S=$(strip $(COCO))
MEGA_BENCHMARK_S=$(strip $(MEGA_BENCHMARK))
MEGA_BENCHMARK_MICRO_S=$(strip $(MEGA_BENCHMARK_MICRO))
TRACKING_LATENCY_S=$(strip $(TRACKING_LATENCY))
SHARDS_S=$(strip $(SHARDS))
MERGE_KEYS_GROUPS_S=$(strip $(MERGE_KEYS_GROUPS))
FAIL_NEW_VERSION_S=$(strip $(FAIL_NEW_VERSION))
SIMULATE_ONE_SHARD_PER_THREAD_S=$(strip $(SIMULATE_ONE_SHARD_PER_THREAD))
TRACKING_ROLLBACK_S=$(strip $(TRACKING_ROLLBACK))


ifeq ($(DEBUG_S),1)	
	OSUFFIX_D=
	MASSTREE_CONFIG+=--enable-assertions
else
	MASSTREE_CONFIG+=--disable-assertions
endif
ifeq ($(CHECK_INVARIANTS_S),1)
	OSUFFIX_S=.check
	MASSTREE_CONFIG+=--enable-invariants --enable-preconditions
else
	MASSTREE_CONFIG+=--disable-invariants --disable-preconditions
endif
ifeq ($(EVENT_COUNTERS_S),1)
	OSUFFIX_E=.ectrs
endif
ifeq ($(STO_RMW_S),1)
	OSUFFIX_R=.rmw
endif
ifeq ($(HASHTABLE_S),1)
        OSUFFIX_H=.ht
endif
OSUFFIX=$(OSUFFIX_D)$(OSUFFIX_S)$(OSUFFIX_E)$(OSUFFIX_H)$(OSUFFIX_R)

ifeq ($(MODE_S),perf)
	O := out-perf$(OSUFFIX)
	CONFIG_H = config/config-perf.h
else ifeq ($(MODE_S),backoff)
	O := out-backoff$(OSUFFIX)
	CONFIG_H = config/config-backoff.h
else ifeq ($(MODE_S),factor-gc)
	O := out-factor-gc$(OSUFFIX)
	CONFIG_H = config/config-factor-gc.h
else ifeq ($(MODE_S),factor-gc-nowriteinplace)
	O := out-factor-gc-nowriteinplace$(OSUFFIX)
	CONFIG_H = config/config-factor-gc-nowriteinplace.h
else ifeq ($(MODE_S),factor-fake-compression)
	O := out-factor-fake-compression$(OSUFFIX)
	CONFIG_H = config/config-factor-fake-compression.h
else ifeq ($(MODE_S),sandbox)
	O := out-sandbox$(OSUFFIX)
	CONFIG_H = config/config-sandbox.h
else
	$(error invalid mode)
endif

#CXXFLAGS := -g -Wall
# suppress all warning: https://gcc.gnu.org/onlinedocs/gcc/Warning-Options.html
CXXFLAGS := -w -Wreturn-type
CXXFLAGS += -MD -MP -Ithird-party/lz4 -Ithird-party/paxos/src -I$(HOME) -DCONFIG_H=\"$(CONFIG_H)\"
ifeq ($(env),ib)
  $(info We use ib device: $(env).)
  CXXFLAGS += $(ERPC_CFLAGS_IB)
else
  $(info We use dpdk device: $(env).)
  CXXFLAGS += $(ERPC_CFLAGS_DPDK)
endif
ifeq ($(GPROF_S),1)
	CXXFLAGS += -pg -static-libstdc++ -static-libgcc
endif

ifeq ($(DEBUG_S),1)
    CXXFLAGS += -fno-omit-frame-pointer -DDEBUG
else
    #CXXFLAGS += -O2 #-funroll-loops -fno-omit-frame-pointer
    CXXFLAGS += -O2 #-funroll-loops -fno-omit-frame-pointer
    # CXXFLAGS += -DENABLE_BENCH_TXN_COUNTERS
endif
ifeq ($(CHECK_INVARIANTS_S),1)
	CXXFLAGS += -DCHECK_INVARIANTS
endif
ifeq ($(EVENT_COUNTERS_S),1)
	CXXFLAGS += -DENABLE_EVENT_COUNTERS
endif
ifeq ($(PAXOS_LIB_ENABLED_S),1)
	CXXFLAGS += -DPAXOS_LIB_ENABLED
endif

ifeq ($(DISABLE_MULTI_VERSION_S),1)
	CXXFLAGS += -DDISABLE_MULTI_VERSION
endif

ifeq ($(MICRO_BENCHMARK_S),1)
	CXXFLAGS += -DMICRO_BENCHMARK
endif

ifeq ($(COCO_S),1)
	CXXFLAGS += -DCOCO
endif

ifeq ($(MEGA_BENCHMARK_S),1)
	CXXFLAGS += -DMEGA_BENCHMARK
endif

ifeq ($(MEGA_BENCHMARK_MICRO_S),1)
	CXXFLAGS += -DMEGA_BENCHMARK_MICRO
endif

ifeq ($(TRACKING_LATENCY_S),1)
    CXXFLAGS += -DTRACKING_LATENCY
endif

ifeq ($(FAIL_NEW_VERSION_S),1)
    CXXFLAGS += -DFAIL_NEW_VERSION
endif

ifeq ($(SIMULATE_ONE_SHARD_PER_THREAD_S),1)
    CXXFLAGS += -DSIMULATE_ONE_SHARD_PER_THREAD
endif

ifeq ($(TRACKING_ROLLBACK_S),1)
    CXXFLAGS += -DTRACKING_ROLLBACK
endif

# AND then adopt the Node
CXXFLAGS += -DSHARDS=$(SHARDS_S)
CXXFLAGS += -DMERGE_KEYS_GROUPS=$(MERGE_KEYS_GROUPS_S)

CXXFLAGS += -include masstree/config.h
OBJDEP += masstree/config.h
O := $(O).masstree
CXXFLAGS += -DREAD_MY_WRITES=$(STO_RMW_S)
CXXFLAGS += -DHASHTABLE=$(HASHTABLE_S)
CXXFLAGS += -DSTO_OPACITY=$(OPACITY)

ifdef LESSER_OPACITY
CXXFLAGS += -DLESSER_OPACITY=$(LESSER_OPACITY)
endif

ifdef GV7_OPACITY
CXXFLAGS += -DGV7_OPACITY
endif

ifdef ABORT_ON_LOCKED
CXXFLAGS += -DSTO_ABORT_ON_LOCKED=$(ABORT_ON_LOCKED)
endif

TOP     := $(shell echo $${PWD-`pwd`})
LDFLAGS := -lpthread -lnuma -lrt -lmemcached
ifeq ($(GPROF_S),1)
        LDFLAGS += -pg -static-libstdc++ -static-libgcc 
endif

LZ4LDFLAGS := -Lthird-party/lz4 -llz4 -Wl,-rpath,$(TOP)/third-party/lz4

ifeq ($(DEBUG_S),1)
	CXXFLAGS+=-DSIMPLE_WORKLOAD
endif

ifeq ($(USE_MALLOC_MODE_S),1)
        CXXFLAGS+=-DUSE_JEMALLOC
        LDFLAGS+=-ljemalloc
	MASSTREE_CONFIG+=--with-malloc=jemalloc
else ifeq ($(USE_MALLOC_MODE_S),2)
        CXXFLAGS+=-DUSE_TCMALLOC
        LDFLAGS+=-ltcmalloc
	MASSTREE_CONFIG+=--with-malloc=tcmalloc
else ifeq ($(USE_MALLOC_MODE_S),3)
        CXXFLAGS+=-DUSE_FLOW
        LDFLAGS+=-lflow
	MASSTREE_CONFIG+=--with-malloc=flow
else
	MASSTREE_CONFIG+=--with-malloc=malloc
endif

ifneq ($(strip $(CUSTOM_LDPATH)), )
        LDFLAGS+=$(CUSTOM_LDPATH)
endif

## Add ERPC flags
ASIO_PATH="$(BUILD_DIR)/eRPC/third_party/asio"
ASIO_CFLAGS := -I $(ASIO_PATH)/include
CFLAGS += $(ASIO_CFLAGS)
ifeq ($(env),ib)
  CFLAGS += $(ERPC_CFLAGS_IB)
  LDFLAGS += $(ERPC_LDFLAGS_IB)
else
  CFLAGS += $(ERPC_CFLAGS_DPDK)
  LDFLAGS += $(ERPC_LDFLAGS_DPDK)
endif
LDFLAGS += -levent_pthreads -pthread -lboost_fiber -lboost_context -lboost_system -lboost_thread
LIBEVENT_CFLAGS := $(shell pkg-config --cflags libevent)
LIBEVENT_LDFLAGS := $(shell pkg-config --libs libevent)
CFLAGS += $(LIBEVENT_CFLAGS)
LDFLAGS += $(LIBEVENT_LDFLAGS)
LDFLAGS += -I$(HOME)
CXXFLAGS += $(ASIO_CFLAGS)

SRCFILES = allocator.cc \
	btree.cc \
	core.cc \
	counter.cc \
	memory.cc \
	rcu.cc \
	stats_server.cc \
	thread.cc \
	ticker.cc \
	tuple.cc \
	txn_btree.cc \
	txn.cc \
	txn_proto2_impl.cc \
	varint.cc

MASSTREE_SRCFILES = masstree/compiler.cc \
	masstree/str.cc \
	masstree/string.cc \
	masstree/straccum.cc \
	masstree/json.cc \
	masstree/kvthread.cc

OBJFILES := $(patsubst %.cc, $(O)/%.o, $(SRCFILES))

MASSTREE_OBJFILES := $(patsubst masstree/%.cc, $(O)/%.o, $(MASSTREE_SRCFILES))

BENCH_CXXFLAGS := $(CXXFLAGS)
BENCH_LDFLAGS := $(LDFLAGS) -lz -lrt -lcrypt -laio -ldl -lssl -lcrypto

BENCH_SRCFILES = benchmarks/bench.cc \
	benchmarks/encstress.cc \
	benchmarks/bid.cc \
	benchmarks/queue.cc \
	benchmarks/tpcc.cc \
	benchmarks/tpcc_simple.cc\
	benchmarks/ycsb.cc \
	benchmarks/sto/Transaction.cc \
	benchmarks/sto/MassTrans.cc \
	benchmarks/sto/common.cc \
	benchmarks/sto/TRcu.cc \
	benchmarks/sto/Packer.cc \
	benchmarks/sto/ReplayDB.cc \
	benchmarks/sto/ThreadPool.cc

BENCH_OBJFILES := $(patsubst %.cc, $(O)/%.o, $(BENCH_SRCFILES))

##################################################################
# Sub-directories
# Eliminate default suffix rules
.SUFFIXES:

# Delete target files if there is an error (or make is interrupted)
.DELETE_ON_ERROR:

# make it so that no intermediate .o files are ever deleted
.PRECIOUS: %.o

# The directory of the current make fragment.  Each file should
# redefine this at the very top with
#  d := $(dir $(lastword $(MAKEFILE_LIST)))
libd :=

# The object directory corresponding to the $(d)
libo = .obj/$(libd)

# SRCS is the list of all non-test-related source files.
SRCS :=
# TEST_SRCS is just like SRCS, but these source files will be compiled
# with testing related flags.
# TEST_SRCS :=
# # GTEST_SRCS is tests that use Google's testing framework
# GTEST_SRCS :=

# PROTOS is the list of protobuf *.proto files
PROTOS :=

# BINS is a list of target names for non-test binaries.  These targets
# should depend on the appropriate object files, but should not
# contain any commands.
# BINS :=
# TEST_BINS is like BINS, but for test binaries.  They will be linked
# using the appropriate flags.  This is also used as the list of tests
# to run for the `test' target.
# TEST_BINS :=

# add-CFLAGS is a utility macro that takes a space-separated list of
# sources and a set of CFLAGS.  It sets the CFLAGS for each provided
# source.  This should be used like
#
#  $(call add-CFLAGS,$(d)a.c $(d)b.c,$(PG_CFLAGS))
define add-CFLAGS
$(foreach src,$(1),$(eval CFLAGS-$(src) += $(2)))
endef

# Like add-CFLAGS, but for LDFLAGS.  This should be given a list of
# binaries.
define add-LDFLAGS
$(foreach bin,$(1),$(eval LDFLAGS-$(bin) += $(2)))
endef

include lib/Rules.mk
##################################################################

# -MD Enable dependency generation and compilation and output to the
# .obj directory.  -MP Add phony targets so make doesn't complain if
# a header file is removed.  -MT Explicitly set the target in the
# generated rule to the object file we're generating.
DEPFLAGS = -M -MF ${@:.o=.d} -MP -MT $@ -MG

# $(call add-CFLAGS,$(TEST_SRCS),$(CHECK_CFLAGS))
OBJS := $(SRCS:%.cc=.obj/%.o) $(TEST_SRCS:%.cc=.obj/%.o) $(GTEST_SRCS:%.cc=.obj/%.o)

define compile
	@mkdir -p $(dir $@)
	$(call trace,$(1),$<,\
	  $(CC) -iquote. $(CFLAGS) $(CFLAGS-$<) $(2) $(DEPFLAGS) -E $<)
	$(Q)$(CC) -iquote. $(CFLAGS) $(CFLAGS-$<) $(2) -E -o .obj/$*.t $<
	$(Q)$(EXPAND) $(EXPANDARGS) -o .obj/$*.i .obj/$*.t
	$(Q)$(CC) $(CFLAGS) $(CFLAGS-$<) $(2) -c -o $@ .obj/$*.i
endef

define compilecxx
	@mkdir -p $(dir $@)
	$(call trace,$(1),$<,\
	  $(CXX) -iquote. $(CFLAGS) $(CXXFLAGS) $(CFLAGS-$<) $(2) $(DEPFLAGS) -E $<)
	$(Q)$(CXX) -iquote. $(CFLAGS) $(CXXFLAGS) $(CFLAGS-$<) $(2) -c -o $@ $<
endef

# All object files come in two flavors: regular and
# position-independent.  PIC objects end in -pic.o instead of just .o.
# Link targets that build shared objects must depend on the -pic.o
# versions.
# Slightly different rules for protobuf object files
# because their source files have different locations.

$(OBJS): .obj/%.o: %.cc $(PROTOSRCS)
	$(call compilecxx,CC,)

$(OBJS:%.o=%-pic.o): .obj/%-pic.o: %.cc $(PROTOSRCS)
	$(call compilecxx,CCPIC,-fPIC)

$(PROTOOBJS): .obj/%.o: .obj/gen/%.pb.cc
	$(call compilecxx,CC,)

$(PROTOOBJS:%.o=%-pic.o): .obj/%-pic.o: .obj/gen/%.pb.cc $(PROTOSRCS)
	$(call compilecxx,CCPIC,-fPIC)

#
# Automatic dependencies
#

DEPS := $(OBJS:.o=.d) $(OBJS:.o=-pic.d)
-include $(DEPS)

all: $(O)/test

$(O)/benchmarks/%.o: benchmarks/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) -Imasstree $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/benchmarks/sto/%.o: benchmarks/sto/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) -Imasstree $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/benchmarks/ut/%.o: benchmarks/ut/%.cc $(O)/buildstamp $(O)/buildstamp.bench $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) -Imasstree $(BENCH_CXXFLAGS) -c $< -o $@

$(O)/%.o: %.cc $(O)/buildstamp $(OBJDEP)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(MASSTREE_OBJFILES) : $(O)/%.o: masstree/%.cc masstree/config.h
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

third-party/lz4/liblz4.so:
	make -C third-party/lz4 library

.PHONY: test
test: $(O)/test

$(O)/test: $(O)/test.o $(OBJFILES) $(MASSTREE_OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/test $^ $(LDFLAGS) $(LZ4LDFLAGS)

.PHONY: persist_test
persist_test: $(O)/persist_test

$(O)/persist_test: $(O)/persist_test.o third-party/lz4/liblz4.so
	$(CXX) -o $(O)/persist_test $(O)/persist_test.o $(LDFLAGS) $(LZ4LDFLAGS)

.PHONY: stats_client
stats_client: $(O)/stats_client

$(O)/stats_client: $(O)/stats_client.o
	$(CXX) -o $(O)/stats_client $(O)/stats_client.o $(LDFLAGS)

masstree/config.h: $(O)/buildstamp.masstree masstree/configure masstree/config.h.in
	rm -f $@
	cd masstree; ./configure $(MASSTREE_CONFIG)
	if test -f $@; then touch $@; fi

masstree/configure masstree/config.h.in: masstree/configure.ac
	cd masstree && autoreconf -i && touch configure config.h.in

.PHONY: dbtest
dbtest: $(O)/benchmarks/dbtest

$(O)/benchmarks/dbtest: $(O)/benchmarks/dbtest.o $(OBJFILES) $(MASSTREE_OBJFILES) $(OBJS) $(BENCH_OBJFILES) third-party/paxos/build/libtxlog.so third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/dbtest $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp -ljemalloc -Lthird-party/paxos/build

DEPFILES := $(wildcard $(O)/*.d $(O)/*/*.d $(O)/*/*/*.d masstree/_masstree_config.d)
ifneq ($(DEPFILES),)
-include $(DEPFILES)
endif

ifeq ($(wildcard masstree/GNUmakefile.in),)
INSTALL_MASSTREE := $(shell git submodule init; git submodule update)
endif

# UPDATE_MASSTREE := $(shell cd ./`git rev-parse --show-cdup` && cur=`git submodule status --cached masstree | head -c 41 | tail -c +2` && if test -z `cd masstree; git rev-list -n1 $$cur^..HEAD 2>/dev/null`; then (echo Updating masstree... 1>&2; cd masstree; git checkout -f master >/dev/null; git pull; cd ..; git submodule update masstree); fi)

ifneq ($(strip $(DEBUG_S).$(CHECK_INVARIANTS_S).$(EVENT_COUNTERS_S)),$(strip $(DEP_MAIN_CONFIG)))
DEP_MAIN_CONFIG := $(shell mkdir -p $(O); echo >$(O)/buildstamp; echo "DEP_MAIN_CONFIG:=$(DEBUG_S).$(CHECK_INVARIANTS_S).$(EVENT_COUNTERS_S)" >$(O)/_main_config.d)
endif

ifneq ($(strip $(MASSTREE_CONFIG)),$(strip $(DEP_MASSTREE_CONFIG)))
DEP_MASSTREE_CONFIG := $(shell mkdir -p $(O); echo >$(O)/buildstamp.masstree; echo DEP_MASSTREE_CONFIG:='$(MASSTREE_CONFIG)' >masstree/_masstree_config.d)
endif

$(O)/buildstamp $(O)/buildstamp.bench $(O)/buildstamp.masstree:
	@mkdir -p $(@D)
	@echo >$@

.PHONY: clean
clean:
	rm -rf out-*
	make -C third-party/lz4 clean
	$(call trace,RM,objects,rm -rf .obj)

##### UTs
.PHONY = simpleTransaction
simpleTransaction:  $(O)/benchmarks/simpleTransaction

$(O)/benchmarks/simpleTransaction: $(O)/benchmarks/ut/simpleTransaction.o $(OBJFILES) $(MASSTREE_OBJFILES) $(OBJS) $(BENCH_OBJFILES) third-party/paxos/build/libtxlog.so third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/simpleTransaction $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp -Lthird-party/paxos/build

.PHONY = simpleShards
simpleShards:  $(O)/benchmarks/simpleShards

$(O)/benchmarks/simpleShards: $(O)/benchmarks/ut/simpleShards.o $(OBJFILES) $(MASSTREE_OBJFILES) $(OBJS) $(BENCH_OBJFILES) third-party/paxos/build/libtxlog.so third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/simpleShards $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp -Lthird-party/paxos/build


.PHONY = basic
basic: $(O)/benchmarks/basic

$(O)/benchmarks/basic: $(O)/benchmarks/ut/basic.o $(OBJFILES) $(MASSTREE_OBJFILES) $(OBJS) $(BENCH_OBJFILES) third-party/paxos/build/libtxlog.so third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/basic -O0 $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp

.PHONY: paxos
paxos:
	cd ./third-party/paxos && make

.PHONY: paxos_async_commit_test
paxos_async_commit_test: $(O)/benchmarks/paxos_async_commit_test

$(O)/benchmarks/paxos_async_commit_test: $(O)/benchmarks/paxos_async_commit_test.o  third-party/paxos/build/libtxlog.so third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/paxos_async_commit_test $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp -Lthird-party/paxos/build


erpc_runner:  $(O)/benchmarks/erpc_runner_test

$(O)/benchmarks/erpc_runner_test: $(O)/benchmarks/erpc_runner/erpc_runner_test.o $(O)/benchmarks/erpc_runner/erpc_runner_queue.o $(O)/benchmarks/erpc_runner/common.o $(O)/benchmarks/erpc_runner/configuration.o $(O)/benchmarks/erpc_runner/erpc_runner.o $(OBJFILES) $(MASSTREE_OBJFILES) $(OBJS) $(BENCH_OBJFILES) third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/erpc_runner_test -O0 $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp

.PHONY = erpc_server
erpc_server:  $(O)/benchmarks/erpc_server

$(O)/benchmarks/erpc_server: $(O)/benchmarks/ut/erpc_server.o third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/erpc_server -O0 $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp

erpc_client:  $(O)/benchmarks/erpc_client

$(O)/benchmarks/erpc_client: $(O)/benchmarks/ut/erpc_client.o third-party/lz4/liblz4.so
	$(CXX) -o $(O)/benchmarks/erpc_client -O0 $^ $(BENCH_LDFLAGS) $(LZ4LDFLAGS) -lyaml-cpp
