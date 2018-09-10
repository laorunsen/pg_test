#include "postgres.h"
#include "executor/executor.h"
#include "postmaster/bgworker.h"
#include "fmgr.h"
#include "funcapi.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include <stdio.h>

PG_MODULE_MAGIC;

#define MAXCONNINFO		1024

extern void _PG_init(void);

void testbdr_main();
void testbdr_perdb();
void perdb_register_worker(const char * dbname);

void _PG_init(void) {
	BackgroundWorker bgw;

	Assert(IsPostmasterEnvironment && !IsUnderPostmaster);

	/*
	 * The supervisor worker accesses shared relations, but does not connect to
	 * any specific database. We still have to flag it as using a connection in
	 * the bgworker API.
	 */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	strncpy(bgw.bgw_library_name, "test.so", BGW_MAXLEN);
	strncpy(bgw.bgw_function_name, "testbdr_main", BGW_MAXLEN);
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "testbdr supervisor");
	bgw.bgw_main_arg = Int32GetDatum(0); /* unused */

	RegisterBackgroundWorker(&bgw);
	elog(LOG, "Init started!");

}

void testbdr_main() {
	testbdr_perdb();
	while (1) 
	{
		int rc;
		rc = WaitLatch(&MyProc->procLatch,
		   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 60);
		ResetLatch(&MyProc->procLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
		if (rc & WL_LATCH_SET)
		{
			testbdr_perdb();
		}
	}
}

void testbdr_write(const char * name) {
	FILE *file = NULL;
	char filename[1024];
	memset(filename, 0x00, sizeof(filename));
	sprintf(filename, "/var/tmp/pg/%s.txt", name);
	file = fopen(filename, "a+");
	fprintf(file, "%s started!\n", name);
	fclose(file);
}
void testbdr_perdb() {
	testbdr_write("perdb");
	perdb_register_worker("lrs");
}

void perdb_register_worker(const char * dbname) {
	BackgroundWorkerHandle *bgw_handle;
	BackgroundWorker	bgw;
	unsigned int		worker_slot_number;
	uint32			worker_arg;


	elog(LOG, "Registering per-db worker for db %s", dbname);


	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	strncpy(bgw.bgw_library_name, "test.so", BGW_MAXLEN);
	strncpy(bgw.bgw_function_name, "perdb_worker_main", BGW_MAXLEN);
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "testbdr db: %s", dbname);

	/*
	 * The main arg is composed of two uint16 parts - the worker
	 * generation number (see bdr_worker_shmem_startup) and the index into
	 * BdrWorkerCtl->slots in shared memory.
	Assert(worker_slot_number <= UINT16_MAX);
	worker_arg = (((uint32)BdrWorkerCtl->worker_generation) << 16) | (uint32)worker_slot_number;
	bgw.bgw_main_arg = Int32GetDatum(worker_arg);
	 */
	bgw.bgw_main_arg = Int32GetDatum(0); /* unused */

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("Registering BDR worker failed, check prior log messages for details")));
	}

	elog(DEBUG2, "Registered per-db worker for %s successfully", dbname);
}

void perdb_worker_main(void) {
	while(1)
	{
		testbdr_write("lrs");
		sleep(10);
	}
}
