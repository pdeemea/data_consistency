import os, sys, re, os.path, subprocess, csv, time

try:
    from optparse import Option, OptionParser
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
    from gppylib.gpcoverage import GpCoverage
    from gppylib import userinput
    from multiprocessing import Process,Queue
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-d', '--dbname',   type='string')
    parser.add_option('-p', '--password', type='string')
    parser.add_option('-n', '--nthreads', type='int')
    parser.add_option('-u', '--user', type='string')
    parser.add_option('-l', '--location', type='string')
    parser.add_option('-m', '--stat_mem', type='string')
    parser.add_option('-f', '--filename', type='string')	
    (options, args) = parser.parse_args()
    if options.help:
        print """Script performs serial restore of the backup files in case
of the cluster itopology change.
Usage:
./parallel_analyze.py -n thread_number -d dbname [-p gpadmin_password]
Parameters:
    thread_number    - number of parallel threads to run
    dbname           - name of the database
    gpadmin_password - password of the gpadmin user"""
        sys.exit(0)
    if not options.dbname:
        logger.error('Failed to start utility. Please, specify database name with "-d" key')
        sys.exit(1)
    if not options.nthreads:
        logger.error('Failed to start utility. Please, specify number of threads with "-n" key')
        sys.exit(1)
    if not options.stat_mem:
        logger.error('Failed to start utility. Please, specify statement_mem parameter  with "-m" key')
        sys.exit(1)
    if not options.filename:
        logger.error('Failed to start utility. Please, specify filename parameter (e.g. initial, compare)  with "-f" key')
        sys.exit(1)
    if not options.location:
        logger.error('Failed to start utility. Please, specify result folder parameter  with "-l" key')
        sys.exit(1)  
    return options

def execute(dburl, query):
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
        conn.close()
        return rows
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)

def get_results(table_lst,dburl,statement_mem):
    try:
        conn = dbconn.connect(dburl)
        dbconn.execSQL(conn,"SET statement_mem TO \'"+statement_mem+"\';")
	conn.commit()
        for table in table_lst:
             query = ("select count(*) from "+table+";")
             curs  = dbconn.execSQL(conn,query)
             row_cnt = curs.fetchall()[0][0]
             res_queue.put(table+"\t"+str(row_cnt))
        conn.commit()
        conn.close()
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)
 
def analyze_tables(table_list, dbname, threads,dburl,statement_mem):
    running = []
    isStopping = 0
    while len(table_list) > 0 or len(running) > 0:
	#print len(running)
        for pid in running:
            #print pid.is_alive()
            if pid.is_alive() is False:
                if pid.exitcode == 0:
                    running.remove(pid)
                else:
                    #pidret = pid.communicate()
                    pid.join()
                    #logger.error ('Restore failed for one of the segments')
                    #logger.error (redret[0])
                    #logger.error (redret[1])
                    isStopping = 1
        if isStopping == 0 and len(running) < threads and len(table_list) > 0:
            i=0
            query_tables_lst=[]
            while i<10 and len(table_list)>0:
                query_tables_lst.append(table_list.pop())
                i+=1
            pid=Process(target=get_results,name="Process Tables",args=(query_tables_lst,dburl,statement_mem,))
            running.append(pid)
            pid.start()
        if isStopping == 1 and len(running) == 0:
            break
    return

def prepare_tables(dburl):
    query = """
        select t.nspname || '.' || t.relname as tablename
            from (
                    select n.nspname,
                           c.relname
                        from pg_class as c,
                             pg_namespace as n
                        where c.relnamespace = n.oid
                            and c.relkind = 'r'
                            and c.relstorage in ('h', 'a', 'c')
                            and n.nspname not in ('pg_catalog','pg_toast','information_schema','gp_toolkit','madlib','pg_aoseg')
                 ) as t
                 left join pg_partitions as p
                    on p.partitiontablename = t.relname
                    and p.partitionschemaname = t.nspname 
                    where p.partitiontablename is null"""
    res = execute (dburl, query)
    #for x in res:
    #    print 'Table to analyze: "%s"' % x[0]
    return [ x[0] for x in res ]

def orchestrator(options):
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = options.dbname,
                         username = options.user,
                         password = options.password)
    table_list = prepare_tables(dburl)
    
    logger.info ('=== Found %d tables to analyze ===' % len(table_list))
    analyze_tables(table_list[::-1], options.dbname, options.nthreads,dburl,options.stat_mem)
    logger.info ('=== Analysis complete ===')

def store_results_to_fs(q,location,dbname,filenam):
	result_file=open(dbname+"_"+filenam+".csv",'w+')
	while not q.empty():
             result_file.write(q.get()+"\n")
        result_file.close()
#------------------------------- Mainline --------------------------------

#Initialization
coverage = GpCoverage()
coverage.start()
logger = get_default_logger()
res_queue=Queue()
err_queue=Queue()
#Parse input parameters and check for validity
options = parseargs()

#Print the partition list
orchestrator(options)


store_results_to_fs(res_queue,options.location,options.dbname,options.filename)
#while not res_queue.empty():
    #print res_queue.get()

#Stopping
coverage.stop()
coverage.generate_report()

# nohup python parallel_analyze.py -n 8 -d gpdb_upgrade_test >parallel_analyze.log 2>parallel_analyze.err &
# python parallel_analyze.py -n 8 -d gpdb_upgrade_test
