/*
 * ***** taome dba team  *******
 *  2012-
 *  billy@taomee.com
 *
 *  CHANGELOG:
 *
 */
#include <glib.h>
#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <signal.h>
#include <unistd.h>

#include <sys/wait.h>
#include <sys/types.h>  // for umask
#include <sys/stat.h>
#include <sys/mman.h>  // for mmap

#include <list>
#include <map>
#include <iostream>
#include <string>
#include <algorithm>
using namespace std;

/*
 * CAUTION: the max db num within one instance is 512
 *
 */

#define MAX_INSTANCE_NUM 64
#define DEFAULT_DB_PORT 3306
#define DBNAME_LEN 64

static int serv_cnt = 0;
typedef struct {
        gchar *dbuser;
        gchar *dbpassword;
        gchar *database_pre;
        gchar *database_start;
        gchar *database_end;
        gchar *table_pre;
        gchar *table_start;
        gchar *table_end;
        gchar *query;
}QUERY;

QUERY *query;

list<string> all_dbs;
list<string> all_tbs;

static uint32_t child_num;

struct instance_info_t{
        char host[16];
        unsigned short port;
}instance_info[MAX_INSTANCE_NUM];

enum task_type_t {
        ADMIN_ERROR,
        ADMIN_CREATE,   ADMIN_DROP, ADMIN_ALTER,
        ADMIN_SHUTDOWN, ADMIN_PING,
        ADMIN_START_SLAVE, ADMIN_STOP_SLAVE,
        ADMIN_UPDATE
};

enum child_status_t { CHD_UNKNOW, CHD_START, CHD_CONN_DB, CHD_WORK, CHD_FINISH, CHD_ERROR };

struct child_data_t{
        pid_t tid;
        enum child_status_t status;
        struct instance_info_t mysql_instance;
};

int quit = 0;
static struct child_data_t *child_ptr;

static void child_main(struct child_data_t *ptr);
static pid_t child_make(struct child_data_t *ptr);
static int build_all_dbs();
static int build_all_tbs();
//
/*
int process_query();
*/

void load_server_list(char **argv)
{
        FILE *srv_lst_fd;
        char *servers_file= NULL;
        char server[32];

        servers_file = argv[1];
        srv_lst_fd = fopen(servers_file, "r");
        if ( NULL == srv_lst_fd ) {
                fprintf(stderr, "could not open the server list file\n");
                exit(EXIT_FAILURE);
        }

        while (fgets(server, 31, srv_lst_fd)) {
                char *nl = strstr(server, "\n");
                if (nl) nl[0] = 0;
                strncpy( instance_info[serv_cnt].host, server, strlen(server));
                instance_info[serv_cnt].port = DEFAULT_DB_PORT;
                serv_cnt++;
        }
        child_num = serv_cnt;
        assert(child_num!=0);
}

void get_query_info(char **argv)
{
        // TODO: lstat()
        char *query_file= NULL;
        query_file = argv[2];

        GKeyFile *keyfile;
        GKeyFileFlags flags;
        GError *error = NULL;

        keyfile = g_key_file_new ();
        flags = (GKeyFileFlags )( G_KEY_FILE_KEEP_COMMENTS | G_KEY_FILE_KEEP_TRANSLATIONS );

        if (!g_key_file_load_from_file (keyfile, query_file, flags, &error)) {
                g_error (error->message);
                exit(EXIT_FAILURE);
        }

        query= g_new(QUERY, 1);
        query->dbuser= g_key_file_get_string(keyfile,
                        "accout_info", "dbuser", NULL);
        query->dbpassword= g_key_file_get_string(keyfile,
                        "accout_info", "dbpassword", NULL);
        query->database_pre = g_key_file_get_string(keyfile,
                        "query_body", "database_pre", NULL);
        query->database_start = g_key_file_get_string(keyfile,
                        "query_body", "database_start", NULL);
        query->database_end = g_key_file_get_string(keyfile,
                        "query_body", "database_end", NULL);
        query->table_pre= g_key_file_get_string(keyfile,
                        "query_body", "table_pre", NULL);
        query->table_start= g_key_file_get_string(keyfile,
                        "query_body", "table_start", NULL);
        query->table_end= g_key_file_get_string(keyfile,
                        "query_body", "table_end", NULL);
        query->query = g_key_file_get_string(keyfile,
                        "query", "query", NULL);
}

MYSQL *create_mysql_conn(char *hostname, char *username, char *password, char *db, unsigned short port, char *socket_path)
{
        MYSQL *conn;
        conn = mysql_init(NULL);
	      int conn_timeout = 10;
        mysql_options(conn,MYSQL_READ_DEFAULT_GROUP,"tmdb");
        mysql_options(conn,MYSQL_OPT_CONNECT_TIMEOUT,(const char *)&conn_timeout);

        if (!mysql_real_connect(conn,  hostname, username, password, db, port, socket_path, 0)) {
                g_critical("Error connecting to database: %s", mysql_error(conn));
                return NULL;
        }
        if ( mysql_query(conn, "SET SESSION wait_timeout = 2147483")){
                fprintf(stderr , "Failed to increase wait_timeout: %s", mysql_error(conn));
        }
        if ( mysql_query(conn, "SET SESSION net_write_timeout = 2147483")){
                fprintf(stderr, "Failed to increase net_write_timeout: %s", mysql_error(conn));
        }
        return conn;
}


static int get_all_dbs_name(MYSQL *conn, list<string> &dbs)
{
        MYSQL_RES *databases;
        MYSQL_ROW row;
        if(mysql_query(conn,"SHOW DATABASES") || !(databases = mysql_store_result(conn))) {
                fprintf(stderr, "Unable to list databases: [%s] [%d]",mysql_error(conn), mysql_errno(conn));
                //exit(EXIT_FAILURE);
                return -1;
        }
        int i=0;
        while ((row=mysql_fetch_row(databases))) {
                if (
                   !strcmp(row[0], "information_schema")
                || !strcmp(row[0], "performance_schema")
                || !strcmp(row[0], "billy"))
                        continue;
                dbs.push_back( row[0] );
                i++;
        }
        mysql_free_result(databases);
        return i;
}

static void child_main(struct child_data_t *ptr)
{
        list<string> priv_dbs;
        list<string>::iterator i, j;
        MYSQL *conn;

        //static int retry = 3;
        while( !quit ) {
                conn = create_mysql_conn(ptr->mysql_instance.host, query->dbuser,query->dbpassword,
                                NULL, DEFAULT_DB_PORT, NULL);
                if ( NULL == conn ) {
                        fprintf(stderr ,"can not connect to %s:%d\n",
                                        ptr->mysql_instance.host,  ptr->mysql_instance.port);
                        sleep(5);
                        continue;
#if 0
                        if ( retry == 0 ){
                                g_print("no try again, will quit\n");
                                exit(1);
                        }
#endif
                }

                int ret = get_all_dbs_name(conn, priv_dbs);
                printf("ret=%d\n", ret);

                list<string>::iterator tb;
                int have=0;
                for( i=priv_dbs.begin(); i!= priv_dbs.end(); i++ ) {
                        //g_print("start to check: [%s]\n", *i);
                        j = find(all_dbs.begin(), all_dbs.end(), *i);
                        if ( j != all_dbs.end() ) {
                                g_print("start to process database: %s\n", (*i).c_str());
                                for( tb=all_tbs.begin(); tb!= all_tbs.end(); tb++ ) {
                                        g_print("start to process table: %s\n", (*tb).c_str());
                                }
                                have++;
                        }
                }

                if ( have > 0 ) {
                        sleep(15);
                        // monitor child num here
                        printf("######### pid = %d #########\n", getpid());
                        g_print("Child DONE will exit\n");
                        ptr->status = CHD_FINISH;
                        exit(0);
                } else {
                        g_print("NOTHING TO DO, will exit\n");
                        ptr->status = CHD_FINISH;
                        exit(0);
                }
        }
}


static pid_t child_make(struct child_data_t *ptr)
{
        pid_t pid;
        if ((pid = fork()) > 0 )
                return pid;
        child_main(ptr);
        return -1;
}


static int build_all_tbs()
{
        int idx = 0;
        int tbsuf_start_len , tbsuf_end_len ;
        tbsuf_start_len = strlen(query->table_start);
        tbsuf_end_len = strlen(query->table_end);

        printf("%s %d %d\n",query->table_pre, tbsuf_start_len, tbsuf_end_len);
        for (idx = atoi(query->table_start);
                        idx<=atoi(query->table_end);
                        idx++) {
                char *tb_tmp_name;
                tb_tmp_name =(char *)malloc(64);  //todo: release ?
                sprintf(tb_tmp_name, "%s%0*d", query->table_pre, tbsuf_start_len, idx);

                all_tbs.push_back( tb_tmp_name );
                //g_print("last element: %s\n", (char *)(g_list_last(all_dbs))->data);
        }
        return idx;
}

static int build_all_dbs()
{
        int idx = 0;
        int dbsuf_start_len , dbsuf_end_len ;
        dbsuf_start_len = strlen(query->database_start);
        dbsuf_end_len = strlen(query->database_end);

        printf("%s %d %d\n",query->database_pre, dbsuf_start_len, dbsuf_end_len);
        for (idx = atoi(query->database_start);
                        idx<=atoi(query->database_end);
                        idx++) {
                char *db_tmp_name;
                db_tmp_name =(char *)malloc(64);  //todo: release ?
                sprintf(db_tmp_name, "%s%0*d", query->database_pre, dbsuf_start_len, idx);

                all_dbs.push_back( db_tmp_name );
                //g_print("last element: %s\n", (char *)(g_list_last(all_dbs))->data);
        }
        return idx;
}

size_t strlcpy (char *dst, const char *src, size_t size)
{
        size_t len = strlen (src);
        size_t ret = len;

        if (len >= size)
                len = size - 1;

        memcpy (dst, src, len);
        dst[len] = '\0';

        return ret;
}


void *malloc_shm(size_t size)
{
        int fd;
        void *ptr;
        char buffer[32];

        static const char *shared_file = "/tmp/tmdba.shared.XXXXXX";

        assert (size > 0);

        strlcpy (buffer, shared_file, sizeof (buffer));

        /* Only allow u+rw bits. This may be required for some versions
         * of glibc so that mkstemp() doesn't make us vulnerable.
         */
        umask (0177);

        if ((fd = mkstemp (buffer)) == -1)
                return MAP_FAILED;
        unlink (buffer);

        if (ftruncate (fd, size) == -1)
                return MAP_FAILED;
        ptr = mmap (NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

        return ptr;

}

void *calloc_shm(size_t nmemb, size_t size)
{
        void *ptr;
        long length;

        assert (nmemb > 0);
        assert (size > 0);

        length = nmemb * size;

        ptr = malloc_shm(length);
        if (ptr == MAP_FAILED)
                return ptr;

        memset (ptr, 0, length);

        return ptr;
}

static int child_pool_create(void );

static int child_pool_create(void )
{

        child_ptr = (struct child_data_t *)
                calloc_shm(child_num,sizeof(struct child_data_t) );
        if ( !child_ptr ) {
                printf("can not calloc memory for childern\n");
                return -1;
        }

        for (unsigned int i=0; i<child_num; i++){
                child_ptr[i].status =  CHD_UNKNOW;
                memcpy(child_ptr[i].mysql_instance.host , instance_info[i].host, strlen(instance_info[i].host));
                child_ptr[i].mysql_instance.port = instance_info[i].port;
        }

        for (unsigned int i=0; i<child_num; i++){
                fprintf(stderr, "Try to create child %d  of %d\n", i+1, child_num);
                child_ptr[i].status =  CHD_START;
                child_ptr[i].tid = child_make( &child_ptr[i] );

                if ( child_ptr[i].tid  < 0 ) {
                        fprintf(stderr, "can not create child %d host [%s] will quit\n",
                                        i, child_ptr[i].mysql_instance.host);
                        return -1;
                }else {
                        fprintf(stderr, "create child %d host [%s] success\n",
                                        i, child_ptr[i].mysql_instance.host);
                }
        }

       fprintf(stderr, "Finished creating all children.\n");

       return 0;
}

typedef void signal_func (int);

/*
 * Pass a singal integer and a function to handle the signal.
 */
extern signal_func *set_signal_handler (int signo, signal_func * func);

/*
 * Pass a signal number and a signal handling function into this function
 * to handle signals sent to the process.
 */
signal_func *set_signal_handler (int signo, signal_func * func)
{
        struct sigaction act, oact;

        act.sa_handler = func;
        sigemptyset (&act.sa_mask);
        act.sa_flags = 0;
        if (signo == SIGALRM) {
#ifdef SA_INTERRUPT
                act.sa_flags |= SA_INTERRUPT;   /* SunOS 4.x */
#endif
        } else {
#ifdef SA_RESTART
                act.sa_flags |= SA_RESTART;     /* SVR4, 4.4BSD */
#endif
        }

        if (sigaction (signo, &act, &oact) < 0)
                return SIG_ERR;

        return oact.sa_handler;
}

static void
takesig (int sig)
{
        pid_t pid;
        int status;

        switch (sig) {
        case SIGTERM:
                quit = 1;
                break;

        case SIGCHLD:
                while ((pid = waitpid (-1, &status, WNOHANG)) > 0) ;
                child_num--;
                break;
        }
        return;
}

#if 0

void child_kill_children (int sig)
{
        unsigned int i;
        for (i = 0; i != child_num ; i++) {
                if (child_ptr[i].status != T_EMPTY)
                        kill (child_ptr[i].tid, sig);
        }
}
#endif

void child_main_loop(void);

//TODO 添加 child num monitor,  if zero  main process will quit
void child_main_loop(void)
{
	if (child_pool_create () < 0) {
		fprintf (stderr, "Could not create the pool of children.\n");
		exit(EXIT_FAILURE);
	}

	while(1){
                printf("[MAIN] -----------------------\n");
#if 0
                for (unsigned int i = 0; i != child_num; i++) {
                        if ( child_ptr[i].status == CHD_FINISH ) {
                                fprintf(stderr , "will kill pid = %d\n", child_ptr[i].tid);
                                kill(child_ptr[i].tid, SIGHUP);
                        }
                }
#endif
                for (unsigned int i = 0; i != child_num; i++) {
                        fprintf(stderr, "child pid=%d status=%d host=%s\n",
                                child_ptr[i].tid, child_ptr[i].status, child_ptr[i].mysql_instance.host
                        );
                }

                printf("quit=%d   child_num=%d\n", quit, child_num);
		if (quit || !child_num) {
                        printf("catch quit signal or all child run done\n");
                        return;
                }
		sleep(5);
#if 0
                int process_id;
                for (unsigned int i = 0; i != child_num; ) {
                        process_id = waitpid((child_ptr[i].tid) - 1, NULL, WNOHANG);
                        if ( process_id < 0 )
                                printf("waitpid error\n");
                        else if (process_id == 0) {
                                printf("no zombine process\n");
                                break;
                        } else {
                                printf("OK revoke a process\n");
                                i++;
                        }
                }
#endif
	}
	// exit(0);
}

int main (int argc, char *argv[])
{
        if ( argc != 3 ) {
                g_print("%s server_list query_config\n", argv[0]);
                exit(EXIT_FAILURE);
        }

        fprintf(stderr ,"Load server list\n");
        load_server_list(argv);
        fprintf(stderr, "Total servers: %d\n", child_num);
        fprintf(stderr, "==========================\n");

        for (unsigned int i=0; i<child_num; i++)
                g_print("host: %s\n", instance_info[i].host);
        fprintf(stderr, "==========================\n");

        fprintf(stderr, "Get query info\n");
        get_query_info(argv);
        g_print("dbusr:%s dbpass:%s\n", query->dbuser, query->dbpassword);
        g_print("dbpre:%s dbs:%s dbe:%s\n", query->database_pre, query->database_start, query->database_end);
        g_print("tbpre:%s tbs:%s tbe:%s\n", query->table_pre, query->table_start, query->table_end);
        g_print("query:%s\n",query->query);
        fprintf(stderr, "==========================\n");

       int ret;
       ret = build_all_dbs();
       g_print("%d\n", ret);

       ret = build_all_tbs();
       g_print("table_num = %d\n", ret);

#ifdef DEBUG
       list<string>::iterator db_it;
       for(db_it=all_dbs.begin(); db_it!= all_dbs.end(); db_it++)
               g_print("GLOBAL: [%s]\n", (*db_it).c_str());
#endif


       fprintf(stderr , "Setting signals.\n");
       if (set_signal_handler (SIGCHLD, takesig) == SIG_ERR) {
	       fprintf (stderr, "%s: Could not set the \"SIGCHLD\" signal.\n",
			       argv[0]);
	       exit (-1);
       }

       if (set_signal_handler (SIGTERM, takesig) == SIG_ERR) {
	       fprintf (stderr, "%s: Could not set the \"SIGTERM\" signal.\n",
                               argv[0]);
               exit (-1);
       }

       child_main_loop();

       return 0;
}


