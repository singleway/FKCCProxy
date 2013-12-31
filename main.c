#include <stdio.h>
#include <stdlib.h>
#include <windows.h>
#include <winsock2.h>

#define DEFAULT_HTTP_PROXY_ADDRESS "202.194.64.201"
#define HTTP_PROXY_PORT 8000
#define LISTEN_PORT 8088
#define EXE_NAME "FKCCProxy"
#define BUFFER_SIZE 64*1024 //64KB
#define LOG_FILE_NAME "C:\\FKCCProxy.log"

typedef struct half_connection_s {
    SOCKET  sock;
} half_connection_t;

typedef struct connection_context_s {
    struct connection_context_s *next;
    half_connection_t             from_client, to_proxy;
    HANDLE                        main_thread;
} connection_context_t;

connection_context_t    g_main_ctx;
SERVICE_STATUS_HANDLE   g_service_status_handle;
CRITICAL_SECTION        g_cs_for_gc;
DWORD                   g_current_status = 0;
HANDLE                  g_stop_signal    = 0;

void report_status_to_os( DWORD status )
{
    g_current_status = status;
    SERVICE_STATUS service_status = {
        SERVICE_WIN32_OWN_PROCESS,
        g_current_status,
        status == SERVICE_START_PENDING ? 0 : SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN,
        NO_ERROR, 0, 0, 0,
    };
    SetServiceStatus( g_service_status_handle, &service_status );
}

DWORD WINAPI service_handler( DWORD ctrl_code, DWORD event_type, void *event_data, void *context )
{
    ( void ) event_type;
    ( void ) event_data;
    ( void ) context;

    switch ( ctrl_code ) {
        case SERVICE_CONTROL_STOP:
            SetEvent( g_stop_signal );
            break;
    }
    return NO_ERROR;
}

void gc() //garbage collection
{
    EnterCriticalSection( &g_cs_for_gc );

    unsigned gc_count = 0;
    for ( connection_context_t **curr = &g_main_ctx.next; *curr; ) {
        connection_context_t *entry = *curr;
        if ( entry->from_client.sock == entry->to_proxy.sock && entry->from_client.sock == 0 ) {
            *curr = entry->next;
            CloseHandle( entry->main_thread );
            free( entry );
            gc_count++;
        } else {
            curr = &entry->next;
        }
    }
    printf( "==== %d block(s) has been freed in this GC process =====\n", gc_count );
    fflush( stdout );

    LeaveCriticalSection( &g_cs_for_gc );
}

DWORD WINAPI waitfor_stop_signal( void *nothing )
{
    ( void )nothing;

    while ( 1 ) {
        int need_break = 0;
        switch ( WaitForSingleObject( g_stop_signal, 1000 * 60 ) ) {
            case WAIT_TIMEOUT://gc per minute
                gc();
                break;
            default:
                need_break = 1;
                break;
        }
        if ( need_break ) { break; }
    }

    printf( "Need To SHUTDOWN!\n" );
    fflush( stdout );
    shutdown( g_main_ctx.from_client.sock, SD_BOTH );
    closesocket( g_main_ctx.from_client.sock );

    return 1;
}

SOCKET make_connection_to_proxy()
{
    struct sockaddr_in proxy_addr;
    memset( &proxy_addr, 0, sizeof( proxy_addr ) );
    proxy_addr.sin_addr.s_addr = inet_addr( DEFAULT_HTTP_PROXY_ADDRESS );
    proxy_addr.sin_family      = AF_INET;
    proxy_addr.sin_port        = htons( HTTP_PROXY_PORT );

    SOCKET sock_to_proxy = socket( AF_INET, SOCK_STREAM, IPPROTO_TCP );
    if ( sock_to_proxy != INVALID_SOCKET ) {
        if ( connect( sock_to_proxy, ( struct sockaddr * )&proxy_addr, sizeof( struct sockaddr ) ) == SOCKET_ERROR ) {
            closesocket( sock_to_proxy );
            sock_to_proxy = INVALID_SOCKET;
        }
    }

    return sock_to_proxy;
}

DWORD WINAPI connection_to_proxy_service( void *cnnct_ctx_ptr )
{
    const connection_context_t *connect_ctx_ptr = ( connection_context_t * )cnnct_ctx_ptr;

    char *buffer_ptr = ( char * )malloc( BUFFER_SIZE );
    while ( 1 ) {
        int recv_size = recv( connect_ctx_ptr->to_proxy.sock, buffer_ptr, BUFFER_SIZE, 0 );
        if ( SOCKET_ERROR == recv_size || recv_size == 0 ) { break; }
        if ( SOCKET_ERROR == send( connect_ctx_ptr->from_client.sock, buffer_ptr, recv_size, 0 ) ) { break; }
    }
    free( buffer_ptr );
    shutdown( connect_ctx_ptr->from_client.sock, SD_BOTH );
    return 1;
}

DWORD WINAPI connection_from_client_service( void *cnnct_ctx_ptr )
{
    connection_context_t *connect_ctx_ptr = ( connection_context_t * )cnnct_ctx_ptr;

    connect_ctx_ptr->to_proxy.sock = make_connection_to_proxy();
    if ( connect_ctx_ptr->to_proxy.sock == INVALID_SOCKET ) {
        return 0;
    }

    HANDLE thread_for_proxy = CreateThread( NULL, 0, connection_to_proxy_service, cnnct_ctx_ptr, 0, NULL );
    char *buffer_ptr = ( char * )malloc( BUFFER_SIZE );
    while ( 1 ) {
        int recv_size = recv( connect_ctx_ptr->from_client.sock, buffer_ptr, BUFFER_SIZE, 0 );
        if ( SOCKET_ERROR == recv_size || recv_size == 0 ) { break; }
        if ( SOCKET_ERROR == send( connect_ctx_ptr->to_proxy.sock, buffer_ptr, recv_size, 0 ) ) { break; }
    }
    free( buffer_ptr );
    shutdown( connect_ctx_ptr->to_proxy.sock, SD_BOTH );
    WaitForSingleObject( thread_for_proxy, INFINITE );
    CloseHandle( thread_for_proxy );

    struct sockaddr_in client_addr;
    int addr_len = sizeof( struct sockaddr );
    getpeername( connect_ctx_ptr->from_client.sock, ( struct sockaddr * )&client_addr, &addr_len );
    printf( "Close a connection to %s:%d\n", inet_ntoa( client_addr.sin_addr ), ntohs( client_addr.sin_port ) );
    fflush( stdout );

    closesocket( connect_ctx_ptr->to_proxy.sock );
    closesocket( connect_ctx_ptr->from_client.sock );
    connect_ctx_ptr->to_proxy.sock = connect_ctx_ptr->from_client.sock = 0;

    return 1;
}

void close_all_sockets()
{
    for ( connection_context_t *curr = g_main_ctx.next; curr; curr = curr->next ) {
        if ( curr->from_client.sock ) {
            shutdown( curr->from_client.sock, SD_BOTH );
            WaitForSingleObject( curr->main_thread, INFINITE );
        }
        CloseHandle( curr->main_thread );
    }
}

int WINAPI ServiceMain( int argc, char **argv )
{
    WSADATA Ws;
    if ( WSAStartup( MAKEWORD( 2, 2 ), &Ws ) != 0 ) {
        return -1;
    }

    g_service_status_handle = RegisterServiceCtrlHandlerEx( EXE_NAME, &service_handler, NULL );
    g_stop_signal = CreateEvent( NULL, TRUE, FALSE, NULL );
    InitializeCriticalSectionAndSpinCount( &g_cs_for_gc, 64 );
    freopen( LOG_FILE_NAME, "w", stdout );

    g_main_ctx.next = NULL;
    g_main_ctx.from_client.sock = socket( AF_INET, SOCK_STREAM, IPPROTO_TCP );
    if ( g_main_ctx.from_client.sock == INVALID_SOCKET ) {
        return -1;
    }

    CloseHandle( CreateThread( NULL, 0, waitfor_stop_signal, NULL, 0, NULL ) );

    report_status_to_os( SERVICE_RUNNING );
    printf( "Hello, please wait for the network subsystem in five minutes!\n" );
    fflush( stdout );

#ifndef DEBUG
    Sleep( 1000 * 60 * 5 ); //wait for the network subsystem setting up
#endif // NOT DEBUG
    printf( "I'm working...\n" );
    fflush( stdout );

    struct sockaddr_in local_addr;
    memset( &local_addr, 0, sizeof( struct sockaddr_in ) );
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_family      = AF_INET;
    local_addr.sin_port        = htons( LISTEN_PORT );
    int nREUSEADDR = 1;
    setsockopt( g_main_ctx.from_client.sock, SOL_SOCKET, SO_REUSEADDR, ( const char * )&nREUSEADDR, sizeof( nREUSEADDR ) );
    if ( bind( g_main_ctx.from_client.sock, ( struct sockaddr * )&local_addr, sizeof( local_addr ) ) != 0 ) {
        printf( "Fail to bind port!\n" );
        fflush( stdout );
        goto here_exit;
    }
    if ( listen( g_main_ctx.from_client.sock, 15 ) != 0 ) {
        printf( "Fail to listen port!\n" );
        fflush( stdout );
        goto here_exit;
    }

    while ( 1 ) {
        connection_context_t *connect_ctx_ptr = ( connection_context_t * )malloc( sizeof( connection_context_t ) );
        memset( connect_ctx_ptr, 0, sizeof( connection_context_t ) );

        //accept connection
        struct sockaddr_in client_addr;
        int addr_len = sizeof( struct sockaddr_in );
        connect_ctx_ptr->from_client.sock = accept( g_main_ctx.from_client.sock, ( struct sockaddr * )&client_addr, &addr_len );
        if ( INVALID_SOCKET == connect_ctx_ptr->from_client.sock ) {
            free( connect_ctx_ptr );
#ifdef DEBUG
            char err_msg[256];
            FormatMessage( FORMAT_MESSAGE_FROM_SYSTEM, ( PCVOID )FORMAT_MESSAGE_FROM_HMODULE, WSAGetLastError(), 0, err_msg, 256, NULL );
            printf( "Accept Error: %s\n", err_msg );
            fflush( stdout );
#endif // DEBUG
            int need_break = 0;
            switch ( WSAGetLastError() ) {
                case WSAENOBUFS:
                case WSAEMFILE:
                    Sleep( 1000 * 60 * 5 );
                    break;
                case WSAECONNRESET:
                    printf( "Incomming connection was indicated, but was subsequently terminated by the remote peer prior to accepting the call\n" );
                    fflush( stdout );
                    break;
                default:
                    need_break = 1;
                    break;
            }
            if ( need_break ) { break; }
        }
        printf( "Make a connection for %s:%d\n", inet_ntoa( client_addr.sin_addr ), ntohs( client_addr.sin_port ) );

        EnterCriticalSection( &g_cs_for_gc );
        connect_ctx_ptr->next = g_main_ctx.next;
        g_main_ctx.next       = connect_ctx_ptr;
        LeaveCriticalSection( &g_cs_for_gc );

        connect_ctx_ptr->main_thread = CreateThread( NULL, 0, connection_from_client_service, connect_ctx_ptr, 0, NULL );
        fflush( stdout );
    }
    close_all_sockets();
    closesocket( g_main_ctx.from_client.sock );

here_exit:
    printf( "Bye!\n" );
    fclose( stdout );

    report_status_to_os( SERVICE_STOPPED );

    CloseHandle( g_stop_signal );
    DeleteCriticalSection( &g_cs_for_gc );
    WSACleanup();
    return 0;
}

int main( int argc, char **argv )
{
    //do some check
    //make sure we have already installed the service
    char sys_path[MAX_PATH] = {0};
    int len = GetSystemDirectory( sys_path, MAX_PATH );
    strcat( sys_path + len, "\\"EXE_NAME".exe" );
    int have_copied = GetFileAttributes( sys_path ) != INVALID_FILE_ATTRIBUTES;

    if ( argc == 2 ) {
        //open service manager
        SC_HANDLE sc_handle = OpenSCManager( NULL, NULL, SC_MANAGER_ALL_ACCESS );
        if ( strcmp( argv[1], "-i" ) == 0 ) {
            if ( !have_copied ) {
                if ( !CopyFile( argv[0], sys_path, TRUE ) ) {
                    printf( "Error Code %d, while coping!\n", GetLastError() );
                    CloseServiceHandle( sc_handle );
                    return -1;
                } else {
                    printf( "Install to %s\n", sys_path );
                }
            } else {
                printf( "The service has existed before!\n" );
            }

            SC_HANDLE service_handle = CreateService( sc_handle, EXE_NAME, EXE_NAME,
                                       SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS, SERVICE_AUTO_START, SERVICE_ERROR_NORMAL,
                                       sys_path,
                                       NULL, NULL, NULL, NULL, NULL );
            SERVICE_DESCRIPTION sd = {"FUCK the poor CCPRoxy!"};
            ChangeServiceConfig2( service_handle, SERVICE_CONFIG_DESCRIPTION, &sd );
            CloseServiceHandle( service_handle );
            printf( "Service is installed.\n" );
        } else if ( strcmp( argv[1], "-r" ) == 0 ) {
            if ( have_copied ) {
                DeleteFile( sys_path );
                printf( "Delete file from %s\n", sys_path );

                SC_HANDLE service_handle = OpenService( sc_handle, EXE_NAME, SERVICE_STOP | DELETE );
                SERVICE_STATUS status;
                ControlService( service_handle, SERVICE_CONTROL_STOP, &status );
                DeleteService( service_handle );
                CloseServiceHandle( service_handle );
                printf( "Service is removed.\n" );
            } else {
                printf( "The service has not existed!\n" );
            }
        } else {
            printf( "Usage:\n\t-i Install as service.\n\t-r Remove the service.\n" );
        }

        CloseServiceHandle( sc_handle );
        return 0;
    } else if ( !have_copied ) {
        printf( "Not Install!\n" );
        printf( "Usage:\n\t-i Install as service.\n\t-r Remove the service.\n" );
        return 0;
    }

    SERVICE_TABLE_ENTRY st[2];
    st[0].lpServiceName = EXE_NAME;
    st[0].lpServiceProc = ( LPSERVICE_MAIN_FUNCTION )ServiceMain;

    st[1].lpServiceName = NULL;
    st[1].lpServiceProc = NULL;

    StartServiceCtrlDispatcher( st );

    return 0;
}
