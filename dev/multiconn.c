#include <sys/time.h>
#include <sys/resource.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>

#define PORT 8082

// create more looback interfaces with:
// I=0; while [ $I -lt 11 ]; do ifconfig lo:$I 127.0.0.$((I+2)) netmask 255.0.0.0 up; I=$((I+1)); done

char request[] =
    "GET /sub/broadcast/%s HTTP/1.1\r\n"
    "Host: localhost:8082\r\n"
    "Accept: text/event-stream\r\n"
    "User-Agent: pubsub.rb NchanTools::Subscriber::EventSourceClient  #%d\r\n"
    "\r\n";

int main(int argc, char const* argv[])
{
    int id = 1;
    int ifc;
    struct rlimit rlimit;

    getrlimit(RLIMIT_NOFILE, &rlimit);
    rlimit.rlim_cur = 1000000;
    setrlimit(RLIMIT_NOFILE, &rlimit);

    for  (ifc = 1; ifc <= atoi(argv[1]); ifc++) {
        while (1) {
            char interface[12];
            int sock = 0, valread, client_fd;
            struct sockaddr_in serv_addr;
            char buffer[1024] = { 0 };

            if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                perror("Socket creation error");
                break;
            }

            if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0) 
                perror("setsockopt(SO_REUSEADDR) failed");

            if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int)) < 0) 
                perror("setsockopt(SO_REUSEADDR) failed");
        
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(PORT);
        
            sprintf(interface, "127.0.0.%d", ifc);
            if (inet_pton(AF_INET, interface, &serv_addr.sin_addr) <= 0) {
                perror("Invalid address - Address not supported");
                break;
            }
        
            if ((client_fd = connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr))) < 0) {
                perror("Connection Failed");
                break;
            }

            sprintf(buffer, request, argv[2], id++);
            send(sock, buffer, strlen(buffer), 0);

            printf("Launched conn %d\n", id);
        }
    }

    while (1)
        sleep(10);
    return 0;
}