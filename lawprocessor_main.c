#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>

#define MAXEVENTDESC 200
#define BUCKET1 1 //2 mins or less
#define BUCKET2 2 //2-5 mins
#define BUCKET3 3 //5-10 mins
#define BUCKET4 4 //Greater than 10 mins
#define BUCKETCOUNT 4 //Number of Buckets

char ** csvopen (char * filename);
char ** csvnext (void);
char ** csvheader (void);
int csvclose (void);

int call_type_final_desc;
int call_type_original_desc;
int received_datetime;
int dispatch_datetime;
int enroute_datetime;
int onscene_datetime;
int subfieldIndex;
char* subfieldVals[3];
int eventsData[MAXEVENTDESC][33];
char* eventTypes[MAXEVENTDESC];
int eventsCount;
pthread_mutex_t eventDescMutex;
pthread_mutex_t eventDataMutex[MAXEVENTDESC];

int parseTime (char* firstTime, char* secondTime)
{
    char* times[2] = {firstTime, secondTime};
    int parsedTime[2][3];
    int result;
    char* token;
    char* savePointer;
    for (int i = 0; i < 2; i++)
    {
        token = strtok_r (times[i], " ", &savePointer); //Seperate date and time
        token = strtok_r (NULL, " ", &savePointer); //Discard date
        token = strtok_r (token, ":", &savePointer); //Seperate into hour, min, and sec
        parsedTime[i][0] = atoi(token); //First element is hours
        token = strtok_r (NULL, ":", &savePointer);
        parsedTime[i][1] = atoi(token); //Second element is minutes
        token = strtok_r (NULL, ":", &savePointer);
        parsedTime[i][2] = atoi(token); //Third element is seconds
    }
    int hours = parsedTime[1][0] - parsedTime[0][0];
    int mins = parsedTime[1][1] - parsedTime[0][1];
    int secs = parsedTime[1][2] - parsedTime[0][2];
    //Account for the next hour
    if (mins < 0)
    {
        hours--;
        mins += 60;
    }
    //Account for the next minute
    if (secs < 0)
    {
        mins--;
        secs += 60;
    }
    //Account for AM/PM shift
    if (hours < 0)
    {
        hours += 12;
    }
    int totalMinutes = ((hours * 60 + mins) * 60 + secs) / 60;
    return totalMinutes;
}

void releaseLine(char** nextLine)
{
    for (int i = 0; nextLine[i] != NULL; i++)
    {
        free(nextLine[i]);
        nextLine[i] = NULL;
    }
    free(nextLine);
    nextLine = NULL;
}

void* processLine()
{
    char** nextLine;
    while ((nextLine = csvnext()) != NULL)
    {
        //Get event description from CSV
        char* eventDesc;
        if (nextLine[call_type_original_desc] == NULL && nextLine[call_type_final_desc] == NULL)
        {
            releaseLine(nextLine);
            continue;
        }
        if (nextLine[call_type_final_desc] != NULL)
        {
            eventDesc = nextLine[call_type_final_desc];
        }
        else
        {
            eventDesc = nextLine[call_type_original_desc];
        }

        //Check if event has been encountered before
        pthread_mutex_lock(&eventDescMutex);
        int eventIndex = -1;
        for (int i = 0; i < eventsCount; i++)
        {
            //Event encountered, increment existing buckets
            if (strcmp(eventTypes[i], eventDesc) == 0)
            {
                eventIndex = i;
            }
        }
        //Event unencountered; append events array & initlize corresponding mutex
        if (eventIndex == -1)
        {
            if (pthread_mutex_init(&eventDataMutex[eventsCount], NULL) != 0)
            {
                perror("Mutex Init Failed");
                exit(EXIT_FAILURE);
            }
            eventIndex = eventsCount;
            char* eventCopy = (char*) malloc(strlen(eventDesc)+1);
            memcpy(eventCopy, eventDesc, strlen(eventDesc)+1);
            eventTypes[eventIndex] = eventCopy;
            eventsCount++;
        }
        pthread_mutex_unlock(&eventDescMutex);

        //Process timestamp into minutes
        char* entryTimes[4] = 
        {
            nextLine[received_datetime],
            nextLine[dispatch_datetime],
            nextLine[enroute_datetime],
            nextLine[onscene_datetime]
        };
        //Skip the entry if it's missing timestamps
        int skipEntry = 0;
        for (int i = 0; i < 4; i++)
        {
            if (entryTimes[i][0] == '\0')
            {
                skipEntry = 1;
            }
        }
        if (skipEntry)
        {
            releaseLine(nextLine);
            continue;
        }

        //Subtract dispatch time from received time to get time in minutes
        int dispatchTime = parseTime(entryTimes[0], entryTimes[1]);
        //Subtract onscene time from enroute time to get time in minutes
        int onSceneTime = parseTime(entryTimes[2], entryTimes[3]);
        
        //Determine if record has a subfield we care about
        int dataSubfieldIndex = 0;
        for (int i = 0; i < 3; i++)
        {
            if (strcmp(nextLine[subfieldIndex], subfieldVals[i]) == 0)
            {
                dataSubfieldIndex = i+1;
            }
        }

        pthread_mutex_lock(&eventDataMutex[eventIndex]);
        eventsData[eventIndex][0]++; //Increment total event type
        //Increment dispatch buckets as appropriate
        if (dispatchTime <= 2)
        {
            eventsData[eventIndex][BUCKET1]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET1]++;
            }
        }
        else if (dispatchTime <= 5)
        {
            eventsData[eventIndex][BUCKET2]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET2]++;
            }
        }
        else if (dispatchTime <= 10)
        {
            eventsData[eventIndex][BUCKET3]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET3]++;
            }
        }
        else
        {
            eventsData[eventIndex][BUCKET4]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET4]++;
            }
        }
        //Increment on scene buckets as appropriate
        if (onSceneTime <= 2)
        {
            eventsData[eventIndex][4+BUCKET1]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET1+BUCKETCOUNT]++;
            }
        }
        else if (onSceneTime <= 5)
        {
            eventsData[eventIndex][4+BUCKET2]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET2+BUCKETCOUNT]++;
            }
        }
        else if (onSceneTime <= 10)
        {
            eventsData[eventIndex][4+BUCKET3]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET3+BUCKETCOUNT]++;
            }
        }
        else
        {
            eventsData[eventIndex][4+BUCKET4]++;
            if (dataSubfieldIndex != 0)
            {
                eventsData[eventIndex][(8*dataSubfieldIndex)+BUCKET4+BUCKETCOUNT]++;
            }
        }
        pthread_mutex_unlock(&eventDataMutex[eventIndex]);
        releaseLine(nextLine);
    }
    pthread_exit(NULL);
}

int main (int argc, char *argv[])
{
    //Exit if less than 6 arguments are provided
    if (argc < 7)
    {
        perror("Not Enough Arguments");
        exit(EXIT_FAILURE);
    }
    //Exit if subfield argument is invalid
    if (strcmp(argv[3], "analysis_neighborhood") != 0 && strcmp(argv[3], "police_district") != 0)
    {
        perror("Invalid Subfield");
        exit(EXIT_FAILURE);
    }
    //Open CSV and setup variables
    char* filename = argv[1];
    int threadCount = atoi(argv[2]);
    if (threadCount < 1)
    {
        perror("Less Than 1 Thread");
        exit(EXIT_FAILURE);
    }
    char **header = csvopen(filename);
    subfieldVals[0] = argv[4];
    subfieldVals[1] = argv[5];
    subfieldVals[2] = argv[6];

    //Index header from CSV
    for (int i = 0; header[i] != NULL; ++i)
    {
        if (strcmp(header[i], "call_type_final_desc") == 0)
        {
            call_type_final_desc = i;
        }
        else if (strcmp(header[i], "call_type_original_desc") == 0)
        {
            call_type_original_desc = i;
        }
        else if (strcmp(header[i], "received_datetime") == 0)
        {
            received_datetime = i;
        }
        else if (strcmp(header[i], "dispatch_datetime") == 0)
        {
            dispatch_datetime = i;
        }
        else if (strcmp(header[i], "enroute_datetime") == 0)
        {
            enroute_datetime = i;
        }
        else if (strcmp(header[i], "onscene_datetime") == 0)
        {
            onscene_datetime = i;
        }
        else if (strcmp(header[i], argv[3]) == 0)
        {
            subfieldIndex = i;
        }
    }

    //Time stamp start
    struct timespec startTime;
    struct timespec endTime;

    clock_gettime(CLOCK_REALTIME, &startTime);
    
    //Initialize mutex
    eventsCount = 0;
    if (pthread_mutex_init(&eventDescMutex, NULL) != 0)
    {
        perror("Mutex Init Failed");
        exit(EXIT_FAILURE);
    }
    //Create threads
    pthread_t threads[threadCount];
    for (int i = 0; i < threadCount; i++)
    {
        if (pthread_create(&threads[i], NULL, processLine, NULL))
        {
            perror("Error Creating Thread");
            exit(EXIT_FAILURE);
        }
    }
    //Wait for all threads to finish
    for (int i = 0; i < threadCount; i++)
    {
        pthread_join(threads[i], NULL);
    }

    //Sort Data Alphabetically (Bubble Sort)
    for (int i = 0; i < eventsCount-1; i++)
    {
        for (int j = 0; j < eventsCount-1-i; j++)
        {
            if (strcmp(eventTypes[j], eventTypes[j+1]) > 0)
            {
                //Swap Event Types
                char* typeCopy = eventTypes[j];
                eventTypes[j] = eventTypes[j+1];
                eventTypes[j+1] = typeCopy;
                //Swap Corresponding Data
                int dataCopy[33];
                for (int k = 0; k < 33; k++)
                {
                    dataCopy[k] = eventsData[j][k];
                }
                for (int k = 0; k < 33; k++)
                {
                    eventsData[j][k] = eventsData[j+1][k];
                    eventsData[j+1][k] = dataCopy[k];
                }
            }
        }
    }

    //Print Header
    printf("\n");
    printf("%31s", "|"); //Offset from Call Types their Count
    printf("%16s%12s%16s%12s", "Total", "|", "Total", "|");
    //Subfield Headers
    for (int i = 0; i < 3; i++)
    {
        printf("%16s%12s", subfieldVals[i], "|");
        printf("%16s%12s", subfieldVals[i], "|");
    }
    printf("\n%31s", "|"); //Offset from Call Types their Count
    for (int i = 0; i < 4; i++)
    {
        printf("%20s%8s", "Dispatch Time", "|");
        printf("%20s%8s", "On Scene Time", "|");
    }
    printf("\n");
    printf("Call Type%14s Total |", "|"); //Header for Types their Count
    for (int i = 0; i < 8; i++)
    {
        printf("  <2  | 3-5  | 6-10 | >10  |");
    }
    printf("\n----------------------+-------+"); //Header Seperator for Types their Count
    for (int i = 0; i < 32; i++)
    {
        printf("------+"); //Header Seperator for Buckets
    }
    printf("\n");
    //Print Data
    for (int i = 0; i < eventsCount; i++)
    {
        printf("%-22s|", eventTypes[i]);
        printf("%7d|", eventsData[i][0]);
        for (int j = 1; j < 33; j++)
        {
            printf("%6d|", eventsData[i][j]);
        }
        printf("\n");
    }
    printf("\n");


    //Clock output
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec)
        {
        --sec;
        n_sec = n_sec + 1000000000L;
        }

    printf("Total Time was %ld.%09ld seconds\n", sec, n_sec);


    //Destroy mutexes, free memory, and close CSV
    pthread_mutex_destroy(&eventDescMutex);
    for (int i = 0; i < eventsCount; i++)
    {
        free(eventTypes[i]);
        eventTypes[i] = NULL;
        pthread_mutex_destroy(&eventDataMutex[i]);
    }
    int linesRead = csvclose();
    if (linesRead == -1)
    {
        perror("File Close Failed");
        exit(EXIT_FAILURE);
    }
}